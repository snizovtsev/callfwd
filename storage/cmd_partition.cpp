#include "cmd_partition_internal.hpp"
#include "cmd_partition.hpp"
#include "bit_magic.hpp"
#include "lrn_schema.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/filesystem/api.h>
#include <fmt/format.h>
#include <glog/logging.h>
#include <folly/Conv.h>
#include <boost/program_options/options_description.hpp>
#include <memory>
#include <mutex>

namespace po = boost::program_options;
using Status = arrow::Status;
template <class T> using Result = arrow::Result<T>;
using arrow::internal::checked_cast;
using arrow::internal::ThreadPool;

Status PartitionTask::Init(const CmdPartitionPriv* cmd) {
  const auto output_schema = cmd->output[0].schema;
  const auto memory_pool = cmd->exec_context->memory_pool();
  const size_t nr_parts = slice.size();
  int64_t capacity = slice[0]->num_rows();

  capacity += capacity / 10;
  options = cmd->options;
  output.resize(nr_parts);
  CHECK(nr_parts);

  for (auto& builder_ptr : output) {
    ARROW_ASSIGN_OR_RAISE(builder_ptr, arrow::RecordBatchBuilder::Make(
        output_schema, memory_pool, capacity));
  }

  return Status::OK();
}

struct ArrayPartitioner {
  template <class T>
  static constexpr bool is_simple_type =
    arrow::has_c_type<typename T::TypeClass>::value ||
    arrow::is_decimal_type<typename T::TypeClass>::value ||
    arrow::is_fixed_size_binary_type<typename T::TypeClass>::value;

  template <typename T>
  std::enable_if_t<is_simple_type<T>, Status>
  Visit(const T& array) {
    using TypeClass = typename T::TypeClass;
    using BuilderType = typename arrow::TypeTraits<TypeClass>::BuilderType;

    const T& source = checked_cast<const T&>(array);
    auto target = reinterpret_cast<BuilderType**>(builders.data());
    const int64_t length = source.length();

    for (int64_t i = 0; i < length; ++i) {
      const uint16_t p = partition->GetView(i);
      DCHECK_LT(p, builders.size());
      ARROW_RETURN_NOT_OK(target[p]->Append(source.GetView(i)));
    }

    return Status::OK();
  }

  template <typename T>
  std::enable_if_t<!is_simple_type<T>, Status>
  Visit(const T& array) {
    return Status::NotImplemented("not a simple type");
  }

  std::vector<arrow::ArrayBuilder*> builders;
  std::shared_ptr<arrow::UInt16Array> partition;
};

Status PartitionTask::RunColumn(int c) {
  ArrayPartitioner visitor;

  visitor.builders.reserve(output.size());
  for (auto& builder : output) {
    visitor.builders.push_back(builder->GetField(c - 1));
  }

  for (auto& batch : slice) {
    std::shared_ptr<arrow::Array> chunk = batch->column(c);
    visitor.partition = std::dynamic_pointer_cast<arrow::UInt16Array>(
        batch->column(0));
    ARROW_RETURN_NOT_OK(VisitArrayInline(*chunk, &visitor));
  }

  return Status::OK();
}

Status PartitionTask::HashKeys() {
  const uint64_t hash_seed = options->hash_seed;
  const uint64_t partition_mask = output.size() - 1;
  std::vector<arrow::UInt64Builder*> hash1;
  std::vector<arrow::UInt64Builder*> hash2;
  arrow::UInt16Builder partition;

  hash1.reserve(output.size());
  hash2.reserve(output.size());
  for (auto& builder : output) {
    hash1.push_back(checked_cast<arrow::UInt64Builder*>(
        builder->GetField(builder->num_fields() - 2)));
    hash2.push_back(checked_cast<arrow::UInt64Builder*>(
        builder->GetField(builder->num_fields() - 1)));
  }

  for (auto& batch : slice) {
    const int64_t num_rows = batch->num_rows();
    auto key_column = std::dynamic_pointer_cast<arrow::UInt64Array>(
        batch->GetColumnByName(options->key_column));
    ARROW_RETURN_NOT_OK(partition.Reserve(num_rows));

    for (int64_t index = 0; index < num_rows; ++index) {
      const auto key_data = key_column->GetView(index);
      const auto h128 = murmur3_128(key_data >> LRN_BITS_PN_SHIFT, hash_seed);
      const uint16_t p = (h128.first ^ h128.second) & partition_mask;
      partition.UnsafeAppend(p);
      ARROW_RETURN_NOT_OK(hash1[p]->Append(h128.first));
      ARROW_RETURN_NOT_OK(hash2[p]->Append(h128.second));
    }

    ARROW_ASSIGN_OR_RAISE(auto new_column, partition.Finish());
    ARROW_ASSIGN_OR_RAISE(batch, batch->AddColumn(
        0, arrow::field("partition", new_column->type()), new_column));
  }

  return Status::OK();
}

Result<arrow::RecordBatchVector> PartitionTask::Run() {
  const int num_columns = slice[0]->num_columns();

  ARROW_RETURN_NOT_OK(HashKeys()); // inserts 'partition' column
  for (int c = 1; c <= num_columns; ++c) {
    ARROW_RETURN_NOT_OK(RunColumn(c));
  }

  arrow::RecordBatchVector result;
  result.reserve(slice.size());
  for (auto& builder : output) {
    ARROW_ASSIGN_OR_RAISE(auto partition, builder->Flush(false));
    result.push_back(std::move(partition));
  }

  return result;
}

std::unique_ptr<CmdPartition>
CmdPartition::Make(arrow::compute::ExecContext* exec_context,
                   const arrow::io::IOContext* io_context)
{
  auto cmd = std::make_unique<CmdPartition>();
  cmd->exec_context = exec_context ?: arrow::compute::threaded_exec_context();
  cmd->io_context = io_context ?: &arrow::io::default_io_context();
  return cmd;
}

Status CmdPartition::Init(const Options& options_) {
  /* Maximum number of tasks queued */
  compute_semaphore = exec_context->executor()->GetCapacity() * 2;
  write_semaphore = io_context->executor()->GetCapacity() * 2;
  options = &options_;

  ARROW_ASSIGN_OR_RAISE(source_file, arrow::io::MemoryMappedFile
                        ::Open(options->source_path, arrow::io::FileMode::READ));
  ARROW_ASSIGN_OR_RAISE(batch_reader, arrow::ipc::RecordBatchFileReader
                        ::Open(source_file, arrow::ipc::IpcReadOptions{}));

  auto output_fields = batch_reader->schema()->fields();
  output_fields.push_back(arrow::field("hash1", arrow::uint64()));
  output_fields.push_back(arrow::field("hash2", arrow::uint64()));
  auto output_schema = arrow::schema(std::move(output_fields),
                                     batch_reader->schema()->metadata());

  arrow::ipc::IpcWriteOptions write_opts;
  write_opts.memory_pool = exec_context->memory_pool();
  output = std::vector<DataPartition>(options->num_partitions);

  for (uint32_t p = 0; p < options->num_partitions; ++p) {
    DataPartition &part = output[p];
    part.schema = output_schema;
    part.metadata = std::make_shared<arrow::KeyValueMetadata>();
    part.file_path = fmt::format(options->output_template, p);
    ARROW_ASSIGN_OR_RAISE(part.ostream, arrow::io::FileOutputStream::Open(
        part.file_path));
    ARROW_ASSIGN_OR_RAISE(part.writer, arrow::ipc::MakeFileWriter(
        part.ostream, part.schema, write_opts, part.metadata));
  }

  return Status::OK();
}

Result<arrow::RecordBatchVector> CmdPartitionPriv::ReadSlice(int64_t batch_index) {
  const int64_t num_batches = batch_reader->num_record_batches();
  arrow::RecordBatchVector slice(options->num_partitions);

  for (auto &batch : slice) {
    if (LIKELY(batch_index < num_batches)) {
      ARROW_ASSIGN_OR_RAISE(
          batch, batch_reader->ReadRecordBatch(batch_index++));
    } else {
      ARROW_ASSIGN_OR_RAISE(batch, arrow::RecordBatch::MakeEmpty(
          batch_reader->schema(), exec_context->memory_pool()));
    }
  }

  return slice;
}

Status CmdPartition::Run(int64_t limit) {
  const int64_t num_batches = batch_reader->num_record_batches();
  const int64_t run_batches = std::min(num_batches, limit);
  const uint32_t num_partitions = options->num_partitions;
  int64_t batch;

  for (batch = 0; batch < run_batches; batch += num_partitions) {
    PartitionTask task;
    ARROW_RETURN_NOT_OK(ReadSlice(batch).Value(&task.slice));
    ARROW_RETURN_NOT_OK(task.Init(this));
    ARROW_RETURN_NOT_OK(Throttle());
    LOG(INFO) << "spawn a job, batch " << batch;
    ARROW_RETURN_NOT_OK(exec_context->executor()->Spawn(
        [this, task = std::move(task)]() mutable {
          ExecTask(std::move(task));
        }));
  }

  if (batch < num_batches) {
    return Status::Cancelled(std::to_string(num_batches - batch) + " batches left");
  }

  return Status::OK();
}

Status CmdPartitionPriv::Throttle() {
  std::unique_lock<std::mutex> lk(scheduler_mutex);
  producer_cv.wait(lk, [this]() {
    return compute_semaphore > 0 && write_semaphore > 0;
  });
  compute_semaphore -= 1;
  return last_status;
}

void CmdPartitionPriv::ExecTask(PartitionTask task) {
  auto result = PartitionTask(std::move(task)).Run();
  arrow::RecordBatchVector to_write;

  std::unique_lock<std::mutex> lk(scheduler_mutex);
  compute_semaphore += 1;
  last_status &= std::move(result).Value(&to_write);

  if (LIKELY(last_status.ok())) {
    write_semaphore -= 1;
    last_status = io_context->executor()->Spawn(
        [this, to_write = std::move(to_write)]() mutable {
          IoTask(std::move(to_write));
        });
  }

  lk.unlock();
  producer_cv.notify_one();
}

void CmdPartitionPriv::IoTask(arrow::RecordBatchVector records) {
  LOG(INFO) << "materialize";
  CHECK_EQ(records.size(), records.size());

  Status st;
  for (size_t i = 0; i < records.size(); ++i) {
    CHECK(records[i]);
    std::unique_lock<std::mutex> lk(output[i].mutex);
    st = output[i].writer->WriteRecordBatch(*records[i]);
    if (!st.ok())
      break;
    output[i].num_rows += records[i]->num_rows();
  }

  std::unique_lock<std::mutex> lk(scheduler_mutex);
  last_status &= std::move(st);
  write_semaphore += 1;
  lk.unlock();
  producer_cv.notify_one();
}

Status CmdPartition::Finish(bool incomplete) {
  if (auto thread_pool = dynamic_cast<ThreadPool*>(exec_context->executor())) {
    LOG(INFO) << "Waiting CPU threads to complete...";
    thread_pool->WaitForIdle();
  }
  if (auto thread_pool = dynamic_cast<ThreadPool*>(io_context->executor())) {
    LOG(INFO) << "Waiting IO threads to complete...";
    thread_pool->WaitForIdle();
  }

  LOG(INFO) << "Finalize partitions...";
  for (size_t p = 0; p < output.size(); ++p) {
    auto& part = output[p];
    // TODO: substrait key expression
    part.metadata->Append("num_partitions", std::to_string(options->num_partitions));
    part.metadata->Append("num_rows", std::to_string(part.num_rows));
    part.metadata->Append("partition", std::to_string(p));
    part.metadata->Append("hash_type", "murmur3");
    part.metadata->Append("hash_seed", std::to_string(options->hash_seed));
    part.metadata->Append("incomplete", std::to_string(incomplete));
    ARROW_RETURN_NOT_OK(part.writer->Close());
  }

  std::unique_lock<std::mutex> lk(scheduler_mutex);
  return last_status;
}

////////////////////////////////////////////////////////////////////////////////

const CmdDescription CmdPartition::description = {
  .name = "partition",
  .args = "arrow_file",
  .abstract = "Split the data into partitions grouped by a hash of key",
  .help = "",
};

template <>
void CmdOps<CmdPartition>::BindOptions(po::options_description& description,
                                       CmdPartitionOptions& options)
{
  description.add_options()
      ("seed,s",
       po::value(&options.hash_seed)->default_value(424242))
      ("key-column,c",
       po::value(&options.key_column)->default_value("pn_bits"))
      ("partitions,n",
       po::value(&options.num_partitions)->default_value(64))
      ("output-template,o",
       po::value(&options.output_template)->default_value("parts/pn-{}.arrow"));
}

template <>
Status CmdOps<CmdPartition>::StoreArgs(const po::variables_map& vm,
                                       const std::vector<std::string>& args,
                                       CmdPartitionOptions& options)
{
  if (args.size() != 1u)
    return Status::Invalid("arrow source path expected");
  options.source_path = args[0];
  return Status::OK();
}
