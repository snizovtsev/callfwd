#include "pthash_partitioner_internal.hpp"
#include "pthash_partitioner.hpp"
#include "bit_magic.hpp"
#include "lrn_schema.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/util/counting_semaphore.h>
#include <fmt/format.h>
#include <glog/logging.h>
#include <folly/experimental/NestedCommandLineApp.h>
#include <folly/Conv.h>
#include <boost/program_options/options_description.hpp>
#include <memory>
#include <mutex>

namespace po = boost::program_options;
using Status = arrow::Status;
template <class T> using Result = arrow::Result<T>;
using arrow::internal::checked_cast;
using arrow::internal::ThreadPool;

Status PartitionTask::Init(arrow::MemoryPool* memory_pool) {
  CHECK(slice.size());
  auto& schema = slice[0]->schema();
  int64_t capacity = slice[0]->num_rows();
  size_t nr_parts = slice.size();

  capacity += capacity / 10;
  partition.resize(nr_parts);
  key_column.resize(nr_parts);

  for (size_t i = 0; i < nr_parts; ++i) {
    ARROW_ASSIGN_OR_RAISE(partition[i], arrow::RecordBatchBuilder
                          ::Make(schema, memory_pool, capacity));
    key_column[i] = slice[i]->GetColumnByName("pn_bits");
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
    auto target = reinterpret_cast<BuilderType**>(partition.data());
    uint64_t mask = partition.size() - 1;

    int64_t length = source.length();
    for (int64_t i = 0; i < length; ++i) {
      auto h = murmur3_128(key_data[i] >> LRN_BITS_PN_SHIFT, hash_seed);
      uint64_t p = (h.first ^ h.second) & mask;
      ARROW_RETURN_NOT_OK(target[p]->Append(source.Value(i)));
    }

    return Status::OK();
  }

  template <typename T>
  std::enable_if_t<!is_simple_type<T>, Status>
  Visit(const T& array) {
    return Status::NotImplemented("not a simple type");
  }

  Status Run(const uint64_t* key_data_, const arrow::Array& array) {
    key_data = key_data_;
    return VisitArrayInline(array, this);
  }

  std::vector<arrow::ArrayBuilder*> partition;
  const uint64_t* key_data = nullptr;
  uint64_t hash_seed;
};

Status PartitionTask::RunColumn(int c) {
  ArrayPartitioner visitor;

  visitor.hash_seed = options->hash_seed;
  for (auto& builder : partition) {
    visitor.partition.push_back(builder->GetField(c));
  }

  for (size_t batch = 0; batch < slice.size(); ++batch) {
    const uint64_t *key_data = key_column[batch]->data()->GetValues<uint64_t>(1);
    std::shared_ptr<arrow::Array> column_chunk = slice[batch]->column(c);
    if (UNLIKELY(!key_data))
      return Status::Invalid("Only uint64 keys are supported");
    ARROW_RETURN_NOT_OK(visitor.Run(key_data, *column_chunk));
  }

  return Status::OK();
}

Result<arrow::RecordBatchVector> PartitionTask::Run() {
  int num_columns = slice[0]->num_columns();
  for (int c = 0; c < num_columns; ++c) {
    ARROW_RETURN_NOT_OK(RunColumn(c));
  }

  arrow::RecordBatchVector result;
  result.reserve(slice.size());
  for (auto& builder : partition) {
    ARROW_ASSIGN_OR_RAISE(auto partition, builder->Flush(false));
    result.push_back(std::move(partition));
  }

  return result;
}

std::unique_ptr<CmdPartition>
CmdPartition::Make(const Options *options,
                   arrow::compute::ExecContext* exec_context,
                   const arrow::io::IOContext* io_context)
{
  auto cmd = std::make_unique<CmdPartition>();
  cmd->exec_context = exec_context ?: arrow::compute::threaded_exec_context();
  cmd->io_context = io_context ?: &arrow::io::default_io_context();
  cmd->options = options;
  return cmd;
}

Status CmdPartition::Init() {
  ARROW_ASSIGN_OR_RAISE(source_file, arrow::io::MemoryMappedFile
                        ::Open(options->source_path, arrow::io::FileMode::READ));
  ARROW_ASSIGN_OR_RAISE(batch_reader, arrow::ipc::RecordBatchFileReader
                        ::Open(source_file, arrow::ipc::IpcReadOptions{}));

  arrow::ipc::IpcWriteOptions write_opts;
  write_opts.memory_pool = exec_context->memory_pool();
  output = std::vector<DataPartition>(options->num_partitions);

  for (uint32_t p = 0; p < options->num_partitions; ++p) {
    DataPartition &part = output[p];
    part.metadata = std::make_shared<arrow::KeyValueMetadata>();
    ARROW_ASSIGN_OR_RAISE(part.ostream, arrow::io::FileOutputStream::Open(
        fmt::format(options->output_template, p)));
    ARROW_ASSIGN_OR_RAISE(part.writer, arrow::ipc::MakeFileWriter(
        part.ostream, batch_reader->schema(), write_opts, part.metadata));
  }

  /* Maximum number of tasks queued */
  compute_semaphore = exec_context->executor()->GetCapacity() * 2;
  write_semaphore = io_context->executor()->GetCapacity() * 2;

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
    PartitionTask task{options};
    ARROW_RETURN_NOT_OK(ReadSlice(batch).Value(&task.slice));
    ARROW_RETURN_NOT_OK(task.Init(exec_context->memory_pool()));
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

const CommandDescription CmdPartition::description = {
  .name = "partition",
  .args = "arrow_file",
  .abstract = "Split the data into partitions grouped by a hash of key",
  .help = "",
};

void CmdPartitionOptions::Bind(po::options_description& description) {
  description.add_options()
      ("seed,s",
       po::value(&hash_seed)->default_value(424242))
      ("partitions,n",
       po::value(&num_partitions)->default_value(64))
      ("output-template,o",
       po::value(&output_template)->default_value("parts/pn-{}.arrow"));
}

Status CmdPartitionOptions::Store(const po::variables_map& options,
                                    const std::vector<std::string>& args) {
  if (args.size() != 1u)
    return Status::Invalid("arrow source path expected");

  source_path = args[0];
  return Status::OK();
}
