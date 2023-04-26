#include "pthash_partitioner_internal.hpp"
#include "pthash_partitioner.hpp"
#include "lrn_schema.hpp"
#include "compute_kernels.hpp"

#include <arrow/filesystem/api.h>
//#include <arrow/compute/exec/exec_plan.h>
//#include <arrow/compute/exec/query_context.h>
#include <arrow/acero/exec_plan.h>
#include <arrow/acero/query_context.h>
#include <folly/experimental/NestedCommandLineApp.h>
#include <folly/Conv.h>
#include <memory>

namespace po = boost::program_options;
namespace cp = arrow::compute;
namespace ds = arrow::dataset;
using Status = arrow::Status;

arrow::Result<std::shared_ptr<arrow::dataset::FileWriter>>
CountingIpcFileFormat::MakeWriter(std::shared_ptr<arrow::io::OutputStream> output,
                                  std::shared_ptr<arrow::Schema> schema,
                                  std::shared_ptr<ds::FileWriteOptions> options,
                                  arrow::fs::FileLocator file_locator) const
{
  auto cloned_options = std::dynamic_pointer_cast<ds::IpcFileWriteOptions>(options);
  std::shared_ptr<arrow::KeyValueMetadata> cloned_metadata;

  cloned_options = std::make_shared<arrow::dataset::IpcFileWriteOptions>(*cloned_options);
  if (cloned_options->metadata)
    cloned_metadata = cloned_options->metadata->Copy();
  else
    cloned_metadata = arrow::KeyValueMetadata::Make({}, {});
  cloned_options->metadata = cloned_metadata;

  ARROW_ASSIGN_OR_RAISE(auto dataset_writer, arrow::dataset::IpcFileFormat
      ::MakeWriter(std::move(output), std::move(schema),
                    std::move(cloned_options), std::move(file_locator)));

  return std::make_shared<CountingFileWriter>(std::move(dataset_writer),
                                              std::move(cloned_metadata));
}

std::unique_ptr<PtHashPartitioner>
PtHashPartitioner::Make(cp::ExecContext* exec_context, arrow::MemoryPool* memory_pool)
{
  auto cmd = std::make_unique<PtHashPartitioner>();
  cmd->exec_context = exec_context ?: cp::threaded_exec_context();
  cmd->memory_pool = memory_pool ?: arrow::default_memory_pool();
  return cmd;
}

Status PtHashPartitioner::Reset(const PtHashPartitionerOptions &options)
{
  ARROW_ASSIGN_OR_RAISE(source_file, arrow::io::MemoryMappedFile
                        ::Open(options.source_path, arrow::io::FileMode::READ));
  ARROW_ASSIGN_OR_RAISE(file_reader, arrow::ipc::RecordBatchFileReader
                        ::Open(source_file, arrow::ipc::IpcReadOptions{}));
  ARROW_ASSIGN_OR_RAISE(exec_plan, acero::ExecPlan::Make(exec_context));
  io_context = exec_plan->query_context()->io_context();
  ARROW_ASSIGN_OR_RAISE(batch_generator, file_reader->GetRecordBatchGenerator(
                          /*coalesce = */ false, *io_context));

  //write_options.existing_data_behavior = ds::ExistingDataBehavior::kDeleteMatchingPartitions;
  write_options.existing_data_behavior = ds::ExistingDataBehavior::kOverwriteOrIgnore;
  write_options.filesystem = std::make_shared<arrow::fs::LocalFileSystem>();
  write_options.base_dir = options.output_dir;
  write_options.basename_template = options.name_template;
  write_options.max_partitions = options.num_partitions;
  write_options.file_write_options = std::make_shared<CountingIpcFileFormat>()
      ->DefaultWriteOptions();
  write_options.partitioning = std::make_shared<ds::FilenamePartitioning>(
    arrow::schema({arrow::field("partition", arrow::uint16())}));
  //write_options.max_rows_per_group = LRN_ROWS_PER_CHUNK;

  bucketer_options = std::make_shared<BucketerOptions>(
    options.num_buckets,
    options.num_partitions,
    options.hash_seed);

  auto write_opts = std::dynamic_pointer_cast<ds::IpcFileWriteOptions>(
    write_options.file_write_options);
  write_opts->metadata = arrow::KeyValueMetadata::Make(
      {
        "x_pthash_seed",
        "x_pthash_buckets"
      }, {
        std::to_string(options.hash_seed),
        std::to_string(options.num_buckets),
      }
    );
  return Status::OK();
}

struct ToExecBatch {
  std::optional<arrow::compute::ExecBatch> operator()(
      const std::shared_ptr<arrow::RecordBatch> &batch) const
  {
    if (batch == NULLPTR)
      return std::nullopt;
    else
      return std::optional<cp::ExecBatch>(cp::ExecBatch(*batch));
  }
};

AsyncExecBatch PtHashPartitionerPriv::GenExecBatch() const
{
  return batch_generator().Then(ToExecBatch{});
}

Status PtHashPartitioner::Drain()
{
  ARROW_LOG(INFO) << "Build a plan";

  acero::Declaration source
      {"source", acero::SourceNodeOptions{
          file_reader->schema(),
          std::bind(&PtHashPartitionerPriv::GenExecBatch,
                    static_cast<PtHashPartitionerPriv*>(this))
        }};

  acero::Declaration project
      {"project", acero::ProjectNodeOptions{
          {cp::call("x_pthash_partition", {
                cp::call("shift_right", {
                    cp::field_ref("pn_bits"),
                    cp::literal<uint64_t>(LRN_BITS_PN_SHIFT)
                  })
              }, bucketer_options),
           cp::field_ref("pn_bits"),
           cp::field_ref("rn_bits")},
          {"partition", "pn_bits", "rn_bits"}
        }};

  acero::Declaration sink
      {"write", ds::WriteNodeOptions{write_options}};

  ARROW_RETURN_NOT_OK(acero::Declaration::Sequence({source, project, sink})
                      .AddToPlan(exec_plan.get()));
  ARROW_RETURN_NOT_OK(exec_plan->Validate());

  ARROW_LOG(INFO) << "Running " << exec_plan->ToString();
  //ARROW_RETURN_NOT_OK(plan->StartProducing());
  exec_plan->StartProducing();

  return exec_plan->finished().status();
}

  // std::shared_ptr<const arrow::KeyValueMetadata> metadata = file_reader->metadata();
  // int64_t num_rows = folly::tryTo<int64_t>(
  //     metadata->Get("num_rows").ValueOr("0")).value_or(0);
  // if (!num_rows) {
  //   ARROW_ASSIGN_OR_RAISE(num_rows, file_reader->CountRows());
  // }

  // ARROW_LOG(INFO) << "#rows: " << num_rows;
  // ARROW_LOG(INFO) << "#batches " << file_reader->num_record_batches();

  // uint32_t num_buckets =
  //   std::ceil((config_c * num_rows) / std::log2(num_rows));

  // ARROW_RETURN_NOT_OK(cp::Declaration::Sequence({
  //   {"source", cp::SourceNodeOptions{reader->schema(), to_exec_batch}},
  //   {"project", cp::ProjectNodeOptions{
  //     {cp::call("x_pthash_partition", {
  //         cp::call("shift_right", {
  //           cp::field_ref("pn_bits"),
  //           cp::literal<uint64_t>(LRN_BITS_PN_SHIFT)
  //         })
  //       }, std::make_shared<BucketerOptions>(num_buckets, num_partitions)),
  //      cp::field_ref("pn_bits"),
  //      cp::field_ref("rn_bits")},
  //     {"partition", "pn_bits", "rn_bits"}
  //   }},
  //   {"write", ds::WriteNodeOptions{write_options}},
  // }).AddToPlan(plan.get()));

////////////////////////////////////////////////////////////////////////////////

static void Main(PtHashPartitionerOptions& options,
                 const po::variables_map& vm,
                 const std::vector<std::string> &args,
                 PtHashPartitioner& command)
{
  Status st;

  options.Store(vm, args);
  st = command.Reset(options);

  if (!st.ok())
    throw folly::ProgramExit(1, "error: " + st.message());

  st = command.Drain();
  if (!st.ok())
    throw folly::ProgramExit(2, "error: " + st.message());
}

void PtHashPartitionerOptions::AddCommand(folly::NestedCommandLineApp &app,
                                          PtHashPartitioner *command)
{
  using namespace std::placeholders;
  static auto default_command = PtHashPartitioner::Make();

  if (!command)
    command = default_command.get();

  po::options_description& options = app.addCommand(
      "pthash-partitioner", "arrow_data",
      "Partition database according to PTHash bucket",
      "\n",
      std::bind(&Main, std::ref(*this), _1, _2, std::ref(*command)));
  BindToOptions(options);
}

void PtHashPartitionerOptions::BindToOptions(po::options_description& description)
{
  description.add_options()
      ("seed,s",
       po::value(&hash_seed)->default_value(424242))
      ("partitions,p",
       po::value(&num_partitions)->default_value(128))
      ("buckets,b",
       po::value(&num_buckets)->required())
      ("output-dir,O",
       po::value(&output_dir)->required())
      ("name-template,T",
       po::value(&name_template)->default_value("pn.{i}.arrow"));
}

void PtHashPartitionerOptions::Store(const po::variables_map& options,
                                     const std::vector<std::string>& args)
{
  if (args.size() != 1u)
    throw folly::ProgramExit(1, "arrow source path expected");
  source_path = args[0];
}
