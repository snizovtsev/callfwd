#include "compute_kernels.hpp"
#include "pn_ordered_join.hpp"
#include "pthash/pthash.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/dataset/api.h>
#include <arrow/dataset/plan.h>
#include <arrow/filesystem/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/exec/query_context.h>
#include <arrow/ipc/feather.h>
#include <arrow/util/unreachable.h>
#include <arrow/util/thread_pool.h>
#include <glog/logging.h>
#include <folly/experimental/NestedCommandLineApp.h>
#include <folly/GroupVarint.h>
#include <folly/Conv.h>

#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <cstdint>
#include <cstdlib>

namespace po = ::boost::program_options;
namespace cp = arrow::compute;
namespace ds = arrow::dataset;

struct BuildPTHashOpts {
  //explicit BuildPTHashOpts() noexcept = default;
  explicit BuildPTHashOpts(const po::variables_map& options,
                           const std::vector<std::string> &args);

  std::string table_path;
# define OPT_PTHASH_OUTPUT "output"
  std::string output_path;
# define OPT_PTHASH_C      "tweak-c"
  float tweak_c; // defaults => description
# define OPT_PTHASH_ALPHA  "tweak-alpha"
  float tweak_alpha;
};

class CountingFileWriter final : public ds::FileWriter {
 public:
  explicit CountingFileWriter(std::shared_ptr<ds::FileWriter> wrapee,
                              std::shared_ptr<arrow::KeyValueMetadata> metadata)
      : FileWriter(wrapee->schema(), wrapee->options(), NULLPTR, wrapee->destination())
      , wrapee_(std::move(wrapee))
      , metadata_(std::move(metadata))
  {}

  arrow::Status Write(const std::shared_ptr<arrow::RecordBatch>& batch) override {
    rows_written_ += batch->num_rows();
    return wrapee_->Write(batch);
  }

  arrow::Future<> FinishInternal() override {
    arrow::Unreachable();
  }

  arrow::Future<> Finish() override {
    metadata_->Append("num_rows", std::to_string(rows_written_));
    return wrapee_->Finish();
  }

 private:
  int64_t rows_written_ = 0;
  std::shared_ptr<ds::FileWriter> wrapee_;
  std::shared_ptr<arrow::KeyValueMetadata> metadata_;
};

class CountingIpcFileFormat : public ds::IpcFileFormat {
 public:
  arrow::Result<std::shared_ptr<ds::FileWriter>> MakeWriter(
      std::shared_ptr<arrow::io::OutputStream> destination,
      std::shared_ptr<arrow::Schema> schema,
      std::shared_ptr<ds::FileWriteOptions> options,
      arrow::fs::FileLocator destination_locator) const override
  {
    auto cloned_options = std::dynamic_pointer_cast<ds::IpcFileWriteOptions>(options);
    std::shared_ptr<arrow::KeyValueMetadata> cloned_metadata;

    cloned_options = std::make_shared<ds::IpcFileWriteOptions>(*cloned_options);
    if (cloned_options->metadata)
      cloned_metadata = cloned_options->metadata->Copy();
    else
      cloned_metadata = arrow::KeyValueMetadata::Make({}, {});
    cloned_options->metadata = cloned_metadata;

    ARROW_ASSIGN_OR_RAISE(auto wrapee, ds::IpcFileFormat
        ::MakeWriter(std::move(destination), std::move(schema),
                     std::move(cloned_options), std::move(destination_locator)));
    return std::make_shared<CountingFileWriter>(std::move(wrapee),
                                                std::move(cloned_metadata));
  }
};

class BucketPartitionerCommand {
 public:
  explicit BucketPartitionerCommand();
  //arrow::Status Main(const BucketPartitionerOptions &opts);

 private:

};

void PartitionByBucket(const po::variables_map& options,
                       const std::vector<std::string> &args)
{
  RegisterCustomFunctions(cp::GetFunctionRegistry());
  arrow::dataset::internal::Initialize();

  CHECK_EQ(args.size(), 1u);

  // construct an ExecPlan first to hold your nodes
  LOG(INFO) << "Make plan";
  cp::ExecContext* exec_context = cp::threaded_exec_context();
  auto plan = cp::ExecPlan::Make(exec_context)
    .ValueOrDie();

  LOG(INFO) << "Open table";

  auto istream = arrow::io::MemoryMappedFile
    ::Open(args[0], arrow::io::FileMode::READ)
    .ValueOrDie();

  auto reader = arrow::ipc::RecordBatchFileReader
      ::Open(istream, arrow::ipc::IpcReadOptions{})
      .ValueOrDie();

  auto async_batch_gen = reader->GetRecordBatchGenerator(
    /*coalesce = */ false,
    *plan->query_context()->io_context())
    .ValueOrDie();

  auto to_exec_batch = [=]() {
    return async_batch_gen()
      .Then([](const std::shared_ptr<arrow::RecordBatch> &batch)
        -> std::optional<cp::ExecBatch>
      {
        if (batch == NULLPTR)
          return std::nullopt;
        else
          return std::optional<cp::ExecBatch>(cp::ExecBatch(*batch));
      });
  };

  uint64_t num_rows = (reader->num_record_batches() - 1) * LRN_ROWS_PER_CHUNK
    + reader->ReadRecordBatch(reader->num_record_batches() - 1).ValueOrDie()->num_rows();
  LOG(INFO) << "#rows: " << num_rows;
  LOG(INFO) << "#batches " << reader->num_record_batches();

  uint32_t num_partitions = 32;
  float config_c = options[OPT_PTHASH_C].as<float>();
  uint32_t num_buckets =
    std::ceil((config_c * num_rows) / std::log2(num_rows));

  ds::FileSystemDatasetWriteOptions write_options;
  write_options.existing_data_behavior = ds::ExistingDataBehavior::kDeleteMatchingPartitions;
  write_options.filesystem = std::make_shared<arrow::fs::LocalFileSystem>();
  write_options.base_dir = "parts/";
  write_options.file_write_options = std::make_shared<CountingIpcFileFormat>()
      ->DefaultWriteOptions();
  write_options.partitioning = std::make_shared<ds::FilenamePartitioning>(
    arrow::schema({arrow::field("partition", arrow::uint16())}));
  write_options.basename_template = "pn.{i}.arrow";
  write_options.max_partitions = num_partitions;
  //write_options.max_rows_per_group = LRN_ROWS_PER_CHUNK;

  LOG(INFO) << "Build plan";
  cp::Declaration::Sequence({
    {"source", cp::SourceNodeOptions{reader->schema(), to_exec_batch}},
    {"project", cp::ProjectNodeOptions{
      {cp::call("x_pthash_partition", {
          cp::call("shift_right", {
            cp::field_ref("pn_bits"),
            cp::literal(UINT64_C(30))
          })
        }, std::make_shared<BucketerOptions>(num_buckets, num_partitions)),
       cp::field_ref("pn_bits"),
       cp::field_ref("rn_bits")},
      {"partition", "pn_bits", "rn_bits"}
    }},
    {"write", ds::WriteNodeOptions{write_options}},
  }).AddToPlan(plan.get())
    .ValueOrDie();

  LOG(INFO) << "Validate plan";
  ARROW_CHECK_OK(plan->Validate());

  LOG(INFO) << "Running...";
  //ARROW_CHECK_OK(plan->StartProducing());
  plan->StartProducing();
  plan->finished().result()
    .ValueOrDie();

  LOG(INFO) << "Finished plan";
}

void BuildPTHash(const po::variables_map& options,
                 const std::vector<std::string> &args)
{
  /* Set up a build configuration. */
  pthash::build_configuration config;
  config.c = options[OPT_PTHASH_C].as<float>();
  config.alpha = options[OPT_PTHASH_ALPHA].as<float>();
  config.verbose_output = true;
  config.num_threads = 8;
  // config.num_partitions = 8;

  arrow::fs::FileSelector selector;
  selector.base_dir = "parts/";

  std::shared_ptr<ds::DatasetFactory> factory = ds::FileSystemDatasetFactory
    ::Make(std::make_shared<arrow::fs::LocalFileSystem>(),
           std::move(selector),
           std::make_shared<ds::IpcFileFormat>(),
           ds::FileSystemFactoryOptions{})
    .ValueOrDie();
  std::shared_ptr<ds::Dataset> dataset = factory->Finish()
    .ValueOrDie();

  // XXX: to metadata
  int64_t local_table_size = 0, partitions = 0, all_rows = 0;

  // Print out the fragments
  for (const auto& maybe_fragment : *dataset->GetFragments()) {
    std::shared_ptr<ds::Fragment> fragment = *maybe_fragment;

    auto fragment_in = arrow::io::ReadableFile::Open(fragment->ToString())
      .ValueOrDie();
    auto fragment_reader = arrow::ipc::RecordBatchFileReader
      ::Open(fragment_in, arrow::ipc::IpcReadOptions{})
      .ValueOrDie();
    std::shared_ptr<const arrow::KeyValueMetadata> metadata
        = fragment_reader->metadata();

    int64_t nrows = folly::to<int64_t>(metadata->Get("num_rows").ValueOr("-1"));
    all_rows += nrows;
    local_table_size = std::max(nrows, local_table_size);

    LOG(INFO) << "Found fragment: " << fragment->ToString()
              << " rows: " << nrows;
    ++partitions;
  }

  LOG(INFO) << "max fragment:\t" << local_table_size;
  LOG(INFO) << "all_rows:\t\t" << all_rows;
  LOG(INFO) << "adjusted:\t\t" << local_table_size * partitions;
  LOG(INFO) << "optimal T:\t\t" << (uint64_t)(all_rows / config.alpha);
  LOG(INFO) << "partitioned:\t\t" << (uint64_t)(local_table_size * partitions / config.alpha);

  local_table_size = local_table_size / config.alpha;

  auto executor = arrow::internal::GetCpuThreadPool();
  for (int i = 0; i < 30; ++i) {
    ARROW_CHECK_OK(executor->Spawn([=]() {
      LOG(INFO) << i << ": sleep 3";
      sleep(3);
      LOG(INFO) << i << ": exit";
    }));
  }

  executor->WaitForIdle();
}

/* Declare the PTHash function. */
typedef pthash::single_phf<
  pthash::hash_128,        // base hasher
  pthash::dictionary_dictionary,  // encoder type
  false                   // minimal
  > pthash128_t;

void PermuteHash(const po::variables_map& options,
                 const std::vector<std::string> &args)
{
#define OPT_PERMUTE_PN_OUTPUT "output"
  std::string output_path = options[OPT_PERMUTE_PN_OUTPUT].as<std::string>();
  CHECK_EQ(args.size(), 2u);

  LOG(INFO) << "reading pthash...";
  pthash128_t f;
  essentials::load(f, args[1].c_str());
  LOG(INFO) << "pthash space: " << f.table_size();

  auto istream = arrow::io::ReadableFile::Open(args[0])
    .ValueOrDie();
  auto table_reader = arrow::ipc::RecordBatchFileReader
    ::Open(istream, arrow::ipc::IpcReadOptions{})
    .ValueOrDie();

  auto schema = table_reader->schema();
  int num_batches = (f.table_size() + LRN_ROWS_PER_CHUNK - 1) / LRN_ROWS_PER_CHUNK;
  CHECK_GE(num_batches, table_reader->num_record_batches());
  std::vector<arrow::UInt64Builder> pn_chunks(num_batches);
  std::vector<arrow::UInt64Builder> rn_chunks(num_batches);

  LOG(INFO) << "permute...";

  std::vector<uint64_t> zeroes(LRN_ROWS_PER_CHUNK);
  uint64_t tot_rows = 0;

  for (int bi = 0; bi < table_reader->num_record_batches(); ++bi) {
    auto batch = table_reader->ReadRecordBatch(bi)
      .ValueOrDie();

    uint64_t num_rows = batch->num_rows();
    if (bi + 1 != table_reader->num_record_batches())
      CHECK(num_rows == LRN_ROWS_PER_CHUNK);
    else
      CHECK(num_rows <= LRN_ROWS_PER_CHUNK);

    const uint64_t *pn_data = batch->column_data(0)->GetValues<uint64_t>(1);
    const uint64_t *rn_data = batch->column_data(1)->GetValues<uint64_t>(1);

    for (uint64_t row_index = 0; row_index < num_rows; ++row_index) {
      const uint64_t pn = pn_data[row_index] >> LRN_BITS_PN_SHIFT;
      const uint64_t target_id = f(pn);
      const uint64_t target_chunk = target_id / LRN_ROWS_PER_CHUNK;
      const uint64_t target_row = target_id % LRN_ROWS_PER_CHUNK;
      arrow::UInt64Builder &target_pn = pn_chunks[target_chunk];
      arrow::UInt64Builder &target_rn = rn_chunks[target_chunk];

      if (/*unlikely*/target_pn.length() == 0) {
        size_t chunk_size = LRN_ROWS_PER_CHUNK;
        if (/*unlikely*/target_chunk + 1 == pn_chunks.size()) {
          chunk_size = f.table_size() % LRN_ROWS_PER_CHUNK;
          if (!chunk_size)
            chunk_size = LRN_ROWS_PER_CHUNK;
        }

        zeroes.resize(chunk_size);
        ARROW_CHECK_OK(target_pn.AppendValues(zeroes));
        ARROW_CHECK_OK(target_rn.AppendValues(zeroes));
        tot_rows += chunk_size;
      }

      //LOG(INFO) << target_id << ": " << target_chunk << ' ' << target_row;
      CHECK_EQ(target_pn[target_row], 0u);
      CHECK_EQ(target_rn[target_row], 0u);
      target_pn[target_row] = pn_data[row_index];
      target_rn[target_row] = rn_data[row_index];
    }
  }
  CHECK_EQ(tot_rows, f.table_size());

  LOG(INFO) << "writing result...";

  arrow::ArrayVector pn_column(num_batches);
  arrow::ArrayVector rn_column(num_batches);
  for (int bi = 0; bi < num_batches; ++bi) {
    pn_column[bi] = pn_chunks[bi].Finish()
      .ValueOrDie();
    rn_column[bi] = rn_chunks[bi].Finish()
      .ValueOrDie();
  }
  arrow::ChunkedArrayVector columns = {
    arrow::ChunkedArray::Make(std::move(pn_column))
          .ValueOrDie(),
    arrow::ChunkedArray::Make(std::move(rn_column))
          .ValueOrDie(),
  };

  auto ostream_ = arrow::io::FileOutputStream::Open(output_path.c_str())
    .ValueOrDie();
  auto table_writer = arrow::ipc::MakeFileWriter(ostream_, schema)
    .ValueOrDie();

  LOG(INFO) << "arrow::Table::Make...";
  auto lrn_table = arrow::Table::Make(schema, columns, f.table_size());
  CHECK(lrn_table);

  LOG(INFO) << "arrow::Table::WriteTable...";
  ARROW_CHECK_OK(table_writer->WriteTable(*lrn_table));
  ARROW_CHECK_OK(table_writer->Close());
}

void Query(const po::variables_map& options,
           const std::vector<std::string> &args)
{
  CHECK_GE(args.size(), 3u);

  std::shared_ptr<arrow::io::RandomAccessFile> ym_file;
  std::shared_ptr<arrow::ipc::RecordBatchFileReader> ym_reader;

  if (options.count("youmail")) {
    ym_file = arrow::io::ReadableFile::Open(options["youmail"].as<std::string>())
      .ValueOrDie();
    ym_reader = arrow::ipc::RecordBatchFileReader
      ::Open(ym_file, arrow::ipc::IpcReadOptions{})
      .ValueOrDie();
  }

  std::shared_ptr<arrow::io::RandomAccessFile> lrn_file;
  std::shared_ptr<arrow::ipc::RecordBatchFileReader> lrn_reader;

  lrn_file = arrow::io::ReadableFile::Open(args[0])
    .ValueOrDie();
  lrn_reader = arrow::ipc::RecordBatchFileReader
    ::Open(lrn_file, arrow::ipc::IpcReadOptions{})
    .ValueOrDie();

  LOG(INFO) << "reading pthash...";
  pthash128_t phf;
  essentials::load(phf, args[1].c_str());
  LOG(INFO) << "pthash space: " << phf.table_size();

  for (unsigned i = 2; i < args.size(); ++i) {
    uint64_t query = atoll(args[i].c_str());
    uint64_t row_index = phf(query);
    uint64_t chunk = row_index / LRN_ROWS_PER_CHUNK;
    uint64_t pos = row_index % LRN_ROWS_PER_CHUNK;

    auto lrn = lrn_reader->ReadRecordBatch(chunk)
      .ValueOrDie();
    auto pn_column = std::dynamic_pointer_cast<arrow::UInt64Array>(lrn->column(0));
    auto rn_column = std::dynamic_pointer_cast<arrow::UInt64Array>(lrn->column(1));

    uint64_t pn_bits = pn_column->Value(pos);
    uint64_t rn_bits = rn_column->Value(pos);
    uint64_t ym_id = pn_bits & LRN_BITS_YM_MASK;
    uint64_t pn = pn_bits >> LRN_BITS_PN_SHIFT;
    uint64_t dno = (pn_bits & LRN_BITS_DNO_MASK) >> LRN_BITS_DNO_SHIFT;

    LOG(INFO) << "pthash: " << row_index << " bits: " << pn_bits;
    std::cout << "pn: " << query << ' ';

    if (pn_bits == 0) {
      std::cout << "empty slot" << std::endl;
      continue;
    }

    if (pn != query) {
      std::cout << "not found" << std::endl;
      continue;
    }

    if (pn_bits & LRN_BITS_LRN_FLAG)
      std::cout << "rn: " << (rn_bits >> LRN_BITS_PN_SHIFT) << ' ';
    if (pn_bits & LRN_BITS_DNC_FLAG)
      std::cout << "dnc ";
    if (dno)
      std::cout << "dno: " << dno << ' ';

    if (ym_id != LRN_BITS_YM_MASK) {
      ym_id >>= LRN_BITS_YM_SHIFT;
      if (ym_reader) {
        uint64_t ychunk = ym_id / YM_ROWS_PER_CHUNK;
        uint64_t ypos = ym_id % YM_ROWS_PER_CHUNK;

        auto ym = ym_reader->ReadRecordBatch(ychunk)
          .ValueOrDie();
        auto c_spam_score  = std::static_pointer_cast<arrow::FloatArray>(ym->column(0));
        auto c_fraud_prob  = std::static_pointer_cast<arrow::FloatArray>(ym->column(1));
        auto c_unlawful_prob = std::static_pointer_cast<arrow::FloatArray>(ym->column(2));
        auto c_tcpa_fraud_prob = std::static_pointer_cast<arrow::FloatArray>(ym->column(3));

        float spam_score = c_spam_score->Value(ypos);
        float fraud_prob = c_fraud_prob->Value(ypos);
        float unlawful_prob = c_unlawful_prob->Value(ypos);
        float tcpa_fraud_prob = c_tcpa_fraud_prob->Value(ypos);

        std::cout << "ym: ["
                  << spam_score << ", " << fraud_prob << ", "
                  << unlawful_prob << ", " << tcpa_fraud_prob << "]";
      } else {
        std::cout << "ym: " << ym_id;
      }
    }

    std::cout << std::endl;
  }
}

// template<class Entrypoint>
// folly::NestedCommandLineApp::Command ArrowCommand(const Entrypoint &entrypoint)
// {
//   return [&](const po::variables_map& options,
//              const std::vector<std::string> &args)
//   {
//     arrow::Status st = entrypoint(options);
//     if (!st.ok())
//       throw folly::ProgramExit(1, st.message());
//   };
// }

int main(int argc, const char* argv[]) {
  using namespace std::placeholders;

  setlocale(LC_ALL, "C");
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();

  folly::NestedCommandLineApp app{argv[0], "0.9", "", "", nullptr};
  app.addGFlags(folly::ProgramOptionsStyle::GNU);
  FLAGS_logtostderr = 1;

  PnOrderedJoinOptions pn_ordered_join;
  pn_ordered_join.AddCommand(app);

  app.addCommand(
      "pthash-partition", "",
      "",
      "",
      PartitionByBucket);

  auto& pthash_pn_cmd = app.addCommand(
      "pthash-build", "arrow_table_path",
      "Build PTHash index for PN dataset",
      "",
      BuildPTHash);

  pthash_pn_cmd.add_options()
      // (OPT_PTHASH_OUTPUT ",O",
      //  po::value<std::string>()->required(),
      //  "Arrow table output path. Required.")
      (OPT_PTHASH_C ",c",
       po::value<float>()->default_value(6.0),
       "Bucket density coefficient")
      (OPT_PTHASH_ALPHA ",a",
       po::value<float>()->default_value(0.94),
       "Value domain density coefficient");

  auto& permute_pn_cmd = app.addCommand(
      "permute-hash", "arrow_table_path pthash_path",
      "Rearrange rows by pthash value",
      "",
      PermuteHash);

  permute_pn_cmd.add_options()
      (OPT_PERMUTE_PN_OUTPUT ",O",
       po::value<std::string>()->required(),
       "Arrow table output path. Required.");

  auto& query_cmd = app.addCommand(
      "query", "lrn_table_path pthash key",
      "Query PN data for number",
      "",
      Query);

  query_cmd.add_options()
    ("youmail",
     po::value<std::string>(),
     "YouMail database path");

  // invert-rn rn_pn.arrow > rn0.arrow
  // verify

  return app.run(argc, argv);
}
