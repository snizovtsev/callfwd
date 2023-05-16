#include "bit_magic.hpp"
#include "lrn_schema.hpp"
#include "pthash/pthash.hpp"
#include "cmd_convert.hpp"
#include "cmd_partition.hpp"
#include "compute_kernels.hpp"
#include "pthash_bucketer.hpp"

#include <algorithm>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/dataset/api.h>
#include <arrow/dataset/plan.h> // Initialize
#include <arrow/filesystem/api.h>
#include <arrow/compute/api.h>
#include <arrow/ipc/feather.h>
#include <arrow/util/thread_pool.h>
#include <glog/logging.h>
#include <arrow/util/logging.h>

#include <folly/experimental/NestedCommandLineApp.h>
#include <folly/Likely.h>

#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <cstdint>
#include <cstdlib>
#include <queue>
#include <unordered_set>

namespace po = boost::program_options;
namespace ds = arrow::dataset;
namespace bit_util = arrow::bit_util;

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

struct PtHashWorker {
  arrow::Status ReadTable(std::shared_ptr<arrow::ipc::RecordBatchFileReader> reader);
  arrow::Status Do(int64_t table_size, uint16_t num_partitions);
  void Reset();

  std::shared_ptr<const arrow::KeyValueMetadata> metadata;
  std::shared_ptr<arrow::Table> table;

  uint64_t hash_seed;
  uint32_t num_buckets;

  // bucket size, indexed by bucket number (#buckets)
  arrow::TypedBufferBuilder<uint16_t> bucket_size;
  // bucket numbers, ordered by bucket size (#non_zero_buckets)
  arrow::TypedBufferBuilder<uint32_t> bucket_desc;
  // a row grouper (#non_zero_buckets)
  arrow::TypedBufferBuilder<int32_t> table_offset;
  // hash values, grouped by `table_offsets` (#rows)
  arrow::TypedBufferBuilder<uint64_t> hashes;
  // hash origin, grouped by `table_offsets` (#rows)
  arrow::TypedBufferBuilder<uint32_t> origin;
  // pthash pilot numbers (#buckets)
  arrow::TypedBufferBuilder<uint16_t> pilot;
  // pthash result (#table)
  arrow::TypedBufferBuilder<uint32_t> take_indices;
  // validity bitmap
  arrow::TypedBufferBuilder<bool> taken;
};

arrow::Status PtHashWorker::ReadTable(std::shared_ptr<arrow::ipc::RecordBatchFileReader> reader)
{
  arrow::RecordBatchVector batches;
  batches.resize(reader->num_record_batches());
  for (size_t i = 0; i < batches.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(batches[i], reader->ReadRecordBatch(i));
  }
  ARROW_ASSIGN_OR_RAISE(table, arrow::Table::FromRecordBatches(reader->schema(), batches));

  metadata = reader->metadata()->Copy();
  ARROW_ASSIGN_OR_RAISE(auto seed_str, metadata->Get("hash_seed"));
  hash_seed = folly::to<uint64_t>(seed_str);
  //num_buckets = folly::to<uint32_t>(metadata->Get("x_pthash_buckets").ValueOrDie());

  return arrow::Status::OK();
}

arrow::Status PtHashWorker::Do(int64_t table_size, uint16_t num_partitions) {
  auto key_chunks = table->GetColumnByName("pn_bits")->chunks();
  SkewBucketer bucketer{num_buckets};

  //LOG(INFO) << "sample bucket: " << sample_bucket;
  //LOG(INFO) << "partition: " << partition;
  //LOG(INFO) << "dense offset: " << local_bucketer.dense_offset;
  //LOG(INFO) << "sparse offset: " << local_bucketer.sparse_offset;
  //LOG(INFO) << "table size: " << table_size;

  bucket_size.Reset();
  bucket_desc.Reset();
  table_offset.Reset();
  pilot.Reset();

  ARROW_RETURN_NOT_OK(bucket_size.Append(num_buckets, 0));
  ARROW_RETURN_NOT_OK(bucket_desc.Reserve(num_buckets));
  ARROW_RETURN_NOT_OK(table_offset.Append(num_buckets, 0));
  ARROW_RETURN_NOT_OK(pilot.Append(num_buckets, 0));

  uint16_t *bucket_size_data = bucket_size.mutable_data();
  uint16_t *pilot_data = pilot.mutable_data();
  int32_t *offset_data = table_offset.mutable_data();

  for (auto& chunk : key_chunks) {
    auto pn_bits = chunk->data()->GetValues<uint64_t>(1, 0);
    for (int64_t i = 0; i < chunk->length(); ++i) {
      uint64_t pn = pn_bits[i] >> LRN_BITS_PN_SHIFT;
      uint64_t h = murmur3_64(pn, hash_seed);
      uint32_t b = bucketer(h);
      bucket_size_data[b]++;
    }
  }

  uint16_t mb = *std::max_element(bucket_size_data,
                                  bucket_size_data + num_buckets);
  LOG(INFO) << "max bucket: " << mb;

  int64_t row_offset = 0;
  for (size_t bs = mb; bs > 0; --bs) {
    for (size_t lb = 0; lb < num_buckets; ++lb) {
      if (LIKELY(bucket_size_data[lb] != bs))
        continue;
      row_offset += bs;
      offset_data[lb] = row_offset;
      bucket_desc.UnsafeAppend(lb);
    }
  }
  CHECK_EQ(row_offset, table->num_rows());
  CHECK_LE(bucket_desc.length(), num_buckets);

  hashes.Reset();
  ARROW_RETURN_NOT_OK(hashes.Append(table->num_rows(), 0));
  uint64_t* hash_data = hashes.mutable_data();

  origin.Reset();
  ARROW_RETURN_NOT_OK(origin.Append(table->num_rows(), 0));
  uint32_t* origin_data = origin.mutable_data();

  int64_t table_index = 0;
  for (auto& chunk : key_chunks) {
    auto pn_bits = chunk->data()->GetValues<uint64_t>(1, 0);
    for (int64_t i = 0; i < chunk->length(); ++i, ++table_index) {
      uint64_t pn = pn_bits[i] >> LRN_BITS_PN_SHIFT;
      auto h = murmur3_128(pn, hash_seed);
      uint32_t b = bucketer(h.first);
      int32_t pos = --offset_data[b];
      hash_data[pos] = h.second;
      origin_data[pos] = table_index;
    }
  }
  offset_data = nullptr;
  table_offset.Reset();

  uint32_t oldreport = 0;
  uint64_t pilot_max = 0;

  uint32_t pilot_limit = INT16_MAX;
  std::vector<uint64_t> hashed_pilot(pilot_limit);
  for (uint32_t p = 0; p < pilot_limit; ++p)
    hashed_pilot[p] = murmur3_64(p, hash_seed);

  const uint32_t* bucket_desc_data = bucket_desc.data();
  int64_t row_idx = 0;

  taken.Reset();
  ARROW_RETURN_NOT_OK(taken.Append(table_size, false));
  uint8_t *taken_bits = taken.mutable_data();

  take_indices.Reset();
  ARROW_RETURN_NOT_OK(take_indices.Append(table_size, 0));
  uint32_t *indices_data = take_indices.mutable_data();

  __uint128_t M = fastmod_computeM_u64(table_size);
  for (int64_t i = 0; i < bucket_desc.length(); ++i) {
    uint32_t lb = bucket_desc_data[i];
    int64_t row_start = row_idx;
    int64_t row_end = row_idx + bucket_size_data[lb];
    bool retry = true;

    for (uint32_t trial = 0; retry && trial < pilot_limit; ++trial) {
      for (retry = false; row_idx < row_end; ++row_idx) {
        uint64_t h = hash_data[row_idx] ^ hashed_pilot[trial];
        uint32_t p = fastmod_u64(h, M, table_size);
        if (!bit_util::GetBit(taken_bits, p)) {
          indices_data[p] = origin_data[row_idx];
          bit_util::SetBit(taken_bits, p);
          origin_data[row_idx] = p;
        } else {
          retry = true;
          break;
        }
      }

      if (retry) {
        for (row_idx -= 1; row_idx >= row_start; --row_idx) {
          uint32_t p = origin_data[row_idx];
          origin_data[row_idx] = indices_data[p];
          indices_data[p] = 0;
          bit_util::ClearBit(taken_bits, p);
        }
        ++row_idx;
      } else {
        pilot_data[lb] = trial;
        if (trial > pilot_max)
          pilot_max = trial;
      }
    }

    CHECK(!retry);

    if (row_idx - oldreport > table->num_rows() / 100) {
      LOG(INFO) << lb << ": " << pilot_data[lb]
                << " maxp " << pilot_max
                << " bs " << bucket_size_data[lb]
                << " off " << row_start;
      oldreport = row_start;
    }
  }

  origin_data = nullptr;
  origin.Reset();
  hash_data = nullptr;
  hashes.Reset();

  size_t show_empty = 100;
  std::cout << "!taken: ";
  for (int64_t i = 0; show_empty && i < taken.length(); ++i) {
    if (bit_util::GetBit(taken_bits, i))
      continue;
    std::cout << i << ' ';
    --show_empty;
  }
  std::cout << "..." << std::endl;

  auto indices = arrow::UInt32Array(
    table_size,
    take_indices.Finish().ValueOrDie(),
    taken.Finish().ValueOrDie());
  std::cout << indices.ToString() << std::endl;

  LOG(INFO) << "Take...";
  auto res = arrow::compute::Take(table, indices)->table();
  std::cout << res->ToString();

  LOG(INFO) << "Verify...";
  table_index = 0;
  for (auto& chunk : res->GetColumnByName("pn_bits")->chunks()) {
    auto pn_bits = chunk->data()->GetValues<uint8_t>(0, 0);
    auto pn_data = chunk->data()->GetValues<uint64_t>(1, 0);
    for (int64_t i = 0; i < chunk->length(); ++i, ++table_index) {
      if (UNLIKELY(!bit_util::GetBit(pn_bits, i)))
        continue;
      uint64_t pn = pn_data[i] >> LRN_BITS_PN_SHIFT;
      auto h = murmur3_128(pn, hash_seed);
      uint32_t b = bucketer(h.first);
      uint64_t th = h.second ^ hashed_pilot[pilot_data[b]];
      uint32_t p = fastmod_u64(th, M, table_size);
      LOG_IF(ERROR, p != table_index) << table_index << ": pn=" << pn << " " << p;
    }
  }

  ARROW_ASSIGN_OR_RAISE(auto pthash_buf, pilot.Finish());
  auto pthash = std::make_shared<arrow::UInt16Array>(num_buckets, pthash_buf);
  LOG(INFO) << "pilot bytes: " << pthash_buf->size() << " ("
    << num_buckets << " buckets)";
  LOG(INFO) << pthash->ToString();

  pthash::compact encoder1;
  pthash::dictionary encoder2;
  pthash::elias_fano encoder3;
  pthash::sdc encoder4;

  encoder1.encode(pilot_data, num_buckets);
  LOG(INFO) << "encoder1 size: " << encoder1.num_bits() / 8 << ' '
            << (encoder1.num_bits() / 8.0) / pthash_buf->size() * 100 << '%';
  encoder2.encode(pilot_data, num_buckets);
  LOG(INFO) << "encoder2 size: " << encoder2.num_bits() / 8 << ' '
            << (encoder2.num_bits() / 8.0) / pthash_buf->size() * 100 << '%';
  encoder3.encode(pilot_data, num_buckets);
  LOG(INFO) << "encoder3 size: " << encoder3.num_bits() / 8 << ' '
            << (encoder3.num_bits() / 8.0) / pthash_buf->size() * 100 << '%';
  encoder4.encode(pilot_data, num_buckets);
  LOG(INFO) << "encoder4 size: " << encoder4.num_bits() / 8 << ' '
            << (encoder4.num_bits() / 8.0) / pthash_buf->size() * 100 << '%';

  auto pthash_table = arrow::Table::Make(
    arrow::schema({arrow::field("pilot", arrow::uint16())}), {pthash});

  uint32_t partition = 666;
  std::string out_path = std::string("final/pn-")+std::to_string(partition)+".arrow";
  ARROW_ASSIGN_OR_RAISE(auto ostream, arrow::io::FileOutputStream
                        ::Open(out_path));
  ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeFileWriter(
                          ostream.get(), res->schema(),
                          arrow::ipc::IpcWriteOptions{},
                          metadata));
  ARROW_RETURN_NOT_OK(writer->WriteTable(*res));
  ARROW_RETURN_NOT_OK(writer->Close());

  std::string pthash_path = std::string("final/pn-")+std::to_string(partition)+".pthash.arrow";
  ARROW_ASSIGN_OR_RAISE(auto ostream_index, arrow::io::FileOutputStream
                        ::Open(pthash_path));
  ARROW_ASSIGN_OR_RAISE(auto writer_index, arrow::ipc::MakeFileWriter(
                          ostream_index.get(), pthash_table->schema(),
                          arrow::ipc::IpcWriteOptions{}));
  ARROW_RETURN_NOT_OK(writer_index->WriteTable(*pthash_table));
  ARROW_RETURN_NOT_OK(writer_index->Close());

  return arrow::Status::OK();
}

arrow::Status BuildPTHash(const po::variables_map& options,
                          const std::vector<std::string> &args)
{
  arrow::fs::FileSelector selector;
  selector.base_dir = "parts/";

  std::shared_ptr<ds::DatasetFactory> factory;
  ARROW_ASSIGN_OR_RAISE(factory, ds::FileSystemDatasetFactory
    ::Make(std::make_shared<arrow::fs::LocalFileSystem>(),
           std::move(selector),
           std::make_shared<ds::IpcFileFormat>(),
           ds::FileSystemFactoryOptions{}));

  std::shared_ptr<ds::Dataset> dataset;
  ARROW_ASSIGN_OR_RAISE(dataset, factory->Finish());
  ARROW_ASSIGN_OR_RAISE(auto fragments, dataset->GetFragments()->ToVector());

  auto executor = arrow::internal::GetCpuThreadPool();

  size_t nr_parts = fragments.size();
  std::vector<PtHashWorker> workers(nr_parts);

  // Print out the fragments
  for (size_t p = 0; p < nr_parts; ++p) {
    auto fragment_path = fragments[p]->ToString();
    ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::MemoryMappedFile
                          ::Open(fragment_path, arrow::io::FileMode::READ));
    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchFileReader
                          ::Open(file, arrow::ipc::IpcReadOptions{}));

    PtHashWorker *worker = &workers[p];
    ARROW_CHECK_OK(executor->Spawn([=]() {
      ARROW_CHECK_OK(worker->ReadTable(reader));
    }));
  }

  executor->WaitForIdle();

  int64_t max_rows = 0, total_rows = 0;
  for (size_t i = 0; i < fragments.size(); ++i) {
    int64_t nrows = workers[i].table->num_rows();
    total_rows += nrows;
    max_rows = std::max(nrows, max_rows);
    LOG(INFO) << "Found fragment: "
              << fragments[i]->ToString()
              << " rows: " << nrows;
  }

  float config_alpha = 0.98;
  LOG(INFO) << "max fragment:\t" << max_rows;
  LOG(INFO) << "total_rows:\t\t" << total_rows;

  int64_t table_size = total_rows / config_alpha / nr_parts;
  CHECK_GT(table_size, int64_t(max_rows / 0.99));
  for (auto& worker : workers) {
    ARROW_CHECK_OK(executor->Spawn([&]() {
      ARROW_CHECK_OK(worker.Do(table_size, nr_parts));
      worker = PtHashWorker();
    }));
  }

  //ARROW_CHECK_OK(executor->Spawn([&]() {
  //  ARROW_CHECK_OK(workers[0].Do(extended_table_size, nr_parts));
  //}));

  executor->WaitForIdle();
  return arrow::Status::OK();
}

/* Declare the PTHash function. */
typedef pthash::single_phf<
  pthash::hash_128,        // base hasher
  pthash::dictionary_dictionary,  // encoder type
  false                   // minimal
  > pthash128_t;

arrow::Status Query(const po::variables_map& options,
                    const std::vector<std::string> &args)
{
  if (args.size() != 3u)
    throw folly::ProgramExit(1, "3 arguments expected");

  std::shared_ptr<arrow::io::RandomAccessFile> ym_file;
  std::shared_ptr<arrow::ipc::RecordBatchFileReader> ym_reader;

  if (options.count("youmail")) {
    ARROW_ASSIGN_OR_RAISE(ym_file, arrow::io::ReadableFile
                          ::Open(options["youmail"].as<std::string>()));
    ARROW_ASSIGN_OR_RAISE(ym_reader, arrow::ipc::RecordBatchFileReader
                          ::Open(ym_file, arrow::ipc::IpcReadOptions{}));
  }

  ARROW_ASSIGN_OR_RAISE(auto lrn_file, arrow::io::ReadableFile
                        ::Open(args[0]));
  ARROW_ASSIGN_OR_RAISE(auto lrn_reader, arrow::ipc::RecordBatchFileReader
                        ::Open(lrn_file, arrow::ipc::IpcReadOptions{
                          }));

  LOG(INFO) << "reading pthash...";
  pthash128_t phf;
  essentials::load(phf, args[1].c_str());
  LOG(INFO) << "pthash space: " << phf.table_size();

  for (unsigned i = 2; i < args.size(); ++i) {
    uint64_t query = atoll(args[i].c_str());
    uint64_t row_index = phf(query);
    uint64_t chunk = row_index / LRN_ROWS_PER_CHUNK;
    uint64_t pos = row_index % LRN_ROWS_PER_CHUNK;

    ARROW_ASSIGN_OR_RAISE(auto lrn, lrn_reader->ReadRecordBatch(chunk));
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

        ARROW_ASSIGN_OR_RAISE(auto ym, ym_reader->ReadRecordBatch(ychunk));
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

  return arrow::Status::OK();
}

arrow::Status Metadata(const po::variables_map& options,
                       const std::vector<std::string> &args)
{
  ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::ReadableFile
                        ::Open(args[0]));
  ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchFileReader
                        ::Open(file, arrow::ipc::IpcReadOptions{}));

  std::cout << "#batches: " << reader->num_record_batches() << "\n\n";
  std::cout << reader->schema()->ToString() << std::endl;
  std::cout << reader->metadata()->ToString() << std::endl;
  return arrow::Status::OK();
}

template <class Entrypoint>
folly::NestedCommandLineApp::Command ArrowCommand(const Entrypoint &entrypoint)
{
  return [&](const po::variables_map& options, const std::vector<std::string> &args)
  {
    arrow::Status st = entrypoint(options, args);
    if (!st.ok())
      throw folly::ProgramExit(1, st.message());
  };
}

static int64_t RUN_LIMIT = INT64_MAX;

template <class CommandType>
struct CommandRegistar {
  void operator()(const po::variables_map& kwargs,
                  const std::vector<std::string>& args)
  {
    status = CmdOps<CommandType>::StoreArgs(kwargs, args, options);
    if (!status.ok())
      throw folly::ProgramExit(1, status.message());

    auto command = CommandType::Make();

    status = command->Init(options);
    if (!status.ok())
      throw folly::ProgramExit(1, std::string("Init: ") + status.message());

    status = command->Run(RUN_LIMIT);
    if (!status.ok()) {
      ARROW_WARN_NOT_OK(command->Finish(true), "Finish");
      throw folly::ProgramExit(2, std::string("Run: ") + status.message());
    }

    status = command->Finish(false);
    if (!status.ok())
      throw folly::ProgramExit(2, std::string("Finish: ") + status.message());
  }

  void Register(folly::NestedCommandLineApp& app) {
    CmdOps<CommandType>::BindOptions(
        app.addCommand(
            CommandType::description.name,
            CommandType::description.args,
            CommandType::description.abstract,
            CommandType::description.help,
            std::ref(*this)),
        options);
  }

  static CommandRegistar<CommandType> instance;
  typename CommandType::Options options;
  arrow::Status status;
};

template<> CommandRegistar<CmdConvert>   CommandRegistar<CmdConvert>::instance{};
template<> CommandRegistar<CmdPartition> CommandRegistar<CmdPartition>::instance{};

int main(int argc, const char* argv[]) {
  using namespace std::placeholders;

  setlocale(LC_ALL, "C");
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  FLAGS_logtostderr = 1;

  RegisterCustomFunctions(arrow::compute::GetFunctionRegistry());
  arrow::dataset::internal::Initialize();

  folly::NestedCommandLineApp app{argv[0], "0.9", "", "", nullptr};
  app.addGFlags(folly::ProgramOptionsStyle::GNU);
  app.globalOptions().add_options()
      ("limit,l", po::value(&RUN_LIMIT)->default_value(INT64_MAX));

  CommandRegistar<CmdConvert>::instance.Register(app);
  CommandRegistar<CmdPartition>::instance.Register(app);

  auto& pthash_pn_cmd = app.addCommand(
      "pthash-build", "arrow_table_path",
      "Build PTHash index for PN dataset",
      "",
      ArrowCommand(BuildPTHash));

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

  auto& query_cmd = app.addCommand(
      "query", "lrn_table_path pthash key",
      "Query PN data for number",
      "",
      ArrowCommand(Query));

  query_cmd.add_options()
    ("youmail",
     po::value<std::string>(),
     "YouMail database path");

  // invert-rn rn_pn.arrow > rn0.arrow
  // verify
  //

  app.addCommand(
    "metadata", "arrow_table_apth",
    "Print table metadata",
    "",
    ArrowCommand(Metadata));

  return app.run(argc, argv);
}
