#include "csv_reader.hpp"

#include "pthash/pthash.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/compute/api.h>

#include <arrow/ipc/feather.h>
#include <arrow/scalar.h>
#include <arrow/type_fwd.h>
#include <arrow/util/logging.h>
#include <folly/experimental/NestedCommandLineApp.h>
#include <folly/GroupVarint.h>

#include <map>
#include <memory>
#include <iostream>
#include <cstdlib>

namespace po = ::boost::program_options;

enum {
  DATA_VERSION       = 1, /* TODO */
  LRN_ROWS_PER_CHUNK = (256 / 16) * (1 << 10) - 12, /* 256 kb block */
  YM_ROWS_PER_CHUNK  = 256,  /*  */
  RN_ROWS_PER_CHUNK  = 128,  /*  */
};

struct MapPnOpts {
  explicit MapPnOpts() noexcept = default;
  explicit MapPnOpts(const po::variables_map& options,
                     const std::vector<std::string> &args);

  std::vector<std::string> lrn_data_paths;
# define OPT_MAP_PN_DNC     "dnc"
  std::string dnc_data_path;
# define OPT_MAP_PN_DNO     "dno"
  std::string dno_data_path;
# define OPT_MAP_PN_YOUMAIL "youmail"
  std::string youmail_data_path;
# define OPT_MAP_PN_OUTPUT  "output"
  std::string output_path;
# define OPT_MAP_PN_YM_OUTPUT "ym-output"
  std::string ym_output_path;
# define OPT_MAP_PN_RN_OUTPUT "rn-output"
  std::string rn_output_path;
};

MapPnOpts::MapPnOpts(const po::variables_map& options,
                     const std::vector<std::string> &args)
    : lrn_data_paths(args)
    , dnc_data_path(options[OPT_MAP_PN_DNC].as<std::string>())
    , dno_data_path(options[OPT_MAP_PN_DNO].as<std::string>())
    , youmail_data_path(options[OPT_MAP_PN_YOUMAIL].as<std::string>())
    , output_path(options[OPT_MAP_PN_OUTPUT].as<std::string>())
    , ym_output_path(options[OPT_MAP_PN_YM_OUTPUT].as<std::string>())
    , rn_output_path(options[OPT_MAP_PN_RN_OUTPUT].as<std::string>())
{
  if (lrn_data_paths.empty())
    throw folly::ProgramExit(1, "At least 1 positional argument required.");
}

class MapPnCommand {
 public:
  explicit MapPnCommand();
  arrow::Status Main(const MapPnOpts &opts);

 private:
  using TableBuilderPtr =
      std::unique_ptr<arrow::RecordBatchBuilder>;
  using OutputStreamPtr =
      std::shared_ptr<arrow::io::FileOutputStream>;
  using RecordBatchWriterPtr =
      std::shared_ptr<arrow::ipc::RecordBatchWriter>;

  PnMultiReader csv_;
  PnRecordJoiner lrn_joiner_;
  TableBuilderPtr lrn_builder_;
  TableBuilderPtr ym_builder_;
  TableBuilderPtr rn_builder_;
  OutputStreamPtr ostream_;
  OutputStreamPtr ym_ostream_;
  OutputStreamPtr rn_ostream_;
  RecordBatchWriterPtr table_writer_;
  RecordBatchWriterPtr ym_writer_;
  RecordBatchWriterPtr rn_writer_;
};

/*
   PN column (UInt64)
  ┌─────────────┬────────────┬─────────┬─────┬─────┬─────┐
  │  63 ... 30  │  29 ... 6  │ 5 ... 3 │  2  │  1  │  0  │
  ├─────────────┼────────────┼─────────┼─────┴─────┴─────┤
  │   34 bits   │  24 bits   │ 3 bits  │      Flags      │
  ├─────────────┼────────────┼─────────┼─────┬─────┬─────┤
  │ 10-digit PN │ YouMail id │   DNO   │ DNO │ DNC │ LRN │
  └─────────────┴────────────┴─────────┴─────┴─────┴─────┘

   RN column (UInt64)
  ┌─────────────┬────────────┐
  │  63 ... 30  │  29 ... 6  │
  ├─────────────┼────────────┤
  │   34 bits   │  30 bits   │
  ├─────────────┼────────────┤
  │ 10-digit RN │  RN seqnum │
  └─────────────┴────────────┘
 */

#define LRN_BITS_MASK(field)                \
  ((1ull << LRN_BITS_##field##_WIDTH) - 1)  \
  << LRN_BITS_##field##_SHIFT

enum {
  LRN_BITS_LRN_FLAG  = 1ull << 0,
  LRN_BITS_DNC_FLAG  = 1ull << 1,
  LRN_BITS_DNO_FLAG  = 1ull << 2,
  LRN_BITS_FLAG_MASK = (1ull << 3) - 1,
  LRN_BITS_DNO_SHIFT = 3,
  LRN_BITS_DNO_WIDTH = 3,
  LRN_BITS_DNO_MASK  = LRN_BITS_MASK(DNO),
  LRN_BITS_YM_SHIFT  = 6,
  LRN_BITS_YM_WIDTH  = 24,
  LRN_BITS_YM_MASK   = LRN_BITS_MASK(YM),
  LRN_BITS_PN_SHIFT  = 30,
  LRN_BITS_PN_WIDTH  = 34,
  LRN_BITS_PN_MASK   = LRN_BITS_MASK(PN),
};

MapPnCommand::MapPnCommand()
    : lrn_builder_{
        arrow::RecordBatchBuilder::Make(arrow::schema({
          arrow::field("pn_bits", arrow::uint64()),
          arrow::field("rn_bits", arrow::uint64()),
        }), arrow::default_memory_pool(), LRN_ROWS_PER_CHUNK)
        .ValueOrDie()
      }
    , ym_builder_{
        arrow::RecordBatchBuilder::Make(arrow::schema({
          arrow::field("spam_score", arrow::float32()),
          arrow::field("fraud_prob", arrow::float32()),
          arrow::field("unlawful_prob", arrow::float32()),
          arrow::field("tcpa_fraud_prob", arrow::float32()),
        }), arrow::default_memory_pool(), YM_ROWS_PER_CHUNK)
        .ValueOrDie()
      }
    , rn_builder_{
        arrow::RecordBatchBuilder::Make(arrow::schema({
          arrow::field("rn", arrow::uint64()),
          arrow::field("pn_set", arrow::binary()),
        }), arrow::default_memory_pool(), RN_ROWS_PER_CHUNK)
        .ValueOrDie()
      }
{}

struct StringAppender {
  std::string data;
  void operator()(folly::StringPiece sp) {
    data.append(sp.data(), sp.size());
  }
};

class RnEncoder {
 public:
  void Add(uint64_t val) {
    CHECK_GT(val, prev_);
    if (!prev_)
      enc_.add(val);
    else
      enc_.add(val - prev_);
    prev_ = val;
  }

  const std::string& Finish() {
    enc_.finish();
    return enc_.output().data;
  }

 private:
  using GroupVarintEncoder = folly::GroupVarintEncoder<uint64_t, StringAppender>;
  GroupVarintEncoder enc_{StringAppender{}};
  uint64_t prev_ = 0;
};

arrow::Status MapPnCommand::Main(const MapPnOpts &options) {
  PnRecord rec;
  std::shared_ptr<arrow::RecordBatch> record;

  auto& pn_bits_builder = *lrn_builder_->GetFieldAs<arrow::UInt64Builder>(0);
  auto& rn_bits_builder = *lrn_builder_->GetFieldAs<arrow::UInt64Builder>(1);
  auto& spam_score = *ym_builder_->GetFieldAs<arrow::FloatBuilder>(0);
  auto& fraud_prob = *ym_builder_->GetFieldAs<arrow::FloatBuilder>(1);
  auto& unlawful_prob = *ym_builder_->GetFieldAs<arrow::FloatBuilder>(2);
  auto& tcpa_fraud_prob = *ym_builder_->GetFieldAs<arrow::FloatBuilder>(3);
  auto& rn_builder = *rn_builder_->GetFieldAs<arrow::UInt64Builder>(0);
  auto& pn_set_builder = *rn_builder_->GetFieldAs<arrow::BinaryBuilder>(1);

  csv_.lrn.clear();
  csv_.lrn.reserve(options.lrn_data_paths.size());
  for (const std::string &lrn_path : options.lrn_data_paths) {
    auto &parser = csv_.lrn.emplace_back();
    parser.Open(lrn_path.c_str());
  }
  csv_.dnc.Open(options.dnc_data_path.c_str());
  csv_.dno.Open(options.dno_data_path.c_str());
  csv_.youmail.Open(options.youmail_data_path.c_str());

  ARROW_ASSIGN_OR_RAISE(ostream_,
                        arrow::io::FileOutputStream::Open(options.output_path));
  ARROW_ASSIGN_OR_RAISE(table_writer_,
                        arrow::ipc::MakeFileWriter(ostream_, lrn_builder_->schema()));

  ARROW_ASSIGN_OR_RAISE(ym_ostream_,
                        arrow::io::FileOutputStream::Open(options.ym_output_path));
  ARROW_ASSIGN_OR_RAISE(ym_writer_,
                        arrow::ipc::MakeFileWriter(ym_ostream_, ym_builder_->schema()));

  ARROW_ASSIGN_OR_RAISE(rn_ostream_,
                        arrow::io::FileOutputStream::Open(options.rn_output_path));
  ARROW_ASSIGN_OR_RAISE(rn_writer_,
                        arrow::ipc::MakeFileWriter(rn_ostream_, rn_builder_->schema()));

  lrn_joiner_.Start(csv_);

  uint32_t ym_id = 0;
  int64_t tb = 0, ytb = 0, rtb = 0;
  std::map<uint64_t, RnEncoder> rn2pn;

  for (; lrn_joiner_.NextRow(csv_, rec); ) {
    uint64_t pn = rec.lrn.pn | rec.dnc.pn | rec.dno.pn | rec.youmail.pn;
    uint64_t pn_bits = pn << LRN_BITS_PN_SHIFT;
    uint64_t rn_bits = rec.lrn.rn << LRN_BITS_PN_SHIFT;

    if (rec.dnc.pn) /* present in DNC */
      pn_bits |= LRN_BITS_DNC_FLAG;

    if (rec.dno.pn) {/* encode DNO type */
      pn_bits |= (rec.dno.type - 1) << LRN_BITS_DNO_SHIFT;
      pn_bits |= LRN_BITS_DNO_FLAG;
    }

    if (rec.lrn.pn) { /* present in LRN */
      pn_bits |= LRN_BITS_LRN_FLAG;
      rn2pn[rec.lrn.rn].Add(pn);
    }

    if (rec.youmail.pn) { /* encode YouMail id */
      pn_bits |= ym_id++ << LRN_BITS_YM_SHIFT;
      ARROW_RETURN_NOT_OK(spam_score.Append(rec.youmail.spam_score));
      ARROW_RETURN_NOT_OK(fraud_prob.Append(rec.youmail.fraud_prob));
      ARROW_RETURN_NOT_OK(unlawful_prob.Append(rec.youmail.unlawful_prob));
      ARROW_RETURN_NOT_OK(tcpa_fraud_prob.Append(rec.youmail.tcpa_fraud_prob));
    } else {
      pn_bits |= LRN_BITS_YM_MASK;
    }

    ARROW_RETURN_NOT_OK(pn_bits_builder.Append(pn_bits));
    ARROW_RETURN_NOT_OK(rn_bits_builder.Append(rn_bits));

    if (pn_bits_builder.length() == LRN_ROWS_PER_CHUNK) {
      ARROW_ASSIGN_OR_RAISE(record, lrn_builder_->Flush());
      ARROW_RETURN_NOT_OK(table_writer_->WriteRecordBatch(*record));

      if (lrn_joiner_.NumRows() <= 10 * LRN_ROWS_PER_CHUNK) {
        LOG(INFO) << "tell: " << ostream_->Tell().ValueOrDie() - tb << std::endl;
        CHECK(ostream_->Tell().Value(&tb).ok());
      }
    }

    if (spam_score.length() == YM_ROWS_PER_CHUNK) {
      ARROW_ASSIGN_OR_RAISE(record, ym_builder_->Flush());
      ARROW_RETURN_NOT_OK(ym_writer_->WriteRecordBatch(*record));

      if (ym_id <= 10 * YM_ROWS_PER_CHUNK) {
        LOG(INFO) << "ytell: " << ym_ostream_->Tell().ValueOrDie() - ytb << std::endl;
        CHECK(ym_ostream_->Tell().Value(&ytb).ok());
      }
    }
  }

  for (auto &lrn : csv_.lrn)
    lrn.Close();
  csv_.dnc.Close();
  csv_.dno.Close();
  csv_.youmail.Close();

  for (auto &lrn : csv_.lrn)
    LOG(INFO) << "#lrn_rows: " << lrn.NumRows(); // TODO: to metadata
  LOG(INFO) << "#dnc_rows: " << csv_.dnc.NumRows();
  LOG(INFO) << "#dno_rows: " << csv_.dno.NumRows();
  LOG(INFO) << "#ym_rows: " << csv_.youmail.NumRows();

  ARROW_ASSIGN_OR_RAISE(record, lrn_builder_->Flush(true));
  ARROW_RETURN_NOT_OK(table_writer_->WriteRecordBatch(*record));
  tb = ostream_->Tell().ValueOrDie();
  ARROW_RETURN_NOT_OK(table_writer_->Close());
  LOG(INFO) << "Arrow footer bytes: "
            << ostream_->Tell().ValueOrDie() - tb << std::endl;

  ARROW_ASSIGN_OR_RAISE(record, ym_builder_->Flush(true));
  ARROW_RETURN_NOT_OK(ym_writer_->WriteRecordBatch(*record));
  ytb = ym_ostream_->Tell().ValueOrDie();
  ARROW_RETURN_NOT_OK(ym_writer_->Close());
  LOG(INFO) << "YM Arrow footer bytes: "
            << ym_ostream_->Tell().ValueOrDie() - ytb << std::endl;

  LOG(INFO) << "Writing RN table";
  for (auto &kv : rn2pn) {
    uint64_t rn = kv.first;
    const std::string &pn_set = kv.second.Finish();
    ARROW_RETURN_NOT_OK(rn_builder.Append(rn));
    ARROW_RETURN_NOT_OK(pn_set_builder.Append(pn_set));

    if (rn_builder.length() == RN_ROWS_PER_CHUNK) {
      ARROW_ASSIGN_OR_RAISE(record, rn_builder_->Flush());
      ARROW_RETURN_NOT_OK(rn_writer_->WriteRecordBatch(*record));
      LOG_EVERY_N(INFO, 10) << "rtell: " << rn_ostream_->Tell().ValueOrDie() - rtb << std::endl;
      CHECK(rn_ostream_->Tell().Value(&rtb).ok());
    }
  }

  rn2pn.clear();

  ARROW_ASSIGN_OR_RAISE(record, rn_builder_->Flush(true));
  ARROW_RETURN_NOT_OK(rn_writer_->WriteRecordBatch(*record));
  rtb = rn_ostream_->Tell().ValueOrDie();
  ARROW_RETURN_NOT_OK(rn_writer_->Close());
  LOG(INFO) << "RN Arrow footer bytes: "
            << rn_ostream_->Tell().ValueOrDie() - rtb << std::endl;

  return arrow::Status::OK();
}

void MapPN(const po::variables_map& optmap,
           const std::vector<std::string> &args)
{
  MapPnOpts options{optmap, args};

  arrow::Status st = MapPnCommand().Main(options);
  if (!st.ok())
    throw folly::ProgramExit(1, st.message());
}


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

/* Declare the PTHash function. */
typedef pthash::single_phf<
  pthash::hash_128,        // base hasher
  pthash::dictionary_dictionary,  // encoder type
  false                   // minimal
  > pthash128_t;

namespace cp = arrow::compute;

arrow::Status SkewBucketerExec(cp::KernelContext* ctx, const cp::ExecSpan& batch,
                               cp::ExecResult* out)
{
  const uint64_t* pn = batch[0].array.GetValues<uint64_t>(1);

  std::unique_ptr<arrow::ArrayBuilder> builder;
  RETURN_NOT_OK(arrow::MakeBuilder(ctx->memory_pool(),
                                   out->array_data()->type,
                                   &builder));
  RETURN_NOT_OK(builder->Reserve(batch.length));

  auto struct_ = dynamic_cast<arrow::StructBuilder*>(builder.get());
  auto bucket = dynamic_cast<arrow::UInt32Builder*>(builder->child(0));
  auto hashval = dynamic_cast<arrow::UInt64Builder*>(builder->child(1));

  RETURN_NOT_OK(bucket->Reserve(batch.length));
  RETURN_NOT_OK(hashval->Reserve(batch.length));

  constexpr uint64_t num_keys = 1115007295;
  constexpr uint64_t num_buckets =
    std::ceil((6.0 * num_keys) / std::log2(num_keys));

  for (int64_t i = 0; i < batch.length; ++i) {
    //auto hv = murmur3_64(pn[i], 123456);
    // RETURN_NOT_OK(bucket->Append(hv.first % num_buckets));
    // RETURN_NOT_OK(hashval->Append(hv.second));
    RETURN_NOT_OK(struct_->Append());
  }

  RETURN_NOT_OK(builder->FinishInternal(&std::get<std::shared_ptr<arrow::ArrayData>>(out->value)));
  return arrow::Status::OK();
}

void RegisterCustomFunctions(cp::FunctionRegistry* registry);

void BuildPTHash(const po::variables_map& options,
                 const std::vector<std::string> &args)
{
  auto func = std::make_shared<cp::ScalarFunction>("pthash_skew_bucketer",
                                                   cp::Arity::Unary(),
                                                   cp::FunctionDoc::Empty());
  auto out_type = arrow::struct_({
    arrow::field("bucket", arrow::uint32()),
    arrow::field("hash", arrow::uint64()),
  });
  cp::ScalarKernel kernel({arrow::uint64()}, out_type, SkewBucketerExec /*,init*/);
  kernel.mem_allocation = cp::MemAllocation::NO_PREALLOCATE;
  kernel.null_handling = cp::NullHandling::OUTPUT_NOT_NULL;
  kernel.parallelizable = true;
  CHECK(func->AddKernel(std::move(kernel)).ok());

  auto registry = cp::GetFunctionRegistry();
  RegisterCustomFunctions(registry);
  CHECK(registry->AddFunction(std::move(func)).ok());

  std::vector<uint64_t> pn_keys;
  {
    CHECK_EQ(args.size(), 1u);

    auto istream = arrow::io::MemoryMappedFile
        ::Open(args[0], arrow::io::FileMode::READ)
        .ValueOrDie();

    // auto reader = arrow::ipc::RecordBatchFileReader
    //     ::Open(istream, arrow::ipc::IpcReadOptions{
    //         .included_fields = {0},
    //       })
    //     .ValueOrDie();

    auto reader = arrow::ipc::feather::Reader
      ::Open(istream)
      .ValueOrDie();

    std::shared_ptr<arrow::Table> lrn_table;
    CHECK(reader->Read(&lrn_table).ok());

    // construct an ExecPlan first to hold your nodes
    std::shared_ptr<arrow::Table> output_table;
    std::shared_ptr<cp::ExecPlan> plan = cp::ExecPlan
      ::Make(cp::threaded_exec_context())
      .ValueOrDie();

    LOG(INFO) << "use_threads: " << cp::default_exec_context()->use_threads();
    if (cp::default_exec_context()->executor())
      LOG(INFO) << "threads: " << cp::default_exec_context()->executor()->GetCapacity();

    cp::Declaration::Sequence({
          {"table_source", cp::TableSourceNodeOptions{lrn_table, /*max_batch_size*/}},
          {"project", cp::ProjectNodeOptions{
            {cp::call("shift_right", {
                cp::field_ref("pn_bits"),
                cp::literal(UINT64_C(30))})},
            {"pn"}
          }},
          {"aggregate", cp::AggregateNodeOptions{
            /*aggregates=*/ {{"x_bucket_counter", nullptr, "pn", "out"}},
          }},
          // {"project", cp::ProjectNodeOptions{
          //   {cp::call("pthash_skew_bucketer", {cp::field_ref("pn")})},
          //   {"pthash_in"}
          // }},
          // {"project", cp::ProjectNodeOptions{
          //   {cp::call("add", {
          //       cp::field_ref({0, 0}),
          //       cp::literal<uint32_t>(0u)}),
          //    cp::call("add", {
          //       cp::field_ref({0, 1}),
          //       cp::literal<uint64_t>(0u)})},
          //   {"bucket", "hash"}
          // }},
          //{"aggregate", cp::AggregateNodeOptions{
          //  /*aggregates=*/ {{"hash_list", nullptr, "hash", "out"}},
          //  /*keys=*/ {"bucket"}
          //}},
          {"table_sink", cp::TableSinkNodeOptions{&output_table}}
        })
      .AddToPlan(plan.get())
      .ValueOrDie();

    LOG(INFO) << "Validate plan";
    CHECK(plan->Validate().ok());

    LOG(INFO) << "Start plan";
    CHECK(plan->StartProducing().ok());

    LOG(INFO) << "Waiting...";

    plan->finished().result().ValueOrDie();

    LOG(INFO) << "Finished plan";

    std::cout << "#old batches " << lrn_table->column(0)->num_chunks() << std::endl;
    std::cout << "#new batches " << output_table->column(0)->num_chunks() << std::endl;

    std::cout << output_table->ToString() << std::endl;

    return;

    arrow::compute::ExecContext exec_ctx;
    exec_ctx.set_preallocate_contiguous(false);
    exec_ctx.set_exec_chunksize(LRN_ROWS_PER_CHUNK);

    auto keys = arrow::compute::CallFunction("shift_right",
                                             {lrn_table->column(0),
                                              arrow::MakeScalar<uint64_t>(30)},
                                             &exec_ctx)
      .ValueOrDie().chunked_array();

    auto result = arrow::compute::CallFunction("pthash_skew_bucketer",
                                               {keys},
                                               &exec_ctx)
      .ValueOrDie().chunked_array();

    std::cout << lrn_table->column(0)->num_chunks() << std::endl;
    std::cout << result->num_chunks() << std::endl;
    //std::cout << result->ToString() << std::endl;

    // pn_keys.reserve(file_reader->num_record_batches() * LRN_ROWS_PER_CHUNK);

    // auto batch_iter = file_reader->GetRecordBatchGenerator()
    //     .ValueOrDie();

    // LOG(INFO) << "decoding keys...";

    // for (int i = 0; i < file_reader->num_record_batches(); ++i) {
    //   auto pn_column = batch_iter().result().ValueOrDie()->column(0);
    //   auto pn_arr = std::dynamic_pointer_cast<arrow::UInt64Array>(pn_column);

    //   for (std::optional<uint64_t> pn_bits : *pn_arr) {
    //     uint64_t pn = *pn_bits >> LRN_BITS_PN_SHIFT;
    //     pn_keys.push_back(pn);
    //   }
    // }
  }

  std::string output_path = options[OPT_PTHASH_OUTPUT]
      .as<std::string>();

  /* Set up a build configuration. */
  pthash::build_configuration config;
  config.c = options[OPT_PTHASH_C].as<float>();
  config.alpha = options[OPT_PTHASH_ALPHA].as<float>();
  config.minimal_output = pthash128_t::minimal;  // mphf
  config.verbose_output = true;
  config.num_threads = 8;
  // config.num_partitions = 8;

  pthash128_t f;

  /* Build the function in internal memory. */
  LOG(INFO) << "building the function...";

  //pthash::internal_memory_builder_single_phf<pthash::hash_128> builder;
  pthash::internal_memory_builder_single_phf<pthash::hash_128> builder;
  builder.build_from_keys(pn_keys.begin(), pn_keys.size(), config);
  LOG(INFO) << "done";
  return;
  // for (int attempt = 0; attempt < 10; ++attempt) {
  //   builder.m_seed = pthash::random_value();
  //   try {
  //     builder.build_from_hashes(hash_generator<RandomAccessIterator>(keys, m_seed),
  //                               pn_keys.size(), config);
  //     break;
  //   } catch (seed_runtime_error const& error) {
  //     LOG(WARN) << "attempt " << attempt + 1 << " failed";
  //   }
  //   if (attempt == 9)
  //     throw pthash::seed_runtime_error();
  // }
  f.build(builder, config);

  /* Compute and print the number of bits spent per key. */
  double bits_per_key = static_cast<double>(f.num_bits()) / f.num_keys();
  LOG(INFO) << "DONE! function uses " << bits_per_key << " [bits/key]";

  /* Serialize the data structure to a file. */
  LOG(INFO) << "serializing the function to disk...";
  essentials::save(f, output_path.c_str());
}

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

  // TODO: new table may have more chunks if pthash isn't minimal
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
        CHECK(target_pn.AppendValues(zeroes).ok());
        CHECK(target_rn.AppendValues(zeroes).ok());
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
  CHECK(table_writer->WriteTable(*lrn_table).ok());
  CHECK(table_writer->Close().ok());
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
    auto pn_column = std::static_pointer_cast<arrow::UInt64Array>(lrn->column(0));
    auto rn_column = std::static_pointer_cast<arrow::UInt64Array>(lrn->column(1));

    uint64_t pn_bits = pn_column->Value(pos);
    uint64_t rn_bits = rn_column->Value(pos);
    uint64_t ym_id = pn_bits & LRN_BITS_YM_MASK;
    uint64_t pn = pn_bits >> LRN_BITS_PN_SHIFT;
    uint64_t dno_raw = (pn_bits & LRN_BITS_DNO_MASK) >> LRN_BITS_DNO_SHIFT;

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
    if (pn_bits & LRN_BITS_DNO_FLAG)
      std::cout << "dno: " << dno_raw + 1 << ' ';

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
  setlocale(LC_ALL, "C");
  google::InstallFailureSignalHandler();

  folly::NestedCommandLineApp app{argv[0], "0.9", "", "", nullptr};
  app.addGFlags(folly::ProgramOptionsStyle::GNU);
  FLAGS_logtostderr = 1;

  auto& map_pn_cmd = app.addCommand(
      "map-pn", "lrn_csv_path [additional_lrn_path...]",
      "Convert PN data from multiple sources into arrow format",
      "Read multiple CSV data sources and convert them\n"
      " to a single arrow table.\n\n"
      "Glossary:\nPN, RN - 10-digit numbers (34 bits)\n"
      "LRN: an ordered adjective mapping from PN to RN\n"
      "DNC: an ordered set of PN numbers (PN concatenated from 2 rows)\n"
      "DNO: an ordered mapping from PN to 1..8\n", MapPN);

  map_pn_cmd.add_options()
      (OPT_MAP_PN_DNC,
       po::value<std::string>()->default_value("/dev/null"),
       "DNC database path (CSV). Optional.\n"
       "Example row:\n  201,0000000")
      (OPT_MAP_PN_DNO,
       po::value<std::string>()->default_value("/dev/null"),
       "DNO database path (CSV). Optional.\n"
       "Example row:\n  2012000000,4")
      (OPT_MAP_PN_YOUMAIL,
       po::value<std::string>()->default_value("/dev/null"),
       "YouMail database path (CSV). Optional.\n"
       "Example row:\n  +12032614649,ALMOST_CERTAINLY,0.95,0.95,0.95")
      (OPT_MAP_PN_OUTPUT ",O",
       po::value<std::string>()->required(),
       "Arrow PN table output path. Required.")
      (OPT_MAP_PN_YM_OUTPUT ",Y",
       po::value<std::string>()->default_value("/dev/null"),
       "Arrow table output path. Required if YouMail present.")
      (OPT_MAP_PN_RN_OUTPUT ",R",
       po::value<std::string>()->required(),
       "Arrow RN table output path. Required.");

  auto& pthash_pn_cmd = app.addCommand(
      "pthash-pn", "arrow_table_path",
      "Build PTHash index for PN dataset",
      "",
      BuildPTHash);

  pthash_pn_cmd.add_options()
      (OPT_PTHASH_OUTPUT ",O",
       po::value<std::string>()->required(),
       "Arrow table output path. Required.")
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
