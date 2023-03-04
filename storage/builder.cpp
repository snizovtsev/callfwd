#include "csv_reader.hpp"

#include "pthash/pthash.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <glog/logging.h>
#include <folly/experimental/NestedCommandLineApp.h>

#include <memory>
#include <iostream>
#include <cstdlib>

using namespace pthash;
namespace po = ::boost::program_options;

struct MapPnOpts {
  explicit MapPnOpts() noexcept = default;
  explicit MapPnOpts(const po::variables_map& options,
                     const std::vector<std::string> &args);

  std::vector<std::string> lrn_data_paths;
# define OPT_MAP_PN_DNC    "dnc"
  std::string dnc_data_path;
# define OPT_MAP_PN_DNO    "dno"
  std::string dno_data_path;
# define OPT_MAP_PN_OUTPUT "output"
  std::string output_path;
};

MapPnOpts::MapPnOpts(const po::variables_map& options,
                     const std::vector<std::string> &args)
    : lrn_data_paths(args)
    , dnc_data_path(options[OPT_MAP_PN_DNC].as<std::string>())
    , dno_data_path(options[OPT_MAP_PN_DNO].as<std::string>())
    , output_path(options[OPT_MAP_PN_OUTPUT].as<std::string>())
{
  if (lrn_data_paths.empty())
    throw folly::ProgramExit(1, "At least 1 positional argument required.");
}

class MapPnCommand {
 public:
  explicit MapPnCommand();
  arrow::Status main(const MapPnOpts &opts);

 private:
  using TableBuilderPtr =
      std::unique_ptr<arrow::RecordBatchBuilder>;
  using OutputStreamPtr =
      std::shared_ptr<arrow::io::FileOutputStream>;
  using RecordBatchWriterPtr =
      std::shared_ptr<arrow::ipc::RecordBatchWriter>;

  PnMultiReader csv_;
  PnRecordJoiner lrn_joiner_;
  TableBuilderPtr table_builder_;
  OutputStreamPtr ostream_;
  RecordBatchWriterPtr table_writer_;
};

#define LRN_BITFIELD_MASK(field)                \
  ((1ull << LRN_BITFIELD_##field##_WIDTH) - 1)  \
  << LRN_BITFIELD_##field##_SHIFT

enum {
  LRN_ROWS_PER_CHUNK     = 2036, /* 32kb message */
  LRN_BITFIELD_PN_SHIFT  = 30,
  LRN_BITFIELD_PN_WIDTH  = 34,
  LRN_BITFIELD_PN_MASK   = LRN_BITFIELD_MASK(PN),
  LRN_BITFIELD_LRN_FLAG  = 1ull << 0,
  LRN_BITFIELD_DNC_FLAG  = 1ull << 1,
  LRN_BITFIELD_DNO_SHIFT = 2,
  LRN_BITFIELD_DNO_WIDTH = 3,
  LRN_BITFIELD_DNO_MASK  = LRN_BITFIELD_MASK(DNO),
};

MapPnCommand::MapPnCommand()
    : table_builder_{
        arrow::RecordBatchBuilder::Make(arrow::schema({
        arrow::field("pn", arrow::uint64()),
        arrow::field("rn", arrow::uint64()),
      }), arrow::default_memory_pool(), LRN_ROWS_PER_CHUNK)
        .ValueOrDie()
      }
{}

arrow::Status MapPnCommand::main(const MapPnOpts &options) {
  PnRecord rec;
  std::shared_ptr<arrow::RecordBatch> record;
  auto *pnBuilder = table_builder_->GetFieldAs<arrow::UInt64Builder>(0);
  auto *rnBuilder = table_builder_->GetFieldAs<arrow::UInt64Builder>(1);

  csv_.lrn.clear();
  csv_.lrn.reserve(options.lrn_data_paths.size());
  for (const std::string &lrn_path : options.lrn_data_paths) {
    auto &parser = csv_.lrn.emplace_back();
    parser.Open(lrn_path.c_str());
  }
  csv_.dnc.Open(options.dnc_data_path.c_str());
  csv_.dno.Open(options.dno_data_path.c_str());

  ARROW_ASSIGN_OR_RAISE(ostream_,
                        arrow::io::FileOutputStream::Open(options.output_path));
  ARROW_ASSIGN_OR_RAISE(table_writer_,
                        arrow::ipc::MakeFileWriter(ostream_, table_builder_->schema()));

  lrn_joiner_.Start(csv_);

  int64_t tb = 0;

  for (; lrn_joiner_.NextRow(csv_, rec); ) {
    uint64_t pn = rec.lrn.pn | rec.dnc.pn | rec.dno.pn;
    uint64_t pn_bits = pn << LRN_BITFIELD_PN_SHIFT;
    uint64_t rn_bits = rec.lrn.rn << LRN_BITFIELD_PN_SHIFT;

    if (rec.lrn.pn) /* present in LRN */
      pn_bits |= LRN_BITFIELD_LRN_FLAG;

    if (rec.dnc.pn) /* present in DNC */
      pn_bits |= LRN_BITFIELD_DNC_FLAG;

    if (rec.dno.pn) /* encode DNO type */
      pn_bits |= rec.dno.type << LRN_BITFIELD_DNO_SHIFT;

    ARROW_RETURN_NOT_OK(pnBuilder->Append(pn_bits));
    ARROW_RETURN_NOT_OK(rnBuilder->Append(rn_bits));

    if ((lrn_joiner_.NumRows() % LRN_ROWS_PER_CHUNK) == 0) {
      ARROW_ASSIGN_OR_RAISE(record, table_builder_->Flush());
      ARROW_RETURN_NOT_OK(table_writer_->WriteRecordBatch(*record));

      if (lrn_joiner_.NumRows() <= 20 * LRN_ROWS_PER_CHUNK) {
        std::cerr << "tell: " << ostream_->Tell().ValueOrDie() - tb << std::endl;
        ostream_->Tell().Value(&tb).Warn();
      }
    }
  }

  for (auto &lrn : csv_.lrn)
    lrn.Close();
  csv_.dnc.Close();
  csv_.dno.Close();

  for (auto &lrn : csv_.lrn)
    printf("#lrn_rows: %zu\n", lrn.NumRows()); // TODO: to metadata
  printf("#dnc_rows: %zu\n", csv_.dnc.NumRows());
  printf("#dno_rows: %zu\n", csv_.dno.NumRows());

  ARROW_ASSIGN_OR_RAISE(record, table_builder_->Flush(true));
  ARROW_RETURN_NOT_OK(table_writer_->WriteRecordBatch(*record));
  tb = ostream_->Tell().ValueOrDie();
  ARROW_RETURN_NOT_OK(table_writer_->Close());
  std::cerr << "footer: " << ostream_->Tell().ValueOrDie() - tb << std::endl;

  return arrow::Status::OK();
}

void MapPN(const po::variables_map& optmap,
           const std::vector<std::string> &args)
{
  MapPnOpts options{optmap, args};

  arrow::Status st = MapPnCommand().main(options);
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
typedef single_phf<murmurhash2_128,        // base hasher
                   dictionary_dictionary,  // encoder type
                   true                    // minimal
                   > pthash_type;

void BuildPTHash(const po::variables_map& options,
                 const std::vector<std::string> &args)
{
  std::vector<uint64_t> pn_keys;
  {
    CHECK_EQ(args.size(), 1u);

    auto istream = arrow::io::MemoryMappedFile
        ::Open(args[0], arrow::io::FileMode::READ)
        .ValueOrDie();

    auto file_reader = arrow::ipc::RecordBatchFileReader
        ::Open(istream, arrow::ipc::IpcReadOptions{
            .included_fields = {0},
          })
        .ValueOrDie();

    pn_keys.reserve(file_reader->num_record_batches() * LRN_ROWS_PER_CHUNK);

    auto batch_iter = file_reader->GetRecordBatchGenerator()
        .ValueOrDie();

    LOG(INFO) << "decoding keys...";

    for (int i = 0; i < file_reader->num_record_batches(); ++i) {
      auto pn_column = batch_iter().result().ValueOrDie()->column(0);
      auto pn_arr = std::dynamic_pointer_cast<arrow::UInt64Array>(pn_column);

      for (std::optional<uint64_t> pn_bits : *pn_arr) {
        uint64_t pn = *pn_bits >> LRN_BITFIELD_PN_SHIFT;
        pn_keys.push_back(pn);
      }
    }
  }

  std::string output_path = options[OPT_PTHASH_OUTPUT]
      .as<std::string>();

  /* Set up a build configuration. */
  build_configuration config;
  config.c = options[OPT_PTHASH_C].as<float>();
  config.alpha = options[OPT_PTHASH_ALPHA].as<float>();
  config.minimal_output = true;  // mphf
  config.verbose_output = true;

  pthash_type f;

  /* Build the function in internal memory. */
  LOG(INFO) << "building the function...";

  pthash::internal_memory_builder_single_phf<murmurhash2_128> builder;
  builder.build_from_keys(pn_keys.begin(), pn_keys.size(), config);
  f.build(builder, config);

  // auto timings = f.build_in_internal_memory(pn_keys.begin(), pn_keys.size(), config);
  // double total_seconds = timings.partitioning_seconds + timings.mapping_ordering_seconds +
  //     timings.searching_seconds + timings.encoding_seconds;
  // LOG(INFO) << "pt computed: " << total_seconds << " seconds";

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
  CHECK_EQ(args.size(), 2u);

  LOG(INFO) << "reading pthash...";
  pthash_type f;
  essentials::load(f, args[1].c_str());

  auto iostream = arrow::io::MemoryMappedFile
    ::Open(args[0], arrow::io::FileMode::READ)
    .ValueOrDie();

  std::shared_ptr<arrow::Table> lrn_table;
  {
    auto table_reader = arrow::ipc::feather::Reader
      ::Open(iostream, arrow::ipc::IpcReadOptions{})
      .ValueOrDie();

    LOG(INFO) << "reading table...";
    CHECK(table_reader->Read(&lrn_table).ok());
  }

  LOG(INFO) << "prepare...";
  uint32_t num_swaps = 0;
  int64_t num_rows = lrn_table->num_rows();
  auto pn_column = lrn_table->column(0);
  auto rn_column = lrn_table->column(1);
  std::vector<uint64_t*> pn_chunks(pn_column->num_chunks());
  std::vector<uint64_t*> rn_chunks(rn_column->num_chunks());
  CHECK(pn_chunks.size() == rn_chunks.size());

  for (uint32_t ci = 0; ci < pn_chunks.size(); ++ci) {
    auto pn_chunk = std::static_pointer_cast<arrow::UInt64Array>(pn_column->chunk(ci));
    pn_chunks[ci] = const_cast<uint64_t*>(pn_chunk->raw_values());
    auto rn_chunk = std::static_pointer_cast<arrow::UInt64Array>(rn_column->chunk(ci));
    rn_chunks[ci] = const_cast<uint64_t*>(rn_chunk->raw_values());
    CHECK(pn_chunk->length() == rn_chunk->length());
    if (ci + 1 != pn_chunks.size())
      CHECK(pn_chunk->length() == LRN_ROWS_PER_CHUNK);
  }

  LOG(INFO) << "permute...";
  for (int64_t row = 0; row < num_rows;) {
    int64_t ci = row / LRN_ROWS_PER_CHUNK;
    int64_t ri = row % LRN_ROWS_PER_CHUNK;
    uint64_t pn = pn_chunks[ci][ri] >> LRN_BITFIELD_PN_SHIFT;
    int64_t hrow = f(pn);
    if (hrow != row) {
      int64_t hci = hrow / LRN_ROWS_PER_CHUNK;
      int64_t hri = hrow % LRN_ROWS_PER_CHUNK;
      std::swap(pn_chunks[ci][ri], pn_chunks[hci][hri]);
      std::swap(rn_chunks[ci][ri], rn_chunks[hci][hri]);
      ++num_swaps;
      if ((num_swaps & 0xfffff) == 0xfffff)
        LOG(INFO) << "#swaps: " << num_swaps << "/" << num_rows;
    } else {
      ++row;
    }
  }

  LOG(INFO) << "#swaps: " << num_swaps << "/" << num_rows;
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
      (OPT_MAP_PN_OUTPUT ",O",
       po::value<std::string>()->required(),
       "Arrow table output path. Required.");

  auto& pthash_cmd = app.addCommand(
      "pthash", "arrow_table_path",
      "Build PTHash function over given set of keys",
      "",
      BuildPTHash);

  pthash_cmd.add_options()
      (OPT_PTHASH_OUTPUT ",O",
       po::value<std::string>()->required(),
       "Arrow table output path. Required.")
      (OPT_PTHASH_C ",c",
       po::value<float>()->default_value(6.0),
       "Bucket density coefficient")
      (OPT_PTHASH_ALPHA ",a",
       po::value<float>()->default_value(0.94),
       "Value domain density coefficient");

  auto permute_pn_cmd = app.addCommand(
      "permute-hash", "arrow_table_path pthash_path",
      "Rearrange rows by pthash value",
      "",
      PermuteHash);

  // invert-rn rn_pn.arrow > rn0.arrow
  // map-rn rn0.arrow >
  // pthash --pn rn.arrow
  // permute rn.arrow rn.pthash
  // query-pn pn.arrow 2012....
  // final: pn.arrow rn.arrow pn.pthash rn.pthash

  return app.run(argc, argv);
}
