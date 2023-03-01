#include "csv_reader.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include "pthash/pthash.hpp"

#include <glog/logging.h>
#include <folly/experimental/NestedCommandLineApp.h>

#include <memory>
#include <iostream>
#include <cstdlib>

using namespace pthash;
namespace po = ::boost::program_options;

/* Declare the PTHash function. */
typedef single_phf<murmurhash2_128,        // base hasher
                   dictionary_dictionary,  // encoder type
                   false                   // minimal
                   > pthash_type;

struct MapPnOpts {
  explicit MapPnOpts() noexcept = default;
  explicit MapPnOpts(const po::variables_map& options,
                     const std::vector<std::string> &args);

  std::vector<std::string> lrn_data_paths;
  std::string dnc_data_path;
  std::string dno_data_path;
  std::string output_path;
};

MapPnOpts::MapPnOpts(const po::variables_map& options,
                     const std::vector<std::string> &args)
  : lrn_data_paths(args)
  , dnc_data_path(options["dnc"].as<std::string>())
  , dno_data_path(options["dno"].as<std::string>())
  , output_path(options["output"].as<std::string>())
{
  if (lrn_data_paths.empty())
    throw folly::ProgramExit(1, "At least 1 positional argument required.");
}

class MapPnCommand {
 public:
  explicit MapPnCommand();
  arrow::Status main(const MapPnOpts &opts);

 private:
  using TableBuilderPtr
    = std::unique_ptr<arrow::RecordBatchBuilder>;
  using OutputStreamPtr
    = std::shared_ptr<arrow::io::FileOutputStream>;
  using RecordBatchWriterPtr
    = std::shared_ptr<arrow::ipc::RecordBatchWriter>;

  PnMultiReader csv_;
  PnRecordJoiner lrn_joiner_;
  TableBuilderPtr table_builder_;
  OutputStreamPtr ostream_;
  RecordBatchWriterPtr table_writer_;
};

#define LRN_BITFIELD_MASK(shift, width) \
  ((1ull << width) - 1) << shift

enum {
  LRN_RECORD_BATCH_SIZE  = 2036, /* 32kb message */
  LRN_BITFIELD_PN_SHIFT  = 30,
  LRN_BITFIELD_PN_WIDTH  = 34,
  LRN_BITFIELD_PN_MASK   = LRN_BITFIELD_MASK(LRN_BITFIELD_PN_SHIFT, LRN_BITFIELD_PN_WIDTH),
  LRN_BITFIELD_LRN_FLAG  = 1ull << 0,
  LRN_BITFIELD_DNC_FLAG  = 1ull << 1,
  LRN_BITFIELD_DNO_SHIFT = 2,
  LRN_BITFIELD_DNO_WIDTH = 3,
  //LRN_BITFIELD_DNO_MASK  = ,
};

MapPnCommand::MapPnCommand()
  : table_builder_{
      arrow::RecordBatchBuilder::Make(arrow::schema({
        arrow::field("pn", arrow::uint64()),
        arrow::field("rn", arrow::uint64()),
      }), arrow::default_memory_pool(), LRN_RECORD_BATCH_SIZE)
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

  // int64_t rb = 0, sb = 0, tb = 0;

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

    if (((lrn_joiner_.NumRows() + 1) % LRN_RECORD_BATCH_SIZE) == 0) {
      ARROW_ASSIGN_OR_RAISE(record, table_builder_->Flush());
      ARROW_RETURN_NOT_OK(table_writer_->WriteRecordBatch(*record));
      // std::cerr << "raw bytes: " << ipc_writer->stats().total_raw_body_size - rb << std::endl;
      // std::cerr << "ser bytes: " << ipc_writer->stats().total_serialized_body_size - sb << std::endl;
      // std::cerr << "tell: " << outfile->Tell().ValueOrDie() - tb << std::endl;
      // rb = ipc_writer->stats().total_raw_body_size;
      // sb = ipc_writer->stats().total_serialized_body_size;
      // tb = outfile->Tell().ValueOrDie();
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
  ARROW_RETURN_NOT_OK(table_writer_->Close());

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

void BuildPTHash(const po::variables_map& options,
                 const std::vector<std::string> &args)
{
  std::vector<uint64_t> pn_keys;

  pn_keys.reserve(949351049);

  std::cerr << "building pthash..." << std::endl;

  /* Set up a build configuration. */
  build_configuration config;
  config.c = 6.0;
  config.alpha = 0.94;
  config.minimal_output = true;  // mphf
  config.verbose_output = true;

  pthash_type f;

  /* Build the function in internal memory. */
  std::cerr << "building the function..." << std::endl;
  auto timings = f.build_in_internal_memory(pn_keys.begin(), pn_keys.size(), config);
  double total_seconds = timings.partitioning_seconds + timings.mapping_ordering_seconds +
                         timings.searching_seconds + timings.encoding_seconds;
  std::cerr << "computed: " << total_seconds << " seconds" << std::endl;
  /* Compute and print the number of bits spent per key. */
  double bits_per_key = static_cast<double>(f.num_bits()) / f.num_keys();
  std::cerr << "function uses " << bits_per_key << " [bits/key]" << std::endl;

  /* Serialize the data structure to a file. */
  std::cerr << "serializing the function to disk..." << std::endl;
  std::string output_filename("pn.pthash");
  essentials::save(f, output_filename.c_str());
}

void PermuteHash(const po::variables_map& options,
                 const std::vector<std::string> &args)
{
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
    ("dnc", po::value<std::string>()->default_value("/dev/null"),
    "DNC database path (CSV). Optional.\n"
    "Example row:\n  201,0000000")
    ("dno", po::value<std::string>()->default_value("/dev/null"),
    "DNO database path (CSV). Optional.\n"
    "Example row:\n  2012000000,4")
    ("output,O", po::value<std::string>()->required(),
    "Arrow table output path. Required.");

  auto& pthash_cmd = app.addCommand(
    "pthash", "arrow_table_path",
    "Build PTHash function over given set of keys",
    "",
    BuildPTHash);

  pthash_cmd.add_options()
    ("output,O", po::value<std::string>()->required(),
     "Arrow table output path. Required.")
    ("tweak-c,c", po::value<float>()->default_value(6.0),
     "Bucket density coefficient")
    ("tweak-alpha", po::value<float>()->default_value(0.94),
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
  // final: pn.arrow rn.arrow pn.pthash rn.pthash

  return app.run(argc, argv);
}
