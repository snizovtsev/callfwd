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

// XXX: TO METADATA
constexpr size_t PN_CHUNK_SIZE = 2036; /* 32kb message */

using namespace pthash;
namespace po = ::boost::program_options;

/* Declare the PTHash function. */
typedef single_phf<murmurhash2_128,        // base hasher
                   dictionary_dictionary,  // encoder type
                   false                   // minimal
                   >
    pthash_type;

arrow::Status MapPN(const po::variables_map& options) {
    std::shared_ptr<arrow::Schema> schema;

    arrow::UInt64Builder *pnBuilder;
    arrow::UInt64Builder *rnBuilder;

    schema = arrow::schema({
        arrow::field("pn", arrow::uint64()),
        arrow::field("rn", arrow::uint64()),
    });

    ARROW_ASSIGN_OR_RAISE(auto builder,
        arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool()));

    pnBuilder = builder->GetFieldAs<arrow::UInt64Builder>(0);
    rnBuilder = builder->GetFieldAs<arrow::UInt64Builder>(1);

    PnJoinReader reader;
    PnRecord rec;

    reader.lrn.Open(options["lrn-data"].as<std::string>().c_str());
    reader.dnc.Open(options["dnc-data"].as<std::string>().c_str());
    reader.dno.Open(options["dno-data"].as<std::string>().c_str());

    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("pn.arrow"));
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ipc::RecordBatchWriter> ipc_writer,
                          arrow::ipc::MakeFileWriter(outfile, schema));

    // int64_t rb = 0, sb = 0, tb = 0;

    for (; reader.NextRow(rec); ) {
        uint64_t pn = rec.lrn.pn | rec.dnc.pn | rec.dno.pn;
        uint64_t pn_bits = pn << 30; /* high 34 bits */
        uint64_t rn_bits = rec.lrn.rn << 30;

        if (rec.lrn.pn)
            pn_bits |= 1u << 0; /* 1b: present in LRN */

        if (rec.dnc.pn)
            pn_bits |= 2u << 1; /* 1b: present in DNC */

        if (rec.dno.pn)
            pn_bits |= rec.dno.type << 2; /* 3b: encode DNO type */

        ARROW_RETURN_NOT_OK(pnBuilder->Append(pn_bits));
        ARROW_RETURN_NOT_OK(rnBuilder->Append(rn_bits));

        //if ((reader.NumRows() & 0x7ff) == 0x7ff) {
        if (((reader.NumRows() + 1) % PN_CHUNK_SIZE) == 0) {
            std::shared_ptr<arrow::RecordBatch> record;
            ARROW_ASSIGN_OR_RAISE(record, builder->Flush());
            ARROW_RETURN_NOT_OK(ipc_writer->WriteRecordBatch(*record));
            // std::cerr << "raw bytes: " << ipc_writer->stats().total_raw_body_size - rb << std::endl;
            // std::cerr << "ser bytes: " << ipc_writer->stats().total_serialized_body_size - sb << std::endl;
            // std::cerr << "tell: " << outfile->Tell().ValueOrDie() - tb << std::endl;
            // rb = ipc_writer->stats().total_raw_body_size;
            // sb = ipc_writer->stats().total_serialized_body_size;
            // tb = outfile->Tell().ValueOrDie();
        }
    }
    reader.Close();

    printf("#lrn_rows: %zu\n", reader.lrn.NumRows()); // TODO: to metadata
    printf("#dnc_rows: %zu\n", reader.dnc.NumRows());
    printf("#dno_rows: %zu\n", reader.dno.NumRows());

    std::shared_ptr<arrow::RecordBatch> record;
    ARROW_ASSIGN_OR_RAISE(record, builder->Flush());
    ARROW_RETURN_NOT_OK(ipc_writer->WriteRecordBatch(*record));
    ARROW_RETURN_NOT_OK(ipc_writer->Close());

    return arrow::Status::OK();
}

arrow::Status BuildPTHash(const po::variables_map& options) {
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

    return arrow::Status::OK();
}

arrow::Status Permute(const po::variables_map& options) {
    return arrow::Status::OK();
}

template<class Entrypoint>
folly::NestedCommandLineApp::Command ArrowCommand(const Entrypoint &entrypoint)
{
    return [&](const po::variables_map& options,
               const std::vector<std::string> &args)
    {
        arrow::Status st = entrypoint(options);
        if (!st.ok())
            throw folly::ProgramExit(1, st.message());
    };
}

int main(int argc, const char* argv[]) {
    folly::NestedCommandLineApp app{"LRN DB builder", "0.9", "", "", nullptr};

    app.addGFlags(folly::ProgramOptionsStyle::GNU);
    app.addCommand("map-pn", "",
                   "Pack PN data into binary format",
                   "Join multiple PN-ascending tables into single one",
                   ArrowCommand(MapPN));

    po::options_description optdesc;

    // map-pn *.csv > pn_sorted.arrow, rn_pn.arrow
    // pthash --pn pn_sorted.arrow > pn.pthash
    // permute pn_sorted.arrow pn.pthash > pn.arrow
    // reduce-rn rn_pn.arrow > rn0.arrow
    // map-rn rn0.arrow >
    // pthash --pn rn.arrow
    // permute rn.arrow rn.pthash
    // final: pn.arrow rn.arrow pn.pthash rn.pthash
    optdesc.add_options()
        ("pthash-c,c",
         po::value<double>()->default_value(6.0),
         "PTHash bucket density")
        ("pthash-alpha",
         po::value<double>()->default_value(0.94),
         "PTHash domain density")
        ("lrn-data","LRN database path (CSV)")
        ("calrn-data","CALRN database path (CSV)")
        ("dnc-data", "DNC database path (CSV)")
        ("dno-data", "DNO database path (CSV)")
        ("help,h", "Show this message");

    po::variables_map options;
    auto result = folly::parseNestedCommandLine(argc, argv, optdesc);
    po::store(result.options, options);
    po::notify(options);

    if (options.count("help")) {
        std::cout << optdesc;
        return 1;
    }

    return 0;

}
