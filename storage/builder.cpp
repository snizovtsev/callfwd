#include "lrn_schema.hpp"
#include "pthash/pthash.hpp"
#include "cmd_convert.hpp"
#include "cmd_partition.hpp"
#include "cmd_pthash.hpp"
#include "pthash_bucketer.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/thread_pool.h>
#include <glog/logging.h>

#include <folly/experimental/NestedCommandLineApp.h>
#include <folly/Likely.h>

#include <algorithm>
#include <functional>
#include <iostream>
#include <memory>
#include <cstdint>
#include <cstdlib>

namespace po = boost::program_options;

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
  if (auto metadata = reader->metadata())
    std::cout << metadata->ToString() << std::endl;
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

template<> CommandRegistar<CmdConvert> CommandRegistar<CmdConvert>::instance{};
template<> CommandRegistar<CmdPartition> CommandRegistar<CmdPartition>::instance{};
template<> CommandRegistar<CmdPtHash> CommandRegistar<CmdPtHash>::instance{};

int main(int argc, const char* argv[]) {
  using namespace std::placeholders;

  setlocale(LC_ALL, "C");
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  FLAGS_logtostderr = 1;

  folly::NestedCommandLineApp app{argv[0], "0.9", "", "", nullptr};
  app.addGFlags(folly::ProgramOptionsStyle::GNU);
  app.globalOptions().add_options()
      ("limit,l", po::value(&RUN_LIMIT)->default_value(INT64_MAX));

  CommandRegistar<CmdConvert>::instance.Register(app);
  CommandRegistar<CmdPartition>::instance.Register(app);
  CommandRegistar<CmdPtHash>::instance.Register(app);

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
