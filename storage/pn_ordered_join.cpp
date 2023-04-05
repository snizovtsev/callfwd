#include "pn_ordered_join_internal.hpp"
#include "pn_ordered_join.hpp"

#include <folly/experimental/NestedCommandLineApp.h>
#include <arrow/util/logging.h>

#include <memory>

namespace po = boost::program_options;
using Status = arrow::Status;

std::shared_ptr<arrow::Schema> PnOrderedJoin::pn_schema() {
  static std::shared_ptr<arrow::Schema> schema = arrow::schema({
      arrow::field("pn_bits", arrow::uint64()),
      arrow::field("rn_bits", arrow::uint64()),
    });
  return schema;
}

std::shared_ptr<arrow::Schema> PnOrderedJoin::ym_schema() {
  static std::shared_ptr<arrow::Schema> schema = arrow::schema({
      arrow::field("spam_score", arrow::float32()),
      arrow::field("fraud_prob", arrow::float32()),
      arrow::field("unlawful_prob", arrow::float32()),
      arrow::field("tcpa_fraud_prob", arrow::float32()),
    });
  return schema;
}

std::shared_ptr<arrow::Schema> PnOrderedJoin::rn_schema() {
  static std::shared_ptr<arrow::Schema> schema = arrow::schema({
      arrow::field("rn", arrow::uint64()),
      arrow::field("pn_set", arrow::binary()),
    });
  return schema;
}

Status PnOrderedJoin::Reset(const PnOrderedJoinOptions &options,
                            arrow::MemoryPool* memory_pool)
{
  reader.lrn.clear();
  reader.lrn.reserve(options.lrn_data_paths.size());
  for (const std::string &lrn_path : options.lrn_data_paths) {
    LRNReader &parser = reader.lrn.emplace_back();
    parser.Open(lrn_path.c_str());
  }
  reader.dnc.Open(options.dnc_data_path.c_str());
  reader.dno.Open(options.dno_data_path.c_str());
  reader.youmail.Open(options.ym_data_path.c_str());

  /* Populates first row by each reader. */
  joiner.Start(reader);

  ARROW_RETURN_NOT_OK(
      pn_writer.Reset(pn_schema(), options.pn_output_path,
                      memory_pool, LRN_ROWS_PER_CHUNK));
  ARROW_RETURN_NOT_OK(
      ym_writer.Reset(ym_schema(), options.ym_output_path,
                      memory_pool, YM_ROWS_PER_CHUNK));
  ARROW_RETURN_NOT_OK(
      rn_writer.Reset(rn_schema(), options.rn_output_path,
                      memory_pool, RN_ROWS_PER_CHUNK));

  return Status::OK();
}

Status RegularTableWriter::Reset(const std::shared_ptr<arrow::Schema>& schema,
                                 const std::string& file_path,
                                 arrow::MemoryPool* memory_pool,
                                 int64_t rows_per_batch)
{
  RegularTableWriter draft;

  draft.file_path = file_path;
  ARROW_ASSIGN_OR_RAISE(draft.builder, arrow::RecordBatchBuilder::Make(
      schema, memory_pool, /*initial_capacity=*/ rows_per_batch));
  ARROW_ASSIGN_OR_RAISE(draft.ostream, arrow::io::FileOutputStream::Open(
      file_path));

  arrow::ipc::IpcWriteOptions options;
  options.memory_pool = memory_pool;
  ARROW_ASSIGN_OR_RAISE(draft.writer, arrow::ipc::MakeFileWriter(
      draft.ostream, schema, options /*,metadata*/));

  *this = std::move(draft);
  return Status::OK();
}

Status PnOrderedJoin::Drain(uint32_t limit) {
  auto& pn_bits_builder = *pn_writer.GetFieldAs<arrow::UInt64Builder>(0);
  auto& rn_bits_builder = *pn_writer.GetFieldAs<arrow::UInt64Builder>(1);
  auto& spam_score = *ym_writer.GetFieldAs<arrow::FloatBuilder>(0);
  auto& fraud_prob = *ym_writer.GetFieldAs<arrow::FloatBuilder>(1);
  auto& unlawful_prob = *ym_writer.GetFieldAs<arrow::FloatBuilder>(2);
  auto& tcpa_fraud_prob = *ym_writer.GetFieldAs<arrow::FloatBuilder>(3);

  for (; joiner.NextRow(reader, row) && limit; --limit) {
    uint64_t pn = row.lrn.pn | row.dnc.pn | row.dno.pn | row.youmail.pn;
    uint64_t pn_bits = pn << LRN_BITS_PN_SHIFT;
    uint64_t rn_bits = 0;

    if (row.dnc.pn) { /* present in DNC */
      pn_bits |= LRN_BITS_DNC_FLAG;
    }

    if (row.dno.pn) { /* encode DNO type */
      pn_bits |= row.dno.type << LRN_BITS_DNO_SHIFT;
    }

    if (row.lrn.pn) { /* present in LRN */
      pn_bits |= LRN_BITS_LRN_FLAG;
      rn_bits |= row.lrn.rn << LRN_BITS_PN_SHIFT;
      rn_data[row.lrn.rn].Add(pn);
    }

    if (row.youmail.pn) { /* encode YouMail handle */
      pn_bits |= youmail_row_index++ << LRN_BITS_YM_SHIFT;
      ARROW_RETURN_NOT_OK(spam_score.Append(row.youmail.spam_score));
      ARROW_RETURN_NOT_OK(fraud_prob.Append(row.youmail.fraud_prob));
      ARROW_RETURN_NOT_OK(unlawful_prob.Append(row.youmail.unlawful_prob));
      ARROW_RETURN_NOT_OK(tcpa_fraud_prob.Append(row.youmail.tcpa_fraud_prob));
      ARROW_RETURN_NOT_OK(ym_writer.Advance());
    } else {
      pn_bits |= LRN_BITS_YM_MASK;
    }

    ARROW_RETURN_NOT_OK(pn_bits_builder.Append(pn_bits));
    ARROW_RETURN_NOT_OK(rn_bits_builder.Append(rn_bits));
    ARROW_RETURN_NOT_OK(pn_writer.Advance());
    // TODO: not ok -> finish -> return
  }

  for (auto &lrn : reader.lrn)
    LOG(INFO) << "#lrn_rows: " << lrn.NumRows(); // TODO: to metadata
  LOG(INFO) << "#dnc_rows: " << reader.dnc.NumRows();
  LOG(INFO) << "#dno_rows: " << reader.dno.NumRows();
  LOG(INFO) << "#ym_rows: " << reader.youmail.NumRows();

  for (auto &lrn : reader.lrn)
    lrn.Close();
  reader.dnc.Close();
  reader.dno.Close();
  reader.youmail.Close();

  // TODO: not ok -> finish others -> return
  ARROW_RETURN_NOT_OK(pn_writer.Finish());
  ARROW_RETURN_NOT_OK(ym_writer.Finish());

  return arrow::Status::OK();
}

Status RegularTableWriter::Advance() {
  arrow::ArrayBuilder *column = builder->GetField(0);
  if (ARROW_PREDICT_TRUE(column->capacity() > column->length()))
    return arrow::Status::OK();

  ARROW_ASSIGN_OR_RAISE(auto batch, builder->Flush());
  ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));

  if (ARROW_PREDICT_FALSE(writer->stats().num_record_batches == 1)) {
    header_bytes = ostream->Tell().ValueOr(0);
  }

  if (ARROW_PREDICT_FALSE(writer->stats().num_record_batches == 2)) {
    record_bytes = ostream->Tell().ValueOr(0) - header_bytes;
    header_bytes -= record_bytes;
    LOG(INFO) << file_path << " layout: "
              << header_bytes << " byte header, "
              << record_bytes << " byte records";
  }

  static constexpr int64_t k100mb = 100 * (1 << 20);
  int64_t data_written = writer->stats().total_serialized_body_size;
  if (ARROW_PREDICT_FALSE(data_written > last_report_bytes + k100mb)) {
    last_report_bytes = data_written;
    LOG(INFO) << file_path << ": "
        << data_written / k100mb << "00 MiB written";
  }

  return Status::OK();
}

Status RegularTableWriter::Finish() {
  ARROW_ASSIGN_OR_RAISE(auto batch, builder->Flush(true));
  ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  LOG(INFO) << "Arrow footer bytes: " << ostream->Tell().ValueOr(-1);
  ARROW_RETURN_NOT_OK(writer->Close());
  LOG(INFO) << "Arrow footer bytes: " << ostream->Tell().ValueOr(-1);
  return arrow::Status::OK();
}

Status PnOrderedJoin::FlushRnData() {
  auto& rn_builder = *rn_writer.GetFieldAs<arrow::UInt64Builder>(0);
  auto& pn_set_builder = *rn_writer.GetFieldAs<arrow::BinaryBuilder>(1);

  LOG(INFO) << "Writing RN table";
  for (auto &kv : rn_data) {
    uint64_t rn = kv.first;
    MonotonicVarintSequenceEncoder &encoder = kv.second;
    const std::string &pn_set = encoder.Finish();

    ARROW_RETURN_NOT_OK(rn_builder.Append(rn));
    ARROW_RETURN_NOT_OK(pn_set_builder.Append(pn_set));
    ARROW_RETURN_NOT_OK(rn_writer.Advance());
  }

  rn_data.clear();
  ARROW_RETURN_NOT_OK(rn_writer.Finish());

  return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////
//

static void Main(PnOrderedJoinOptions& options,
                 const po::variables_map& vm,
                 const std::vector<std::string> &args,
                 PnOrderedJoin& command)
{
  arrow::Status st;

  options.Store(vm, args);
  st = command.Reset(options, arrow::default_memory_pool());

  if (!st.ok())
    throw folly::ProgramExit(1, "error: " + st.message());

  st = command.Drain();
  if (!st.ok())
    throw folly::ProgramExit(2, "error: " + st.message());

  st = command.FlushRnData();
  if (!st.ok())
    throw folly::ProgramExit(2, "error: " + st.message());
}

void PnOrderedJoinOptions::AddCommand(folly::NestedCommandLineApp &app,
                                      PnOrderedJoin *command)
{
  using namespace std::placeholders;
  static PnOrderedJoin default_command;

  if (!command)
    command = &default_command;

  po::options_description& options = app.addCommand(
      "pn-ordered-join", "lrn_csv_path [additional_lrn_path...]",
      "Convert PN data from multiple sources into arrow format",
      "Read multiple CSV data sources and convert them\n"
      " to a single arrow table.\n\n"
      "Glossary:\nPN, RN - 10-digit numbers (34 bits)\n"
      "LRN: an ordered adjective mapping from PN to RN\n"
      "DNC: an ordered set of PN numbers (PN concatenated from 2 rows)\n"
      "DNO: an ordered mapping from PN to 1..8\n",
      std::bind(&Main, std::ref(*this), _1, _2, std::ref(*command)));
  BindToOptions(options);
}

void PnOrderedJoinOptions::BindToOptions(po::options_description& description) {
  static const std::string devnull = "/dev/null";
  description.add_options()
      ("dnc",
       po::value(&dnc_data_path)->default_value(devnull),
       "DNC database path (CSV). Optional.\n"
       "Example row:\n  201,0000000")
      ("dno",
       po::value(&dno_data_path)->default_value(devnull),
       "DNO database path (CSV). Optional.\n"
       "Example row:\n  2012000000,4")
      ("youmail",
       po::value(&ym_data_path)->default_value(devnull),
       "YouMail database path (CSV). Optional.\n"
       "Example row:\n  +12032614649,ALMOST_CERTAINLY,0.95,0.95,0.95")
      ("output,O",
       po::value(&pn_output_path)->required(),
       "Arrow PN table output path. Required.")
      ("ym-output,Y",
       po::value(&ym_output_path)->default_value(devnull),
       "Arrow table output path. Required if YouMail present.")
      ("rn-output,R",
       po::value(&rn_output_path)->required(),
       "Arrow RN table output path. Required.");
}

void PnOrderedJoinOptions::Store(const po::variables_map& vm,
                                 const std::vector<std::string> &args)
{
  lrn_data_paths = args;
  if (lrn_data_paths.empty())
    throw folly::ProgramExit(1, "At least 1 positional argument required.");
}
