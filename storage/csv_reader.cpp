#include "csv_reader.hpp"
#include "csv_records.hpp"

#if !defined(restrict)
#define restrict __restrict
#endif

extern "C" {
#include <zsv.h>
}

#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <cstdint>
#include <cstdlib>
#include <cmath>

#define likely GOOGLE_PREDICT_TRUE
#define unlikely GOOGLE_PREDICT_FALSE

void ZsvReader::Open(const char *csv_path) {
  Close();

  pipe_ = std::fopen(csv_path, "r");
  CHECK(pipe_ && !std::ferror(pipe_));

  struct zsv_opts opts;
  std::memset(&opts, 0, sizeof(opts));
  opts.stream = pipe_;

  zsv_ = zsv_new(&opts);
  // zsv_set_context(zsv_, this);
  CHECK(zsv_);
}

void ZsvReader::Close() noexcept {
  if (zsv_) {
    zsv_delete(zsv_);
    zsv_ = nullptr;
  }

  if (pipe_) {
    std::fclose(pipe_);
    pipe_ = nullptr;
  }

  num_rows_ = 0;
}

ZsvReader::~ZsvReader() noexcept {
  Close();
}

bool ZsvReader::NextRow() {
  if (unlikely(zsv_next_row(zsv_) != zsv_status_row))
    return false;
  ++num_rows_;
  return true;
}

bool LRNReader::NextRow(LRNRow &row) {
  if (!ZsvReader::NextRow()) {
    row.pn = UINT64_MAX;
    row.rn = 0;
    return false;
  }

  CHECK_EQ(zsv_cell_count(zsv_), 2u);

  struct zsv_cell cell = zsv_get_cell(zsv_, 0);
  char *end, *val = reinterpret_cast<char*>(cell.str);
  row.pn = strtoull(val, &end, 10);
  CHECK(end == val + cell.len);

  cell = zsv_get_cell(zsv_, 1);
  val = reinterpret_cast<char*>(cell.str);
  row.rn = strtoull(val, &end, 10);
  CHECK(end == val + cell.len);

  return true;
}

bool DNCReader::NextRow(DNCRow &row) {
  if (!ZsvReader::NextRow()) {
    row.pn = UINT64_MAX;
    return false;
  }

  CHECK_EQ(zsv_cell_count(zsv_), 2u);
  struct zsv_cell cell = zsv_get_cell(zsv_, 0);
  char *end, *val = reinterpret_cast<char*>(cell.str);
  row.pn = strtoul(val, &end, 10);
  row.pn *= 10000000;
  CHECK(end == val + cell.len);

  cell = zsv_get_cell(zsv_, 1);
  val = reinterpret_cast<char*>(cell.str);
  row.pn += strtoul(val, &end, 10);
  CHECK(end == val + cell.len);

  return true;
}

bool DNOReader::NextRow(DNORow &row) {
  if (!ZsvReader::NextRow()) {
    row.pn = UINT64_MAX;
    row.type = 0;
    return false;
  }

  CHECK_EQ(zsv_cell_count(zsv_), 2u);
  struct zsv_cell cell = zsv_get_cell(zsv_, 0);
  char *end, *val = reinterpret_cast<char*>(cell.str);
  row.pn = strtoull(val, &end, 10);
  CHECK(end == val + cell.len);

  cell = zsv_get_cell(zsv_, 1);
  val = reinterpret_cast<char*>(cell.str);
  row.type = strtoul(val, &end, 10);
  CHECK(end == val + cell.len);
  CHECK_GE(row.type, 1u);
  CHECK_LE(row.type, 9u);

  return true;
}

/* A concatenated string of constants found in YouMail columns. */
static const char* kFraudProbabilityAtoms =
  "0:DEFINITELY_NOT:"
  "1:ALMOST_CERTAINLY_NOT:"
  "2:PROBABLY_NOT:"
  "3:MAYBE_NOT:"
  "4:UNKNOWN:"
  "5:MAYBE:"
  "6:PROBABLY:"
  "7:ALMOST_CERTAINLY:"
  "8:DEFINITELY:";

/* Values corresponding to named constants. */
static const float kFraudProbabilityValues[9] =
  {-1.0, -0.8, -0.6, -0.4, 0.0, 0.4, 0.6, 0.8, 1.0};

static float ParseYouMailProbability(const char *token, uint32_t len) {
  std::string safe_token;
  const char *pos;

  if (len == 0)
    return std::nanf("");

  safe_token.reserve(len + 2);
  safe_token.append(":");
  safe_token.append(token, len);
  safe_token.append(":");

  pos = std::strstr(kFraudProbabilityAtoms,
                    std::move(safe_token).c_str());

  if (pos) {
    int index = pos[-1] - '0';
    return kFraudProbabilityValues[index];
  }

  char *end = nullptr;
  float ret = strtof(token, &end);
  CHECK(end == token + len);

  return ret;
}

bool YouMailReader::NextRow(YouMailRow &row) {
  if (!ZsvReader::NextRow()) {
    row.pn = UINT64_MAX;
    return false;
  }

  CHECK_EQ(zsv_cell_count(zsv_), 5u);

  struct zsv_cell cell = zsv_get_cell(zsv_, 0);
  CHECK(cell.len == 10 || cell.len == 11 || cell.len == 12);
  char *end, *val = reinterpret_cast<char*>(cell.str);
  if (cell.len == 11) {
    CHECK_EQ(*val, '1');
    val += 1;
    cell.len -= 1;
  } else if (cell.len == 12) {
    CHECK_EQ(val[0], '+');
    CHECK_EQ(val[1], '1');
    val += 2;
    cell.len -= 2;
  }
  row.pn = strtoull(val, &end, 10);
  CHECK(end == val + cell.len);

  cell = zsv_get_cell(zsv_, 1);
  val = reinterpret_cast<char*>(cell.str);
  row.spam_score = ParseYouMailProbability(val, cell.len);

  cell = zsv_get_cell(zsv_, 2);
  val = reinterpret_cast<char*>(cell.str);
  row.fraud_prob = ParseYouMailProbability(val, cell.len);

  cell = zsv_get_cell(zsv_, 3);
  val = reinterpret_cast<char*>(cell.str);
  row.unlawful_prob = ParseYouMailProbability(val, cell.len);

  cell = zsv_get_cell(zsv_, 4);
  val = reinterpret_cast<char*>(cell.str);
  row.tcpa_fraud_prob = ParseYouMailProbability(val, cell.len);

  return true;
}

void PnRecordJoiner::Start(PnMultiReader &reader) {
  num_rows_ = 0;
  free(rowbuf_);
  rowbuf_ = reinterpret_cast<PnEdgeBuf*>(
    calloc(sizeof(PnEdgeBuf) + reader.lrn.size() * sizeof(LRNRow), 1));
  CHECK(rowbuf_);

  for (size_t i = 0; i < reader.lrn.size(); ++i) {
    reader.lrn[i].NextRow(rowbuf_->lrn[i]);
  }
  reader.dnc.NextRow(rowbuf_->dnc);
  reader.dno.NextRow(rowbuf_->dno);
  reader.youmail.NextRow(rowbuf_->youmail);
}

PnRecordJoiner::~PnRecordJoiner() {
  free(rowbuf_);
}

bool PnRecordJoiner::NextRow(PnMultiReader &reader, PnRecord &rec) {
  // every row in buf are ready
  uint64_t next_pn = std::min({
    rowbuf_->dnc.pn,
    rowbuf_->dno.pn,
    rowbuf_->youmail.pn,
  });

  for (size_t i = 0; i < reader.lrn.size(); ++i)
    next_pn = std::min(next_pn, rowbuf_->lrn[i].pn);

  std::memset(&rec, 0, sizeof(rec));
  if (next_pn == UINT64_MAX)
    return false;

  CHECK_GT(next_pn, 0u);
  CHECK_LT(next_pn, 1ull << 34);

  for (size_t i = 0; i < reader.lrn.size(); ++i) {
    if (rowbuf_->lrn[i].pn == next_pn) {
      rec.lrn = rowbuf_->lrn[i];
      reader.lrn[i].NextRow(rowbuf_->lrn[i]);
      CHECK_GT(rowbuf_->lrn[i].pn, next_pn);
      break;
    }
  }

  if (rowbuf_->dnc.pn == next_pn) {
    rec.dnc = rowbuf_->dnc;
    reader.dnc.NextRow(rowbuf_->dnc);
    CHECK_GT(rowbuf_->dnc.pn, next_pn);
  }

  if (rowbuf_->dno.pn == next_pn) {
    rec.dno = rowbuf_->dno;
    reader.dno.NextRow(rowbuf_->dno);
    CHECK_GT(rowbuf_->dno.pn, next_pn);
  }

  if (rowbuf_->youmail.pn == next_pn) {
    rec.youmail = rowbuf_->youmail;
    reader.youmail.NextRow(rowbuf_->youmail);
    CHECK_GT(rowbuf_->youmail.pn, next_pn);
  }

  ++num_rows_;
  return true;
}

PnMultiReader::PnMultiReader() = default;
PnMultiReader::~PnMultiReader() = default;

void PnMultiReader::Close() {
  for (auto &lrn_item : lrn)
    lrn_item.Close();
  dnc.Close();
  dno.Close();
  youmail.Close();
}
