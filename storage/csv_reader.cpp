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

#define likely GOOGLE_PREDICT_TRUE
#define unlikely GOOGLE_PREDICT_FALSE

void ZsvReader::Open(const char *csv_path) {
  struct zsv_opts opts;

  pipe_ = std::fopen(csv_path, "r");
  CHECK(pipe_ && !std::ferror(pipe_));

  std::memset(&opts, 0, sizeof(opts));
  opts.stream = pipe_;
  num_rows_ = 0;

  zsv_ = zsv_new(&opts);
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
}

ZsvReader::~ZsvReader() noexcept {
  Close();
}

bool ZsvReader::NextRow() {
  if (std::feof(pipe_))
    return false;
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
  CHECK_LE(row.type, 8u);

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

  ++num_rows_;
  return true;
}
