#ifndef CALLFWD_STORE_CSV_READER_H
#define CALLFWD_STORE_CSV_READER_H

#include "csv_records.hpp"

#include <vector>
#include <cstddef>
#include <cstdio>

class ZsvReader {
 public:
  void Open(const char *csv_path);
  size_t NumRows() noexcept { return num_rows_; }
  void Close() noexcept;
  ~ZsvReader() noexcept;

 protected:
  bool NextRow();

 protected:
  struct zsv_scanner *zsv_ = nullptr;
  FILE *pipe_ = nullptr;
  size_t num_rows_ = 0;
};

struct LRNReader : ZsvReader {
  bool NextRow(LRNRow &row);
};

struct DNCReader : ZsvReader {
  bool NextRow(DNCRow &row);
};

struct DNOReader : ZsvReader {
  bool NextRow(DNORow &row);
};

struct YouMailReader : ZsvReader {
  bool NextRow(YouMailRow &row);
};

struct PnMultiReader {
  std::vector<LRNReader> lrn;
  DNCReader              dnc;
  DNOReader              dno;
  YouMailReader          youmail;
};

struct PnEdgeBuf {
  DNCRow     dnc;
  DNORow     dno;
  YouMailRow youmail;
  LRNRow     lrn[];
};

class PnRecordJoiner {
 public:
  PnRecordJoiner() = default;
  ~PnRecordJoiner();

  void Start(PnMultiReader &reader);
  bool NextRow(PnMultiReader &reader, PnRecord &rec);
  size_t NumRows() noexcept { return num_rows_; }

 private:
  PnEdgeBuf *rowbuf_ = nullptr;
  uint32_t num_rows_ = 0;
};

#endif // CALLFWD_STORE_CSV_READER_H
