#ifndef CALLFWD_STORE_CSV_RECORDS_H
#define CALLFWD_STORE_CSV_RECORDS_H

#include <cstdint>

struct LRNRow {
  uint64_t pn;
  uint64_t rn;
};

struct DNORow {
  uint64_t pn;
  uint16_t type;
};

struct DNCRow {
  uint64_t pn;
};

struct YouMailRow {
  uint64_t pn;
  float spam_score;
  float fraud_prob;/* optional */
  float unlawful_prob;
  float tcpa_fraud_prob;
};

struct PnRecord {
  LRNRow     lrn;
  DNCRow     dnc;
  DNORow     dno;
  YouMailRow youmail;
};

#endif // CALLFWD_STORE_CSV_RECORDS_H
