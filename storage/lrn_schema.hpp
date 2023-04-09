#ifndef CALLFWD_LRN_SCHEMA_H_
#define CALLFWD_LRN_SCHEMA_H_

enum {
  LRN_FORMAT = 1,
  LRN_ROWS_PER_CHUNK = (256 / 16) * (1 << 10) - 12, /* 256 kb block */
  YM_ROWS_PER_CHUNK  = 256,  /*  */
  RN_ROWS_PER_CHUNK  = 128,  /*  */
};

/*
   PN column (UInt64)
  ┌─────────────┬────────────┬─────────┬─────┬─────┐
  │  63 ... 30  │  29 ... 6  │ 5 ... 2 │  1  │  0  │
  ├─────────────┼────────────┼─────────┼─────┴─────┤
  │   34 bits   │  24 bits   │ 4 bits  │   Flags   │
  ├─────────────┼────────────┼─────────┼─────┬─────┤
  │ 10-digit PN │ YouMail id │   DNO   │ DNC │ LRN │
  └─────────────┴────────────┴─────────┴─────┴─────┘

   RN column (UInt64)
  ┌─────────────┬───────────┐
  │  63 ... 30  │  29 ... 6 │
  ├─────────────┼───────────┤
  │   34 bits   │  30 bits  │
  ├─────────────┼───────────┤
  │ 10-digit RN │ RN seqnum │
  └─────────────┴───────────┘
 */

#define LRN_BITS_MASK(field)                \
  ((1ull << LRN_BITS_##field##_WIDTH) - 1)  \
  << LRN_BITS_##field##_SHIFT

enum {
  LRN_BITS_LRN_FLAG  = 1ull << 0,
  LRN_BITS_DNC_FLAG  = 1ull << 1,
  LRN_BITS_DNO_SHIFT = 2,
  LRN_BITS_DNO_WIDTH = 4,
  LRN_BITS_DNO_MASK  = LRN_BITS_MASK(DNO),
  LRN_BITS_YM_SHIFT  = 6,
  LRN_BITS_YM_WIDTH  = 24,
  LRN_BITS_YM_MASK   = LRN_BITS_MASK(YM),
  LRN_BITS_PN_SHIFT  = 30,
  LRN_BITS_PN_WIDTH  = 34,
  LRN_BITS_PN_MASK   = LRN_BITS_MASK(PN),
};

#endif // CALLFWD_LRN_SCHEMA_H_
