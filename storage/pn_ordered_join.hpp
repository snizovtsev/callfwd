#ifndef CALLFWD_PN_ORDERED_JOIN_H_
#define CALLFWD_PN_ORDERED_JOIN_H_

#include <arrow/api.h>

#include <vector>
#include <string>
#include <memory>

namespace folly {
  class NestedCommandLineApp;
}

namespace boost::program_options {
  class options_description;
  class variables_map;
}

enum {
  DATA_VERSION       = 1, /* TODO */
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

struct PnOrderedJoin;

struct PnOrderedJoinOptions {
  void AddCommand(folly::NestedCommandLineApp &app,
                  PnOrderedJoin *command = nullptr);
  void BindToOptions(boost::program_options::options_description& description);
  void Store(const boost::program_options::variables_map& options,
             const std::vector<std::string> &args);

  std::vector<std::string> lrn_data_paths;
  std::string dnc_data_path;
  std::string dno_data_path;
  std::string ym_data_path;
  std::string pn_output_path;
  std::string ym_output_path;
  std::string rn_output_path;
  /* TODO: Converted FTC path */
  /* LRN chunk size */
  /* YM chunk size */
  /* RN chunk size */
};

struct PnOrderedJoin
#ifdef CALLFWD_PN_ORDERED_JOIN_INTERNAL_H_
    : private PnOrderedJoinPriv
#endif
{
  static std::unique_ptr<PnOrderedJoin> Make(
      arrow::MemoryPool* memory_pool = arrow::default_memory_pool());

  std::shared_ptr<arrow::Schema> pn_schema();
  std::shared_ptr<arrow::Schema> ym_schema();
  std::shared_ptr<arrow::Schema> rn_schema();

  arrow::Status Reset(const PnOrderedJoinOptions &options,
                      arrow::MemoryPool* memory_pool);
  arrow::Status Drain(uint32_t limit = UINT32_MAX);
  arrow::Status FlushRnData();
};

#endif // CALFWD_PN_ORDERED_JOIN_H_
