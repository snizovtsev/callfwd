#ifndef CALLFWD_PN_ORDERED_JOIN_H_
#define CALLFWD_PN_ORDERED_JOIN_H_

#include "lrn_schema.hpp"

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
  uint32_t pn_rows_per_batch;
  uint32_t ym_rows_per_batch;
  uint32_t rn_rows_per_batch;
  /* FTC path */
};

struct PnOrderedJoin
#ifdef CALLFWD_PN_ORDERED_JOIN_INTERNAL_H_
    : private PnOrderedJoinPriv
#endif
{
  static std::unique_ptr<PnOrderedJoin> Make(
      arrow::MemoryPool* memory_pool = nullptr);
  static std::shared_ptr<arrow::Schema> pn_schema();
  static std::shared_ptr<arrow::Schema> ym_schema();
  static std::shared_ptr<arrow::Schema> rn_schema();

  arrow::Status Reset(const PnOrderedJoinOptions &options);
  arrow::Status Drain(uint32_t limit = UINT32_MAX);
  arrow::Status FlushRnData();
};

#endif // CALLFWD_PN_ORDERED_JOIN_H_
