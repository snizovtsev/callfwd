#ifndef CALLFWD_CMD_CONVERT_H_
#define CALLFWD_CMD_CONVERT_H_

#include "cmd.hpp"
#include "lrn_schema.hpp"

#include <arrow/type_fwd.h>

#include <vector>
#include <string>
#include <memory>

struct CmdConvertOptions {
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

struct CmdConvert
#ifdef CALLFWD_CMD_CONVERT_INTERNAL_H_
    : private CmdConvertPriv
#endif
{
  using Options = CmdConvertOptions;
  static const CmdDescription description;

  static std::unique_ptr<CmdConvert> Make(
      arrow::MemoryPool* memory_pool = nullptr);

  static std::shared_ptr<arrow::Schema> pn_schema();
  static std::shared_ptr<arrow::Schema> ym_schema();
  static std::shared_ptr<arrow::Schema> rn_schema();

  arrow::Status Init(const Options& options);
  arrow::Status Run(int64_t limit = UINT32_MAX);
  arrow::Status Finish(bool incomplete = false);
};

#endif // CALLFWD_CMD_CONVERT_H_
