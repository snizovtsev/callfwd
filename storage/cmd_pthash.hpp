#ifndef CALLFWD_CMD_PTHASH_H_
#define CALLFWD_CMD_PTHASH_H_

#include "cmd.hpp"

struct CmdPtHashOptions {
  std::string partition_path;
  std::string data_output_path;
  std::string index_output_path;
  float tweak_c;
  float tweak_alpha;
};

struct CmdPtHash
#ifdef CALLFWD_CMD_PTHASH_INTERNAL_H_
  : private CmdPtHashPriv
#endif
{
  using Options = CmdPtHashOptions;
  static const CmdDescription description;

  static std::unique_ptr<CmdPtHash> Make(
      arrow::MemoryPool* memory_pool = nullptr);

  arrow::Status Init(const Options& options);
  arrow::Status Run(int64_t limit = INT64_MAX);
  arrow::Status Finish(bool incomplete = false);
};

#endif // CALLFWD_CMD_PTHASH_H_
