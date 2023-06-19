#ifndef CALLFWD_CMD_PARTITION_H
#define CALLFWD_CMD_PARTITION_H

#include "cmd.hpp"

#include <arrow/type_fwd.h>
#include <arrow/io/type_fwd.h>
#include <arrow/compute/type_fwd.h>

#include <vector>
#include <string>
#include <memory>

struct CmdPartitionOptions {
  uint64_t hash_seed;
  uint32_t num_partitions;
  std::string key_column;
  std::string source_path;
  std::string output_template;
};

struct CmdPartition
#ifdef CALLFWD_CMD_PARTITION_INTERNAL_H_
    : private CmdPartitionPriv
#endif
{
  using Options = CmdPartitionOptions;
  static const CmdDescription description;

  static std::unique_ptr<CmdPartition> Make(
      arrow::compute::ExecContext* exec_context = nullptr,
      const arrow::io::IOContext* io_context = nullptr);

  arrow::Status Init(const Options& options);
  arrow::Status Run(int64_t limit = INT64_MAX);
  arrow::Status Finish(bool incomplete = false);
};

#endif // CALLFWD_CMD_PARTITION_H
