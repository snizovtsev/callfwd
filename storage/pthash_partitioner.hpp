#ifndef CALLFWD_CMD_PARTITION_H
#define CALLFWD_CMD_PARTITION_H

#include <arrow/type_fwd.h>
#include <arrow/compute/type_fwd.h>

#include <vector>
#include <string>
#include <memory>

namespace boost::program_options {
  class options_description;
  class variables_map;
}

struct CommandDescription {
  const char *name;
  const char *args;
  const char *abstract;
  const char *help;
};

struct CmdPartitionOptions {
  void Bind(
    boost::program_options::options_description& description);

  arrow::Status Store(
    const boost::program_options::variables_map& options,
    const std::vector<std::string>& args);

  uint64_t hash_seed;
  uint32_t num_partitions;
  std::string source_path;
  std::string output_template;
};

struct CmdPartition
#ifdef CALLFWD_CMD_PARTITION_INTERNAL_H_
    : private CmdPartitionPriv
#endif
{
  using Options = CmdPartitionOptions;
  static const CommandDescription description;

  static std::unique_ptr<CmdPartition> Make(
    const Options *options,
    arrow::compute::ExecContext* exec_context = nullptr,
    const arrow::io::IOContext* io_context = nullptr);

  arrow::Status Init();
  arrow::Status Run(int64_t limit = INT64_MAX);
  arrow::Status Finish(bool incomplete = false);
};

#endif // CALLFWD_CMD_PARTITION_H
