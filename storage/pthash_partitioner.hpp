#ifndef CALLFWD_PTHASH_PARTITIONER_H
#define CALLFWD_PTHASH_PARTITIONER_H

#include <arrow/type_fwd.h>
#include <arrow/compute/type_fwd.h>

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

struct PtHashPartitioner;

struct PtHashPartitionerOptions {
  void AddCommand(folly::NestedCommandLineApp &app,
                  PtHashPartitioner *command = nullptr);
  void BindToOptions(boost::program_options::options_description& description);
  void Store(const boost::program_options::variables_map& options,
             const std::vector<std::string>& args);

  uint64_t hash_seed;
  uint32_t num_partitions;
  uint32_t num_buckets;
  std::string source_path;
  std::string output_dir;
  std::string name_template;
};

struct PtHashPartitioner
#ifdef CALLFWD_PTHASH_PARTITIONER_INTERNAL_H_
    : private PtHashPartitionerPriv
#endif
{
  static std::unique_ptr<PtHashPartitioner> Make(
    arrow::compute::ExecContext* exec_context = nullptr,
    arrow::MemoryPool* memory_pool = nullptr);

  arrow::Status Reset(const PtHashPartitionerOptions &options);
  arrow::Status Drain();
};

#endif // CALLFWD_PTHASH_PARTITIONER_H
