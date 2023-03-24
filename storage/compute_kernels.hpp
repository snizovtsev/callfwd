#ifndef HIPERF_LRN_COMPUTE_KERNELS
#define HIPERF_LRN_COMPUTE_KERNELS

#include <arrow/compute/api.h>

class BucketerOptions : public arrow::compute::FunctionOptions {
 public:
  explicit BucketerOptions(uint32_t num_buckets = 20,
                           uint32_t num_partitions = 1,
                           uint64_t hash_seed = 424242);
  static constexpr char const kTypeName[] = "XBucketerOptions";

  uint64_t hash_seed;
  uint32_t num_buckets;
  uint32_t num_partitions;
};

void RegisterCustomFunctions(arrow::compute::FunctionRegistry* registry);

#endif // HIPERF_LRN_COMPUTE_KERNELS
