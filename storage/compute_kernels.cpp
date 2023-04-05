#include "compute_kernels.hpp"

#include "bit_magic.hpp"

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/function_internal.h>
#include <arrow/compute/kernel.h>
#include <folly/Likely.h>
#include <folly/lang/Hint.h>
#include <glog/logging.h>
#include <arrow/util/logging.h>

using arrow::Result;
using arrow::Status;
namespace cp = arrow::compute;

namespace {

struct SkewBucketer {
  explicit SkewBucketer(uint32_t num_buckets);

  /* Returns an integer in range [-sparse_buckets .. dense_buckets) */
  int32_t operator()(uint64_t hashed_key) const;

  /* Return an integer in range [0 .. num_buckets) */
  uint32_t to_index(int32_t bucket) const {
    return (bucket >= 0) ? bucket : (dense_buckets - bucket - 1);
  }

  /* Equals to construction time parameter. */
  uint32_t num_buckets() const {
    return dense_buckets + sparse_buckets;
  }

  uint32_t dense_buckets;
  uint32_t sparse_buckets;
  __uint128_t M_dense_buckets;
  __uint128_t M_sparse_buckets;
};

// TODO: adjust to primes?
SkewBucketer::SkewBucketer(uint32_t num_buckets)
    : dense_buckets(0.3 * num_buckets)
    , sparse_buckets(num_buckets - dense_buckets)
    , M_dense_buckets(fastmod_computeM_u64(dense_buckets))
    , M_sparse_buckets(fastmod_computeM_u64(sparse_buckets))
{}

int32_t SkewBucketer::operator()(uint64_t hashed_key) const {
  static constexpr uint64_t threshold = 0.6 * UINT64_MAX;
  int32_t ret;
  if (FOLLY_BUILTIN_EXPECT_WITH_PROBABILITY(hashed_key < threshold, 1, 0.6)) {
    ret = fastmod_u64(hashed_key, M_dense_buckets, dense_buckets);
    folly::compiler_may_unsafely_assume(ret >= 0);
  } else {
    ret = -1 - fastmod_u64(hashed_key, M_sparse_buckets, sparse_buckets);
    folly::compiler_may_unsafely_assume(ret < 0);
  }
  return ret;
}

template<class IntegerA, class IntegerB>
IntegerA DivideAndRoundUp(IntegerA a, IntegerB b) {
  return (a + b - 1) / b;
}

struct BucketPartitioner {
  explicit BucketPartitioner(const SkewBucketer& bucketer,
                             uint16_t num_partitions);
  explicit BucketPartitioner(uint32_t dense_block,
                             uint32_t sparse_block);

  /* Returns a number in range [0, num_partitions) */
  uint16_t operator()(int32_t bucket) const;

  uint64_t M_dense_block;
  uint64_t M_sparse_block;
};

BucketPartitioner::BucketPartitioner(const SkewBucketer& bucketer,
                                     uint16_t num_partitions)
  : BucketPartitioner(DivideAndRoundUp(bucketer.dense_buckets, num_partitions),
                      DivideAndRoundUp(bucketer.sparse_buckets, num_partitions))
{}

BucketPartitioner::BucketPartitioner(uint32_t dense_block, uint32_t sparse_block)
    : M_dense_block(fastmod_computeM_u32(dense_block))
    , M_sparse_block(fastmod_computeM_u32(sparse_block))
{}

uint16_t BucketPartitioner::operator()(int32_t bucket) const {
  if (bucket >= 0) {
    return fastdiv_u32(bucket, M_dense_block);
  } else {
    return fastdiv_u32(-(bucket + 1), M_sparse_block);
  }
}

struct LocalBucketer {
  explicit LocalBucketer(const SkewBucketer& bucketer,
                         uint16_t num_partitions,
                         int32_t sample_bucket);

  explicit LocalBucketer(uint32_t dense_buckets, uint32_t sparse_buckets,
                         uint16_t num_partitions, uint16_t partition);

  /* Returns a number in range [0, DivideAndRoundUp(num_buckets, num_partitions)] */
  uint32_t operator()(int32_t bucket) const;

  uint32_t dense_offset;
  uint32_t sparse_offset;
};

LocalBucketer::LocalBucketer(const SkewBucketer& bucketer,
                             uint16_t num_partitions,
                             int32_t sample_bucket)
    : LocalBucketer(bucketer.dense_buckets, bucketer.sparse_buckets,
                    num_partitions,
                    BucketPartitioner(bucketer, num_partitions)(sample_bucket))
{}

LocalBucketer::LocalBucketer(uint32_t dense_buckets, uint32_t sparse_buckets,
                             uint16_t num_partitions, uint16_t partition)
    : dense_offset(DivideAndRoundUp(dense_buckets, num_partitions) * partition)
    , sparse_offset(DivideAndRoundUp(sparse_buckets, num_partitions) * partition)
{}

uint32_t LocalBucketer::operator()(int32_t bucket) const {
  uint32_t ret;
  if (bucket >= 0) {
    ret = bucket - dense_offset;
  } else {
    ret = -(bucket + 1) - sparse_offset;
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// Arrow Compute plug-in
////////////////////////////////////////////////////////////////////////////////

using arrow::internal::DataMember;
using arrow::internal::checked_cast;
using arrow::internal::checked_pointer_cast;

static auto kBucketerOptionsType =
    cp::internal::GetFunctionOptionsType<BucketerOptions>(
        DataMember("hash_seed", &BucketerOptions::hash_seed),
        DataMember("num_buckets", &BucketerOptions::num_buckets),
        DataMember("num_partitions", &BucketerOptions::num_partitions)
    );

} // anonymous namespace

BucketerOptions::BucketerOptions(uint32_t num_buckets,
                                 uint32_t num_partitions,
                                 uint64_t hash_seed)
    : cp::FunctionOptions(kBucketerOptionsType)
    , hash_seed(hash_seed)
    , num_buckets(num_buckets)
    , num_partitions(num_partitions)
{}
constexpr char BucketerOptions::kTypeName[];

namespace {

// TODO: derive bucket size type (uint32_t) from output arg
struct BucketCounter final : cp::KernelState {
  static Result<std::unique_ptr<cp::KernelState>>
  Init(cp::KernelContext* ctx, const cp::KernelInitArgs& args);

  static Status Consume(cp::KernelContext*, const cp::ExecSpan&);
  static Status Merge(cp::KernelContext*, cp::KernelState&&, cp::KernelState*);
  static Status Finalize(cp::KernelContext*, arrow::Datum*);
  static void AddKernels(cp::ScalarAggregateFunction* func);

  explicit BucketCounter(const BucketerOptions& options, arrow::MemoryPool* memory_pool)
      : hash_seed(options.hash_seed)
      , bucketer(options.num_buckets)
      , counts(memory_pool)
  {}

  uint64_t                            hash_seed;
  SkewBucketer                        bucketer;
  arrow::TypedBufferBuilder<uint16_t> counts;
};

Result<std::unique_ptr<cp::KernelState>>
BucketCounter::Init(cp::KernelContext* ctx, const cp::KernelInitArgs& args) {
  auto options = checked_cast<const BucketerOptions*>(args.options);
  auto state = std::make_unique<BucketCounter>(*options, ctx->memory_pool());
  VLOG(1) << "Make job " << state.get();
  return state;
}

Status BucketCounter::Consume(cp::KernelContext* ctx, const cp::ExecSpan& batch) {
  auto& state = checked_cast<BucketCounter&>(*ctx->state());
  const SkewBucketer& bucketer = state.bucketer;

  /* Arrow allocates state for each thread including IO ones.
   * To avoid wasting large chunks of memory we have to allocate it lazily. */
  if (ARROW_PREDICT_FALSE(state.counts.length() == 0)) {
    ARROW_RETURN_NOT_OK(state.counts.Append(bucketer.num_buckets(), 0));
    VLOG(1) << "Init job " << &state << ", bytes allocated: "
            << state.counts.bytes_builder()->capacity();
  }

  const uint64_t* pn = batch[0].array.GetValues<uint64_t>(1);
  uint16_t* counts = state.counts.mutable_data();
  uint64_t seed = state.hash_seed;

  VLOG(2) << "Job " << &state << ": "
          << pn[0] << " to " << pn[batch.length-1]
          << "of length " << batch.length;

  // XXX: Analyze vectorizer assembly and decide whether to split hashing
  for (uint32_t i = 0; i < batch.length; ++i) {
    uint64_t hashed_key = murmur3_64(pn[i], seed);
    int32_t bucket = bucketer(hashed_key);
    uint32_t index = bucketer.to_index(bucket);
    DCHECK_LT(index, bucketer.num_buckets());
    counts[index] += 1;
  }
  return Status::OK();
}

Status BucketCounter::Merge(cp::KernelContext*, cp::KernelState&& other_state,
                            cp::KernelState* kernel_state)
{
  auto& state = checked_cast<BucketCounter&>(*kernel_state);
  auto& other = checked_cast<BucketCounter&>(other_state);
  uint32_t num_buckets = state.bucketer.num_buckets();
  uint16_t* counts = state.counts.mutable_data();
  const uint16_t* increment = other.counts.data();

  if (other.counts.length() == 0)
    return Status::OK();

  if (state.counts.length() == 0) {
    state.counts = std::move(other.counts);
    return Status::OK();
  }

  VLOG(1) << "Merge job " << kernel_state << ' ' << &other_state;
  DCHECK_EQ(num_buckets, other.bucketer.num_buckets());

  for (uint32_t i = 0; i < num_buckets; ++i)
    counts[i] += increment[i];
  other.counts.Reset();

  return Status::OK();
}

Status BucketCounter::Finalize(cp::KernelContext* ctx, arrow::Datum* out) {
  VLOG(1) << "Finalize " << ctx->state();

  auto& state = checked_cast<BucketCounter&>(*ctx->state());
  std::shared_ptr<arrow::Buffer> counts_data;

  ARROW_RETURN_NOT_OK(state.counts.Finish(&counts_data));
  *out = arrow::ArrayData::Make(arrow::uint16(), state.bucketer.num_buckets(),
                                {NULLPTR, std::move(counts_data)},
                                /*null_count=*/0);
  return Status::OK();
}

void BucketCounter::AddKernels(cp::ScalarAggregateFunction* func) {
  auto sig = cp::KernelSignature::Make({arrow::uint64()}, arrow::uint16());
  cp::ScalarAggregateKernel kernel(std::move(sig),
                                   BucketCounter::Init,
                                   BucketCounter::Consume,
                                   BucketCounter::Merge,
                                   BucketCounter::Finalize);
  // Set the simd level
  kernel.simd_level = cp::SimdLevel::NONE;
  DCHECK_OK(func->AddKernel(std::move(kernel)));
}

//
/////////////////////////////////////////////////////////////////////////////////

struct PTHashPartitioner final : cp::KernelState {
  static Result<std::unique_ptr<cp::KernelState>> Init(
      cp::KernelContext* ctx, const cp::KernelInitArgs& args);
  static Status Exec(
      cp::KernelContext* ctx, const cp::ExecSpan& batch, cp::ExecResult *out);
  static void AddKernels(
      cp::ScalarFunction* func);

  explicit PTHashPartitioner(const BucketerOptions& options)
      : hash_seed(options.hash_seed)
      , bucketer(options.num_buckets)
      , partitioner(bucketer, options.num_partitions)
  {}

  uint64_t          hash_seed;
  SkewBucketer      bucketer;
  BucketPartitioner partitioner;
};

Result<std::unique_ptr<cp::KernelState>>
PTHashPartitioner::Init(cp::KernelContext* ctx, const cp::KernelInitArgs& args) {
  auto options = checked_cast<const BucketerOptions*>(args.options);
  return std::make_unique<PTHashPartitioner>(*options);
}

Status PTHashPartitioner::Exec(cp::KernelContext* ctx, const cp::ExecSpan& batch,
                               cp::ExecResult *out) {
  auto& state = checked_cast<PTHashPartitioner&>(*ctx->state());
  const uint64_t* pn = batch[0].array.GetValues<uint64_t>(1);
  uint16_t* partition = out->array_span()->GetValues<uint16_t>(1);

  VLOG(2) << "PTHash partitioner job " << &state << ": "
          << pn[0] << " to " << pn[batch.length-1]
          << "of length " << batch.length;

  for (int64_t i = 0; i < batch.length; ++i) {
    uint64_t hashed_key = murmur3_64(pn[i], state.hash_seed);
    int32_t bucket = state.bucketer(hashed_key);
    partition[i] = state.partitioner(bucket);
  }
  return Status::OK();
}

void PTHashPartitioner::AddKernels(cp::ScalarFunction* func) {
  cp::ScalarKernel kernel{
    {arrow::uint64()}, arrow::uint16(),
    PTHashPartitioner::Exec,
    PTHashPartitioner::Init
  };
  kernel.simd_level = cp::SimdLevel::NONE;
  //kernel.null_handling = cp::NullHandling::COMPUTED_PREALLOCATE;
  kernel.mem_allocation = cp::MemAllocation::PREALLOCATE;
  kernel.parallelizable = true;
  DCHECK_OK(func->AddKernel(std::move(kernel)));
}

} // anonymous namespace

void RegisterCustomFunctions(cp::FunctionRegistry* registry) {
  static BucketerOptions default_bucketer_options;

  DCHECK_OK(registry->AddFunctionOptionsType(kBucketerOptionsType));

  auto func1 = std::make_shared<cp::ScalarAggregateFunction>(
      "x_bucket_counter", cp::Arity::Unary(), cp::FunctionDoc::Empty(), &default_bucketer_options);
  BucketCounter::AddKernels(func1.get());
  DCHECK_OK(registry->AddFunction(std::move(func1)));

  auto func2 = std::make_shared<cp::ScalarFunction>(
      "x_pthash_partition", cp::Arity::Unary(), cp::FunctionDoc::Empty(), &default_bucketer_options);
  PTHashPartitioner::AddKernels(func2.get());
  DCHECK_OK(registry->AddFunction(std::move(func2)));
}
