#include "compute_kernels.hpp"

#include "bit_magic.hpp"

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/function_internal.h>
#include <folly/Likely.h>
#include <glog/logging.h>
#include <arrow/util/logging.h>

using arrow::Result;
using arrow::Status;
namespace cp = arrow::compute;

namespace {

struct SkewBucketer {
  explicit SkewBucketer(uint32_t num_buckets, uint32_t num_partitions = 1);
  uint32_t bucket(uint64_t hashed_key) const;
  uint32_t partition(uint64_t hashed_key) const;
  uint32_t num_buckets() const { return dense_buckets + sparse_buckets; }

  uint32_t dense_buckets;
  uint32_t sparse_buckets;
  uint64_t M_dense_block;
  uint64_t M_sparse_block;
  __uint128_t M_dense_buckets;
  __uint128_t M_sparse_buckets;
};

// TODO: adjust to primes?
SkewBucketer::SkewBucketer(uint32_t num_buckets, uint32_t num_partitions)
    : dense_buckets(0.3 * num_buckets)
    , sparse_buckets(num_buckets - dense_buckets)
    , M_dense_block(fastmod_computeM_u32((dense_buckets + num_partitions - 1) / num_partitions))
    , M_sparse_block(fastmod_computeM_u32((sparse_buckets + num_partitions - 1) / num_partitions))
    , M_dense_buckets(fastmod_computeM_u64(dense_buckets))
    , M_sparse_buckets(fastmod_computeM_u64(sparse_buckets))
{}

uint32_t SkewBucketer::bucket(uint64_t hashed_key) const {
  static constexpr uint64_t threshold = 0.6 * UINT64_MAX;
  if (FOLLY_BUILTIN_EXPECT_WITH_PROBABILITY(hashed_key < threshold, 1, 0.6)) {
    return fastmod_u64(hashed_key, M_dense_buckets, dense_buckets);
  } else {
    return dense_buckets
        + fastmod_u64(hashed_key, M_sparse_buckets, sparse_buckets);
  }
}

uint32_t SkewBucketer::partition(uint64_t hashed_key) const {
  static constexpr uint64_t threshold = 0.6 * UINT64_MAX;
  if (FOLLY_BUILTIN_EXPECT_WITH_PROBABILITY(hashed_key < threshold, 1, 0.6)) {
    return fastdiv_u32(fastmod_u64(hashed_key, M_dense_buckets, dense_buckets),
                       M_dense_block);
  } else {
    return fastdiv_u32(fastmod_u64(hashed_key, M_sparse_buckets, sparse_buckets),
                       M_sparse_block);
  }
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
      , bucketer(options.num_buckets, options.num_partitions)
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
    uint32_t bucket_index = bucketer.bucket(hashed_key);
    DCHECK_LT(bucket_index, bucketer.num_buckets());
    counts[bucket_index] += 1;
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

// TODO: derive bucket size type (uint32_t) from output arg
struct PTHashPartitioner final : cp::KernelState {
  static Result<std::unique_ptr<cp::KernelState>>
  Init(cp::KernelContext* ctx, const cp::KernelInitArgs& args);

  static Status Exec(cp::KernelContext* ctx, const cp::ExecSpan& batch, cp::ExecResult *out);
  static void AddKernels(cp::ScalarFunction* func);

  explicit PTHashPartitioner (const BucketerOptions& options)
      : hash_seed(options.hash_seed)
      , bucketer(options.num_buckets, options.num_partitions)
  {}

  uint64_t     hash_seed;
  SkewBucketer bucketer;
};

Result<std::unique_ptr<cp::KernelState>>
PTHashPartitioner::Init(cp::KernelContext* ctx, const cp::KernelInitArgs& args) {
  auto options = checked_cast<const BucketerOptions*>(args.options);
  auto state = std::make_unique<PTHashPartitioner>(*options);
  state->hash_seed = options->hash_seed;
  return state;
}

Status PTHashPartitioner::Exec(cp::KernelContext* ctx, const cp::ExecSpan& batch, cp::ExecResult *out)
{
  auto& state = checked_cast<PTHashPartitioner&>(*ctx->state());
  const SkewBucketer& bucketer = state.bucketer;

  const uint64_t* pn = batch[0].array.GetValues<uint64_t>(1);
  uint32_t* partition = out->array_span()->GetValues<uint32_t>(1);
  uint64_t seed = state.hash_seed;

  VLOG(2) << "Job " << &state << ": "
          << pn[0] << " to " << pn[batch.length-1]
          << "of length " << batch.length;

  for (uint32_t i = 0; i < batch.length; ++i) {
    uint64_t hashed_key = murmur3_64(pn[i], seed);
    partition[i] = bucketer.partition(hashed_key);
  }
  return Status::OK();
}

void PTHashPartitioner::AddKernels(cp::ScalarFunction* func) {
  auto sig = cp::KernelSignature::Make({arrow::uint64()}, arrow::uint32());
  cp::ScalarKernel kernel(std::move(sig), PTHashPartitioner::Exec, PTHashPartitioner::Init);
  // Set the simd level
  kernel.simd_level = cp::SimdLevel::NONE;
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
      "x_pthash_partitioner", cp::Arity::Unary(), cp::FunctionDoc::Empty(), &default_bucketer_options);
  PTHashPartitioner::AddKernels(func2.get());
  DCHECK_OK(registry->AddFunction(std::move(func2)));
}
