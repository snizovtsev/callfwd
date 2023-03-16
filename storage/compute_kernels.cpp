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
  explicit SkewBucketer(uint32_t num_buckets);
  uint32_t operator()(uint64_t hashed_key) const;
  uint32_t num_buckets() const { return dense_buckets + sparse_buckets; }

  uint32_t dense_buckets;
  uint32_t sparse_buckets;
  __uint128_t M_dense_buckets;
  __uint128_t M_sparse_buckets;
};

SkewBucketer::SkewBucketer(uint32_t num_buckets)
    : dense_buckets(0.3 * num_buckets)
    , sparse_buckets(num_buckets - dense_buckets)
    , M_dense_buckets(fastmod_computeM_u64(dense_buckets))
    , M_sparse_buckets(fastmod_computeM_u64(sparse_buckets))
{}

uint32_t SkewBucketer::operator()(uint64_t hashed_key) const {
  static constexpr uint64_t threshold = 0.6 * UINT64_MAX;
  if (FOLLY_BUILTIN_EXPECT_WITH_PROBABILITY(hashed_key < threshold, 1, 0.6)) {
    return fastmod_u64(hashed_key, M_dense_buckets, dense_buckets);
  } else {
    return dense_buckets
        + fastmod_u64(hashed_key, M_sparse_buckets, sparse_buckets);
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
        DataMember("num_buckets", &BucketerOptions::num_buckets),
        DataMember("hash_seed", &BucketerOptions::hash_seed)
    );

} // anonymous namespace

BucketerOptions::BucketerOptions(uint32_t num_buckets, uint64_t hash_seed)
    : cp::FunctionOptions(kBucketerOptionsType)
    , hash_seed(hash_seed)
    , num_buckets(num_buckets)
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

  explicit BucketCounter(uint32_t num_buckets, arrow::MemoryPool* memory_pool)
      : bucketer(num_buckets)
      , counts(memory_pool)
  {}

  uint64_t                            hash_seed;
  SkewBucketer                        bucketer;
  arrow::TypedBufferBuilder<uint32_t> counts;
};

Result<std::unique_ptr<cp::KernelState>>
BucketCounter::Init(cp::KernelContext* ctx, const cp::KernelInitArgs& args) {
  auto options = checked_cast<const BucketerOptions*>(args.options);
  auto state = std::make_unique<BucketCounter>(options->num_buckets, ctx->memory_pool());
  ARROW_RETURN_NOT_OK(state->counts.Reserve(options->num_buckets));
  state->counts.UnsafeAppend(options->num_buckets, 0);
  state->hash_seed = options->hash_seed;
  VLOG(1) << "Init " << state.get();
  return state;
}

Status BucketCounter::Consume(cp::KernelContext* ctx, const cp::ExecSpan& batch) {
  auto& state = checked_cast<BucketCounter&>(*ctx->state());
  const uint64_t* pn = batch[0].array.GetValues<uint64_t>(1);
  const SkewBucketer& bucketer = state.bucketer;
  uint32_t* counts = state.counts.mutable_data();
  uint64_t seed = state.hash_seed;

  VLOG(1) << &state << ": consume batch " << pn[0] << " to " << pn[batch.length-1];

  // XXX: Analyze vectorizer assembly and decide whether to split hashing
  for (uint32_t i = 0; i < batch.length; ++i) {
    uint64_t hashed_key = murmur3_64(pn[i], seed);
    uint32_t bucket_index = bucketer(hashed_key);
    DCHECK_LT(bucket_index, bucketer.num_buckets());
    counts[bucket_index] += 1;
  }
  return Status::OK();
}

Status BucketCounter::Merge(cp::KernelContext*, cp::KernelState&& other_state,
                            cp::KernelState* kernel_state)
{
  auto& state = checked_cast<BucketCounter&>(*kernel_state);
  auto& other = checked_cast<const BucketCounter&>(other_state);
  uint32_t num_buckets = state.bucketer.num_buckets();
  uint32_t* counts = state.counts.mutable_data();
  const uint32_t* increment = other.counts.data();

  LOG(INFO) << "Merge " << kernel_state << ' ' << &other_state;
  DCHECK_EQ(num_buckets, other.bucketer.num_buckets());

  for (uint32_t i = 0; i < num_buckets; ++i)
    counts[i] += increment[i];

  return Status::OK();
}

Status BucketCounter::Finalize(cp::KernelContext* ctx, arrow::Datum* out) {
  VLOG(1) << "Finalize " << ctx->state();

  auto& state = checked_cast<BucketCounter&>(*ctx->state());
  std::shared_ptr<arrow::Buffer> counts_data;

  ARROW_RETURN_NOT_OK(state.counts.Finish(&counts_data));
  *out = arrow::ArrayData::Make(arrow::uint32(), state.bucketer.num_buckets(),
                                {NULLPTR, std::move(counts_data)},
                                /*null_count=*/0);
  return Status::OK();
}

void AddBucketCounterKernels(cp::ScalarAggregateFunction* func) {
  auto sig = cp::KernelSignature::Make({arrow::uint64()}, arrow::uint32());
  cp::ScalarAggregateKernel kernel(std::move(sig),
                                   BucketCounter::Init,
                                   BucketCounter::Consume,
                                   BucketCounter::Merge,
                                   BucketCounter::Finalize);
  // Set the simd level
  kernel.simd_level = cp::SimdLevel::NONE;
  DCHECK_OK(func->AddKernel(std::move(kernel)));
}

} // anonymous namespace

void RegisterCustomFunctions(cp::FunctionRegistry* registry) {
  static BucketerOptions default_bucketer_options;

  auto func = std::make_shared<cp::ScalarAggregateFunction>(
      "x_bucket_counter", cp::Arity::Unary(), cp::FunctionDoc::Empty(), &default_bucketer_options);
  AddBucketCounterKernels(func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));
  DCHECK_OK(registry->AddFunctionOptionsType(kBucketerOptionsType));
}
