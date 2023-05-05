#include "compute_kernels.hpp"
#include "pthash_bucketer.hpp"

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/function_internal.h>
#include <arrow/compute/kernel.h>
#include <arrow/util/logging.h>

namespace cp = arrow::compute;
using arrow::Result;
using arrow::Status;
using arrow::internal::DataMember;
using arrow::internal::checked_cast;
using arrow::internal::checked_pointer_cast;

static auto kBucketerOptionsType =
    cp::internal::GetFunctionOptionsType<BucketerOptions>(
        DataMember("num_partitions", &BucketerOptions::num_partitions),
        DataMember("hash_seed", &BucketerOptions::hash_seed)
    );

BucketerOptions::BucketerOptions(uint32_t num_partitions,
                                 uint64_t hash_seed)
    : cp::FunctionOptions(kBucketerOptionsType)
    , hash_seed(hash_seed)
    , num_partitions(num_partitions)
{}
constexpr char BucketerOptions::kTypeName[];

namespace {

struct PTHashPartitioner final : cp::KernelState {
  static Result<std::unique_ptr<cp::KernelState>> Init(
      cp::KernelContext* ctx, const cp::KernelInitArgs& args);
  static Status Exec(
      cp::KernelContext* ctx, const cp::ExecSpan& batch, cp::ExecResult *out);
  static void AddKernels(
      cp::ScalarFunction* func);

  explicit PTHashPartitioner(const BucketerOptions& options)
      : hash_seed(options.hash_seed)
      , partition_mask(options.num_partitions - 1)
  {}

  uint64_t hash_seed;
  uint64_t partition_mask;
};

Result<std::unique_ptr<cp::KernelState>>
PTHashPartitioner::Init(cp::KernelContext* ctx, const cp::KernelInitArgs& args) {
  auto options = checked_cast<const BucketerOptions*>(args.options);
  auto state = std::make_unique<PTHashPartitioner>(*options);
  if (options->num_partitions & state->partition_mask)
    return Status::NotImplemented("Number of partitions must be a power of 2");
  else
    return state;
}

Status PTHashPartitioner::Exec(cp::KernelContext* ctx, const cp::ExecSpan& batch,
                               cp::ExecResult *out) {
  const auto& state = checked_cast<PTHashPartitioner&>(*ctx->state());
  const uint64_t* pn = batch[0].array.GetValues<uint64_t>(1);
  uint16_t* partition = out->array_span_mutable()->GetValues<uint16_t>(1);

  ARROW_LOG(DEBUG) << "PTHash partitioner job " << &state << ": "
                   << pn[0] << " to " << pn[batch.length-1]
                   << "of length " << batch.length;

  for (int64_t i = 0; i < batch.length; ++i) {
    auto h = murmur3_128(pn[i], state.hash_seed);
    partition[i] = (h.first ^ h.second) & state.partition_mask;
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
  kernel.mem_allocation = cp::MemAllocation::PREALLOCATE;
  kernel.parallelizable = true;
  DCHECK_OK(func->AddKernel(std::move(kernel)));
}

} // anonymous namespace

void RegisterCustomFunctions(cp::FunctionRegistry* registry) {
  static BucketerOptions default_bucketer_options;

  DCHECK_OK(registry->AddFunctionOptionsType(kBucketerOptionsType));

  auto func = std::make_shared<cp::ScalarFunction>(
      "x_pthash_partition", cp::Arity::Unary(), cp::FunctionDoc::Empty(), &default_bucketer_options);
  PTHashPartitioner::AddKernels(func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));
}
