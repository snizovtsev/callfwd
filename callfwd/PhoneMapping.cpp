#include "PhoneMapping.h"

#include <algorithm>
#include <stdexcept>
#if HAVE_STD_PARALLEL
#include <execution>
#endif

static inline bool operator< (const PhoneList &lhs, const PhoneList &rhs) {
  return std::tie(lhs.phone, lhs.next) < std::tie(rhs.phone, rhs.next);
}

static inline bool operator== (const PhoneList &lhs, const PhoneList &rhs) {
  return lhs.phone == rhs.phone;
}

uint64_t PhoneMapping::findTarget(uint64_t source) const
{
  const auto it = targetMapping_.find(source);
  if (it != targetMapping_.cend())
    return it->second;
  else
    return NONE;
}

std::vector<uint64_t> PhoneMapping::reverseTarget(uint64_t targetMin, uint64_t targetMax) const
{
  std::vector<uint64_t> ret;

  PhoneList vmin = {targetMin, NULL};
  PhoneList vmax = {targetMax, NULL};

  auto targetIter = std::lower_bound(sortedTargets_.begin(), sortedTargets_.end(), vmin);
  auto targetEnd = std::upper_bound(targetIter, sortedTargets_.end(), vmax);

  const PhoneList *pfirst = targetIter == sortedTargets_.end() ? nullptr : targetIter->next;
  const PhoneList *pend = targetEnd == sortedTargets_.end() ? nullptr : targetEnd->next;

  for (const PhoneList *p = pfirst; p != pend; p = p->next) {
    ret.push_back(p->phone);
  }

  return ret;
}

PhoneMappingBuilder& PhoneMappingBuilder::addMapping(uint64_t source, uint64_t target)
{
  if (targetMapping_.count(source))
    throw std::runtime_error("PhoneMappingBuilder: duplicate key");

  targetMapping_.emplace(source, target);
  sourceNumbers_.push_back(PhoneList{source, NULL});
  sortedTargets_.push_back(PhoneList{target, NULL});
  return *this;
}

void PhoneMappingBuilder::SizeHint(uint64_t numRecords)
{
  sourceNumbers_.reserve(numRecords);
  sortedTargets_.reserve(numRecords);
  targetMapping_.reserve(numRecords);
}

std::shared_ptr<PhoneMapping> PhoneMapping::detach(PhoneMapping &b) {
  auto ret = std::make_shared<PhoneMapping>();
  ret->targetMapping_ = std::move(b.targetMapping_);
  ret->sourceNumbers_ = std::move(b.sourceNumbers_);
  ret->sortedTargets_ = std::move(b.sortedTargets_);
  return ret;
}

std::shared_ptr<PhoneMapping> PhoneMappingBuilder::build()
{
  size_t N = sourceNumbers_.size();

  // Connect sortedTargets_ with sourceNumbers_ before shuffling
  for (size_t i = 0; i < N; ++i)
    sortedTargets_[i].next = &sourceNumbers_[i];

#if HAVE_STD_PARALLEL
  std::stable_sort(std::execution::par_unseq, sortedTargets_.begin(), sortedTargets_.end());
#else
  std::stable_sort(sortedTargets_.begin(), sortedTargets_.end());
#endif

  // Wire sourceNumbers_ list by target
  for (size_t i = 0; i + 1 < N; ++i)
    sortedTargets_[i].next->next = sortedTargets_[i+1].next;

  auto last = std::unique(sortedTargets_.begin(), sortedTargets_.end());
  sortedTargets_.erase(last, sortedTargets_.end());
  sortedTargets_.shrink_to_fit();

  return detach(*this);
}
