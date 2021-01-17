#include "PhoneMapping.h"

#include <algorithm>
#include <array>
#include <stdexcept>
#include <vector>
#if HAVE_STD_PARALLEL
#include <execution>
#endif
#include <glog/logging.h>
#include <folly/Likely.h>
#include <folly/small_vector.h>
#include <folly/container/F14Map.h>
#include <folly/synchronization/Hazptr.h>
#include <folly/portability/GFlags.h>


// TODO: benchmark prefetch size
DEFINE_uint32(f14map_prefetch, 16, "Maximum number of keys to prefetch");

struct PhoneList {
  uint64_t phone;
  PhoneList *next;
};

class PhoneMapping::Data : public folly::hazptr_obj_base<PhoneMapping::Data> {
 public:
  void getRNs(size_t N, const uint64_t *pn, uint64_t *rn) const;
  std::unique_ptr<Cursor> inverseRNs(uint64_t fromRN, uint64_t toRN) const;
  std::unique_ptr<Cursor> visitRows() const;
  void build();
  ~Data() noexcept;

  // pn->rn mapping
  folly::F14ValueMap<uint64_t, uint64_t> dict;
  // pn column joined with sorted rn column
  std::vector<PhoneList> pnColumn;
  // unique-sorted rn column joined with pn
  std::vector<PhoneList> rnIndex;
};

PhoneMapping::Data::~Data() noexcept {
  LOG_IF(INFO, pnColumn.size() > 0) << "Reclaiming memory";
}

class PhoneMapping::Cursor {
 public:
  virtual ~Cursor() = default;

  bool hasRow() const noexcept { return size_ != 0; }
  uint64_t currentPN() const noexcept { return pn_[pos_]; }
  uint64_t currentRN() const noexcept { return rn_[pos_]; }
  void prefetch(const Data *data) noexcept;
  void advance(const Data *data) noexcept;
  virtual void refill() = 0;

 protected:
  std::array<uint64_t, 8> pn_;
  std::array<uint64_t, 8> rn_;
  unsigned size_;
  unsigned pos_;
};

void PhoneMapping::Data::getRNs(size_t N, const uint64_t *pn, uint64_t *rn) const {
  folly::small_vector<folly::F14HashToken, 1> token;
  token.resize(std::min<size_t>(N, FLAGS_f14map_prefetch));

  while (N > 0) {
    size_t M = std::min<size_t>(N, FLAGS_f14map_prefetch);

    // Compute hash and prefetch buckets into CPU cache
    for (size_t i = 0; i < M; ++i)
      token[i] = dict.prehash(pn[i]);

    // Fill output vector
    for (size_t i = 0; i < M; ++i) {
      const auto it = dict.find(token[i], pn[i]);
      if (it != dict.cend())
        rn[i] = it->second;
      else
        rn[i] = PhoneNumber::NOTFOUND;
    }

    pn += M;
    rn += M;
    N -= M;
  }
}

void PhoneMapping::getRNs(size_t N, const uint64_t *pn, uint64_t *rn) const {
  data_->getRNs(N, pn, rn);
}

uint64_t PhoneMapping::getRN(uint64_t pn) const {
  uint64_t rn;
  getRNs(1, &pn, &rn);
  return rn;
}

class InverseRNVisitor final : public PhoneMapping::Cursor {
 public:
  InverseRNVisitor(const PhoneMapping::Data *data, const PhoneList *it, const PhoneList *end)
    : it_(it), end_(end)
  {
    prefetch(data);
  }
  void refill() override;
 private:
  const PhoneList *it_, *end_;
};

static inline bool operator< (const PhoneList &lhs, const PhoneList &rhs) {
  return std::tie(lhs.phone, lhs.next) < std::tie(rhs.phone, rhs.next);
}

class RowVisitor final : public PhoneMapping::Cursor {
 public:
  using Iterator = std::vector<PhoneList>::const_iterator;
  RowVisitor(const PhoneMapping::Data *data, Iterator it, Iterator end)
    : it_(it), end_(end)
  {
    prefetch(data);
  }
  void refill() override;
 private:
  Iterator it_, end_;
};

std::unique_ptr<PhoneMapping::Cursor>
PhoneMapping::Data::inverseRNs(uint64_t fromRN, uint64_t toRN) const
{
  std::vector<uint64_t> ret;

  auto rnLeft = std::lower_bound(rnIndex.begin(), rnIndex.end(),
                                 PhoneList{fromRN, NULL});
  auto rnRight = std::upper_bound(rnLeft, rnIndex.end(),
                                  PhoneList{toRN, NULL});

  const PhoneList *pnBegin = rnLeft == rnIndex.end() ? nullptr : rnLeft->next;
  const PhoneList *pnEnd = rnRight == rnIndex.end() ? nullptr : rnRight->next;

  if (pnBegin != pnEnd)
    return std::make_unique<InverseRNVisitor>(this, pnBegin, pnEnd);
  else
    return nullptr;
}

PhoneMapping& PhoneMapping::inverseRNs(uint64_t fromRN, uint64_t toRN) & {
  cursor_ = data_->inverseRNs(fromRN, toRN);
  return *this;
}

PhoneMapping&& PhoneMapping::inverseRNs(uint64_t fromRN, uint64_t toRN) && {
  cursor_ = data_->inverseRNs(fromRN, toRN);
  return std::move(*this);
}

std::unique_ptr<PhoneMapping::Cursor>
PhoneMapping::Data::visitRows() const
{
  if (pnColumn.size() > 0)
    return std::make_unique<RowVisitor>(this, pnColumn.begin(), pnColumn.end());
  else
    return nullptr;
}

PhoneMapping& PhoneMapping::visitRows() & {
  cursor_ = data_->visitRows();
  return *this;
}

PhoneMapping&& PhoneMapping::visitRows() && {
  cursor_ = data_->visitRows();
  return std::move(*this);
}

void InverseRNVisitor::refill() {
  for (; it_ != end_ && size_ < pn_.size(); it_ = it_->next) {
    pn_[size_++] = it_->phone;
  }
}

void RowVisitor::refill() {
  for (; it_ != end_ && size_ < pn_.size(); ++it_) {
    pn_[size_++] = it_->phone;
  }
}

void PhoneMapping::Cursor::prefetch(const Data *data) noexcept {
  pos_ = size_ = 0;
  refill();
  data->getRNs(size_, pn_.begin(), rn_.begin());
}

void PhoneMapping::Cursor::advance(const Data *data) noexcept {
  ++pos_;
  if (UNLIKELY(pos_ == size_))
    prefetch(data);
}

PhoneMapping::Builder::Builder()
  : data_(std::make_unique<Data>())
{}

PhoneMapping::Builder::~Builder() noexcept = default;

void PhoneMapping::Builder::sizeHint(size_t numRecords)
{
  data_->pnColumn.reserve(numRecords);
  data_->rnIndex.reserve(numRecords);
  data_->dict.reserve(numRecords);
}

PhoneMapping::Builder& PhoneMapping::Builder::addRow(uint64_t pn, uint64_t rn)
{
  if (data_->dict.count(pn))
    throw std::runtime_error("PhoneMapping::Builder: duplicate key");

  data_->dict.emplace(pn, rn);
  data_->pnColumn.push_back(PhoneList{pn, NULL});
  data_->rnIndex.push_back(PhoneList{rn, NULL});
  return *this;
}

static inline bool operator== (const PhoneList &lhs, const PhoneList &rhs) {
  return lhs.phone == rhs.phone;
}

void PhoneMapping::Data::build()
{
  size_t N = pnColumn.size();

  // Connect rnIndex_ with pnColumn_ before shuffling
  for (size_t i = 0; i < N; ++i)
    rnIndex[i].next = &pnColumn[i];

#if HAVE_STD_PARALLEL
  std::stable_sort(std::execution::par_unseq, rnIndex.begin(), rnIndex.end());
#else
  std::stable_sort(rnIndex.begin(), rnIndex.end());
#endif

  // Wire pnColumn_ list by target
  for (size_t i = 0; i + 1 < N; ++i)
    rnIndex[i].next->next = rnIndex[i+1].next;

  auto last = std::unique(rnIndex.begin(), rnIndex.end());
  rnIndex.erase(last, rnIndex.end());
  rnIndex.shrink_to_fit();
}

PhoneMapping PhoneMapping::Builder::build()
{
  auto data = std::make_unique<Data>();
  std::swap(data, data_);
  data->build();
  return PhoneMapping(std::move(data));
}

void PhoneMapping::Builder::commit(std::atomic<Data*> &global)
{
  if (Data *veteran = global.exchange(data_.release()))
    veteran->retire();
}

PhoneMapping::PhoneMapping(std::unique_ptr<Data> data)
{
  CHECK(FLAGS_f14map_prefetch > 0);
  holder_.reset(data.get());
  data->retire();
  data_ = data.release();
}

PhoneMapping::PhoneMapping(std::atomic<Data*> &global)
  : data_(holder_.get_protected(global))
{
  CHECK(FLAGS_f14map_prefetch > 0);
}

PhoneMapping::PhoneMapping(PhoneMapping&& rhs) noexcept = default;
PhoneMapping::~PhoneMapping() noexcept = default;

size_t PhoneMapping::size() const noexcept {
  return data_->pnColumn.size();
}

bool PhoneMapping::hasRow() const noexcept {
  return !!cursor_;
}

uint64_t PhoneMapping::currentPN() const noexcept {
  return cursor_->currentPN();
}

uint64_t PhoneMapping::currentRN() const noexcept {
  return cursor_->currentRN();
}

PhoneMapping& PhoneMapping::advance() noexcept {
  cursor_->advance(data_);
  if (!cursor_->hasRow())
    cursor_.reset();
  return *this;
}
