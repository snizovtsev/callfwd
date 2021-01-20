#include "PhoneMapping.h"

#include <algorithm>
#include <array>
#include <stdexcept>
#include <vector>
#if HAVE_STD_PARALLEL
#include <execution>
#endif
#include <glog/logging.h>
#include <folly/json.h>
#include <folly/dynamic.h>
#include <folly/Likely.h>
#include <folly/String.h>
#include <folly/Conv.h>
#include <folly/small_vector.h>
#include <folly/container/F14Map.h>
#include <folly/synchronization/Hazptr.h>
#include <folly/portability/GFlags.h>


// TODO: benchmark prefetch size
DEFINE_uint32(f14map_prefetch, 16, "Maximum number of keys to prefetch");

struct PhoneList {
  uint64_t phone : 34, next : 30;
};
static constexpr uint64_t MAXROWS = (1 << 30) - 1;
static_assert(sizeof(PhoneList) == 8, "");

class PhoneMapping::Data : public folly::hazptr_obj_base<PhoneMapping::Data> {
 public:
  void getRNs(size_t N, const uint64_t *pn, uint64_t *rn) const;
  std::unique_ptr<Cursor> inverseRNs(uint64_t fromRN, uint64_t toRN) const;
  std::unique_ptr<Cursor> visitRows() const;
  void build();
  ~Data() noexcept;

  // metadata
  folly::dynamic meta;
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
        rn[i] = PhoneNumber::NONE;
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
  InverseRNVisitor(const PhoneMapping::Data *data, uint64_t it, uint64_t end)
    : base_(data->pnColumn.data())
    , it_(it), end_(end)
  {
    prefetch(data);
  }
  void refill() override;
 private:
  const PhoneList *base_;
  uint64_t it_, end_;
};

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
PhoneMapping::Data::inverseRNs(uint64_t fromRN, uint64_t toRN) const {
  std::vector<uint64_t> ret;

  static auto cmp = [](const PhoneList &lhs, const PhoneList &rhs) {
    return lhs.phone < rhs.phone;
  };
  auto rnLeft = std::lower_bound(rnIndex.begin(), rnIndex.end(), PhoneList{fromRN, 0}, cmp);
  auto rnRight = std::lower_bound(rnLeft, rnIndex.end(), PhoneList{toRN, 0}, cmp);
  uint64_t pnBegin = rnLeft == rnIndex.end() ? MAXROWS : rnLeft->next;
  uint64_t pnEnd = rnRight == rnIndex.end() ? MAXROWS : rnRight->next;

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
PhoneMapping::Data::visitRows() const {
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
  for (; it_ != end_ && size_ < pn_.size(); it_ = base_[it_].next) {
    pn_[size_++] = base_[it_].phone;
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

void PhoneMapping::Builder::sizeHint(size_t numRecords) {
  data_->pnColumn.reserve(numRecords);
  data_->rnIndex.reserve(numRecords);
  data_->dict.reserve(numRecords);
}

void PhoneMapping::Builder::setMetadata(const folly::dynamic &meta) {
  data_->meta = meta;
}

PhoneMapping::Builder& PhoneMapping::Builder::addRow(uint64_t pn, uint64_t rn) {
  if (data_->dict.count(pn))
    throw std::runtime_error("PhoneMapping::Builder: duplicate key");
  if (data_->pnColumn.size() >= MAXROWS)
    throw std::runtime_error("PhoneMapping::Builder: too much rows");

  data_->dict.emplace(pn, rn);
  data_->pnColumn.push_back(PhoneList{pn, MAXROWS});
  data_->rnIndex.push_back(PhoneList{rn, MAXROWS});
  return *this;
}

void PhoneMapping::Builder::fromCSV(std::istream &in, size_t &line, size_t limit) {
  std::string linebuf;
  std::vector<uint64_t> rowbuf;

  for (limit += line; line < limit; ++line) {
    if (in.peek() == EOF)
      break;
    std::getline(in, linebuf);
    rowbuf.clear();
    folly::split(',', linebuf, rowbuf);
    if (rowbuf.size() == 2)
      addRow(rowbuf[0], rowbuf[1]);
    else
      throw std::runtime_error("bad number of columns");
  }
}

void PhoneMapping::Data::build() {
  size_t N = pnColumn.size();

  // Connect rnIndex_ with pnColumn_ before shuffling
  for (size_t i = 0; i < N; ++i)
    rnIndex[i].next = i;

  static auto cmp = [](const PhoneList &lhs, const PhoneList &rhs) {
    if (lhs.phone == rhs.phone)
      return lhs.next < rhs.next;
    return lhs.phone < rhs.phone;
  };

#if HAVE_STD_PARALLEL
  std::stable_sort(std::execution::par_unseq, rnIndex.begin(), rnIndex.end(), cmp);
#else
  std::stable_sort(rnIndex.begin(), rnIndex.end(), cmp);
#endif

  // Wire pnColumn_ list by target
  for (size_t i = 0; i + 1 < N; ++i)
    pnColumn[rnIndex[i].next].next = rnIndex[i+1].next;

  static auto equal = [](const PhoneList &lhs, const PhoneList &rhs) {
    return lhs.phone == rhs.phone;
  };
  auto last = std::unique(rnIndex.begin(), rnIndex.end(), equal);
  rnIndex.erase(last, rnIndex.end());
  rnIndex.shrink_to_fit();
}

PhoneMapping PhoneMapping::Builder::build() {
  auto data = std::make_unique<Data>();
  std::swap(data, data_);
  data->build();
  return PhoneMapping(std::move(data));
}

void PhoneMapping::Builder::commit(std::atomic<Data*> &global) {
  auto data = std::make_unique<Data>();
  std::swap(data, data_);
  data->build();

  size_t pn_count = data->pnColumn.size();
  size_t rn_count = data->rnIndex.size();
  if (Data *veteran = global.exchange(data.release()))
    veteran->retire();
  LOG(INFO) << "Database updated: PNs=" << pn_count << " RNs=" << rn_count;
}

PhoneMapping::PhoneMapping(std::unique_ptr<Data> data) {
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

void PhoneMapping::printMetadata() {
  if (!data_)
    return;
  LOG(INFO) << "Current mapping info:";
  for (auto kv : data_->meta.items())
    LOG(INFO) << "  " << kv.first.asString()
                  << ": " << folly::toJson(kv.second);
}

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

uint64_t PhoneNumber::fromString(folly::StringPiece s) {
  std::string digits;

  // Remove punctuation
  s = folly::trimWhitespace(s);
  if (s.size() > 30) // Don't waste time on garbage
    return NONE;

  digits.reserve(s.size());
  std::copy_if(s.begin(), s.end(), std::back_inserter(digits),
               [](char c) { return strchr(" -()", c) == NULL; });
  s = digits;

  if (s.size() == 12) {
    s.removePrefix("+1");
  } else if (s.size() == 11) {
    s.removePrefix("1");
  }

  if (s.size() != 10)
    return NONE;

  if (auto asInt = folly::tryTo<uint64_t>(s))
    return asInt.value();
  return NONE;
}
