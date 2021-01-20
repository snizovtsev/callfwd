#include "ACL.h"

#include <unordered_map>
#include <iomanip> // get_time
#include <folly/TokenBucket.h>
#include <folly/IPAddress.h>
#include <folly/Optional.h>
#include <folly/String.h>
#include <folly/Conv.h>
#include <folly/synchronization/Hazptr.h>
#include <proxygen/lib/utils/Time.h>

using folly::IPAddress;
using folly::Optional;
using proxygen::SystemTimePoint;

class ACL::Rule {
 public:
  SystemTimePoint created;
  Optional<SystemTimePoint> expire;
  mutable folly::TokenBucket callBudget{1e10, 1e10};
};

class ACL::Data : public folly::hazptr_obj_base<ACL::Data> {
 public:
  std::unordered_map<IPAddress, ACL::Rule> origin;
};

int ACL::isCallAllowed(const IPAddress &peer) const
{
  /* Deny unless rule exists */
  if (!data_)
    return 401;
  auto it = data_->origin.find(peer);
  if (it == data_->origin.cend())
    return 401;

  const ACL::Rule& rule = it->second;
  auto now = SystemTimePoint::clock::now();

  if (rule.expire && now >= *rule.expire)
    return 401;

  if (rule.callBudget.consume(1))
    return 200;
  else
    return 429;
}

static SystemTimePoint parsePostgresTime(std::string value) {
  std::tm tm = {};
  std::stringstream ss(std::move(value));
  ss.exceptions(std::ios_base::failbit);
  //2015-10-09 08:00:00+00
  ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
  return SystemTimePoint::clock::from_time_t(std::mktime(&tm));
}

std::unique_ptr<ACL::Data> ACL::fromCSV(std::istream &in, size_t& line) {
  std::string linebuf;
  std::vector<folly::StringPiece> row;
  auto data = std::make_unique<Data>();
  ACL::Rule rule;

  while (in.peek() != EOF) {
    row.clear();
    std::getline(in, linebuf);
    folly::split(',', linebuf, row);
    ++line;

    if (row.size() == 4) {
      if (!row[1].empty())
        rule.created = parsePostgresTime(row[1].str());
      if (!row[2].empty())
        rule.expire = parsePostgresTime(row[2].str());
      if (auto cps = folly::tryTo<double>(row[3]))
        rule.callBudget.reset(*cps, std::max(*cps, 1.));
      data->origin[folly::IPAddress(row[0])] = std::move(rule);
    } else {
      throw std::runtime_error("bad number of columns");
    }
  }

  return data;
}

void ACL::commit(std::unique_ptr<Data> recruit, std::atomic<Data*> &global) {
  // TODO: preserve current bucket levels
  auto veteran = global.exchange(recruit.release());
  if (veteran)
    veteran->retire();
}

ACL::ACL(std::unique_ptr<Data> data) {
  holder_.reset(data.get());
  data->retire();
  data_ = data.release();
}

ACL::ACL(std::atomic<Data*> &global)
  : data_(holder_.get_protected(global))
{
}

ACL::ACL(ACL&& rhs) noexcept = default;
ACL::~ACL() noexcept = default;
void std::default_delete<ACL::Data>::operator()(ACL::Data *ptr) const { delete ptr; }
