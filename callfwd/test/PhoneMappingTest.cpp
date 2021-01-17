#include <callfwd/PhoneMapping.h>
#include <folly/portability/GTest.h>
#include <folly/portability/GMock.h>

using namespace testing;

static auto drain(PhoneMapping &db) {
  std::vector<std::pair<uint64_t, uint64_t>> ret;
  for (; db.hasRow(); db.advance())
    ret.emplace_back(db.currentPN(), db.currentRN());
  return ret;
}

TEST(PhoneMappingTest, Empty) {
  PhoneMapping db = PhoneMapping::Builder().build();
  ASSERT_EQ(db.size(), 0);
  ASSERT_EQ(db.getRN(555), PhoneNumber::NOTFOUND);
  ASSERT_FALSE(db.visitRows().hasRow());
  ASSERT_FALSE(db.inverseRNs(111, 222).hasRow());
  folly::hazptr_cleanup();
}

TEST(PhoneMappingTest, One) {
  PhoneMapping db = PhoneMapping::Builder()
    .addRow(555, 111)
    .build();
  ASSERT_EQ(db.size(), 1);
  ASSERT_EQ(db.getRN(555), 111);
  ASSERT_EQ(db.getRN(666), PhoneNumber::NOTFOUND);
  ASSERT_THAT(drain(db.visitRows()), ElementsAre(Pair(555, 111)));
  ASSERT_FALSE(db.inverseRNs(111, 111).hasRow());
  ASSERT_THAT(drain(db.inverseRNs(111, 112)), ElementsAre(Pair(555, 111)));
  ASSERT_THAT(drain(db.inverseRNs(0, 1000)), ElementsAre(Pair(555, 111)));
  ASSERT_FALSE(db.inverseRNs(112, 111).hasRow());
  folly::hazptr_cleanup();
}

TEST(PhoneMappingTest, Identity) {
  PhoneMapping::Builder builder;
  size_t from = 100;
  size_t to = 300;
  for (size_t i = from; i < to; ++i)
    builder.addRow(i, i);

  PhoneMapping db = builder.build();
  for (size_t l = from-10; l < to+10; ++l) {
    for (size_t r = from-10; r <= to+10; ++r) {
      db.inverseRNs(l, r);
      if (l >= r || r <= from || l >= to) {
        ASSERT_FALSE(db.hasRow());
        continue;
      }

      size_t vl = std::max<size_t>(l, from);
      size_t vr = std::min<size_t>(r, to);
      for (size_t i = vl; i < vr; ++i) {
        ASSERT_TRUE(db.hasRow());
        ASSERT_EQ(db.currentPN(), i);
        ASSERT_EQ(db.currentRN(), i);
        db.advance();
      }
      ASSERT_FALSE(db.hasRow());
    }
  }
  ASSERT_EQ(drain(db.visitRows()).size(), to-from);
  folly::hazptr_cleanup();
}

TEST(PhoneMappingTest, Constant) {
  PhoneMapping::Builder builder;
  for (size_t i = 100; i <= 999; ++i)
    builder.addRow(i, 42);

  PhoneMapping db = builder.build();
  db.inverseRNs(42, 43);
  for (size_t i = 100; i <= 999; ++i) {
    ASSERT_TRUE(db.hasRow());
    ASSERT_EQ(db.currentPN(), i);
    ASSERT_EQ(db.currentRN(), 42);
    db.advance();
  }
  ASSERT_FALSE(db.hasRow());
  folly::hazptr_cleanup();
}

TEST(PhoneMappingTest, LastDigit) {
  PhoneMapping::Builder builder;
  for (size_t i = 999; i >= 100; --i)
    builder.addRow(i, i % 10);

  PhoneMapping db = builder.build();
  ASSERT_EQ(drain(db.inverseRNs(2, 5)).size(), 90*3);
  ASSERT_EQ(drain(db.inverseRNs(8, 15)).size(), 90*2);
  folly::hazptr_cleanup();
}
