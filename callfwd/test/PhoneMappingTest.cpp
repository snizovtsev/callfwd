#include <callfwd/PhoneMapping.h>

#include <folly/portability/GTest.h>
#include <folly/portability/GMock.h>

using namespace testing;

TEST(PhoneMappingTest, Empty) {
  auto db = PhoneMappingBuilder().build();
  ASSERT_EQ(db->size(), 0);
  ASSERT_EQ(db->findTarget(555), PhoneMapping::NONE);
  ASSERT_TRUE(db->reverseTarget(111, 222).empty());
}

TEST(PhoneMappingTest, Single) {
  auto db = PhoneMappingBuilder()
    .addMapping(555, 111)
    .build();
  ASSERT_EQ(db->size(), 1);
  ASSERT_EQ(db->findTarget(555), 111);
  ASSERT_EQ(db->findTarget(666), PhoneMapping::NONE);
  ASSERT_TRUE(db->reverseTarget(111, 111).empty());
  ASSERT_THAT(db->reverseTarget(111, 112), ElementsAre(555));
  ASSERT_TRUE(db->reverseTarget(112, 111).empty());
}

TEST(PhoneMappingTest, Bijection) {
  auto builder = PhoneMappingBuilder();
  for (size_t i = 100; i <= 999; ++i)
    builder.addMapping(i, i);

  auto db = builder.build();
  for (size_t i = 102; i <= 999; ++i) {
    ASSERT_EQ(db->findTarget(i), i);
    ASSERT_THAT(db->reverseTarget(i, i+1), ElementsAre(i));
    ASSERT_THAT(db->reverseTarget(i-2, i+1), ElementsAre(i-2, i-1, i));
  }
  ASSERT_EQ(db->reverseTarget(0, 1000).size(), 900);
}

TEST(PhoneMappingTest, Constant) {
  auto builder = PhoneMappingBuilder();
  for (size_t i = 100; i <= 999; ++i)
    builder.addMapping(i, 42);

  auto db = builder.build();
  ASSERT_EQ(db->reverseTarget(42, 43).size(), 900);
}

TEST(PhoneMappingTest, LastDigit) {
  auto builder = PhoneMappingBuilder();
  for (size_t i = 100; i <= 999; ++i)
    builder.addMapping(i, i % 10);

  auto db = builder.build();
  ASSERT_EQ(db->reverseTarget(2, 5).size(), 90*3);
}
