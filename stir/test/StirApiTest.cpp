#include <stir/StirApiTypes.h>

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <folly/Preprocessor.h>
#include <folly/dynamic.h>
#include <proxygen/httpserver/Mocks.h>
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>

using namespace proxygen;
using namespace testing;
using folly::dynamic;

TEST(StirApiTypes, ErrorCodeSanity) {
  for (int c = 0; c < STIR_API_ERROR_MAX; ++c) {
    StirApiError err;
    ASSERT_FALSE(err);

    switch (static_cast<StirApiErrorCode>(c)) {
#define GENTEST(token)                                          \
      case token:                                               \
        err = token;                                            \
        EXPECT_STREQ(FOLLY_PP_STRINGIZE(token), err.reflect());    \
        break;
      GENTEST(STIR_SVC_MISSING_BODY);
      GENTEST(STIR_SVC_MISSING_INFORMATION);
      GENTEST(STIR_SVC_NOT_ACCEPTABLE_RESPONSE_BODY_TYPE);
      GENTEST(STIR_SVC_RESOURCE_NOT_FOUND);
      GENTEST(STIR_SVC_UNSUPPORTED_REQUEST_BODY_TYPE);
      GENTEST(STIR_SVC_INVALID_PARAMETER_VALUE);
      GENTEST(STIR_SVC_FAILED_TO_PARSE_MSG_BODY);
      GENTEST(STIR_SVC_MISSING_BODY_LENGTH);
      GENTEST(STIR_POL_METHOD_NOT_ALLOWED);
      GENTEST(STIR_POL_INTERNAL_ERROR);
#undef GENTEST
    case STIR_API_ERROR_MAX:
      folly::assume_unreachable();
    // intentionally leave no default case to force
    // compiler generate warning on a missing case
    }
    ASSERT_TRUE(err);
  }
}

template<class Ops>
void ExpectParseError(const dynamic& bad, const char* id, const char* param) {
  auto result = Ops::fromJson(bad);
  if (!result) {
    dynamic detail = result.error().toJson()["serviceException"];
    EXPECT_THAT(detail["messageId"].asString(), Eq(id));
    EXPECT_THAT(detail["variables"][0].asString(), Eq(param));
  } else {
    FAIL();
  }
}

#define EXPECT_PARSE_ERROR(id, param)           \
  do {                                          \
    SCOPED_TRACE(param);                        \
    ExpectParseError<Ops>(bad, id, param);      \
  } while (0)

TEST(StirApiTypes, Signing) {
  using Ops = StirApiType<SigningRequest>;

  dynamic sample = dynamic::object
    ("attest", "A")
    ("orig", dynamic::object("tn", "+1-2155-551-212"))
    ("dest", dynamic::object("tn", dynamic::array("12355551212")))
    ("iat", 1443208345)
    ("origid", "de305d54-75b4-431b-adb2-eb6b9e546014");

  SigningRequest msg = Ops::fromJson(sample).value();
  EXPECT_THAT(msg.attest, Eq("A"));
  EXPECT_THAT(msg.orig, Eq("+1-2155-551-212"));
  EXPECT_THAT(msg.dest, ElementsAre("12355551212"));
  EXPECT_EQ(msg.iat, 1443208345);
  EXPECT_THAT(msg.origid, Eq("de305d54-75b4-431b-adb2-eb6b9e546014"));

  // SVC4001: missing parameter
  for (auto param : {"attest", "orig", "dest", "iat", "origid"}) {
    dynamic bad = sample;
    bad.erase(param);
    EXPECT_PARSE_ERROR("SVC4001", param);
  }

  // SVC4005: invalid value
  dynamic bad;
#define CORRUPT(param, val)                     \
  bad = sample; bad[param] = val;               \
  EXPECT_PARSE_ERROR("SVC4005", param);

  CORRUPT("iat", "nonnumber"); // must be number
  CORRUPT("iat", -1000); // must be positive
  CORRUPT("iat", "99999999999999999999999"); // too large
  CORRUPT("iat", std::nan("")); // bad value
  CORRUPT("iat", 1e100); // too large
  CORRUPT("origid", dynamic::array(1)); // bad type
  CORRUPT("dest", dynamic::object("tn", "12155551212")); // bad type
  CORRUPT("dest", dynamic::object("tn", dynamic::array())); // empty
  CORRUPT("attest", dynamic::object("type", "A")); // bad type
  CORRUPT("attest", "X"); // bad value
  CORRUPT("origid", "not-an-uuid"); // bad value
  CORRUPT("orig", "+1 2155 551 212"); // bad value
#undef CORRUPT
}

TEST(StirApiTypes, Verification) {
  using Ops = StirApiType<VerificationRequest>;

  dynamic sample = dynamic::object
    ("from", dynamic::object("tn", "12155551212"))
    ("to", dynamic::object("tn", dynamic::array("12355551212")))
    ("time", 1443208345)
    ("identity",
     "eyJhbGciOiJFUzI1NiIsInR5cCI6InBhc3Nwb3J0IiwicHB0Ijoic2hha2VuIiwieDV1IjoiaHR0cDov"
     "L2NlcnQtYXV0aC5wb2Muc3lzLmNvbWNhc3QubmV0L2V4YW1wbGUuY2VydCJ9eyJhdHRlc3QiOiJBIiwiZGVzdC"
     "I6eyJ0biI6IisxMjE1NTU1MTIxMyJ9LCJpYXQiOiIxNDcxMzc1NDE4Iiwib3JpZyI6eyJ0biI64oCdKzEyMTU1NTUxMj"
     "EyIn0sIm9yaWdpZCI6IjEyM2U0NTY3LWU4OWItMTJkMy1hNDU2LTQyNjY1NTQ0MDAwMCJ9._28kAwRWnheX"
     "yA6nY4MvmK5JKHZH9hSYkWI4g75mnq9Tj2lW4WPm0PlvudoGaj7wM5XujZUTb_3MA4modoDtCA;info=<https:/"
     "/cert.example2.net/example.cert>");

  VerificationRequest msg = Ops::fromJson(sample).value();
  EXPECT_THAT(msg.from, Eq("12155551212"));
  EXPECT_THAT(msg.to, ElementsAre("12355551212"));
  EXPECT_EQ(msg.time, 1443208345);
  EXPECT_TRUE(msg.identity.size() > 0);
}

#if 0
std::unique_ptr<RequestHandlerFactory> makeHttpStirSigning();

class ShakenRestFixture : public testing::Test {
 public:
  void SetUp() override {
    auto router = makeHttpStirSigning();
    request.reset(new HTTPMessage);
    request->setURL("/stir/v1/signing");
    handler = router->onRequest(nullptr, request.get());
    responseHandler = std::make_unique<MockResponseHandler>(handler);
    handler->setResponseHandler(responseHandler.get());
  }

  void TearDown() override {
    Mock::VerifyAndClear(responseHandler.get());
  }

 protected:
  std::unique_ptr<HTTPMessage> request;
  RequestHandler* handler{nullptr};
  std::unique_ptr<MockResponseHandler> responseHandler;
  HTTPMessage response;
};

TEST_F(ShakenRestFixture, RequestId) {
  HTTPMessage response;
  EXPECT_CALL(*responseHandler, sendHeaders(_))
      .WillOnce(DoAll(SaveArg<0>(&response), Return()));
  EXPECT_CALL(*responseHandler, sendEOM()).WillOnce(Return());

  // Since we know we dont touch request, its ok to pass an empty message here.
  request->getHeaders().add("X-RequestID", "passthrough");
  handler->onRequest(std::move(request));
  handler->onEOM();
  handler->requestComplete();

  EXPECT_EQ("passthrough", response.getHeaders().getSingleOrEmpty("X-RequestID"));
  //EXPECT_EQ(200, response.getStatusCode());
}
#endif
