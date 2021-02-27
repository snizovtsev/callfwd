#ifndef STIR_STIR_API_TYPES_H
#define STIR_STIR_API_TYPES_H

#include <folly/FBString.h>
#include <folly/FBVector.h>
#include <folly/Range.h>
#include <folly/Expected.h>

namespace folly {
  struct dynamic;
}

struct SigningRequest {
  folly::fbstring attest;
  folly::fbvector<folly::fbstring> dest;
  uint64_t iat;
  folly::fbstring orig;
  folly::fbstring origid;
};

struct SigningResponse {
  folly::fbstring identity;
};

struct VerificationRequest {
  folly::fbstring identity;
  folly::fbvector<folly::fbstring> to;
  folly::fbstring from;
  uint64_t time;
};

struct VerificationResponse {
  int reasonCode;
  folly::fbstring verStat;
  folly::fbstring reasonText;
  folly::fbstring reasonDesc;
};

enum StirApiErrorCode {
  STIR_SVC_MISSING_BODY = 0,
  STIR_SVC_MISSING_INFORMATION,
  STIR_SVC_NOT_ACCEPTABLE_RESPONSE_BODY_TYPE,
  STIR_SVC_RESOURCE_NOT_FOUND,
  STIR_SVC_UNSUPPORTED_REQUEST_BODY_TYPE,
  STIR_SVC_INVALID_PARAMETER_VALUE,
  STIR_SVC_FAILED_TO_PARSE_MSG_BODY,
  STIR_SVC_MISSING_BODY_LENGTH,
  STIR_POL_METHOD_NOT_ALLOWED,
  STIR_POL_INTERNAL_ERROR,
  STIR_API_ERROR_MAX,
};

class StirApiErrorClass;
class StirApiError {
 public:
  StirApiError() noexcept = default;
  /* implicit */ StirApiError(StirApiErrorCode code) noexcept;

  void putVariable(folly::StringPiece value);
  folly::dynamic toJson() const;
  std::string toBody() const;

  operator bool() const noexcept { return kind_ != nullptr; }
  const char* reflect() const noexcept;
  int http_status() const noexcept;

 private:
  const StirApiErrorClass *kind_ = nullptr;
  folly::fbstring vars_;
};

template<class M>
struct ResponseFor {
  using type = void;
};

template<class M>
struct StirApiType {
  static StirApiError validate(const M &msg);
  static folly::Expected<M, StirApiError> fromJson(const folly::dynamic& json);
  static folly::dynamic toJson(const typename ResponseFor<M>::type &msg);
  static std::string toBody(const typename ResponseFor<M>::type &msg);
};

template<>
struct ResponseFor<SigningRequest> {
  using type = SigningResponse;
};

template<>
struct ResponseFor<VerificationRequest> {
  using type = VerificationResponse;
};


#endif // STIR_STIR_API_TYPES_H
