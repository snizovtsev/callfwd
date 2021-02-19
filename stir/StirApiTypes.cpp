#include "StirApiTypes.h"

#include <folly/lang/Assume.h>
#include <folly/dynamic.h>
#include <folly/json.h>
#include <folly/Conv.h>
#include <folly/DynamicConverter.h>

#include <uuid.h>
#include <algorithm>

using folly::dynamic;
using folly::StringPiece;
using folly::fbstring;
using folly::fbvector;


struct StirApiErrorClass {
  const char *id;
  const char *reflect;
  const char *text;
  int http_status;
};

static StirApiErrorClass shakenApiError[] = {
  { "SVC4000", "SHAKEN_SVC_MISSING_BODY", "Missing request body", 400 },
  { "SVC4001", "SHAKEN_SVC_MISSING_INFORMATION", "Missing mandatory parameter ‘%1’", 400 },
  { "SVC4002", "SHAKEN_SVC_NOT_ACCEPTABLE_RESPONSE_BODY_TYPE", "Requested response body type ‘%1’ is not supported", 406 },
  { "SVC4003", "SHAKEN_SVC_RESOURCE_NOT_FOUND", "Requested resource was not found", 404 },
  { "SVC4004", "SHAKEN_SVC_UNSUPPORTED_REQUEST_BODY_TYPE", "Unsupported request body type, expected ‘%1‘", 415 },
  { "SVC4005", "SHAKEN_SVC_INVALID_PARAMETER_VALUE", "Invalid ‘%1’ parameter value: %2", 400 },
  { "SVC4006", "SHAKEN_SVC_FAILED_TO_PARSE_MSG_BODY", "Failed to parse received message body: %1", 400 },
  { "SVC4007", "SHAKEN_SVC_MISSING_BODY_LENGTH", "Missing mandatory Content-Length header", 411 },
  { "POL4050", "SHAKEN_POL_METHOD_NOT_ALLOWED", "Method not allowed", 405 },
  { "POL5000", "SHAKEN_POL_INTERNAL_ERROR", "Internal Server Error. Please try again later", 500 },
};

StirApiError::StirApiError(StirApiErrorCode code) noexcept
  : kind_(&shakenApiError[code])
{
}

int StirApiError::http_status() const noexcept {
  return kind_->http_status;
}

const char* StirApiError::reflect() const noexcept {
  return kind_->reflect;
}

void StirApiError::putVariable(StringPiece value) {
  if (!vars_.empty())
    vars_ += '\t';
  vars_.append(value.data(), value.size());
}

dynamic StirApiError::toJson() const {
  dynamic vars = dynamic::array;
  if (!vars_.empty()) {
    folly::splitTo<StringPiece>('\t', vars_, std::back_inserter(vars));
  }

  dynamic body = dynamic::object
    ("messageId", kind_->id)
    ("text", kind_->text)
    ("variables", std::move(vars));

  StringPiece type = StringPiece(kind_->id, 3);
  dynamic exception;

  if (type == "SVC") {
    exception = dynamic::object("serviceException", std::move(body));
  } else if (type == "POL") {
    exception = dynamic::object("policyException", std::move(body));
  } else {
    folly::assume_unreachable();
  }

  return exception;
}

std::string StirApiError::toBody() const {
  return folly::toJson(dynamic::object("requestError", toJson()));
}

template<class Message>
struct FromJsonVisitor {
  Message msg;
  const char* param = nullptr;
  const dynamic& unwrap(const dynamic &d);
  void visit(const dynamic &d);
};

template<class M> folly::Expected<M, StirApiError>
StirApiType<M>::fromJson(const dynamic &d) {
  FromJsonVisitor<M> visitor;
  StirApiError err;

  try {
    if (d.isString()) {
      dynamic json = parseJson(d.stringPiece());
      visitor.visit(visitor.unwrap(json));
    } else {
      visitor.visit(d);
    }
    if (err = validate(visitor.msg))
      return folly::makeUnexpected(std::move(err));
    return std::move(visitor.msg);
  } catch (const folly::json::parse_error& e) {
    err = STIR_SVC_FAILED_TO_PARSE_MSG_BODY;
    err.putVariable("invalid JSON body");
  } catch (const std::out_of_range &ex) {
    err = STIR_SVC_MISSING_INFORMATION;
    err.putVariable(visitor.param);
  } catch (const folly::TypeError &ex) {
    err = STIR_SVC_INVALID_PARAMETER_VALUE;
    err.putVariable(visitor.param);
    err.putVariable(ex.what());
  } catch (const folly::ConversionError &ex) {
    err = STIR_SVC_INVALID_PARAMETER_VALUE;
    err.putVariable(visitor.param);
    err.putVariable(ex.what());
  }
  return folly::makeUnexpected(std::move(err));
}

static bool validateTN(const fbstring &tn) {
  static const char* acceptedChars = "0123456789*#+.-()";
  auto pred = [](char c) { return !!strchr(acceptedChars, c); };
  return std::all_of(tn.begin(), tn.end(), pred);
}

template<> StirApiError
StirApiType<SigningRequest>::validate(const SigningRequest &m) {
  StirApiError err = STIR_SVC_INVALID_PARAMETER_VALUE;
  uuid_t origid;

  if (m.attest != "A" && m.attest != "B" && m.attest != "C") {
    err.putVariable("attest");
    err.putVariable("Must be 'A', 'B' or 'C'");
  } else if (uuid_parse(m.origid.c_str(), origid) == -1) {
    err.putVariable("origid");
    err.putVariable("should correspond to a UUID (RFC 4122)");
  } else if (m.dest.empty()) {
    err.putVariable("dest");
    err.putVariable("should contain one or more TN");
  } else if (!validateTN(m.orig)) {
    err.putVariable("orig");
    err.putVariable("Only [0-9*#+.-()] characters allowed for TN");
  } else if (!std::all_of(m.dest.begin(), m.dest.end(), validateTN)) {
    err.putVariable("dest");
    err.putVariable("Only [0-9*#+.-()] characters allowed for TN");
  } else {
    return {};
  }
  return err;
}

template<> StirApiError
StirApiType<VerificationRequest>::validate(const VerificationRequest &m) {
  StirApiError err = STIR_SVC_INVALID_PARAMETER_VALUE;
  if (m.to.empty()) {
    err.putVariable("to");
    err.putVariable("should contain one or more TN");
  } else if (!validateTN(m.from)) {
    err.putVariable("from");
    err.putVariable("Only [0-9*#+.-()] characters allowed for TN");
  } else if (!std::all_of(m.to.begin(), m.to.end(), validateTN)) {
    err.putVariable("to");
    err.putVariable("Only [0-9*#+.-()] characters allowed for TN");
  } else {
    return {};
  }
  return err;
}

template<> void FromJsonVisitor<SigningRequest>::visit(const dynamic &d) {
  param = "attest";
  msg.attest = folly::convertTo<fbstring>(d[param]);
  param = "dest";
  msg.dest = folly::convertTo<fbvector<fbstring>>(d[param]["tn"]);
  param = "orig";
  msg.orig = folly::convertTo<fbstring>(d[param]["tn"]);
  param = "origid";
  msg.origid = folly::convertTo<fbstring>(d[param]);
  param = "iat";
  msg.iat = folly::convertTo<uint64_t>(d[param]);
}

template<> const dynamic& FromJsonVisitor<SigningRequest>::unwrap(const dynamic &d) {
  param = "signingRequest";
  return d[param];
}

template<> dynamic
StirApiType<SigningRequest>::toJson(const SigningResponse& resp) {
  return dynamic::object
    ("identity", StringPiece(resp.identity));
}

template<> std::string
StirApiType<SigningRequest>::toBody(const SigningResponse& resp) {
  return folly::toJson(dynamic::object("signingResponse", toJson(resp)));
}

template<> void FromJsonVisitor<VerificationRequest>::visit(const dynamic &d) {
  param = "identity";
  msg.identity = folly::convertTo<fbstring>(d[param]);
  param = "time";
  msg.time = folly::convertTo<uint64_t>(d[param]);
  param = "from";
  msg.from = folly::convertTo<fbstring>(d[param]["tn"]);
  param = "to";
  msg.to = folly::convertTo<fbvector<fbstring>>(d[param]["tn"]);
}

template<> const dynamic& FromJsonVisitor<VerificationRequest>::unwrap(const dynamic &d) {
  param = "verificationRequest";
  return d[param];
}

template<> dynamic
StirApiType<VerificationRequest>::toJson(const VerificationResponse& resp) {
  return dynamic::object
    ("reasoncode", resp.reasonCode)
    ("reasontext", StringPiece(resp.reasonText))
    ("reasondesc", StringPiece(resp.reasonDesc))
    ("verstat", StringPiece(resp.verStat));
}

template<> std::string
StirApiType<VerificationRequest>::toBody(const VerificationResponse& resp) {
  return folly::toJson(dynamic::object("verificationResponse", toJson(resp)));
}

template struct StirApiType<SigningRequest>;
template struct StirApiType<VerificationRequest>;
