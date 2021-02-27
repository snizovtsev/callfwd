#include <stir/StirApiTypes.h>
#include <stir/Passport.h>

#include <folly/Range.h>
#include <folly/String.h>
#include <folly/dynamic.h>
#include <proxygen/lib/http/RFC2616.h>
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <proxygen/httpserver/filters/DirectResponseHandler.h>

#include <uuid.h>
#include <memory>

using namespace proxygen;
using folly::StringPiece;

DEFINE_uint32(stir_api_body_limit, 32768, "Maximum size of STIR API message");


StringPiece getHttpAcceptValue(StringPiece headerValue) {
  RFC2616::TokenPairVec acceptTok;
  RFC2616::parseQvalues(headerValue, acceptTok);
  if (acceptTok.size() > 0)
    return folly::trimWhitespace(acceptTok[0].first);
  else
    return {};
}

struct StirApiHeaderVisitor {
  StirApiError error;
  std::string requestId;
  int64_t contentLength = -1;
  bool jsonInput = false;

  void visit(HTTPHeaderCode code, StringPiece name, StringPiece value);
  StirApiError finalize();

  void operator()(HTTPHeaderCode code, StringPiece name, StringPiece value) {
    visit(code, name, value);
  }
};

void StirApiHeaderVisitor::visit(HTTPHeaderCode code, StringPiece name,
                                 StringPiece value) {
  StringPiece token;

  switch (code) {
  case HTTP_HEADER_CONTENT_LENGTH:
    if (auto maybe = folly::tryTo<uint32_t>(value))
      contentLength = maybe.value();
    else
      contentLength = 0;
    break;
  case HTTP_HEADER_CONTENT_TYPE:
    token = value;
    token = token.split_step(";");
    token = folly::trimWhitespace(token);
    if (token == "application/json")
      jsonInput = true;
    break;
  case HTTP_HEADER_ACCEPT:
    token = getHttpAcceptValue(value);
    if (token != "application/json" && token != "*/*") {
      error = STIR_SVC_NOT_ACCEPTABLE_RESPONSE_BODY_TYPE;
      error.putVariable(token);
    }
    break;
  case HTTP_HEADER_OTHER:
    token = name;
    if (token.equals("X-RequestID", folly::AsciiCaseInsensitive()))
      requestId = value;
    break;
  default:
    // ignore
    break;
  }
}

StirApiError StirApiHeaderVisitor::finalize() {
  if (!jsonInput) {
    error = STIR_SVC_UNSUPPORTED_REQUEST_BODY_TYPE;
    error.putVariable("application/json");
  } else if (contentLength == -1) {
    error = STIR_SVC_MISSING_BODY_LENGTH;
  } else if (contentLength >= FLAGS_stir_api_body_limit) {
    error = STIR_SVC_FAILED_TO_PARSE_MSG_BODY;
    error.putVariable("invalid message body length specified");
  }
  return std::move(error);
}

class StirApiHandler final : public RequestHandler {
 public:
  void onRequest(std::unique_ptr<HTTPMessage> req) noexcept override {
    const StringPiece path = req->getPathAsStringPiece();
    StirApiHeaderVisitor hVisitor;

    // Find or generate RequestId
    req->getHeaders().forEachWithCode(std::ref(hVisitor));
    if (hVisitor.requestId.empty()) {
      generateRequestId();
    } else {
      requestId_ = std::move(hVisitor.requestId);
    }

    if (req->getMethod() != HTTPMethod::POST) {
      error_ = STIR_POL_METHOD_NOT_ALLOWED;
      return;
    }

    if (path == "/stir/v1/signing") {
      endpoint_ = Endpoint::SIGN;
    } else if (path == "/stir/v1/verification") {
      endpoint_ = Endpoint::VERIFY;
    } else {
      error_ = STIR_SVC_RESOURCE_NOT_FOUND;
      return;
    }

    error_ = hVisitor.finalize();
    if (!error_)
      body_.reserve(hVisitor.contentLength);
  }

  void onBody(std::unique_ptr<folly::IOBuf> pkt) noexcept override {
    if (!error_)
      body_ += StringPiece(pkt->coalesce());
  }

  void generateRequestId() {
    uuid_t randomId;
    uuid_generate_random(randomId);
    requestId_.resize(37);
    uuid_unparse(randomId, requestId_.data());
  }

  void onEOM() noexcept override {
    if (!error_)
      handleRequest();

    if (error_) {
      body_ = error_.toBody();
      sendResponse(error_.http_status());
    } else {
      sendResponse(200);
    }
  }

  void handleRequest() {
    using SignMsg = StirApiType<SigningRequest>;
    using VerifyMsg = StirApiType<VerificationRequest>;

    folly::Expected<std::string, StirApiError> response;

    switch (endpoint_) {
    case Endpoint::SIGN:
      response = SignMsg::fromJson(std::move(body_))
        .then([this](SigningRequest req) {
          return SignMsg::toBody(doSigning(req));
        });
      break;
    case Endpoint::VERIFY:
      response = VerifyMsg::fromJson(std::move(body_))
        .then([this](VerificationRequest req) {
          return VerifyMsg::toBody(doVerification(req));
        });
      break;
    }

    if (response.hasError())
      error_ = std::move(response).error();
    else
      body_ = std::move(response).value();
  }

  SigningResponse doSigning(const SigningRequest &req) {
    return { makePassport(req) };
  }

  VerificationResponse doVerification(const VerificationRequest &req) {
    return verifyPassport(req);
  }

  void sendResponse(int status) {
    ResponseBuilder(downstream_)
      .status(status, HTTPMessage::getDefaultReason(status))
      .header("X-RequestID", requestId_)
      .header(HTTP_HEADER_CONTENT_TYPE, "application/json")
      .body(std::move(body_))
      .sendWithEOM();
  }

  void onUpgrade(UpgradeProtocol proto) noexcept override {
    // handler doesn't support upgrades
  }

  void requestComplete() noexcept override {
    delete this;
  }

  void onError(ProxygenError err) noexcept override {
    delete this;
  }

 private:
  enum class Endpoint { SIGN, VERIFY };

  std::string requestId_;
  Endpoint endpoint_;
  StirApiError error_;
  std::string body_;
};

class StirApiFactory : public RequestHandlerFactory {
 public:
  void onServerStart(folly::EventBase* /*evb*/) noexcept override {
  }

  void onServerStop() noexcept override {
  }

  RequestHandler* onRequest(RequestHandler *upstream, HTTPMessage *msg) noexcept override {
    const StringPiece path = msg->getPathAsStringPiece();
    if (path.startsWith("/stir/v1/")) {
      return new StirApiHandler;
    } else {
      return upstream;
    }
  }
};

std::unique_ptr<RequestHandlerFactory> makeStirApi()
{
  return std::make_unique<StirApiFactory>();
}

class HttpNotFound : public RequestHandlerFactory {
 public:
  void onServerStart(folly::EventBase* /*evb*/) noexcept override {
  }

  void onServerStop() noexcept override {
  }

  RequestHandler* onRequest(RequestHandler *upstream, HTTPMessage *msg) noexcept override {
    return new DirectResponseHandler(404, "Not Found", "");
  }
};

std::unique_ptr<RequestHandlerFactory> makeHttpNotFound()
{
  return std::make_unique<HttpNotFound>();
}
