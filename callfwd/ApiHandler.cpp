#include <functional>
#include <gflags/gflags.h>
#include <folly/small_vector.h>
#include <proxygen/lib/http/HTTPCommonHeaders.h>
#include <proxygen/lib/http/HTTPMethod.h>
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <proxygen/httpserver/filters/DirectResponseHandler.h>

#include "PhoneMapping.h"
#include "CallFwd.h"

using namespace proxygen;
using folly::StringPiece;

DEFINE_uint32(max_query_length, 32768,
              "Maximum length of POST x-www-form-urlencoded body");


class TargetHandler final : public RequestHandler {
 public:
  void onRequest(std::unique_ptr<HTTPMessage> req) noexcept override {
    if (req->getMethod() == HTTPMethod::GET) {
      onQueryString(req->getQueryStringAsStringPiece());
      onQueryComplete();
      return;
    }

    if (req->getMethod() == HTTPMethod::POST) {
      using namespace std::placeholders;
      needBody_ = true;
      req->getHeaders()
        .forEachWithCode(std::bind(&TargetHandler::sanitizePostHeader,
                                   this, _1, _2, _3));
      if (needBody_)
        return;
    }

    ResponseBuilder(downstream_)
      .status(400, "Bad Request")
      .sendWithEOM();
  }

  void sanitizePostHeader(HTTPHeaderCode code, const std::string& name,
                          const std::string& value) noexcept {
    switch (code) {
    case HTTP_HEADER_CONTENT_LENGTH:
      if (folly::to<size_t>(value) > FLAGS_max_query_length)
        needBody_ = false;
      break;
    case HTTP_HEADER_CONTENT_TYPE:
      if (value != "application/x-www-form-urlencoded")
        needBody_ = false;
      break;
    default:
      break;
    }
  }

  void onQueryComplete() noexcept {
    for (uint64_t phone : query_) {
      uint64_t target = PhoneMapping::get().getRN(phone);
      if (target != PhoneNumber::NOTFOUND)
        resp_.push_back(target);
      else
        resp_.push_back(phone);
    }

    ResponseBuilder(downstream_)
      .status(200, "OK")
      .header(HTTP_HEADER_CONTENT_TYPE, "text/plain")
      .body(folly::join(",", resp_)+'\n')
      .sendWithEOM();
  }

  void onQueryString(StringPiece query) {
    using namespace std::placeholders;
    auto paramFn = std::bind(&TargetHandler::onQueryParam, this, _1, _2);
    HTTPMessage::splitNameValuePieces(query, '&', '=', std::move(paramFn));
  }

  void onQueryParam(StringPiece name, StringPiece value) {
    if (name == "phone%5B%5D" || name == "phone[]") {
      auto intValue = folly::tryTo<uint64_t>(value);
      if (intValue.hasValue())
        query_.push_back(intValue.value());
    }
  }

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override {
    if (!needBody_)
      return;

    if (body_) {
      body_->prependChain(std::move(body));
    } else {
      body_ = std::move(body);
    }

    if (body_->computeChainDataLength() > FLAGS_max_query_length) {
      needBody_ = false;
      body_.release();
      ResponseBuilder(downstream_)
        .status(400, "Bad Request")
        .sendWithEOM();
    }
  }

  void onEOM() noexcept override {
    if (needBody_) {
      onQueryString(body_ ? StringPiece(body_->coalesce()) : "");
      onQueryComplete();
    }
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
  bool needBody_ = false;
  std::unique_ptr<folly::IOBuf> body_;
  folly::small_vector<uint64_t, 16> query_;
  folly::small_vector<uint64_t, 16> resp_;
};

class ReverseHandler final : public RequestHandler {
 public:
  void onRequest(std::unique_ptr<HTTPMessage> req) noexcept override {
    if (req->getMethod() != HTTPMethod::GET) {
      ResponseBuilder(downstream_)
        .status(400, "Bad Request")
        .send();
      return;
    }

    HTTPMessage::splitNameValuePieces(req->getQueryStringAsStringPiece(), '&', '=',
                                      std::bind(&ReverseHandler::onQueryParam, this,
                                                std::placeholders::_1,
                                                std::placeholders::_2));

    PhoneMapping db = PhoneMapping::get();
    for (std::pair<uint64_t, uint64_t> range : query_) {
      db.inverseRNs(range.first, range.second);
      for (; db.hasRow(); db.advance()) {
        resp_.push_back(db.currentPN());
      }
    }

    ResponseBuilder(downstream_)
      .status(200, "OK")
      .header("Content-Type", "text/plain")
      .body(folly::join(",", resp_))
      .send();
  }

  void onQueryParam(StringPiece name, StringPiece value) {
    if (name == "prefix%5B%5D" && value.size() <= 10) {
      uint64_t from, to;
      auto intValue = folly::tryTo<uint64_t>(value);
      if (!intValue.hasValue())
        return;

      from = intValue.value();
      to = from + 1;

      for (size_t i = 0; i < 10 - value.size(); ++i) {
        from *= 10;
        to *= 10;
      }
      query_.emplace_back(from, to);
    }
  }

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override {
  }

  void onEOM() noexcept override {
    ResponseBuilder(downstream_).sendWithEOM();
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
  std::vector<std::pair<uint64_t, uint64_t>> query_;
  std::vector<uint64_t> resp_;
};

class ApiHandlerFactory : public RequestHandlerFactory {
 public:
  void onServerStart(folly::EventBase* /*evb*/) noexcept override {
  }

  void onServerStop() noexcept override {
  }

  RequestHandler* onRequest(RequestHandler *upstream, HTTPMessage *msg) noexcept override {
    const StringPiece path = msg->getPathAsStringPiece();

    if (path == "/target") {
      return new TargetHandler;
    } else if (path == "/reverse") {
      return new ReverseHandler;
    }

    return new DirectResponseHandler(404, "Not found", "");
  }
};

std::unique_ptr<RequestHandlerFactory> makeApiHandlerFactory()
{
  return std::make_unique<ApiHandlerFactory>();
}
