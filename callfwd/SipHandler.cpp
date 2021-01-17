#include <glog/logging.h>
#include <folly/Format.h>
#include <folly/portability/GFlags.h>
#include <folly/io/async/AsyncUDPSocket.h>
#include <folly/io/IOBufQueue.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>

#include "CallFwd.h"
#include "PhoneMapping.h"

extern "C" {
#include <lib/osips_parser/msg_parser.h>
#include <lib/osips_parser/parse_uri.h>
}

using proxygen::RequestHandlerFactory;
using proxygen::RequestHandler;
using proxygen::TimePoint;
using folly::StringPiece;
using folly::NetworkSocket;
using folly::SocketAddress;
using folly::AsyncUDPSocket;
using folly::EventBase;

DEFINE_uint32(sip_max_length, 1500, "Maximum length of a SIP payload");

inline StringPiece SP(str s) { return StringPiece(s.s, s.len); }

class SIPHandler : public AsyncUDPSocket::ReadCallback {
 public:
  explicit SIPHandler(EventBase *evb, NetworkSocket sockfd)
    : socket_(evb)
    , recvbuf_(FLAGS_sip_max_length)
  {
    socket_.setFD(sockfd, AsyncUDPSocket::FDOwnership::SHARED);
    socket_.resumeRead(this);
  }

  void getReadBuffer(void** buf, size_t* len) noexcept override {
    *buf = recvbuf_.data();
    *len = recvbuf_.size();
  }

  void onReadError(const folly::AsyncSocketException& ex) noexcept override {
    LOG_FIRST_N(ERROR, 200) << ex.what();
    socket_.resumeRead(this);
  }

  void onReadClosed() noexcept override {
  }

  void onDataAvailable(
      const SocketAddress& client,
      size_t len, bool truncated,
      OnDataAvailableParams /*params*/) noexcept override
  {
    status_ = 0;
    recvtime_ = TimePoint::clock::now();
    peer_ = client;
    reply_.clear();

    if (parseMessage(len, truncated) != 0) {
      finishWithError();
      return;
    }

    log_.onRequest(peer_, SP(REQ_LINE(&msg_).method), SP(REQ_LINE(&msg_).uri),
                   proxygen::toTimeT(recvtime_));

    switch (msg_.REQ_METHOD) {
    case METHOD_OPTIONS:
      replyAndFinish(200, "OK");
      break;
    case METHOD_INVITE:
      handleInvite();
      break;
    }
  }

  int parseMessage(size_t len, bool truncated) {
    if (len == 0 || truncated)
      return -1;

    memset(&msg_, 0, sizeof(msg_));
    msg_.buf = recvbuf_.data();
    msg_.len = len;
    if (parse_msg(msg_.buf, msg_.len, &msg_) != 0)
      return -1;
    if (msg_.first_line.type != SIP_REQUEST)
      return -1;

    return 0;
  }

  void handleInvite()
  {
    switch (checkACL(peer_.getIPAddress())) {
    case 429:
      replyAndFinish(429, "Too Many Requests");
      return;
    case 401:
      replyAndFinish(401, "Unauthorized");
      return;
    }

    if (parse_sip_msg_uri(&msg_) != 0) {
      finishWithError();
      return;
    }

    StringPiece user = SP(msg_.parsed_uri.user);
    if (!(user.removePrefix("+1") || user.removePrefix("1"))) {
      finishWithError();
      return;
    }
    uint64_t userPhone;
    if (auto result = folly::tryTo<uint64_t>(user)) {
      userPhone = result.value();
    } else {
      finishWithError();
      return;
    }

    uint64_t targetPhone = PhoneMapping::get().getRN(userPhone);
    if (targetPhone == PhoneNumber::NOTFOUND)
      targetPhone = userPhone;
    std::string target = "+1";
    target += folly::to<std::string>(targetPhone);

    reply(302, "Moved Temporarily");
    StringPiece host = SP(msg_.parsed_uri.host);
    StringPiece port = SP(msg_.parsed_uri.port);
    output("Contact: <sip:+1{};rn={};ndpi@{}:{}>\r\n",
           user, target, host, port);
    output("Location-Info: N\r\n");
    finish();
  }

  void reply(uint64_t status, StringPiece message)
  {
    status_ = status;
    output("SIP/2.0 {} {}\r\n", status, message);
    outputHeader(msg_.h_via1);
    outputHeader(msg_.from);
    outputHeader(msg_.to);
    outputHeader(msg_.callid);
    outputHeader(msg_.cseq);
  }

  void replyAndFinish(uint64_t status, StringPiece message)
  {
    reply(status, message);
    finish();
  }

  void outputHeader(const struct hdr_field* hdr) {
    if (hdr) {
      output("{}: {}\r\n", SP(hdr->name),
             rtrimWhitespace(SP(hdr->body)));
    }
  }

  template <class... Args>
  void output(StringPiece fmt, Args&&... args) {
    fmtbuf_.clear();
    folly::format(&fmtbuf_, fmt, std::forward<Args>(args)...);
    reply_.append(fmtbuf_);
  }

  void finishWithError() {
    LOG_FIRST_N(WARNING, 200) << "Bad message received";
  }

  void finish() {
    output("Content-Length: 0\r\n\r\n");
    socket_.writeChain(peer_, reply_.move(), {});
    log_.onResponse(status_, 0);
  }

 private:
  AsyncUDPSocket socket_;
  AccessLogFormatter log_;
  std::vector<char> recvbuf_;
  TimePoint recvtime_;
  std::string fmtbuf_;
  SocketAddress peer_;
  folly::IOBufQueue reply_;

  uint64_t status_;
  struct sip_msg msg_;
};

class SipHandlerFactory : public RequestHandlerFactory {
 public:
  explicit SipHandlerFactory(std::vector<std::shared_ptr<AsyncUDPSocket>> server)
  {
    server_.reserve(server.size());
    for (const auto& socket : server)
      server_.push_back(socket->getNetworkSocket());
  }

  void onServerStart(EventBase* evb) noexcept override {
    for (auto sockFd : server_) {
      auto handler = std::make_unique<SIPHandler>(evb, sockFd);
      handler_.push_back(std::move(handler));
    }
  }

  void onServerStop() noexcept override {
    handler_.clear();
  }

  RequestHandler* onRequest(RequestHandler *h, proxygen::HTTPMessage*) noexcept override {
    return h;
  }

 private:
  std::vector<NetworkSocket> server_;
  std::vector<std::unique_ptr<SIPHandler>> handler_;
};


std::unique_ptr<RequestHandlerFactory>
makeSipHandlerFactory(std::vector<std::shared_ptr<folly::AsyncUDPSocket>> socket)
{
  return std::make_unique<SipHandlerFactory>(std::move(socket));
}
