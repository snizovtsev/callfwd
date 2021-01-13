#include <glog/logging.h>
#include <folly/Format.h>
#include <folly/portability/GFlags.h>
#include <folly/io/async/AsyncUDPSocket.h>
#include <folly/io/IOBufQueue.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>

#include "PhoneMapping.h"
#include "Control.h"

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

DEFINE_uint32(sip_max_length, 1500,
              "Maximum length of a SIP payload");

std::string formatTime(TimePoint tp); // FIXME

inline StringPiece ToStringPiece(str s) {
  return StringPiece(s.s, s.len);
}

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
    LOG(ERROR) << ex.what();
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

    StringPiece user = ToStringPiece(msg_.parsed_uri.user);
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

    auto db = getPhoneMapping();
    uint64_t targetPhone = db->findTarget(userPhone);
    if (targetPhone == PhoneMapping::NONE)
      targetPhone = userPhone;
    std::string target = "+1";
    target += folly::to<std::string>(targetPhone);

    reply(302, "Moved Temporarily");
    StringPiece host = ToStringPiece(msg_.parsed_uri.host);
    StringPiece port = ToStringPiece(msg_.parsed_uri.port);
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
      output("{}: {}\r\n", ToStringPiece(hdr->name),
             rtrimWhitespace(ToStringPiece(hdr->body)));
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

    if (VLOG_IS_ON(1)) {
      google::LogMessage("", 0).stream()
        << peer_ << " - - [" << formatTime(recvtime_) << "] \""
        << ToStringPiece(REQ_LINE(&msg_).method) << " "
        << ToStringPiece(REQ_LINE(&msg_).uri) << "\" "
        << status_ << " 0";
    }
  }

 private:
  AsyncUDPSocket socket_;
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
      handler_.push_back(std::make_unique<SIPHandler>(evb, sockFd));
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


std::unique_ptr<RequestHandlerFactory> makeSipHandlerFactory(std::vector<std::shared_ptr<AsyncUDPSocket>> server)
{
  return std::make_unique<SipHandlerFactory>(std::move(server));
}
