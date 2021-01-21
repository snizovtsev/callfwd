#include <glog/logging.h>
#include <folly/Format.h>
#include <folly/Likely.h>
#include <folly/portability/GFlags.h>
#include <folly/io/async/AsyncUDPSocket.h>
#include <folly/io/IOBufQueue.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>

#include "PhoneMapping.h"
#include "AccessLog.h"
#include "ACL.h"

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
DEFINE_bool(rfc4694, false, "Follow RFC4694 for non-ported numbers");

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
      LOG_FIRST_N(WARNING, 200) << "Bad SIP message received from " << peer_;
      return;
    }

    log_.onRequest(peer_,
                   SP(REQ_LINE(&msg_).method),
                   SP(REQ_LINE(&msg_).uri),
                   proxygen::toTimeT(recvtime_));

    if (UNLIKELY(!PhoneMapping::isAvailable())) {
      reply(503, "Service Unavailable");
      goto finish;
    }

    switch (msg_.REQ_METHOD) {
    case METHOD_OPTIONS:
      reply(200, "OK");
      break;
    case METHOD_INVITE:
      handleInvite();
      break;
    default:
      reply(405, "Method Not Allowed");
      break;
    }

  finish:
    output("Content-Length: 0\r\n\r\n");
    socket_.writeChain(peer_, reply_.move(), {});
    log_.onResponse(status_, 0);
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
    if (parse_sip_msg_uri(&msg_) != 0)
      return -1;

    return 0;
  }

  void handleInvite()
  {
    switch (ACL::get().isCallAllowed(peer_.getIPAddress())) {
    case 429:
      reply(429, "Too Many Requests");
      return;
    case 401:
      reply(401, "Unauthorized");
      return;
    }

    StringPiece user = SP(msg_.parsed_uri.user);
    StringPiece host = SP(msg_.parsed_uri.host);
    StringPiece port = SP(msg_.parsed_uri.port);

    uint64_t pn = PhoneNumber::fromString(user);
    uint64_t rn = PhoneNumber::NONE;
    if (pn != PhoneNumber::NONE)
      rn = PhoneMapping::getUS().getRN(pn);
    if (rn == PhoneNumber::NONE)
      rn = PhoneMapping::getCA().getRN(pn);

    reply(302, "Moved Temporarily");
    if (rn != PhoneNumber::NONE) {
      output("Contact: <sip:+1{};rn=+1{};ndpi@{}:{}>\r\n",
             pn, rn, host, port);
    } else if (FLAGS_rfc4694) {
      output("Contact: <sip:{};ndpi@{}:{}>\r\n",
             user, host, port);
    } else {
      output("Contact: <sip:{};rn={};ndpi@{}:{}>\r\n",
             user, user, host, port);
    }
    output("Location-Info: N\r\n");
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
