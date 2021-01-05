#include <cstdio>
#include <folly/FileUtil.h>
#include <glog/logging.h>

extern "C" {
#include <lib/osips_parser/msg_parser.h>
#include <lib/osips_parser/parse_uri.h>
#include <lib/osips_parser/contact/parse_contact.h>
}

uint64_t mapNumber(uint64_t phone) {
  if (phone == 2018660805)
    return 2018209999;
  return phone;
}

int main(int argc, const char* argv[]) {
  std::vector<char> buf;
  struct sip_uri contact;
  str contact_uri;
  struct sip_msg msg;

  memset(&msg, 0, sizeof(msg));
  memset(&contact, 0, sizeof(contact));

  CHECK(folly::readFile(argv[1], buf));
  msg.buf = buf.data();
  msg.len = buf.size();

  if (msg.len > 0 && parse_msg(msg.buf, msg.len, &msg) != 0) {
    printf("parse_msg: ERROR\n");
    return 1;
  }
  if (parse_sip_msg_uri(&msg) != 0) {
    printf("parse_sip_msg_uri: ERROR\n");
    return 1;
  }
  if (parse_contact(msg.contact) != 0) {
    printf("parse_contact: ERROR\n");
    return 1;
  }
  contact_uri = ((contact_body_t*) msg.contact->parsed)->contacts->uri;
  if (parse_uri(contact_uri.s, contact_uri.len, &contact) != 0) {
    printf("parse_uri: ERROR\n");
    return 1;
  }

  printf("OK\n");
  printf(" method:  <%.*s>\n",msg.first_line.u.request.method.len,
         msg.first_line.u.request.method.s);
  printf(" uri:     <%.*s>\n",msg.first_line.u.request.uri.len,
         msg.first_line.u.request.uri.s);
  printf(" user:    <%.*s>\n",msg.parsed_uri.user.len,
         msg.parsed_uri.user.s);
  printf(" version: <%.*s>\n",msg.first_line.u.request.version.len,
         msg.first_line.u.request.version.s);

  printf(">=======\n%.*s", msg.len, msg.buf);
  printf("<=======\n");
  printf("SIP/2.0 302 Moved Temporarily\r\n");
  printf("%.*s: %.*s\r\n", msg.h_via1->name.len, msg.h_via1->name.s,
         msg.h_via1->body.len, msg.h_via1->body.s);
  printf("%.*s: %.*s\r\n", msg.from->name.len, msg.from->name.s,
         msg.from->body.len, msg.from->body.s);
  printf("%.*s: %.*s\r\n", msg.to->name.len, msg.to->name.s,
         msg.to->body.len, msg.to->body.s);
  printf("%.*s: %.*s\r\n", msg.callid->name.len, msg.callid->name.s,
         msg.callid->body.len, msg.callid->body.s);
  printf("%.*s: %.*s\r\n", msg.cseq->name.len, msg.cseq->name.s,
         msg.cseq->body.len, msg.cseq->body.s);

  char cbufa[100];
  str cbuf; cbuf.s = cbufa; cbuf.len = 100;
  contact.transport.s = NULL;
  contact.transport.len = 0;
  contact.u_name[contact.u_params_no].s = "rn";
  contact.u_name[contact.u_params_no].len = 2;
  contact.u_val[contact.u_params_no].s = "+1235";
  contact.u_val[contact.u_params_no++].len = 4;
  contact.u_name[contact.u_params_no].s = "ndpi";
  contact.u_name[contact.u_params_no++].len = 4;

  print_uri(&contact, &cbuf);
  printf("Contact: <%.*s>\r\n", cbuf.len, cbuf.s);


  printf("Location-Info: N\r\n");
  printf("Content-Length: 0\r\n");
  printf("\r\n");

  //Via: SIP/2.0/UDP 204.9.202.155:4319;branch=z9h64bK1953406209
  //From: <sip:18666654386@204.9.202.155:4319;user=phone>;tag=339948729
  //To: <sip:12018660805@204.9.202.153:5060;user=phone>;tag=13bd22f6
  //Call-ID: s0-9bca09cc-0fd4fc38-7f21bd45-2d08fdc6
  //CSeq: 2311677411 INVITE
  //Contact: <sip:+12018660805;rn=+12018209999;npdi@204.9.202.153:5060>
  //Location-Info: N
  //Content-Length: 0

  return 0;
}

#if 0
// the main logic of our echo server; receives a string and writes it straight
// back
class SipHandler : public wangle::HandlerAdapter<wangle::AcceptPipelineType> {
 public:
  void read(Context* ctx, wangle::AcceptPipelineType msg) override {
    std::cout << "handling " << "msg" << std::endl;
    write(ctx, std::move(msg));
  }
};

// where we define the chain of handlers for each messeage received
class SipPipelineFactory final : public wangle::AcceptPipelineFactory {
 public:
  typename wangle::AcceptPipeline::Ptr newPipeline(wangle::Acceptor* acceptor) override {
    auto pipeline = wangle::AcceptPipeline::create();
    //pipeline->addBack(wangle::AsyncSocketHandler(sock));
    //pipeline->addBack(wangle::LineBasedFrameDecoder(8192));
    //pipeline->addBack(wangle::StringCodec());
    std::cout << "udp connection!" << std::endl;
    pipeline->addBack(SipHandler());
    pipeline->finalize();
    return pipeline;
  }
};

class UDPAcceptor : public folly::AsyncUDPServerSocket::Callback {
 public:
  UDPAcceptor(folly::EventBase* evb) : evb_(evb) {}

  void onListenStarted() noexcept override {}

  void onListenStopped() noexcept override {}

  void onDataAvailable(
      std::shared_ptr<folly::AsyncUDPSocket> socket,
      const folly::SocketAddress& client,
      std::unique_ptr<folly::IOBuf> data,
      bool /*unused*/,
      OnDataAvailableParams /*unused*/) noexcept override {
    // send pong
    socket->write(client, data->clone());
  }

 private:
  folly::EventBase* const evb_{nullptr};
};

class UDPServer {
 public:
  UDPServer(folly::EventBase* evb, folly::SocketAddress addr)
    : evb_(evb), addr_(addr)
  {}

  void start() {
    CHECK(evb_->isInEventBaseThread());

    socket_ = std::make_unique<folly::AsyncUDPServerSocket>(evb_, 5060);

    try {
      socket_->bind(addr_);
      VLOG(4) << "Server listening on " << socket_->address().describe();
    } catch (const std::exception& ex) {
      LOG(FATAL) << ex.what();
    }

    // Add numWorkers thread
    #if 0
    int i = 0;
    for (auto& evb : evbs_) {
      acceptors_.emplace_back(&evb);

      socket_->addListener(&evb, &acceptors_[i]);
      threads_.emplace_back(std::move(t));
      ++i;
    }
    #endif

    socket_->listen();
  }

  void shutdown() {
    CHECK(evb_->isInEventBaseThread());
    socket_->close();
    socket_.reset();
  }

 private:
  folly::EventBase* evb_;
  const folly::SocketAddress addr_;
  std::unique_ptr<folly::AsyncUDPServerSocket> socket_;
};
#endif
