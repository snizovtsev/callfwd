#include <fstream>
#include <folly/Memory.h>
#include <folly/io/async/EventBaseManager.h>
#include <folly/portability/GFlags.h>
#include <folly/portability/Unistd.h>
#include <folly/system/HardwareConcurrency.h>
#include <proxygen/httpserver/HTTPServer.h>

#include "ApiHandler.h"
#include "PhoneMapping.h"


using namespace proxygen;

using folly::SocketAddress;

using Protocol = HTTPServer::Protocol;

DEFINE_int32(http_port, 11000, "Port to listen on with HTTP protocol");
DEFINE_int32(h2_port, 11001, "Port to listen on with HTTP/2 protocol");
DEFINE_string(ip, "localhost", "IP/Hostname to bind to");
DEFINE_int32(threads,
             0,
             "Number of threads to listen on. Numbers <= 0 "
             "will use the number of cores on this machine.");


std::shared_ptr<PhoneMapping> loadMappingFile(const char* fname)
{
  PhoneMappingBuilder builder;
  std::ifstream in(fname);
  std::string line;

  while (getline(in, line)) {
    std::vector<uint64_t> row;
    folly::splitTo<uint64_t>(",", line, std::back_inserter(row));
    builder.addMapping(row[0], row[1]);
  }

  return builder.build();
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();

  std::vector<HTTPServer::IPConfig> IPs = {
    {SocketAddress(FLAGS_ip, FLAGS_http_port, true), Protocol::HTTP},
    {SocketAddress(FLAGS_ip, FLAGS_h2_port, true), Protocol::HTTP2},
  };

  if (FLAGS_threads <= 0) {
    FLAGS_threads = folly::hardware_concurrency();
    CHECK(FLAGS_threads > 0);
  }

  auto db = loadMappingFile(argv[1]);

  HTTPServerOptions options;
  options.threads = static_cast<size_t>(FLAGS_threads);
  options.idleTimeout = std::chrono::milliseconds(60000);
  options.shutdownOn = {SIGINT, SIGTERM};
  options.enableContentCompression = false;
  options.handlerFactories = RequestHandlerChain()
    .addThen(std::move(makeApiHandlerFactory(db)))
    .build();
  // Increase the default flow control to 1MB/10MB
  options.initialReceiveWindow = uint32_t(1 << 20);
  options.receiveStreamWindowSize = uint32_t(1 << 20);
  options.receiveSessionWindowSize = 10 * (1 << 20);
  options.h2cEnabled = true;

  HTTPServer server(std::move(options));
  server.bind(IPs);
  server.start();

  return 0;
}
