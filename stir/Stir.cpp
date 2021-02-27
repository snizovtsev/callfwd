#include <folly/init/Init.h>
#include <folly/ssl/Init.h>
#include <folly/portability/GFlags.h>
#include <folly/system/HardwareConcurrency.h>
#include <proxygen/httpserver/HTTPServer.h>

using namespace proxygen;

DEFINE_uint32(http_port, 12000, "Port to listen on with HTTP protocol");
DEFINE_uint32(http_idle_timeout, 60, "A timeout to close inactive sessions");
DEFINE_string(http_if1, "localhost", "IP/Hostname to bind HTTP to");
DEFINE_string(http_if2, "", "Additional address to bind HTTP to");
DEFINE_string(http_if3, "", "Additional address to bind HTTP to");
DEFINE_int32(threads, 0,
             "Number of threads to listen on. Numbers <= 0 "
             "will use the number of cores on this machine.");

std::unique_ptr<RequestHandlerFactory> makeStirApi();
std::unique_ptr<RequestHandlerFactory> makeHttpNotFound();

int main(int argc, char *argv[])
{
  folly::Init init(&argc, &argv);
  folly::ssl::init();
  google::InstallFailureSignalHandler();

  std::vector<HTTPServer::IPConfig> IPs;
  for (std::string intf : {FLAGS_http_if1, FLAGS_http_if2, FLAGS_http_if3}) {
    if (!intf.empty()) {
      uint16_t port = FLAGS_http_port;
      IPs.emplace_back(folly::SocketAddress{intf, port, true},
                       HTTPServer::Protocol::HTTP);
    }
  }

  if (FLAGS_threads <= 0) {
    FLAGS_threads = folly::hardware_concurrency();
    CHECK(FLAGS_threads > 0);
  }

  HTTPServerOptions options;
  options.threads = static_cast<size_t>(FLAGS_threads);
  options.idleTimeout = std::chrono::seconds(FLAGS_http_idle_timeout);
  options.shutdownOn = {SIGINT, SIGTERM};
  options.enableContentCompression = false;
  options.h2cEnabled = false;
  // Increase the default flow control to 1MB/10MB
  options.initialReceiveWindow = uint32_t(1 << 20);
  options.receiveStreamWindowSize = uint32_t(1 << 20);
  options.receiveSessionWindowSize = 10 * (1 << 20);
  options.handlerFactories = RequestHandlerChain()
    .addThen(makeStirApi())
    .addThen(makeHttpNotFound())
    .build();

  HTTPServer server(std::move(options));
  LOG(INFO) << "Starting HTTP server on port " << FLAGS_http_port;
  server.bind(IPs);

  LOG(INFO) << "Serving requests";
  server.start();

  return EXIT_SUCCESS;
}
