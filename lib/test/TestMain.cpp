// Use this main function in gtest unit tests to enable glog
#include <folly/portability/GFlags.h>
#include <folly/portability/GTest.h>
#include <folly/init/Init.h>
#include <folly/synchronization/HazptrDomain.h>
#include <glog/logging.h>

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  google::InstallFailureSignalHandler();
  folly::Init init(&argc, &argv);
  google::LogToStderr();
  int status = RUN_ALL_TESTS();
  folly::hazptr_cleanup();
  return status;
}
