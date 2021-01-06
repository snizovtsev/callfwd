#include "Control.h"

#include <systemd/sd-daemon.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <fstream>
#include <functional>
#include <thread>
#include <folly/concurrency/CoreCachedSharedPtr.h>
#include <folly/portability/Unistd.h>
#include <folly/portability/SysStat.h>
#include <folly/String.h>
#include <folly/Conv.h>

static folly::AtomicCoreCachedSharedPtr<PhoneMapping> currentMapping;

std::shared_ptr<PhoneMapping> getPhoneMapping()
{
  return currentMapping.get();
}

static void loadMappingFile(std::ifstream &in, PhoneMappingBuilder &builder)
{
  std::vector<char> rbuf(1ull << 19);
  std::string line;
  std::vector<uint64_t> row;
  size_t nrows = 0;

  in.rdbuf()->pubsetbuf(rbuf.data(), rbuf.size());
  while (getline(in, line)) {
    try {
      folly::splitTo<uint64_t>(",", line, std::back_inserter(row));
      builder.addMapping(row[0], row[1]);
    } catch (std::runtime_error& e) {
      LOG(ERROR) << "Read failed on line " << nrows << ": " << e.what();
      in.close();
      return;
    }
    row.clear();
    ++nrows;
  }

  if (!in.eof()) {
    in.close();
    LOG(ERROR) << "Read failed on line " << nrows;
    return;
  }
  in.close();

  LOG(INFO) << "Building index (" << nrows << " rows)...";

  currentMapping.reset(builder.build());
}

static void loadMappingFile(int fd)
{
  PhoneMappingBuilder builder;

  std::string fname = "/proc/self/fd/";
  fname += folly::to<std::string>(fd);
  std::ifstream in(fname);

  CHECK(!in.fail());

  struct stat s;
  memset(&s, 0, sizeof(s));

  if (fstat(fd, &s) == 0) {
    if (s.st_size > 1000) {
      size_t lineEstimate = s.st_size / 23;
      builder.SizeHint(lineEstimate + lineEstimate / 10);
      LOG(INFO) << "Estimated number of lines: " << lineEstimate;
    }
  }

  LOG(INFO) << "Reloading database";
  loadMappingFile(in, builder);
}

void loadMappingFile(const char* fname)
{
  PhoneMappingBuilder builder;
  std::ifstream in(fname);
  CHECK(!in.fail()) << "Failed to open " << fname;

  struct stat s;
  memset(&s, 0, sizeof(s));

  if (lstat(fname, &s) == 0) {
    if (s.st_size > 1000) {
      size_t lineEstimate = s.st_size / 23;
      builder.SizeHint(lineEstimate + lineEstimate / 10);
      LOG(INFO) << "Estimated number of lines: " << lineEstimate;
    }
  }

  LOG(INFO) << "Reading database from " << fname << " ...";
  loadMappingFile(in, builder);
}

static void controlThread(int fd) {
  std::string pkt(1500, 'x');
  union {
      char buf[256];
      struct cmsghdr align;
  } u;

  struct iovec iov{pkt.data(), pkt.length()};
  struct msghdr msg;
  struct cmsghdr *c;

  while (true) {
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_name = NULL;
    msg.msg_namelen = 0;
    msg.msg_flags = 0;
    msg.msg_control = u.buf;
    msg.msg_controllen = sizeof(u.buf);

    ssize_t ret = recvmsg(fd, &msg, MSG_CMSG_CLOEXEC);
    if (ret <= 0) {
      LOG(WARNING) << "Control socket error";
      continue;
    }

    folly::StringPiece cmd{pkt.data(), (size_t)ret};
    std::vector<int> fds;

    for (c = CMSG_FIRSTHDR(&msg); c != NULL; c = CMSG_NXTHDR(&msg, c)) {
      if (c->cmsg_len == 0)
        continue;
      if (c->cmsg_level == SOL_SOCKET && c->cmsg_type == SCM_RIGHTS) {
        fds.resize((c->cmsg_len - sizeof(struct cmsghdr)) / sizeof(int));
        memcpy(fds.data(), CMSG_DATA(c), fds.size()*sizeof(int));
        break;
      }
    }

    if (cmd == "LOAD_DB" && fds.size() == 1) {
      loadMappingFile(fds[0]);
    } else {
      LOG(WARNING) << "Unrecognized command: " << cmd << "(fds: " << fds.size() << ")";
    }

    for (int fd : fds)
      close(fd);
  }
}

void startControlSocket() {
  CHECK(sd_listen_fds(0) == 1) << "callfwd should be launched using systemd";
  int fd = SD_LISTEN_FDS_START + 0;
  std::thread(std::bind(controlThread, fd)).detach();
}
