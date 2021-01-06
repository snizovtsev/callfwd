#include "Control.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <fstream>
#include <functional>
#include <thread>
#include <systemd/sd-daemon.h>
#include <glog/logging.h>
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

static bool loadMappingFile(std::ifstream &in, PhoneMappingBuilder &builder)
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
      return false;
    }
    row.clear();
    ++nrows;
  }

  if (!in.eof()) {
    in.close();
    LOG(ERROR) << "Read failed on line " << nrows;
    return false;
  }
  in.close();

  LOG(INFO) << "Building index (" << nrows << " rows)...";
  currentMapping.reset(builder.build());
  return true;
}

static bool loadMappingFile(int fd)
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
  return loadMappingFile(in, builder);
}

bool loadMappingFile(const char* fname)
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
  return loadMappingFile(in, builder);
}

static bool verifyMappingFile(int fd)
{
  std::string fname = "/proc/self/fd/";
  fname += folly::to<std::string>(fd);
  std::ifstream in(fname);

  CHECK(!in.fail());

  LOG(INFO) << "Verifying database";
  auto db = getPhoneMapping();
  std::string line;
  std::vector<uint64_t> row;
  size_t nrows = 0;

  while (getline(in, line)) {
    try {
      folly::splitTo<uint64_t>(",", line, std::back_inserter(row));
    } catch (std::runtime_error& e) {
      LOG(ERROR) << "Read failed on line " << nrows << ": " << e.what();
      in.close();
      return false;
    }

    if (db->findTarget(row[0]) != row[1]) {
      LOG(ERROR) << (nrows+1) << ": key " << row[0] << " differs";
    }

    row.clear();
    ++nrows;
  }

  if (!in.eof()) {
    in.close();
    LOG(ERROR) << "Read failed on line " << nrows;
    return false;
  }

  if (nrows != db->size()) {
    LOG(ERROR) << "Loaded DB has " << db->size() - nrows << " extra rows";
    return false;
  }

  in.close();
  return true;
}

static bool dumpMappingFile(int fd)
{
  std::string fname = "/proc/self/fd/";
  fname += folly::to<std::string>(fd);
  std::ofstream out(fname);

  LOG(INFO) << "Dumping database";
  size_t nrows = 0;
  PhoneMappingDumper dumper(getPhoneMapping());

  while (dumper.hasNext()) {
    out << dumper.currentSource() << "," << dumper.currentTarget() << "\r\n";
    dumper.moveNext();
  }

  out.flush();
  if (out.fail()) {
    out.close();
    LOG(ERROR) << "Write failed on line " << nrows;
    return false;
  }

  out.close();
  return true;
}

class FdLogSink : public google::LogSink {
public:
  FdLogSink(int fd)
    : fd_(fd)
  {
  }

  ~FdLogSink() override = default;

  void send(google::LogSeverity severity, const char* full_filename,
            const char* base_filename, int line,
            const struct ::tm* tm_time,
            const char* message, size_t message_len) override
  {
    std::string buf = ToString(severity, base_filename, line,
                               tm_time, message, message_len);
    buf += '\n';
    write(fd_, buf.data(), buf.size());
  }

private:
  int fd_;
};

static void controlThread(int sock) {
  std::string pkt(1500, 'x');
  union {
      char buf[256];
      struct cmsghdr align;
  } u;

  struct iovec iov{pkt.data(), pkt.length()};
  struct msghdr msg;
  struct cmsghdr *c;
  struct sockaddr_un peer;

  while (true) {
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_name = &peer;
    msg.msg_namelen = sizeof(peer);
    msg.msg_flags = 0;
    msg.msg_control = u.buf;
    msg.msg_controllen = sizeof(u.buf);

    ssize_t ret = recvmsg(sock, &msg, MSG_CMSG_CLOEXEC);
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

    std::unique_ptr<google::LogSink> stderr;

    if (fds.size() > 0) {
      stderr.reset(new FdLogSink(fds[0]));
      google::AddLogSink(stderr.get());
    }

    char success = 'F';
    if (cmd == "LOAD_DB" && fds.size() == 2) {
      if (loadMappingFile(fds[1]))
        success = 'S';
    } else if (cmd == "VERIFY_DB" && fds.size() == 2) {
      if (verifyMappingFile(fds[1]))
        success = 'S';
    } else if (cmd == "DUMP_DB" && fds.size() == 2) {
      if (dumpMappingFile(fds[1]))
        success = 'S';
    } else {
      LOG(WARNING) << "Unrecognized command: " << cmd << "(fds: " << fds.size() << ")";
    }

    if (msg.msg_namelen > 0) {
      if (sendto(sock, &success, 1, 0, (struct sockaddr*)&peer, msg.msg_namelen) < 0)
        PLOG(WARNING) << "sendto";
    }

    google::RemoveLogSink(stderr.get());
    for (int fd : fds)
      close(fd);
  }
}

void startControlSocket() {
  CHECK(sd_listen_fds(0) == 1) << "callfwd should be launched using systemd";
  int fd = SD_LISTEN_FDS_START + 0;
  std::thread(std::bind(controlThread, fd)).detach();
}
