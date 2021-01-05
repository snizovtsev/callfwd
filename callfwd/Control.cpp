#include "Control.h"

#include <folly/concurrency/CoreCachedSharedPtr.h>
#include <folly/portability/Unistd.h>
#include <folly/portability/SysStat.h>
#include <folly/String.h>
#include <fstream>

static folly::AtomicCoreCachedSharedPtr<PhoneMapping> currentMapping;

void loadMappingFile(const char* fname)
{
  PhoneMappingBuilder builder;
  std::string line;
  std::vector<uint64_t> row;
  size_t nrows = 0;

  struct stat fstat;
  std::vector<char> rbuf(1ull << 19);
  std::ifstream in(fname);

  if (in.fail()) {
    LOG(FATAL) << "Failed to open " << fname;
  }

  memset(&fstat, 0, sizeof(fstat));
  if (lstat(fname, &fstat) == 0) {
    if (fstat.st_size > 1000) {
      size_t lineEstimate = fstat.st_size / 23;
      builder.SizeHint(lineEstimate + lineEstimate / 10);
      LOG(INFO) << "Estimated number of lines: " << lineEstimate;
    }
  }


  LOG(INFO) << "Reading database from " << fname << " ...";
  in.rdbuf()->pubsetbuf(rbuf.data(), rbuf.size());
  while (getline(in, line)) {
    folly::splitTo<uint64_t>(",", line, std::back_inserter(row));
    builder.addMapping(row[0], row[1]);
    row.clear();
    ++nrows;
  }

  if (!in.eof()) {
    LOG(FATAL) << "Read failed on line " << nrows;
  }
  in.close();

  LOG(INFO) << "Building index (" << nrows << " rows)...";

  currentMapping.reset(builder.build());
}

std::shared_ptr<PhoneMapping> getPhoneMapping()
{
  return currentMapping.get();
}
