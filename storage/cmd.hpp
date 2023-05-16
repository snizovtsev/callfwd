#ifndef CALLFWD_CMD_H_
#define CALLFWD_CMD_H_

#include <arrow/type_fwd.h>

namespace boost::program_options {
  class options_description;
  class variables_map;
}

struct CmdDescription {
  const char *name;
  const char *args;
  const char *abstract;
  const char *help;
};

template <class CmdType>
struct CmdOps {
  static void BindOptions(boost::program_options::options_description& description,
                          typename CmdType::Options& options);
  static arrow::Status StoreArgs(const boost::program_options::variables_map& vm,
                                 const std::vector<std::string>& args,
                                 typename CmdType::Options& options);
};

#endif // CALLFWD_CMD_H_
