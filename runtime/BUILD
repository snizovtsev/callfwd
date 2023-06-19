load("@rules_cc//cc:defs.bzl", "cc_binary")

package(
    default_visibility = ["//visibility:public"],
)

cc_binary(
    name = "callfwd",
    srcs = [
        "AccessLog.cpp",
        "AccessLog.h",
        "ACL.cpp",
        "ACL.h",
        "ApiHandler.cpp",
        "CallFwd.cpp",
        "CallFwd.h",
        "Control.cpp",
        "Location.cpp",
        "Location.h",
        "PhoneMapping.cpp",
        "PhoneMapping.h",
        "SipHandler.cpp",
    ],
    linkopts = ["-lsystemd"],
    deps = [
        "//third_party/arrow:arrow",
        "//osips_parser:osips_parser",
        "//third_party/proxygen:proxygen",
    ],
)
