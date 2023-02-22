load("@rules_cc//cc:defs.bzl", "cc_library")

package(
    default_visibility = ["//visibility:public"],
)

cc_library(
    name = "xsimd",
    includes = [
        "include",
    ],
    hdrs = glob(["include/**/*.hpp"]),
    deps = []
)
