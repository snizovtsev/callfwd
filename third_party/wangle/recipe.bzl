load("@rules_cc//cc:defs.bzl", "cc_library")
load("@//tools:cmake_configure_file.bzl", "cmake_configure_file")

package(
    default_visibility = ["//visibility:public"],
)

wangle_defs = []
wangle_undefs = []

excludes = [
    "wangle/example/**",
    "wangle/**/test/**",
]
glob_srcs = glob(["wangle/**/*.cpp"], exclude = excludes)
glob_hdrs = glob(["wangle/**/*.h"], exclude = excludes)

cc_library(
    name = "wangle",
    includes = ["."],
    hdrs = glob_hdrs,
    srcs = glob_srcs,
    deps = [
        "@com_github_facebookincubator_fizz//:fizz",
    ]
)
