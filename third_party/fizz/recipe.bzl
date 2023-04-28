load("@rules_cc//cc:defs.bzl", "cc_library")
load("@//tools:cmake_configure_file.bzl", "cmake_configure_file")

package(
    default_visibility = ["//visibility:public"],
)

fizz_defs = []
fizz_undefs = []

excludes = [
    "fizz/experimental/crypto/exchange/*",
    "fizz/extensions/javacrypto/*",
    "fizz/protocol/BrotliCertificate*",
    "fizz/**/test/**",
    "fizz/tool/*",
]
glob_srcs = glob(["fizz/**/*.cpp"], exclude = excludes)
glob_hdrs = glob(["fizz/**/*.h"], exclude = excludes)

cc_library(
    name = "fizz",
    includes = ["."],
    hdrs = glob_hdrs,
    srcs = glob_srcs,
    linkopts = ["-lssl", "-lcrypto", "-lsodium"],
    deps = [
        "@com_github_facebook_folly//:folly",
    ]
)
