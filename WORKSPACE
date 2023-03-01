load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

# Import toolchain repositories for remote executions, but register the
# toolchains using --extra_toolchains on the command line to get precedence.
local_repository(
    name = "remote_config_cc",
    path = "tools/remote-toolchains/ubuntu-act-22-04/local_config_cc",
)

local_repository(
    name = "remote_config_sh",
    path = "tools/remote-toolchains/ubuntu-act-22-04/local_config_sh",
)

git_repository(
    name = "com_github_nelhage_rules_boost",
    remote = "https://github.com/nelhage/rules_boost",
    commit = "0c7d4fe3ada3c15d12449387bb39075c95f61134",
    shallow_since = "1676626529 +0000",
)

load("@com_github_nelhage_rules_boost//:boost/boost.bzl", "boost_deps")
boost_deps()

git_repository(
    name = "com_github_fmtlib_fmt",
    remote = "https://github.com/fmtlib/fmt",
    commit = "a33701196adfad74917046096bf5a2aa0ab0bb50", # v9.1.0
    shallow_since = "1661615830 -0700",
    patch_cmds = [
        "mv support/bazel/.bazelrc .bazelrc",
        "mv support/bazel/.bazelversion .bazelversion",
        "mv support/bazel/BUILD.bazel BUILD.bazel",
        "mv support/bazel/WORKSPACE.bazel WORKSPACE.bazel",
    ],
)

git_repository(
    name = "com_github_facebook_folly",
    remote = "https://github.com/facebook/folly",
    commit = "1c74accaed8e224a7867404822ab58acb70b5c8a", # v2023.02.13.00
    shallow_since = "1676136354 -0800",
    build_file = "//third_party/folly:recipe.bzl",
)

git_repository(
	name = "double-conversion",
    remote = "https://github.com/google/double-conversion",
    commit = "af09fd65fcf24eee95dc62813ba9123414635428", # v3.2.1
    shallow_since = "1657216703 +0200",
)

git_repository(
    name = "com_github_gflags_gflags",
    remote = "https://github.com/gflags/gflags",
    commit = "e171aa2d15ed9eb17054558e0b3a6a413bb01067", # v2.2.2
    shallow_since = "1541971260 +0000",
)

git_repository(
    name = "com_github_google_glog",
    remote = "https://github.com/google/glog",
    commit = "b33e3bad4c46c8a6345525fd822af355e5ef9446", # v0.6.0
    shallow_since = "1649109807 +0200",
)

git_repository(
    name = "com_github_liquidaty_zsv",
    remote = "https://github.com/liquidaty/zsv",
    commit = "9172d237bbeae9bbd2d717ed6f45c5169fb20a51", # v0.3.5-alpha
    shallow_since = "1674061223 -0800",
    build_file = "//third_party/zsv:recipe.bzl",
)

git_repository(
    name = "com_github_apache_arrow",
    remote = "https://github.com/apache/arrow",
    commit = "f10f5cfd1376fb0e602334588b3f3624d41dee7d", # v11.0.0
    shallow_since = "1674031127 +0100",
    build_file = "//third_party/arrow:recipe.bzl",
)

git_repository(
    name = "com_github_xtensor-stack_xsimd",
    remote = "https://github.com/xtensor-stack/xsimd",
    commit = "e12bf0a928bd6668ff701db55803a9e316cb386c", # v10.0.0
    shallow_since = "1669803947 +0100",
    build_file = "//third_party/xsimd:recipe.bzl",
)

git_repository(
    name = "com_grail_bazel_compdb",
    remote = "https://github.com/grailbio/bazel-compilation-database",
    commit = "765d43e7e0f8f28e07a48609c79407e7c133a6f8", # v0.5.2
)

load("@com_grail_bazel_compdb//:deps.bzl", "bazel_compdb_deps")
bazel_compdb_deps()
