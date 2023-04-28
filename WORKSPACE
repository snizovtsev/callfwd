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
    commit = "e3b9b1e936e1d27908a18ee27c62274db87ca5ab", # v2023.04.24.00
    shallow_since = "1682123835 -0700",
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
    # https://github.com/google/glog/issues/847
    # commit = "b33e3bad4c46c8a6345525fd822af355e5ef9446", # v0.6.0
    # shallow_since = "1649109807 +0200",
    commit = "674283420118bb919f83ceb3d9dee31ef43ff3aa",
    shallow_since = "1677583609 +0100",
)

git_repository(
    name = "com_github_liquidaty_zsv",
    remote = "https://github.com/liquidaty/zsv",
    commit = "f77bc9e3e1124e3ca70e282d2a4789f7071408af",
    shallow_since = "1677774739 -0800",
    build_file = "//third_party/zsv:recipe.bzl",
)

git_repository(
    name = "com_github_apache_arrow",
    build_file = "//third_party/arrow:recipe.bzl",
    #remote = "https://github.com/apache/arrow",
    #commit = "f10f5cfd1376fb0e602334588b3f3624d41dee7d", # v11.0.0
    #shallow_since = "1674031127 +0100",
    #commit = "d422137d8a4d7578bdf9d5b0fb51b286a8bc92c2", # v12.0.0.dev
    #shallow_since = "1675089126 +0100",
    remote = "https://github.com/westonpace/arrow/",
    commit = "9278f2747ffd20718515f4067f56d024de91a893",
    shallow_since = "1681308258 -0700",
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
    shallow_since = "1631243742 +0000",
)

git_repository(
    name = "com_github_facebook_proxygen",
    remote = "https://github.com/facebook/proxygen",
    commit = "c677c88c2566d3aec0f08769fd5d7fde8d9deab7", # v2023.04.24.00
    shallow_since = "1682220536 -0700",
    build_file = "//third_party/proxygen:recipe.bzl",
)

git_repository(
    name = "com_github_facebookincubator_fizz",
    remote = "https://github.com/facebookincubator/fizz",
    commit = "d7bcaa8d7272ba152d80e2ce5e0dd1f3a030995c", # v2023.04.24.00
    shallow_since = "1682128324 -0700",
    build_file = "//third_party/fizz:recipe.bzl",
)

git_repository(
    name = "com_github_facebook_wangle",
    remote = "https://github.com/facebook/wangle",
    commit = "379c9092f06420d010509b8f33bcdcdf75e00d0c", # v2023.04.24.00
    shallow_since = "1682194417 -0700",
    build_file = "//third_party/wangle:recipe.bzl",
)

load("@com_grail_bazel_compdb//:deps.bzl", "bazel_compdb_deps")
bazel_compdb_deps()
