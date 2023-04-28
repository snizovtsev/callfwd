load("@rules_cc//cc:defs.bzl", "cc_library")
load("@//tools:cmake_configure_file.bzl", "cmake_configure_file")

package(
    default_visibility = ["//visibility:public"],
)

folly_defs = [
    "FOLLY_HAVE_PTHREAD",
    "FOLLY_HAVE_PTHREAD_ATFORK",
    "FOLLY_HAVE_ACCEPT4",
    "FOLLY_HAVE_GETRANDOM",
    "FOLLY_HAVE_PREADV",
    "FOLLY_HAVE_PWRITEV",
    "FOLLY_HAVE_CLOCK_GETTIME",
    "FOLLY_HAVE_PIPE2",
    "FOLLY_HAVE_SENDMMSG",
    "FOLLY_HAVE_RECVMMSG",
    "FOLLY_HAVE_OPENSSL_ASN1_TIME_DIFF",
    "FOLLY_HAVE_IFUNC",
    "FOLLY_HAVE_STD__IS_TRIVIALLY_COPYABLE",
    "FOLLY_HAVE_UNALIGNED_ACCESS",
    "FOLLY_HAVE_VLA",
    "FOLLY_HAVE_WEAK_SYMBOLS",
    "FOLLY_HAVE_LINUX_VDSO",
    "FOLLY_HAVE_MALLOC_USABLE_SIZE",
    "FOLLY_HAVE_INT128_T",
    "FOLLY_HAVE_WCHAR_SUPPORT",
    "FOLLY_HAVE_EXTRANDOM_SFMT19937",
    "HAVE_VSNPRINTF_ERRORS",
    "FOLLY_HAVE_SHADOW_LOCAL_WARNINGS",
    "FOLLY_SUPPORT_SHARED_LIBRARY",

    "FOLLY_HAVE_LIBGFLAGS",
    "FOLLY_HAVE_LIBGLOG",
    "FOLLY_GFLAGS_NAMESPACE=gflags",
    "FOLLY_HAVE_LIBZ",
    "FOLLY_HAVE_ELF",
    "FOLLY_HAVE_SWAPCONTEXT",
    "FOLLY_HAVE_BACKTRACE",
    "FOLLY_USE_SYMBOLIZER",
]

folly_undefs = [
    "FOLLY_UNUSUAL_GFLAGS_NAMESPACE",
    "FOLLY_LIBRARY_SANITIZE_ADDRESS",
    "FOLLY_HAVE_LIBRT",
    #"FOLLY_HAVE_LIBLZ4"
    #"FOLLY_HAVE_LIBLZMA"
    #"FOLLY_HAVE_LIBSNAPPY",
    #"FOLLY_HAVE_LIBZSTD"
    #"FOLLY_HAVE_LIBBZ2"
    #"FOLLY_USE_JEMALLOC"
    #"FOLLY_HAVE_LIBUNWIND"
    #"FOLLY_HAVE_DWARF"
]

cmake_configure_file(
    name = "folly_config_h",
    src = "CMake/folly-config.h.cmake",
    out = "folly/folly-config.h",
    defines = folly_defs,
    undefines = folly_undefs,
)

glob_exclude_dirs = [
    "folly/build/**",
    "folly/logging/example/**",
    "folly/docs/**",
    "folly/python/**" # XXX
]

glob_hdrs = glob(["folly/**/*.h"], exclude = glob_exclude_dirs + [
    "folly/test/**/*.h",
    "folly/**/test/**/*.h",
    "folly/python/fibers.h",
    "folly/python/GILAwareManualExecutor.h",
    # with_sodium=0
    "folly/experimental/crypto/Blake2xb.h",
    "folly/experimental/crypto/detail/LtHashInternal.h",
    "folly/experimental/crypto/LtHash-inl.h",
    "folly/experimental/crypto/LtHash.h",
])

glob_srcs = glob(["folly/**/*.cpp"], exclude = glob_exclude_dirs + [
    "folly/Benchmark.cpp",
    "folly/test/**/*.cpp",
    "folly/**/test/**/*.cpp",
    "folly/**/tool/**/*.cpp",
    "folly/**/*Test.cpp",
    "folly/python/error.cpp",
    "folly/python/executor.cpp",
    "folly/python/fibers.cpp",
    "folly/python/GILAwareManualExecutor.cpp",
    "folly/cybld/folly/executor.cpp",
    # with_sodium=0
    "folly/experimental/crypto/Blake2xb.cpp",
    "folly/experimental/crypto/detail/MathOperation_AVX2.cpp",
    "folly/experimental/crypto/detail/MathOperation_Simple.cpp",
    "folly/experimental/crypto/detail/MathOperation_SSE2.cpp",
    "folly/experimental/crypto/LtHash.cpp",
])

cc_library(
    name = "folly",
    includes = [
        ".",
        "../double-conversion",
    ],
    hdrs = glob_hdrs + ["folly/folly-config.h"],
    srcs = glob_srcs,
    linkopts = ["-levent"],
    deps = [
        "@boost//:algorithm",
        "@boost//:context",
        "@boost//:crc",
        "@boost//:filesystem",
        "@boost//:functional",
        "@boost//:intrusive",
        "@boost//:multi_index",
        "@boost//:operators",
        "@boost//:preprocessor",
        "@boost//:program_options",
        "@boost//:variant",
        "@double-conversion//:double-conversion",
        "@com_github_fmtlib_fmt//:fmt",
        "@com_github_google_glog//:glog",
    ]
)
