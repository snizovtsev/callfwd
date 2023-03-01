load("@rules_cc//cc:defs.bzl", "cc_library")
load("@//tools:cmake_configure_file.bzl", "cmake_configure_file")

package(
    default_visibility = ["//visibility:public"],
)

arrow_defs = [
    "ARROW_VERSION_MAJOR=0",
    "ARROW_VERSION_MINOR=0",
    "ARROW_VERSION_PATCH=0",
    "ARROW_VERSION=0.0.0",
    "ARROW_SO_VERSION=unknown",
    "ARROW_FULL_SO_VERSION=unknown",
    "CMAKE_CXX_COMPILER_ID=unknown",
    "CMAKE_CXX_COMPILER_VERSION=unknown",
    "CMAKE_CXX_FLAGS=unknown",
    "UPPERCASE_BUILD_TYPE=unknown",
    "ARROW_GIT_ID=unknown",
    "ARROW_GIT_DESCRIPTION=unknown",
    "ARROW_PACKAGE_KIND=unknown",
    "ARROW_USE_NATIVE_INT128",
    #"ARROW_S3",
]

arrow_undefs = [
    "ARROW_FLIGHT",
    "ARROW_FLIGHT_SQL",
    "ARROW_CUDA",
    "ARROW_ORC",
    "ARROW_PARQUET",
    "ARROW_SUBSTRAIT",
    "ARROW_GCS",
    "ARROW_WITH_MUSL",
    "ARROW_WITH_OPENTELEMETRY",
    "ARROW_WITH_UCX",
]

cmake_configure_file(
    name = "arrow_util_config_h",
    src = "cpp/src/arrow/util/config.h.cmake",
    out = "cpp/src/arrow/util/config.h",
    defines = arrow_defs,
    undefines = arrow_undefs,
)

glob_exclude_files = [
    "**/benchmark_*",
    "cpp/src/arrow/adapters/**",
    "cpp/src/arrow/dataset/file_orc*",
    "cpp/src/arrow/dataset/file_parquet*",
    "cpp/src/arrow/engine/substrait/**",
    "cpp/src/arrow/filesystem/gcsfs*",
    "cpp/src/arrow/filesystem/s3fs*", # XXX
    "cpp/src/arrow/flight/**",
    "cpp/src/arrow/gpu/**",
    "cpp/src/arrow/json/**",
    "cpp/src/arrow/memory_pool_jemalloc.*",
    "cpp/src/arrow/**/test_common.*",
    "cpp/src/arrow/testing/**",
    "cpp/src/arrow/util/compression_lz4.*",
    "cpp/src/arrow/util/compression_snappy.*",
    "cpp/src/arrow/util/tracing_internal.cc",
]

glob_hdrs = glob([
    "cpp/src/arrow/**/*.h",
    "cpp/thirdparty/**/*.h",
    "cpp/src/arrow/vendored/pcg/*.hpp",
], exclude = glob_exclude_files + [
    "**/*_test.h",
    "**/*test_util.h",
])

glob_srcs = glob([
    "cpp/src/arrow/**/*.cc",
    "cpp/src/generated/*_generated.h",
], exclude = glob_exclude_files + [
    "**/*_test.cc",
    "**/*_benchmark.cc",
    "**/*test_util.cc",
    "**/generate_*fuzz_corpus.cc",
    "**/*_fuzz.cc",
    "cpp/src/arrow/flight/sql/test_*_cli.cc",
    "cpp/src/arrow/flight/integration_tests/*",
    "cpp/src/arrow/ipc/json_simple.cc",
    "cpp/src/arrow/ipc/stream_to_file.cc",
    "cpp/src/arrow/ipc/file_to_stream.cc",
])

cc_library(
    name = "arrow",
    includes = [
        "cpp/src",
        "cpp/thirdparty/flatbuffers/include",
        "cpp/thirdparty/hadoop/include",
    ],
    copts = ["-march=skylake-avx512"],
    local_defines = [
        "ARROW_HAVE_AVX512",
        "ARROW_HAVE_AVX2",
        "ARROW_HAVE_BMI2",
        "ARROW_HAVE_SSE4_2",
        "ARROW_USE_GLOG",
    ],
    hdrs = glob_hdrs + ["cpp/src/arrow/util/config.h"],
    srcs = glob_srcs,
    deps = [
        "@com_github_xtensor-stack_xsimd//:xsimd",
        "@com_github_google_glog//:glog",
    ]
)
