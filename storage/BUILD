load("@rules_cc//cc:defs.bzl", "cc_binary")

package(
    default_visibility = ["//visibility:public"],
)

cc_binary(
    name = "builder",
    srcs = [
        "builder.cpp",
        "bit_magic.hpp",
        "compute_kernels.cpp",
        "compute_kernels.hpp",
        "csv_reader.cpp",
        "csv_reader.hpp",
        "csv_records.hpp",
    ],
    deps = [
        "//third_party/arrow:arrow",
        "//third_party/folly:folly",
        "//third_party/zsv:zsv",
        "//pthash:pthash",
    ],
)
