load("@rules_cc//cc:defs.bzl", "cc_binary")

package(
    default_visibility = ["//visibility:public"],
)

cc_binary(
    name = "builder",
    srcs = [
        "bit_magic.hpp",
        "builder.cpp",
        "compute_kernels.cpp",
        "compute_kernels.hpp",
        "csv_reader.cpp",
        "csv_reader.hpp",
        "csv_records.hpp",
        "pn_ordered_join.cpp",
        "pn_ordered_join.hpp",
        "pn_ordered_join_internal.hpp",
    ],
    deps = [
        "//third_party/arrow:arrow",
        "//third_party/folly:folly",
        "//third_party/zsv:zsv",
        "//pthash:pthash",
    ],
)
