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
        "lrn_schema.hpp",
        "pthash_bucketer.hpp",
        "cmd.hpp",
        "cmd_convert.cpp",
        "cmd_convert.hpp",
        "cmd_convert_internal.hpp",
        "cmd_partition.cpp",
        "cmd_partition.hpp",
        "cmd_partition_internal.hpp",
    ],
    deps = [
        "//third_party/arrow:arrow",
        "//third_party/folly:folly",
        "//third_party/zsv:zsv",
        "//pthash:pthash",
    ],
)
