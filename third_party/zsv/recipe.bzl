load("@rules_cc//cc:defs.bzl", "cc_library")

package(
    default_visibility = ["//visibility:public"],
)

cc_library(
    name = "zsv_internal",
    hdrs = [
        "src/vector_delim.c",
        "src/zsv_internal.c",
        "src/zsv_scan_delim.c",
        "src/zsv_scan_fixed.c",
    ],
)

cc_library(
    name = "zsv",
    includes = ["include"],
    defines = ["ZSV_EXTRAS"],
    copts = ["-march=skylake-avx512"],
    hdrs = glob(["include/**/*.h"]),
    srcs = ["src/zsv.c"],
    implementation_deps = [":zsv_internal"],
)
