# https://bazel.build/# https://github.com/bazelbuild/bazel-toolchains/blob/master/rules/exec_properties/exec_properties.bzl
load("@bazel_toolchains//rules/exec_properties:exec_properties.bzl", "create_rbe_exec_properties_dict")

# https://bazel.build/docs/platforms
platform(
    name = "archlinux-x86_64-docker",
    parents = ["@local_config_platform//:host"],
    constraint_values = [
        "@platforms//os:linux",  # https://github.com/bazelbuild/platforms/blob/main/os/BUILD
        "@platforms//cpu:x86_64",  # https://github.com/bazelbuild/platforms/blob/main/cpu/BUILD
    ],
    # We specify exec_properties because we have an RBE instance that matches this platform.
    # exec_properties specify some information that is passed along the Remote Execution API
    # (REAPI) to the dispatcher which will use it to direct the request to the appropriate
    # machine/worker, using the Remote Worker API (RWAPI).
    # http://go/skolo-rbe
    # These properties will be ignored when running locally, but we must specify them if we want
    # the action cache to treat things built on a local Linux machine to be the same as built on
    # a Linux RBE worker (which they should, assuming our hermetic toolchain *is* hermetic).
    # See https://github.com/bazelbuild/bazel/blob/f28209df2b0ebeff1de0b8b7f6b9e215d890e753/src/main/java/com/google/devtools/build/lib/actions/ActionKeyCacher.java#L67-L73
    # for how the exec_properties and execution platform impact the action cache.
    exec_properties = create_rbe_exec_properties_dict(
        container_image = "docker://docker.io/archlinux/base-devel@sha256:9dd936a5a5358fa2e3515e1daabe4c0e6b4c1e9fcf369528d229955590c93472",
        os_family = "Linux",
    ),
)

# This is the entry point for --crosstool_top.  Toolchains are found
# # by lopping off the name of --crosstool_top and searching for
# # the "${CPU}" entry in the toolchains attribute.
# cc_toolchain_suite(
#     name = "custom_toolchain",
#     toolchains = {
#         "k8|gcc": ":cc-compiler-k8",
#         "k8": ":cc-compiler-k8",
#     },
# )
#
# filegroup(
#     name = "compiler_deps",
#     srcs = glob(["extra_tools/**"], allow_empty = True) + [":builtin_include_directory_paths"],
# )
