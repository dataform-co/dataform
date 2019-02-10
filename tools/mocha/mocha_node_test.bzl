
load("@build_bazel_rules_nodejs//:defs.bzl", "nodejs_test")
load("//tools/common:devmode_js_sources.bzl", "devmode_js_sources")
def mocha_node_test(
    name,
    test_entrypoints = [], # these should NOT contain the preceding workspace name.
    srcs = [],
    deps = [],
    data = [],
    tags = [],
    expected_exit_code = 0,
    **kwargs):
    """Runs tests in NodeJS using the Mocha test framework.
    To debug the test, see debugging notes in `nodejs_test`.
    Args:
    name: name of the resulting label
    srcs: spec files containing assertions
    deps: JavaScript code or rules producing JavaScript that is being tested
    data: Runtime dependencies that the mocha tests need access to
    tags: Arbitrary tags to apply to all generated targets
    expected_exit_code: The expected exit code for the test. Defaults to 0
    **kwargs: remaining arguments passed to the test rule
    """
    devmode_js_sources(
      name = "%s_devmode_srcs" % name,
      deps = srcs + deps,
      testonly = 1,
      tags = tags,
    )
    all_data = data + srcs + [
      Label("//tools/mocha:mocha_runner.js"),
      ":%s_devmode_srcs.MF" % name
    ]
    entry_point = "df/tools/mocha/mocha_runner.js"
    nodejs_test(
      name = name,
      data = all_data,
      entry_point = entry_point,
      templated_args = ["$(location :%s_devmode_srcs.MF)" % name],
      expected_exit_code = expected_exit_code,
      tags = tags,
      **kwargs
    )
