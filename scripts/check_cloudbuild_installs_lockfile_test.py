#!/usr/bin/env python3
"""Tests for scripts/check_cloudbuild_installs_lockfile.

Usage: ./scripts/check_cloudbuild_installs_lockfile_test.py
"""

import importlib.machinery
import importlib.util
import os
import sys
import unittest

import yaml

SCRIPTS = os.path.dirname(os.path.abspath(__file__))
sys.dont_write_bytecode = True
_loader = importlib.machinery.SourceFileLoader(
    "check_cloudbuild_installs_lockfile", os.path.join(SCRIPTS, "check_cloudbuild_installs_lockfile"))
_spec = importlib.util.spec_from_loader(_loader.name, _loader)
guard = importlib.util.module_from_spec(_spec)
_loader.exec_module(guard)

NPMRC = {"name": "gcr.io/cloud-builders/gcloud", "id": "create-npmrc",
         "script": "#!/usr/bin/env bash\n./scripts/create_npmrc\n"}
VERIFY = {"name": "gcr.io/cloud-builders/bazel:7.3.2", "id": "verify-npm-from-ci-registry",
          "waitFor": ["create-npmrc"],
          "script": '#!/usr/bin/env bash\nbazel run -- @pnpm//:pnpm --dir "$PWD" install --frozen-lockfile\n'}
TESTS = {"name": "gcr.io/cloud-builders/bazel:7.3.2", "id": "tests",
         "waitFor": ["verify-npm-from-ci-registry"],
         "script": "#!/usr/bin/env bash\n./scripts/run_tests_on_cloudbuild\n"}


def config(*steps):
    return {"steps": [dict(s) for s in steps]}


def with_script(step, script):
    return dict(step, script=script)


class CheckTest(unittest.TestCase):

    def assert_error(self, cfg, text):
        errors = guard.check(cfg)
        self.assertTrue(any(text in e for e in errors), "%r not in %r" % (text, errors))

    def test_valid(self):
        self.assertEqual(guard.check(config(NPMRC, VERIFY, TESTS)), [])

    def test_valid_without_wait_for(self):
        steps = [{k: v for k, v in s.items() if k != "waitFor"} for s in (NPMRC, VERIFY, TESTS)]
        self.assertEqual(guard.check(config(*steps)), [])

    def test_repository_config_passes(self):
        with open(os.path.join(SCRIPTS, "..", "cloudbuild-test.yaml")) as f:
            self.assertEqual(guard.check(yaml.safe_load(f)), [])

    def test_reindented_yaml_passes(self):
        text = yaml.safe_dump(config(NPMRC, VERIFY, TESTS), indent=4)
        self.assertEqual(guard.check(yaml.safe_load(text)), [])

    def test_missing_step(self):  # The #2458 case.
        self.assert_error(config(NPMRC, TESTS), "exactly one step")

    def test_renamed_step(self):
        self.assert_error(config(NPMRC, dict(VERIFY, id="something-else"), TESTS), "exactly one step")

    def test_duplicate_step(self):
        self.assert_error(config(NPMRC, VERIFY, VERIFY, TESTS), "found 2")

    def test_prod(self):
        self.assert_error(config(NPMRC, with_script(
            VERIFY, VERIFY["script"].replace("--frozen-lockfile", "--frozen-lockfile --prod")), TESTS),
            "without --prod")

    def test_filter(self):
        self.assert_error(config(NPMRC, with_script(
            VERIFY, VERIFY["script"].replace("--frozen-lockfile", "--frozen-lockfile --filter x")), TESTS),
            "without --filter")

    def test_no_frozen_lockfile(self):
        self.assert_error(config(NPMRC, with_script(
            VERIFY, VERIFY["script"].replace(" --frozen-lockfile", "")), TESTS), "--frozen-lockfile")

    def test_commented_out(self):
        self.assert_error(config(NPMRC, with_script(
            VERIFY, VERIFY["script"].replace("bazel run", "# bazel run")), TESTS), "@pnpm//:pnpm")

    def test_after_tests(self):
        self.assert_error(config(NPMRC, TESTS, VERIFY), "must come before")

    def test_before_npmrc(self):
        self.assert_error(config(VERIFY, NPMRC, TESTS), "must come after")

    def test_wait_for_without_npmrc(self):
        self.assert_error(config(NPMRC, dict(VERIFY, waitFor=["-"]), TESTS), "must wait for")

    def test_tests_not_waiting_for_verify(self):
        self.assert_error(config(NPMRC, VERIFY, dict(TESTS, waitFor=["create-npmrc"])),
                          "must wait for the verify-npm-from-ci-registry step")


if __name__ == "__main__":
    unittest.main()
