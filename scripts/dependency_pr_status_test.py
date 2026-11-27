#!/usr/bin/env python3
"""Tests for scripts/dependency_pr_status.

Usage: ./scripts/dependency_pr_status_test.py
"""

import importlib.machinery
import importlib.util
import os
import sys
import unittest

SCRIPTS = os.path.dirname(os.path.abspath(__file__))
sys.dont_write_bytecode = True
os.environ.setdefault("GITHUB_REPO", "dataform-co/dataform")
_loader = importlib.machinery.SourceFileLoader(
    "dependency_pr_status", os.path.join(SCRIPTS, "dependency_pr_status"))
_spec = importlib.util.spec_from_loader(_loader.name, _loader)
status = importlib.util.module_from_spec(_spec)
_loader.exec_module(status)


def pr(ref, author="dependabot[bot]", repo=None):
    return {"user": {"login": author},
            "head": {"ref": ref, "repo": {"full_name": repo or status.REPO}}}


def run(name, conclusion, app="github-actions", state="completed"):
    return {"name": name, "status": state, "conclusion": conclusion, "app": {"slug": app}}


class IsNpmDependencyPrTest(unittest.TestCase):

    def test_dependabot_npm(self):
        self.assertTrue(status.is_npm_dependency_pr(pr("dependabot/npm_and_yarn/glob-14.0.6")))
        self.assertTrue(status.is_npm_dependency_pr(
            pr("dependabot/npm_and_yarn/npm-runtime-a1b2c3")))

    def test_dependabot_other_ecosystems_are_skipped(self):
        for ref in ("dependabot/github_actions/github-actions-a1b2c3",
                    "dependabot/bazel/bazel-modules-a1b2c3"):
            self.assertFalse(status.is_npm_dependency_pr(pr(ref)), ref)

    def test_security_updates_branch(self):
        self.assertTrue(status.is_npm_dependency_pr(
            pr("deps/security-updates", author="dataform-automation[bot]")))

    def test_toolchain_updates_branch_is_skipped(self):
        # It doesn't change pnpm-lock.yaml, so rerun_dependency_pr_tests can't
        # tell when it would pass; don't label it as waiting.
        self.assertFalse(status.is_npm_dependency_pr(
            pr("deps/toolchain-updates", author="dataform-automation[bot]")))

    def test_security_updates_branch_from_a_fork(self):
        self.assertFalse(status.is_npm_dependency_pr(
            pr("deps/security-updates", author="someone", repo="someone/dataform")))

    def test_other_prs(self):
        self.assertFalse(status.is_npm_dependency_pr(pr("my-feature", author="someone")))
        self.assertFalse(status.is_npm_dependency_pr(
            pr("dependabot/npm_and_yarn/x", author="someone")))


class ClassifyTest(unittest.TestCase):
    """The states that decide the labels (unchanged behavior)."""

    PUBLIC = status.PUBLIC_BUILD_CHECK
    CB = status.CLOUD_BUILD_APP

    def test_waiting(self):
        runs = [run("test (cb)", "failure", app=self.CB), run(self.PUBLIC, "success")]
        self.assertEqual(status.classify(runs, 1), "waiting")

    def test_stale(self):
        runs = [run("test (cb)", "failure", app=self.CB), run(self.PUBLIC, "success")]
        self.assertEqual(status.classify(runs, status.MAX_WAIT_DAYS), "stale")

    def test_green(self):
        self.assertEqual(status.classify([run("test (cb)", "success", app=self.CB)], 1), "green")

    def test_real_failure(self):
        runs = [run("test (cb)", "failure", app=self.CB), run(self.PUBLIC, "failure")]
        self.assertEqual(status.classify(runs, 1), "failure")

    def test_waiting_for_gcbrun_is_pending(self):
        runs = [run("test (cb)", "action_required", app=self.CB), run(self.PUBLIC, "success")]
        self.assertEqual(status.classify(runs, 1), "pending")


if __name__ == "__main__":
    unittest.main()
