#!/usr/bin/env python3
"""Tests for scripts/update_toolchain_versions.

Usage: ./scripts/update_toolchain_versions_test.py
"""

import datetime
import importlib.machinery
import importlib.util
import io
import os
import sys
import unittest
from unittest import mock

SCRIPTS = os.path.dirname(os.path.abspath(__file__))
sys.dont_write_bytecode = True
_loader = importlib.machinery.SourceFileLoader(
    "update_toolchain_versions", os.path.join(SCRIPTS, "update_toolchain_versions"))
_spec = importlib.util.spec_from_loader(_loader.name, _loader)
tool = importlib.util.module_from_spec(_spec)
_loader.exec_module(tool)

NOW = datetime.datetime(2026, 11, 27, tzinfo=datetime.timezone.utc)
INTEGRITY = "sha512-aARhQYk8ZvrQHAeSMRKOmvuJ74fiaR1p5NQO7iKJiClf1GghgbrlW1hBjDolO95lpQXsfF+UA+zlzDzTfc8lMQ=="


def days_ago(n):
    return NOW - datetime.timedelta(days=n)


MODULE = '''\
node = use_extension("@rules_nodejs//nodejs:extensions.bzl", "node")
node.toolchain(node_version = "24.13.0")

pnpm = use_extension("@aspect_rules_js//npm:extensions.bzl", "pnpm")
pnpm.pnpm(pnpm_version = "9.7.1")
use_repo(pnpm, "pnpm")
'''

PACKAGE_JSON = '''\
{
  "devDependencies": {"typescript": "5.5.4"},
  "packageManager": "pnpm@9.7.1"
}
'''

NODE_RELEASES = [
    ("24.13.0", days_ago(100)),
    ("24.20.0", days_ago(30)),
    ("24.22.3", days_ago(3)),        # Too new for a 7-day cooldown.
    ("25.0.0", days_ago(60)),        # Another major.
    ("24.21.0-rc.1", days_ago(20)),  # Pre-release.
]


def pnpm_doc(integrity=INTEGRITY):
    return {
        "time": {"created": "2016-01-01T00:00:00Z", "modified": NOW.isoformat(),
                 "9.7.1": days_ago(500).isoformat(), "9.15.9": days_ago(400).isoformat(),
                 "10.0.0": days_ago(300).isoformat(), "9.15.10": days_ago(5).isoformat()},
        "versions": {"9.15.9": {"dist": {"integrity": integrity}}},
    }


class VersionTest(unittest.TestCase):

    def test_parse_version(self):
        self.assertEqual(tool.parse_version("v24.13.0"), (24, 13, 0))
        self.assertIsNone(tool.parse_version("25.0.0-rc.1"))
        self.assertIsNone(tool.parse_version("9.7"))

    def test_major_line(self):
        self.assertEqual(tool.major_line((24, 13, 0)), (24,))
        self.assertEqual(tool.major_line((0, 3, 0)), (0, 3))

    def test_newest_after_cooldown(self):
        self.assertEqual(tool.newest_in_line((24, 13, 0), NODE_RELEASES, NOW, 7)[0], (24, 20, 0))

    def test_no_cooldown(self):
        self.assertEqual(tool.newest_in_line((24, 13, 0), NODE_RELEASES, NOW, 0)[0], (24, 22, 3))

    def test_up_to_date(self):
        self.assertIsNone(tool.newest_in_line((24, 22, 3), NODE_RELEASES, NOW, 0))

    def test_zero_major_stays_in_minor(self):
        releases = [("0.3.5", days_ago(30)), ("0.4.0", days_ago(30))]
        self.assertEqual(tool.newest_in_line((0, 3, 0), releases, NOW, 7)[0], (0, 3, 5))


class EditTest(unittest.TestCase):

    def test_read_versions(self):
        self.assertEqual(tool.node_version(MODULE), "24.13.0")
        self.assertEqual(tool.pnpm_version(MODULE), "9.7.1")
        self.assertEqual(tool.package_manager_version(PACKAGE_JSON), "9.7.1")

    def test_apply_updates(self):
        updates = [
            {"name": "Node.js", "new": "24.22.3"},
            {"name": "pnpm", "new": "9.15.9", "integrity": INTEGRITY},
        ]
        module, package_json = tool.apply_updates(MODULE, PACKAGE_JSON, updates)
        self.assertIn('node.toolchain(node_version = "24.22.3")', module)
        self.assertIn('pnpm.pnpm(\n    pnpm_version = "9.15.9",\n'
                      '    pnpm_version_integrity = "%s",\n)' % INTEGRITY, module)
        self.assertEqual(package_json.replace("9.15.9", "9.7.1"), PACKAGE_JSON)

    def test_pnpm_keeps_other_attributes(self):
        module = MODULE.replace('pnpm.pnpm(pnpm_version = "9.7.1")',
                                'pnpm.pnpm(\n    name = "pnpm",\n    pnpm_version = "9.7.1",\n'
                                '    pnpm_version_integrity = "sha512-old==",\n)')
        module = tool.set_pnpm_version(module, "9.15.9", INTEGRITY)
        self.assertIn('pnpm.pnpm(\n    name = "pnpm",\n    pnpm_version = "9.15.9",\n'
                      '    pnpm_version_integrity = "%s",\n)' % INTEGRITY, module)

    def test_pnpm_adds_integrity_to_other_attributes(self):
        module = MODULE.replace('pnpm.pnpm(pnpm_version = "9.7.1")',
                                'pnpm.pnpm(name = "pnpm", pnpm_version = "9.7.1")')
        module = tool.set_pnpm_version(module, "9.15.9", INTEGRITY)
        self.assertIn('pnpm.pnpm(\n    name = "pnpm",\n    pnpm_version = "9.15.9",\n'
                      '    pnpm_version_integrity = "%s",\n)' % INTEGRITY, module)

    def test_rejects_invalid_integrity(self):
        for integrity in ('sha512-abc"; evil()', "sha1-abc=", ""):
            with self.assertRaises(ValueError, msg=integrity):
                tool.set_pnpm_version(MODULE, "9.15.9", integrity)


class PlanUpdatesTest(unittest.TestCase):

    def plan(self, **kwargs):
        with mock.patch("sys.stderr", new_callable=io.StringIO) as stderr:
            updates = tool.plan_updates(MODULE, NOW, **kwargs)
        return updates, stderr.getvalue()

    def test_updates(self):
        updates, _ = self.plan(fetch_node_releases=lambda: NODE_RELEASES,
                               fetch_pnpm=lambda: pnpm_doc())
        self.assertEqual([(u["name"], u["current"], u["new"]) for u in updates],
                         [("Node.js", "24.13.0", "24.20.0"), ("pnpm", "9.7.1", "9.15.9")])
        self.assertEqual(updates[1]["integrity"], INTEGRITY)

    def test_invalid_integrity_skips_pnpm(self):
        updates, stderr = self.plan(fetch_node_releases=lambda: [],
                                    fetch_pnpm=lambda: pnpm_doc('sha512-x"); evil()'))
        self.assertEqual(updates, [])
        self.assertIn("unexpected integrity", stderr)

    def test_network_errors_skip_with_a_warning(self):
        def fail():
            raise TimeoutError("read timed out")
        updates, stderr = self.plan(fetch_node_releases=fail, fetch_pnpm=fail)
        self.assertEqual(updates, [])
        self.assertIn("can't get the Node.js releases", stderr)
        self.assertIn("can't get the pnpm releases", stderr)


class SummaryTest(unittest.TestCase):

    def test_summary(self):
        self.assertIn("up to date", tool.update_summary([]))
        summary = tool.update_summary([{"name": "Node.js", "current": "24.13.0",
                                        "new": "24.22.3", "released": days_ago(10)}])
        self.assertIn("| Node.js | `24.13.0` | `24.22.3` | 2026-11-17 |", summary)
        self.assertIn("MODULE.bazel.lock", summary)


if __name__ == "__main__":
    unittest.main()
