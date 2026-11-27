#!/usr/bin/env python3
"""Tests for scripts/update_toolchain_versions.

Usage: ./scripts/update_toolchain_versions_test.py
"""

import datetime
import importlib.machinery
import importlib.util
import os
import sys
import unittest

SCRIPTS = os.path.dirname(os.path.abspath(__file__))
sys.dont_write_bytecode = True
_loader = importlib.machinery.SourceFileLoader(
    "update_toolchain_versions", os.path.join(SCRIPTS, "update_toolchain_versions"))
_spec = importlib.util.spec_from_loader(_loader.name, _loader)
tool = importlib.util.module_from_spec(_spec)
_loader.exec_module(tool)

NOW = datetime.datetime(2026, 11, 27, tzinfo=datetime.timezone.utc)


def days_ago(n):
    return NOW - datetime.timedelta(days=n)


MODULE = '''\
bazel_dep(name = "aspect_rules_js", version = "2.0.1")
bazel_dep(name = "protobuf", version = "27.3", repo_name = "com_google_protobuf")

node = use_extension("@rules_nodejs//nodejs:extensions.bzl", "node")
node.toolchain(node_version = "24.13.0")

pnpm = use_extension("@aspect_rules_js//npm:extensions.bzl", "pnpm")
pnpm.pnpm(pnpm_version = "9.7.1")
use_repo(pnpm, "pnpm")
'''

PACKAGE_JSON = '''\
{
  "dependencies": {"yargs": "^16.2.0", "local": "file:../x"},
  "devDependencies": {"typescript": "5.5.4", "@types/node": "^16.16.0"},
  "packageManager": "pnpm@9.7.1"
}
'''


class VersionTest(unittest.TestCase):

    def test_parse_version(self):
        self.assertEqual(tool.parse_version("v24.13.0"), (24, 13, 0))
        self.assertIsNone(tool.parse_version("25.0.0-rc.1"))
        self.assertIsNone(tool.parse_version("9.7"))

    def test_parse_loose_version(self):
        self.assertEqual(tool.parse_loose_version("27.3"), (27, 3))
        self.assertEqual(tool.parse_loose_version("36.1.bcr.1"), (36, 1))
        self.assertIsNone(tool.parse_loose_version("37.0-rc1"))

    def test_major_line(self):
        self.assertEqual(tool.major_line((24, 13, 0)), (24,))
        self.assertEqual(tool.major_line((0, 3, 0)), (0, 3))

    def test_spec_version(self):
        self.assertEqual(tool.spec_version("^16.2.0"), (16, 2, 0))
        self.assertEqual(tool.spec_version("~8.3"), (8, 3, 0))
        self.assertIsNone(tool.spec_version("file:../x"))


class NewestInLineTest(unittest.TestCase):

    RELEASES = [
        ("24.13.0", days_ago(100)),
        ("24.20.0", days_ago(30)),
        ("24.22.3", days_ago(3)),       # Too new for a 7-day cooldown.
        ("25.0.0", days_ago(60)),       # Another major.
        ("24.21.0-rc.1", days_ago(20)),  # Pre-release.
    ]

    def test_newest_after_cooldown(self):
        best = tool.newest_in_line((24, 13, 0), self.RELEASES, NOW, 7)
        self.assertEqual(best[0], (24, 20, 0))

    def test_no_cooldown(self):
        best = tool.newest_in_line((24, 13, 0), self.RELEASES, NOW, 0)
        self.assertEqual(best[0], (24, 22, 3))

    def test_up_to_date(self):
        self.assertIsNone(tool.newest_in_line((24, 22, 3), self.RELEASES, NOW, 0))

    def test_zero_major_stays_in_minor(self):
        releases = [("0.3.5", days_ago(30)), ("0.4.0", days_ago(30))]
        self.assertEqual(tool.newest_in_line((0, 3, 0), releases, NOW, 7)[0], (0, 3, 5))


class EditTest(unittest.TestCase):

    def test_read_versions(self):
        self.assertEqual(tool.node_version(MODULE), "24.13.0")
        self.assertEqual(tool.pnpm_version(MODULE), "9.7.1")
        self.assertEqual(tool.package_manager_version(PACKAGE_JSON), "9.7.1")
        self.assertEqual(tool.bazel_deps(MODULE),
                         [("aspect_rules_js", "2.0.1"), ("protobuf", "27.3")])

    def test_apply_updates(self):
        updates = [
            {"name": "Node.js", "new": "24.22.3"},
            {"name": "pnpm", "new": "9.15.9", "integrity": "sha512-abc=="},
        ]
        module, package_json = tool.apply_updates(MODULE, PACKAGE_JSON, updates)
        self.assertIn('node.toolchain(node_version = "24.22.3")', module)
        self.assertIn('pnpm.pnpm(\n    pnpm_version = "9.15.9",\n'
                      '    pnpm_version_integrity = "sha512-abc==",\n)', module)
        self.assertIn('"packageManager": "pnpm@9.15.9"', package_json)
        # Only the versions change.
        self.assertEqual(module.count("\n") - MODULE.count("\n"), 3)
        self.assertEqual(package_json.replace("9.15.9", "9.7.1"), PACKAGE_JSON)

    def test_update_again_replaces_integrity(self):
        module, _ = tool.apply_updates(MODULE, PACKAGE_JSON,
                                       [{"name": "pnpm", "new": "9.15.8", "integrity": "a"}])
        module, _ = tool.apply_updates(module, PACKAGE_JSON,
                                       [{"name": "pnpm", "new": "9.15.9", "integrity": "b"}])
        self.assertEqual(tool.pnpm_version(module), "9.15.9")
        self.assertIn('pnpm_version_integrity = "b"', module)
        self.assertNotIn('"a"', module)


class ReportTest(unittest.TestCase):

    def test_summary(self):
        self.assertIn("up to date", tool.update_summary([]))
        summary = tool.update_summary([{"name": "Node.js", "current": "24.13.0",
                                        "new": "24.22.3", "released": days_ago(10)}])
        self.assertIn("| Node.js | `24.13.0` | `24.22.3` | 2026-11-17 |", summary)

    def test_eol_note(self):
        eol = {"7": {"eol": "2026-12-31"}, "6": {"eol": True}}
        self.assertEqual(tool.eol_note(eol, 7), "end of life 2026-12-31")
        self.assertEqual(tool.eol_note(eol, 6), "end of life")
        self.assertEqual(tool.eol_note(eol, 9), "")

    def test_majors_report(self):
        report = tool.majors_report(
            [("Bazel", "7.3.2", "7.7.3", "9.3.3", "end of life 2026-12-31")],
            [("protobuf", "27.3", "36.2")], [("yargs", "^16.2.0", "18.2.0")], NOW)
        self.assertIn("| Bazel | `7.3.2` | `7.7.3` | `9.3.3` | end of life 2026-12-31 |", report)
        self.assertIn("| `protobuf` | `27.3` | `36.2` |", report)
        self.assertIn("| `yargs` | `^16.2.0` | `18.2.0` |", report)
        self.assertIn(tool.REPORT_MARKER, report)

    def test_majors_report_empty(self):
        self.assertIn("newest major", tool.majors_report([], [], [], NOW))


if __name__ == "__main__":
    unittest.main()
