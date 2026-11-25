#!/usr/bin/env python3
"""Tests for scripts/auto_assign_pr.

Usage: ./scripts/auto_assign_pr_test.py
"""

import importlib.machinery
import importlib.util
import io
import os
import sys
import unittest
from unittest import mock
import urllib.error

SCRIPTS = os.path.dirname(os.path.abspath(__file__))
sys.dont_write_bytecode = True
_loader = importlib.machinery.SourceFileLoader(
    "auto_assign_pr", os.path.join(SCRIPTS, "auto_assign_pr"))
_spec = importlib.util.spec_from_loader(_loader.name, _loader)
auto_assign_pr = importlib.util.module_from_spec(_spec)
_loader.exec_module(auto_assign_pr)

REPO = "dataform-co/dataform"


def make_user(login, user_type="User"):
    return {"login": login, "type": user_type}


def make_pr(
    number=101,
    state="open",
    user=None,
    assignees=None,
    requested_reviewers=None,
    requested_teams=None,
    title="Test PR",
):
    return {
        "number": number,
        "title": title,
        "state": state,
        "user": user if user is not None else make_user("author"),
        "assignees": list(assignees or []),
        "assignee": assignees[0] if assignees else None,
        "requested_reviewers": list(requested_reviewers or []),
        "requested_teams": list(requested_teams or []),
    }


def http_error(code):
    return urllib.error.HTTPError(
        url="https://api.github.com/test",
        code=code,
        msg="HTTP %d" % code,
        hdrs=None,
        fp=io.BytesIO(b""),
    )


class FakeGitHubApi:
    """In-memory stub for auto_assign_pr.api calls."""

    def __init__(self, permissions=None, reviews_by_pr=None, open_prs=None, prs_by_number=None):
        self.permissions = dict(permissions or {})
        self.reviews_by_pr = {k: list(v) for k, v in (reviews_by_pr or {}).items()}
        self.open_prs = list(open_prs or [])
        self.prs_by_number = dict(prs_by_number or {})
        self.calls = []

    def __call__(self, method, path, body=None):
        self.calls.append((method, path, body))
        if method == "POST" and path.endswith("/assignees"):
            return body
        if method != "GET":
            raise AssertionError("Unexpected call: %s %s" % (method, path))

        path_only, _, query = path.partition("?")
        params = dict(part.split("=", 1) for part in query.split("&") if "=" in part)
        page = int(params.get("page", "1"))
        per_page = int(params.get("per_page", "100"))

        if path_only == "/repos/%s/pulls" % REPO:
            start = (page - 1) * per_page
            return self.open_prs[start:start + per_page]

        prefix = "/repos/%s/pulls/" % REPO
        if path_only.startswith(prefix) and path_only.endswith("/reviews"):
            pr_num = int(path_only[len(prefix):-len("/reviews")])
            reviews = self.reviews_by_pr.get(pr_num, [])
            start = (page - 1) * per_page
            return reviews[start:start + per_page]

        if path_only.startswith(prefix):
            pr_num = int(path_only[len(prefix):])
            return self.prs_by_number[pr_num]

        collab_prefix = "/repos/%s/collaborators/" % REPO
        if path_only.startswith(collab_prefix) and path_only.endswith("/permission"):
            login = path_only[len(collab_prefix):-len("/permission")]
            perm = self.permissions.get(login)
            if perm == 404 or perm is None:
                raise http_error(404)
            if isinstance(perm, int):
                raise http_error(perm)
            return {"permission": perm}

        raise AssertionError("Unhandled GET path: %s" % path)

    def post_calls(self):
        return [c for c in self.calls if c[0] == "POST"]

    def permission_calls(self):
        return [c[1] for c in self.calls if c[0] == "GET" and "/collaborators/" in c[1]]


class AutoAssignPrTest(unittest.TestCase):

    def run_process(self, pr, fake_api, cache=None, dry_run=False):
        permission_cache = {} if cache is None else cache
        with mock.patch.object(auto_assign_pr, "api", side_effect=fake_api), \
                mock.patch.object(auto_assign_pr, "DRY_RUN", dry_run), \
                mock.patch.object(auto_assign_pr, "REPO", REPO), \
                mock.patch("sys.stdout", new_callable=io.StringIO):
            result = auto_assign_pr.process(pr, permission_cache, repo=REPO)
        return result

    def test_already_assigned_pr_is_skipped(self):
        fake = FakeGitHubApi(permissions={"author": "write"})
        pr = make_pr(assignees=[make_user("existing-owner")])

        result = self.run_process(pr, fake)

        self.assertIsNone(result)
        self.assertEqual(fake.calls, [])

    def test_closed_pr_is_skipped(self):
        fake = FakeGitHubApi(permissions={"author": "write"})
        pr = make_pr(state="closed")

        result = self.run_process(pr, fake)

        self.assertIsNone(result)
        self.assertEqual(fake.calls, [])

    def test_eligible_write_creator_is_assigned(self):
        fake = FakeGitHubApi(permissions={"alice": "write"})
        pr = make_pr(
            number=12,
            user=make_user("alice"),
            requested_reviewers=[make_user("bob")],
        )

        result = self.run_process(pr, fake)

        self.assertEqual(result, "alice")
        self.assertEqual(
            fake.post_calls(),
            [("POST", "/repos/%s/issues/12/assignees" % REPO, {"assignees": ["alice"]})],
        )
        # Should not check bob's permissions or fetch reviews when creator is eligible.
        self.assertEqual(
            fake.permission_calls(),
            ["/repos/%s/collaborators/alice/permission" % REPO],
        )

    def test_eligible_admin_creator_is_assigned(self):
        fake = FakeGitHubApi(permissions={"admin-user": "admin"})
        pr = make_pr(number=15, user=make_user("admin-user"))

        result = self.run_process(pr, fake)

        self.assertEqual(result, "admin-user")
        self.assertEqual(
            fake.post_calls(),
            [("POST", "/repos/%s/issues/15/assignees" % REPO, {"assignees": ["admin-user"]})],
        )

    def test_read_only_org_member_creator_falls_back_to_reviewer(self):
        fake = FakeGitHubApi(permissions={
            "readonly-member": "read",
            "triage-member": "triage",
            "maintainer": "write",
        })
        pr = make_pr(
            number=20,
            user=make_user("readonly-member"),
            requested_reviewers=[make_user("triage-member"), make_user("maintainer")],
            requested_teams=[{"slug": "dataform-reviewers"}],
        )

        result = self.run_process(pr, fake)

        self.assertEqual(result, "maintainer")
        self.assertEqual(
            fake.post_calls(),
            [("POST", "/repos/%s/issues/20/assignees" % REPO, {"assignees": ["maintainer"]})],
        )

    def test_non_collaborator_404_treated_as_none(self):
        fake = FakeGitHubApi(permissions={"external-contributor": 404, "maintainer": "write"})
        pr = make_pr(
            number=21,
            user=make_user("external-contributor"),
            requested_reviewers=[make_user("maintainer")],
        )

        result = self.run_process(pr, fake)

        self.assertEqual(result, "maintainer")
        self.assertEqual(
            fake.post_calls(),
            [("POST", "/repos/%s/issues/21/assignees" % REPO, {"assignees": ["maintainer"]})],
        )

    def test_bots_and_shared_accounts_are_excluded_as_creators_and_reviewers(self):
        # Even if shared accounts have admin/write access in GitHub, they must never be picked.
        fake = FakeGitHubApi(
            permissions={
                "dataform-team": "write",
                "google-admin": "admin",
                "googlebot": "admin",
                "copybara-service": "write",
                "google-ospo-team": "admin",
                "real-maintainer": "write",
            },
        )
        pr = make_pr(
            number=30,
            user=make_user("dependabot[bot]", user_type="Bot"),
            requested_reviewers=[
                make_user("gemini-code-assist[bot]", user_type="User"),
                make_user("dataform-security-updates[bot]", user_type="Bot"),
                make_user("dataform-team", user_type="User"),
                make_user("google-admin", user_type="User"),
                make_user("googlebot", user_type="User"),
                make_user("copybara-service", user_type="User"),
                make_user("google-ospo-team", user_type="User"),
                make_user("real-maintainer", user_type="User"),
            ],
            requested_teams=[{"slug": "dataform-reviewers"}],
        )

        result = self.run_process(pr, fake)

        self.assertEqual(result, "real-maintainer")
        # Permission API should not even be queried for bots or blocklisted shared accounts.
        self.assertEqual(
            fake.permission_calls(),
            ["/repos/%s/collaborators/real-maintainer/permission" % REPO],
        )

    def test_shared_account_creator_is_not_assigned(self):
        fake = FakeGitHubApi(
            permissions={"dataform-team": "write", "human-reviewer": "admin"},
        )
        pr = make_pr(
            number=31,
            user=make_user("dataform-team", user_type="User"),
            requested_reviewers=[make_user("human-reviewer")],
        )

        result = self.run_process(pr, fake)

        self.assertEqual(result, "human-reviewer")
        self.assertEqual(
            fake.permission_calls(),
            ["/repos/%s/collaborators/human-reviewer/permission" % REPO],
        )

    def test_multiple_reviewers_picks_first_eligible_only(self):
        fake = FakeGitHubApi(permissions={
            "external": 404,
            "reader": "read",
            "first-maintainer": "write",
            "second-maintainer": "admin",
        })
        pr = make_pr(
            number=40,
            user=make_user("external"),
            requested_reviewers=[
                make_user("reader"),
                make_user("first-maintainer"),
                make_user("second-maintainer"),
            ],
        )

        result = self.run_process(pr, fake)

        self.assertEqual(result, "first-maintainer")
        self.assertEqual(
            fake.post_calls(),
            [("POST", "/repos/%s/issues/40/assignees" % REPO, {"assignees": ["first-maintainer"]})],
        )
        # Stops at first-maintainer without querying second-maintainer.
        self.assertNotIn(
            "/repos/%s/collaborators/second-maintainer/permission" % REPO,
            fake.permission_calls(),
        )

    def test_falls_back_to_submitted_reviews_when_no_requested_reviewers_eligible(self):
        fake = FakeGitHubApi(
            permissions={
                "external-author": 404,
                "commenter-without-write": "read",
                "approving-maintainer": "write",
                "later-maintainer": "admin",
            },
            reviews_by_pr={
                50: [
                    {"user": make_user("gemini-code-assist[bot]", user_type="Bot")},
                    {"user": None},  # Deleted/ghost account in review history.
                    {"user": make_user("dataform-team")},
                    {"user": make_user("commenter-without-write")},
                    {"user": make_user("approving-maintainer")},
                    {"user": make_user("later-maintainer")},
                ],
            },
        )
        pr = make_pr(
            number=50,
            user=make_user("external-author"),
            requested_reviewers=[],
            requested_teams=[{"slug": "dataform-reviewers"}],
        )

        result = self.run_process(pr, fake)

        self.assertEqual(result, "approving-maintainer")
        self.assertEqual(
            fake.post_calls(),
            [("POST", "/repos/%s/issues/50/assignees" % REPO, {"assignees": ["approving-maintainer"]})],
        )

    def test_leaves_unassigned_when_no_eligible_candidate_exists(self):
        fake = FakeGitHubApi(
            permissions={"external-author": 404, "outside-reviewer": "read"},
            reviews_by_pr={
                60: [
                    {"user": make_user("gemini-code-assist[bot]", user_type="Bot")},
                    {"user": make_user("outside-reviewer")},
                ],
            },
        )
        pr = make_pr(
            number=60,
            user=make_user("external-author"),
            requested_reviewers=[make_user("dataform-team")],
        )

        result = self.run_process(pr, fake)

        self.assertIsNone(result)
        self.assertEqual(fake.post_calls(), [])

    def test_permission_lookups_are_cached_across_prs_and_case_insensitive(self):
        fake = FakeGitHubApi(
            permissions={"external-user": 404, "alice": "write"},
            reviews_by_pr={70: [], 71: [{"user": make_user("Alice")}]},
        )
        cache = {}
        pr1 = make_pr(
            number=70,
            user=make_user("external-user"),
            requested_reviewers=[make_user("EXTERNAL-USER")],
        )
        pr2 = make_pr(
            number=71,
            user=make_user("external-user"),
            requested_reviewers=[make_user("alice")],
        )
        pr3 = make_pr(
            number=72,
            user=make_user("Alice"),
        )

        self.assertIsNone(self.run_process(pr1, fake, cache=cache))
        self.assertEqual(self.run_process(pr2, fake, cache=cache), "alice")
        self.assertEqual(self.run_process(pr3, fake, cache=cache), "Alice")

        self.assertEqual(
            fake.permission_calls(),
            [
                "/repos/%s/collaborators/external-user/permission" % REPO,
                "/repos/%s/collaborators/alice/permission" % REPO,
            ],
        )

    def test_dry_run_skips_post_request(self):
        fake = FakeGitHubApi(permissions={"alice": "write"})
        pr = make_pr(number=80, user=make_user("alice"))

        result = self.run_process(pr, fake, dry_run=True)

        self.assertEqual(result, "alice")
        self.assertEqual(fake.post_calls(), [])

    def test_fetch_prs_paginates_open_prs(self):
        prs = [make_pr(number=i) for i in range(1, 155)]
        fake = FakeGitHubApi(open_prs=prs)
        with mock.patch.object(auto_assign_pr, "api", side_effect=fake):
            fetched = auto_assign_pr.fetch_prs(repo=REPO)
        self.assertEqual(len(fetched), 154)
        self.assertEqual(
            [c[1] for c in fake.calls],
            [
                "/repos/%s/pulls?state=open&per_page=100&page=1" % REPO,
                "/repos/%s/pulls?state=open&per_page=100&page=2" % REPO,
            ],
        )


if __name__ == "__main__":
    unittest.main()
