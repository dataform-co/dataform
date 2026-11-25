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
        "user": user if user is not None else make_user("apilaskowski"),
        "assignees": list(assignees or []),
        "assignee": assignees[0] if assignees else None,
        "requested_reviewers": list(requested_reviewers or []),
        "requested_teams": list(requested_teams or []),
    }


class FakeGitHubApi:
    """In-memory stub for auto_assign_pr.api calls."""

    def __init__(self, reviews_by_pr=None, open_prs=None, prs_by_number=None):
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

        raise AssertionError("Unhandled GET path: %s" % path)

    def post_calls(self):
        return [c for c in self.calls if c[0] == "POST"]


class AutoAssignPrTest(unittest.TestCase):

    def run_process(self, pr, fake_api, dry_run=False):
        with mock.patch.object(auto_assign_pr, "api", side_effect=fake_api), \
                mock.patch.object(auto_assign_pr, "DRY_RUN", dry_run), \
                mock.patch.object(auto_assign_pr, "REPO", REPO), \
                mock.patch("sys.stdout", new_callable=io.StringIO):
            result = auto_assign_pr.process(pr, repo=REPO)
        return result

    def test_eligible_humans_allowlist(self):
        self.assertEqual(
            auto_assign_pr.ELIGIBLE_HUMANS,
            frozenset({
                "apilaskowski",
                "Ford544",
                "k-matuszek",
                "mbiernacik",
                "Pawbag123",
                "rafal-hawrylak",
            }),
        )

    def test_already_assigned_pr_is_skipped(self):
        fake = FakeGitHubApi()
        pr = make_pr(assignees=[make_user("existing-owner")])

        result = self.run_process(pr, fake)

        self.assertIsNone(result)
        self.assertEqual(fake.calls, [])

    def test_closed_pr_is_skipped(self):
        fake = FakeGitHubApi()
        pr = make_pr(state="closed")

        result = self.run_process(pr, fake)

        self.assertIsNone(result)
        self.assertEqual(fake.calls, [])

    def test_eligible_creator_is_assigned_without_fetching_reviews(self):
        fake = FakeGitHubApi()
        pr = make_pr(
            number=12,
            user=make_user("rafal-hawrylak"),
            requested_reviewers=[make_user("apilaskowski")],
        )

        result = self.run_process(pr, fake)

        self.assertEqual(result, "rafal-hawrylak")
        self.assertEqual(
            fake.calls,
            [("POST", "/repos/%s/issues/12/assignees" % REPO, {"assignees": ["rafal-hawrylak"]})],
        )

    def test_case_insensitive_allowlist_matching(self):
        fake = FakeGitHubApi()
        pr = make_pr(number=15, user=make_user("ford544"))

        result = self.run_process(pr, fake)

        self.assertEqual(result, "ford544")
        self.assertEqual(
            fake.post_calls(),
            [("POST", "/repos/%s/issues/15/assignees" % REPO, {"assignees": ["ford544"]})],
        )

    def test_non_allowlisted_creator_falls_back_to_eligible_reviewer(self):
        fake = FakeGitHubApi()
        pr = make_pr(
            number=20,
            user=make_user("external-contributor"),
            requested_reviewers=[make_user("other-org-member"), make_user("mbiernacik")],
            requested_teams=[{"slug": "dataform-reviewers"}],
        )

        result = self.run_process(pr, fake)

        self.assertEqual(result, "mbiernacik")
        self.assertEqual(
            fake.post_calls(),
            [("POST", "/repos/%s/issues/20/assignees" % REPO, {"assignees": ["mbiernacik"]})],
        )

    def test_bots_and_shared_accounts_are_excluded_as_creators_and_reviewers(self):
        fake = FakeGitHubApi()
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
                make_user("k-matuszek", user_type="User"),
            ],
            requested_teams=[{"slug": "dataform-reviewers"}],
        )

        result = self.run_process(pr, fake)

        self.assertEqual(result, "k-matuszek")
        self.assertEqual(
            fake.post_calls(),
            [("POST", "/repos/%s/issues/30/assignees" % REPO, {"assignees": ["k-matuszek"]})],
        )

    def test_non_user_type_is_rejected_even_if_login_matches(self):
        fake = FakeGitHubApi(reviews_by_pr={31: []})
        pr = make_pr(
            number=31,
            user=make_user("apilaskowski", user_type="Bot"),
        )

        result = self.run_process(pr, fake)

        self.assertIsNone(result)
        self.assertEqual(fake.post_calls(), [])

    def test_multiple_reviewers_picks_first_eligible_only(self):
        fake = FakeGitHubApi()
        pr = make_pr(
            number=40,
            user=make_user("external"),
            requested_reviewers=[
                make_user("unlisted-user"),
                make_user("Pawbag123"),
                make_user("rafal-hawrylak"),
            ],
        )

        result = self.run_process(pr, fake)

        self.assertEqual(result, "Pawbag123")
        self.assertEqual(
            fake.post_calls(),
            [("POST", "/repos/%s/issues/40/assignees" % REPO, {"assignees": ["Pawbag123"]})],
        )

    def test_falls_back_to_submitted_reviews_when_no_requested_reviewers_eligible(self):
        fake = FakeGitHubApi(
            reviews_by_pr={
                50: [
                    {"user": make_user("gemini-code-assist[bot]", user_type="Bot")},
                    {"user": None},  # Deleted/ghost account in review history.
                    {"user": make_user("dataform-team")},
                    {"user": make_user("unlisted-reviewer")},
                    {"user": make_user("apilaskowski")},
                    {"user": make_user("rafal-hawrylak")},
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

        self.assertEqual(result, "apilaskowski")
        self.assertEqual(
            fake.post_calls(),
            [("POST", "/repos/%s/issues/50/assignees" % REPO, {"assignees": ["apilaskowski"]})],
        )

    def test_leaves_unassigned_when_no_eligible_candidate_exists(self):
        fake = FakeGitHubApi(
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

    def test_dry_run_skips_post_request(self):
        fake = FakeGitHubApi()
        pr = make_pr(number=80, user=make_user("apilaskowski"))

        result = self.run_process(pr, fake, dry_run=True)

        self.assertEqual(result, "apilaskowski")
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
