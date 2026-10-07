# Copyright (C) 2026 ScyllaDB
"""Tests for the published documentation version list."""

import unittest

from released_versions import select_versions


def release(tag, published_at, draft=False):
    return {"tag_name": tag, "published_at": published_at, "draft": draft}


class SelectVersionsTest(unittest.TestCase):
    def test_ignores_drafts_other_modules_and_malformed_tags(self):
        releases = [
            release("v1.20.0", "2026-10-06"),
            release("v1.21.0", "2026-10-07", draft=True),
            release("lz4/v1.20.0", "2026-10-06"),
            release("v1.22", "2026-10-08"),
        ]

        self.assertEqual(
            select_versions(releases),
            {"tags": ["v1.20.0"], "latest": "v1.20.0", "unstable": []},
        )

    def test_uses_semantic_latest_and_keeps_recent_prereleases(self):
        releases = [
            release("v1.20.0-rc.1", "2026-10-09"),
            release("v1.19.1", "2026-10-08"),
            release("v1.20.0", "2026-10-06"),
            release("v1.18.3", "2026-06-29"),
            release("v1.18.2", "2026-06-16"),
            release("v1.18.1", "2026-06-10"),
        ]

        self.assertEqual(
            select_versions(releases),
            {
                "tags": ["v1.20.0-rc.1", "v1.19.1", "v1.20.0", "v1.18.3", "v1.18.2"],
                "latest": "v1.20.0",
                "unstable": ["v1.20.0-rc.1"],
            },
        )

    def test_adds_requested_published_tag_outside_recent_five(self):
        releases = [release(f"v1.{minor}.0", f"2026-10-{minor:02d}") for minor in range(10, 16)]

        selected = select_versions(releases, "v1.10.0")

        self.assertEqual(selected["tags"], [
            "v1.15.0", "v1.14.0", "v1.13.0", "v1.12.0", "v1.11.0", "v1.10.0",
        ])
        self.assertEqual(selected["latest"], "v1.15.0")

    def test_keeps_highest_stable_version_after_newer_backfilled_releases(self):
        releases = [release(f"v1.19.{patch}", f"2026-10-{patch + 10:02d}") for patch in range(5)]
        releases.append(release("v1.20.0", "2026-09-01"))

        selected = select_versions(releases)

        self.assertEqual(selected["latest"], "v1.20.0")
        self.assertEqual(selected["tags"][-1], "v1.20.0")

    def test_rejects_unpublished_requested_tag(self):
        with self.assertRaisesRegex(ValueError, "not a published root release"):
            select_versions([release("v1.20.0", "2026-10-06")], "v1.21.0")

    def test_requires_stable_release(self):
        with self.assertRaisesRegex(ValueError, "no published stable root release"):
            select_versions([release("v1.21.0-rc.1", "2026-10-07")])


if __name__ == "__main__":
    unittest.main()
