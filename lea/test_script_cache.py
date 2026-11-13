from __future__ import annotations

import os

import pytest

from .dialects import DuckDBDialect
from .scripts import (
    _load_cache,
    _save_cache,
    read_scripts,
)


@pytest.fixture
def scripts_dir(tmp_path):
    """Create a minimal scripts directory with one SQL file."""
    core = tmp_path / "scripts" / "core"
    core.mkdir(parents=True)
    (core / "users.sql").write_text("SELECT 1 AS id, 'alice' AS name")
    return tmp_path / "scripts"


@pytest.fixture
def cache_dir(tmp_path):
    return tmp_path / ".lea_cache"


class TestScriptCache:
    def test_cache_miss_then_hit(self, scripts_dir, cache_dir):
        """First read is a cache miss (populates cache), second is a hit."""
        kwargs = dict(
            scripts_dir=scripts_dir,
            sql_dialect=DuckDBDialect(),
            dataset_name="test",
            project_name=None,
            cache_dir=cache_dir,
        )

        # First read — cache miss, should create cache file
        scripts_1 = read_scripts(**kwargs)  # ty: ignore[invalid-argument-type]
        assert len(scripts_1) == 1
        assert (cache_dir / "cache.pkl").exists()

        # Second read — cache hit
        scripts_2 = read_scripts(**kwargs)  # ty: ignore[invalid-argument-type]
        assert len(scripts_2) == 1

        # Fields and dependencies should match
        assert scripts_1[0].fields == scripts_2[0].fields
        assert scripts_1[0].dependencies == scripts_2[0].dependencies

    def test_cache_eviction_on_mtime_change(self, scripts_dir, cache_dir):
        """Modifying a script file should invalidate its cache."""
        kwargs = dict(
            scripts_dir=scripts_dir,
            sql_dialect=DuckDBDialect(),
            dataset_name="test",
            project_name=None,
            cache_dir=cache_dir,
        )

        # Populate cache
        scripts_1 = read_scripts(**kwargs)  # ty: ignore[invalid-argument-type]
        assert scripts_1[0].fields[0].name == "id"  # ty: ignore[not-subscriptable]

        # Modify the script — change field names
        sql_file = scripts_dir / "core" / "users.sql"
        sql_file.write_text("SELECT 1 AS user_id, 'alice' AS user_name")

        # Force mtime change (some filesystems have coarse resolution)
        mtime = sql_file.stat().st_mtime
        os.utime(sql_file, (mtime + 1, mtime + 1))

        # Should get new fields (cache miss due to mtime change)
        scripts_2 = read_scripts(**kwargs)  # ty: ignore[invalid-argument-type]
        assert scripts_2[0].fields[0].name == "user_id"  # ty: ignore[not-subscriptable]

    def test_cache_hit_still_has_code(self, scripts_dir, cache_dir):
        """On cache hit, code is still loaded (needed for execution), but parsing is skipped."""
        kwargs = dict(
            scripts_dir=scripts_dir,
            sql_dialect=DuckDBDialect(),
            dataset_name="test",
            project_name=None,
            cache_dir=cache_dir,
        )

        # Populate cache
        scripts_1 = read_scripts(**kwargs)  # ty: ignore[invalid-argument-type]
        assert scripts_1[0].code != ""

        # Cache hit — code should still be present
        scripts_2 = read_scripts(**kwargs)  # ty: ignore[invalid-argument-type]
        assert scripts_2[0].code == scripts_1[0].code

    def test_corrupt_cache_is_ignored(self, scripts_dir, cache_dir):
        """A corrupt cache file should be treated as a cache miss."""
        cache_dir.mkdir(parents=True)

        # Write a corrupt cache file
        (cache_dir / "cache.pkl").write_bytes(b"not a pickle file")

        scripts = read_scripts(
            scripts_dir=scripts_dir,
            sql_dialect=DuckDBDialect(),
            dataset_name="test",
            project_name=None,
            cache_dir=cache_dir,
        )
        assert len(scripts) == 1
        assert scripts[0].fields[0].name == "id"  # ty: ignore[not-subscriptable]

    def test_no_cache_dir(self, scripts_dir):
        """Without cache_dir, scripts are read normally."""
        scripts = read_scripts(
            scripts_dir=scripts_dir,
            sql_dialect=DuckDBDialect(),
            dataset_name="test",
            project_name=None,
            cache_dir=None,
        )
        assert len(scripts) == 1

    def test_dependencies_cached(self, scripts_dir, cache_dir):
        """Dependencies should be correctly cached and restored."""
        # Create a script with a dependency
        analytics = scripts_dir / "analytics"
        analytics.mkdir()
        (analytics / "user_count.sql").write_text("SELECT COUNT(*) AS n FROM core.users")

        kwargs = dict(
            scripts_dir=scripts_dir,
            sql_dialect=DuckDBDialect(),
            dataset_name="test",
            project_name=None,
            cache_dir=cache_dir,
        )

        scripts_1 = read_scripts(**kwargs)  # ty: ignore[invalid-argument-type]
        user_count_1 = [s for s in scripts_1 if s.table_ref.name == "user_count"][0]

        scripts_2 = read_scripts(**kwargs)  # ty: ignore[invalid-argument-type]
        user_count_2 = [s for s in scripts_2 if s.table_ref.name == "user_count"][0]

        assert user_count_1.dependencies == user_count_2.dependencies
        # Should have a dependency on core.users
        dep_names = {d.name for d in user_count_2.dependencies}
        assert "users" in dep_names


class TestCacheHelpers:
    def test_save_and_load_round_trip(self, tmp_path):
        """Round-trip save/load."""
        cp = tmp_path / "cache.pkl"
        entries = {
            "core/users.sql": {"mtime": 123.456, "fields": [{"name": "id"}], "dependencies": set()},
        }
        _save_cache(cp, entries)
        loaded = _load_cache(cp)
        assert "core/users.sql" in loaded
        assert loaded["core/users.sql"]["fields"] == [{"name": "id"}]

    def test_load_corrupt_returns_empty(self, tmp_path):
        """Corrupt file returns empty dict."""
        cp = tmp_path / "cache.pkl"
        cp.write_bytes(b"garbage")
        assert _load_cache(cp) == {}

    def test_load_missing_returns_empty(self, tmp_path):
        """Missing file returns empty dict."""
        cp = tmp_path / "nonexistent.pkl.gz"
        assert _load_cache(cp) == {}
