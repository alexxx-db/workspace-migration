"""Guard that the user_guide documents the like-for-like Hive SPN permissions."""

from __future__ import annotations

import pathlib

_GUIDE = (pathlib.Path(__file__).resolve().parents[2] / "docs" / "user_guide.md").read_text()


class TestUserGuideHiveLikeForLike:
    def test_has_spn_permissions_section(self):
        assert "SPN permissions on hive_metastore" in _GUIDE

    def test_documents_key_requirements(self):
        for token in (
            "hive_dbfs_staging_path",
            "shared staging",
            "DBFS root enabled",
            "/mnt",
            "like-for-like",
        ):
            assert token in _GUIDE, f"user_guide missing {token!r}"

    def test_drops_uc_upgrade_wording(self):
        # The retired UC-upgrade Hive config keys must not linger in the guide.
        assert "hive_target_catalog" not in _GUIDE
        assert "hive_dbfs_target_path" not in _GUIDE

    def test_grants_file_access_on_staging_external_location(self):
        # Finding #24: the Step 4 grant recipe must grant READ FILES + WRITE FILES
        # on the external location backing hive_dbfs_staging_path. Without them the
        # SPN's staging write (and the check_hive_dbfs_root_config probe) fails when
        # migrate_hive_dbfs_root=true.
        assert "GRANT READ FILES" in _GUIDE, "user_guide missing READ FILES grant for staging ext-loc"
        assert "GRANT WRITE FILES" in _GUIDE, "user_guide missing WRITE FILES grant for staging ext-loc"
