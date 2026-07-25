"""Unit tests for models_worker — MLflow-based version creation (#17),
idempotency, error handling.

Finding #17: the previous implementation called
``client.model_versions.create(...)`` which does NOT exist on the
Databricks SDK ``ModelVersionsAPI`` (only delete/get/get_by_alias/
list/update). UC model versions are created via MLflow. These tests
exercise the real call path (MLflow client injected via the
``_registry_client`` seam) — NOT a mock of a fictional SDK method,
which is what let the original bug ship green.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from databricks.sdk.errors import AlreadyExists, PermissionDenied

from migrate import models_worker


def _model_with_one_version():
    return {
        "model_fqn": "c.s.m",
        "comment": None,
        "storage_location": None,
        "versions": [
            {
                "version": "1",
                "source": "runs:/abc/model",
                "storage_location": "abfss://src/model/v1",
                "run_id": "abc",
                "aliases": [],
            },
        ],
    }


def _model_with_three_versions():
    return {
        "model_fqn": "c.s.churn",
        "comment": None,
        "storage_location": None,
        "versions": [
            {
                "version": "1", "source": "runs:/r1/model",
                "storage_location": "abfss://src/churn/v1", "run_id": "r1", "aliases": [],
            },
            {
                "version": "2", "source": "runs:/r2/model",
                "storage_location": "abfss://src/churn/v2", "run_id": "r2", "aliases": [],
            },
            {
                "version": "3", "source": "runs:/r3/model",
                "storage_location": "abfss://src/churn/v3", "run_id": "r3", "aliases": ["champion"],
            },
        ],
    }


def _fake_registry(version_seq=("1",)):
    """A fake MLflow registry client whose create_model_version returns
    objects with sequential target version numbers."""
    reg = MagicMock()
    created = []

    def _create(name, source, run_id=None, **kwargs):
        mv = MagicMock()
        mv.version = version_seq[len(created)] if len(created) < len(version_seq) else str(len(created) + 1)
        created.append({"name": name, "source": source, "run_id": run_id, "version": mv.version})
        return mv

    reg.create_model_version.side_effect = _create
    reg._created = created
    return reg


def _auth():
    return MagicMock()


def test_version_created_via_mlflow_not_sdk(monkeypatch):
    """The worker creates versions through the MLflow registry client's
    create_model_version — NOT the (nonexistent) SDK model_versions.create."""
    auth = _auth()
    reg = _fake_registry(("1",))
    monkeypatch.setattr(models_worker, "_registry_client", lambda a: reg)

    results = models_worker.apply_model(_model_with_one_version(), auth=auth, dry_run=False)

    reg.create_model_version.assert_called_once()
    _, kwargs = reg.create_model_version.call_args
    call = reg._created[0]
    # source resolved to the version's storage_location (reliable cross-workspace)
    assert call["source"] == "abfss://src/model/v1"
    assert call["name"] == "c.s.m"
    assert results[0]["status"] == "validated"
    # The SDK model_versions API must never be used to CREATE a version.
    assert not auth.target_client.model_versions.create.called


def test_run_id_passed_through(monkeypatch):
    """Original run_id is preserved when present (provenance)."""
    auth = _auth()
    reg = _fake_registry(("1",))
    monkeypatch.setattr(models_worker, "_registry_client", lambda a: reg)

    models_worker.apply_model(_model_with_one_version(), auth=auth, dry_run=False)
    assert reg._created[0]["run_id"] == "abc"


def test_source_falls_back_to_source_field_when_no_storage_location(monkeypatch):
    """If storage_location is empty, fall back to the source URI."""
    auth = _auth()
    reg = _fake_registry(("1",))
    monkeypatch.setattr(models_worker, "_registry_client", lambda a: reg)

    model = _model_with_one_version()
    model["versions"][0]["storage_location"] = None
    model["versions"][0]["source"] = "abfss://only/source/v1"

    models_worker.apply_model(model, auth=auth, dry_run=False)
    assert reg._created[0]["source"] == "abfss://only/source/v1"


def test_version_with_no_artifact_location_is_skipped_not_crashed(monkeypatch):
    """Scenario E: a version with neither storage_location nor source can't
    be registered — recorded validation_failed with a clear message, not a
    crash, and create_model_version is not called for it."""
    auth = _auth()
    reg = _fake_registry(())
    monkeypatch.setattr(models_worker, "_registry_client", lambda a: reg)

    model = _model_with_one_version()
    model["versions"][0]["storage_location"] = None
    model["versions"][0]["source"] = None

    results = models_worker.apply_model(model, auth=auth, dry_run=False)
    reg.create_model_version.assert_not_called()
    assert results[0]["status"] == "validation_failed"
    assert "no artifact location" in results[0]["error_message"].lower()


def test_unreadable_source_surfaces_uri_and_validation_failed(monkeypatch):
    """If create_model_version fails (e.g. target can't read source URI),
    the offending URI is surfaced and the row is validation_failed so a
    re-run retries after the operator grants access."""
    auth = _auth()
    reg = MagicMock()
    reg.create_model_version.side_effect = PermissionDenied("cannot read abfss://src/model/v1")
    monkeypatch.setattr(models_worker, "_registry_client", lambda a: reg)

    results = models_worker.apply_model(_model_with_one_version(), auth=auth, dry_run=False)
    assert results[0]["status"] == "validation_failed"
    assert "abfss://src/model/v1" in results[0]["error_message"]


def test_aliases_applied_to_target_version_number(monkeypatch):
    """MLflow assigns its own target version numbers. Aliases must be set
    against the RETURNED target version, never the assumed source number."""
    auth = _auth()
    # target allocates 10, 11, 12 for source versions 1, 2, 3
    reg = _fake_registry(("10", "11", "12"))
    monkeypatch.setattr(models_worker, "_registry_client", lambda a: reg)

    results = models_worker.apply_model(_model_with_three_versions(), auth=auth, dry_run=False)

    # champion alias was on source v3 -> must be set on target version 12
    auth.target_client.registered_models.set_alias.assert_called_once()
    _, kwargs = auth.target_client.registered_models.set_alias.call_args
    assert kwargs["alias"] == "champion"
    assert kwargs["version_num"] == 12
    assert results[0]["status"] == "validated"


def test_model_shell_created_on_target(monkeypatch):
    """The registered-model shell is still created via the SDK on target."""
    auth = _auth()
    reg = _fake_registry(("1",))
    monkeypatch.setattr(models_worker, "_registry_client", lambda a: reg)

    models_worker.apply_model(_model_with_one_version(), auth=auth, dry_run=False)
    auth.target_client.registered_models.create.assert_called_once()


def test_model_shell_already_exists_is_idempotent(monkeypatch):
    """AlreadyExists on the shell create is tolerated (re-run)."""
    auth = _auth()
    auth.target_client.registered_models.create.side_effect = AlreadyExists("exists")
    reg = _fake_registry(("1",))
    monkeypatch.setattr(models_worker, "_registry_client", lambda a: reg)

    results = models_worker.apply_model(_model_with_one_version(), auth=auth, dry_run=False)
    assert results[0]["status"] != "failed"


def test_shell_create_permission_denied_is_failed(monkeypatch):
    """A non-AlreadyExists shell error is recorded failed."""
    auth = _auth()
    auth.target_client.registered_models.create.side_effect = PermissionDenied("nope")
    reg = _fake_registry(("1",))
    monkeypatch.setattr(models_worker, "_registry_client", lambda a: reg)

    results = models_worker.apply_model(_model_with_one_version(), auth=auth, dry_run=False)
    assert results[0]["status"] == "failed"
    assert "nope" in results[0]["error_message"]


def test_dry_run_creates_nothing(monkeypatch):
    """dry_run records skipped and never calls MLflow or the SDK."""
    auth = _auth()
    reg = _fake_registry(("1",))
    monkeypatch.setattr(models_worker, "_registry_client", lambda a: reg)

    results = models_worker.apply_model(_model_with_one_version(), auth=auth, dry_run=True)
    assert results[0]["status"] == "skipped"
    reg.create_model_version.assert_not_called()
    auth.target_client.registered_models.create.assert_not_called()


def test_no_legacy_sdk_model_versions_create_call():
    """Guard (mirrors #5 SDK-drift guard): the source must never call the
    nonexistent ``model_versions.create`` — it doesn't exist on the SDK
    ModelVersionsAPI and reintroducing it reintroduces finding #17."""
    import pathlib

    src = (pathlib.Path(__file__).resolve().parents[2] / "src" / "migrate" / "models_worker.py").read_text()
    # Ignore comment lines — the fix's rationale mentions the old call by name.
    code_lines = [ln for ln in src.splitlines() if not ln.lstrip().startswith("#")]
    code = "\n".join(code_lines)
    assert "model_versions.create" not in code, (
        "models_worker must not call the nonexistent SDK model_versions.create "
        "(finding #17) — use MLflow create_model_version via _registry_client."
    )
