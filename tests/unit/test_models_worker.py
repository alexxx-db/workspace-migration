"""Unit tests for models_worker — MLflow-based version creation (#17),
idempotency, error handling.

Finding #17: the previous implementation called
``client.model_versions.create(...)`` which does NOT exist on the
Databricks SDK ``ModelVersionsAPI`` (only delete/get/get_by_alias/
list/update). UC model versions are created via MLflow.

Option-2 (stage-then-register): a direct ``create_model_version`` from
the source's ``abfss://unity-catalog@…`` storage FAILS cross-metastore
(the target-pointed MLflow client falls back to DefaultAzureCredential,
which has no creds on serverless — proven live 2026-07-25). So the
worker first DOWNLOADS the source version's artifacts to local disk via
the SOURCE registry (UC-vended creds), then registers on the target from
that local path. Two seams the tests inject: ``_download_source_artifacts``
(source-side download → local path) and ``_registry_client`` (target
registry). Neither imports mlflow in the unit tests.
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


def _fake_downloader(paths=None):
    """Fake source-side downloader: records (model_fqn, version) and returns
    a deterministic local path per call."""
    calls = []

    def _dl(auth, model_fqn, version):
        calls.append({"model_fqn": model_fqn, "version": str(version)})
        return f"/local_disk0/dl/{model_fqn}/{version}"

    _dl.calls = calls
    return _dl


def test_version_registered_from_local_download_not_source_uri(monkeypatch):
    """Option-2: the worker DOWNLOADS the source version's artifacts to a
    local path, then registers on target from that LOCAL path — not from
    the source's abfss:// URI (which fails cross-metastore)."""
    auth = _auth()
    reg = _fake_registry(("1",))
    dl = _fake_downloader()
    monkeypatch.setattr(models_worker, "_registry_client", lambda a: reg)
    monkeypatch.setattr(models_worker, "_download_source_artifacts", dl)

    results = models_worker.apply_model(_model_with_one_version(), auth=auth, dry_run=False)

    # Downloaded from the source model + version.
    assert dl.calls == [{"model_fqn": "c.s.m", "version": "1"}]
    reg.create_model_version.assert_called_once()
    call = reg._created[0]
    # source is the LOCAL download path, NOT the abfss:// URI.
    assert call["source"] == "/local_disk0/dl/c.s.m/1"
    assert call["name"] == "c.s.m"
    assert results[0]["status"] == "validated"
    # The SDK model_versions API must never be used to CREATE a version.
    assert not auth.target_client.model_versions.create.called


def test_run_id_passed_through(monkeypatch):
    """Original run_id is preserved when present (provenance)."""
    auth = _auth()
    reg = _fake_registry(("1",))
    monkeypatch.setattr(models_worker, "_registry_client", lambda a: reg)
    monkeypatch.setattr(models_worker, "_download_source_artifacts", _fake_downloader())

    models_worker.apply_model(_model_with_one_version(), auth=auth, dry_run=False)
    assert reg._created[0]["run_id"] == "abc"


def test_version_with_no_artifact_location_is_skipped_not_crashed(monkeypatch):
    """Scenario E: a version with neither storage_location nor source can't
    be downloaded/registered — recorded validation_failed with a clear
    message, not a crash; download + create are not called for it."""
    auth = _auth()
    reg = _fake_registry(())
    dl = _fake_downloader()
    monkeypatch.setattr(models_worker, "_registry_client", lambda a: reg)
    monkeypatch.setattr(models_worker, "_download_source_artifacts", dl)

    model = _model_with_one_version()
    model["versions"][0]["storage_location"] = None
    model["versions"][0]["source"] = None

    results = models_worker.apply_model(model, auth=auth, dry_run=False)
    reg.create_model_version.assert_not_called()
    assert dl.calls == []
    assert results[0]["status"] == "validation_failed"
    assert "no artifact location" in results[0]["error_message"].lower()


def test_download_failure_surfaces_and_validation_failed(monkeypatch):
    """If the source-side artifact download fails (e.g. artifacts GC'd or
    unreadable), the version is validation_failed with the error surfaced,
    and create_model_version is not attempted for it."""
    auth = _auth()
    reg = _fake_registry(("1",))
    monkeypatch.setattr(models_worker, "_registry_client", lambda a: reg)

    def _boom(auth, model_fqn, version):
        raise RuntimeError("artifacts not found at source")

    monkeypatch.setattr(models_worker, "_download_source_artifacts", _boom)

    results = models_worker.apply_model(_model_with_one_version(), auth=auth, dry_run=False)
    reg.create_model_version.assert_not_called()
    assert results[0]["status"] == "validation_failed"
    assert "artifacts not found at source" in results[0]["error_message"]


def test_register_failure_surfaces_and_validation_failed(monkeypatch):
    """If target create_model_version fails, the row is validation_failed
    with the error surfaced so a re-run retries."""
    auth = _auth()
    reg = MagicMock()
    reg.create_model_version.side_effect = PermissionDenied("target register denied")
    monkeypatch.setattr(models_worker, "_registry_client", lambda a: reg)
    monkeypatch.setattr(models_worker, "_download_source_artifacts", _fake_downloader())

    results = models_worker.apply_model(_model_with_one_version(), auth=auth, dry_run=False)
    assert results[0]["status"] == "validation_failed"
    assert "target register denied" in results[0]["error_message"]


def test_aliases_applied_to_target_version_number(monkeypatch):
    """MLflow assigns its own target version numbers. Aliases must be set
    against the RETURNED target version, never the assumed source number."""
    auth = _auth()
    # target allocates 10, 11, 12 for source versions 1, 2, 3
    reg = _fake_registry(("10", "11", "12"))
    monkeypatch.setattr(models_worker, "_registry_client", lambda a: reg)
    monkeypatch.setattr(models_worker, "_download_source_artifacts", _fake_downloader())

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
    monkeypatch.setattr(models_worker, "_download_source_artifacts", _fake_downloader())

    models_worker.apply_model(_model_with_one_version(), auth=auth, dry_run=False)
    auth.target_client.registered_models.create.assert_called_once()


def test_model_shell_already_exists_is_idempotent(monkeypatch):
    """AlreadyExists on the shell create is tolerated (re-run)."""
    auth = _auth()
    auth.target_client.registered_models.create.side_effect = AlreadyExists("exists")
    reg = _fake_registry(("1",))
    monkeypatch.setattr(models_worker, "_registry_client", lambda a: reg)
    monkeypatch.setattr(models_worker, "_download_source_artifacts", _fake_downloader())

    results = models_worker.apply_model(_model_with_one_version(), auth=auth, dry_run=False)
    assert results[0]["status"] != "failed"


def test_model_shell_already_exists_by_message_is_idempotent(monkeypatch):
    """The runtime doesn't always map an existing-model error to the
    AlreadyExists class — live it raised a generic error 'Routine or Model
    <name> already exists'. Idempotency must be message-based, not solely
    class-based, so a re-run continues to versions rather than failing."""
    auth = _auth()
    auth.target_client.registered_models.create.side_effect = Exception(
        "Routine or Model 'm' already exists"
    )
    reg = _fake_registry(("1",))
    monkeypatch.setattr(models_worker, "_registry_client", lambda a: reg)
    monkeypatch.setattr(models_worker, "_download_source_artifacts", _fake_downloader())

    results = models_worker.apply_model(_model_with_one_version(), auth=auth, dry_run=False)
    # Must NOT be 'failed' — the shell already exists, proceed to versions.
    assert results[0]["status"] != "failed"
    reg.create_model_version.assert_called_once()


def test_shell_create_permission_denied_is_failed(monkeypatch):
    """A non-AlreadyExists shell error is recorded failed."""
    auth = _auth()
    auth.target_client.registered_models.create.side_effect = PermissionDenied("nope")
    reg = _fake_registry(("1",))
    monkeypatch.setattr(models_worker, "_registry_client", lambda a: reg)
    monkeypatch.setattr(models_worker, "_download_source_artifacts", _fake_downloader())

    results = models_worker.apply_model(_model_with_one_version(), auth=auth, dry_run=False)
    assert results[0]["status"] == "failed"
    assert "nope" in results[0]["error_message"]


def test_dry_run_creates_nothing(monkeypatch):
    """dry_run records skipped and never calls download, MLflow, or the SDK."""
    auth = _auth()
    reg = _fake_registry(("1",))
    dl = _fake_downloader()
    monkeypatch.setattr(models_worker, "_registry_client", lambda a: reg)
    monkeypatch.setattr(models_worker, "_download_source_artifacts", dl)

    results = models_worker.apply_model(_model_with_one_version(), auth=auth, dry_run=True)
    assert results[0]["status"] == "skipped"
    reg.create_model_version.assert_not_called()
    assert dl.calls == []
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
