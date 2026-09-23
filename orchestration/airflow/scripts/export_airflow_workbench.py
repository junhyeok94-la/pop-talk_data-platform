"""Export a standalone source distribution without deployment data or business DAGs."""

import argparse
import hashlib
import json
import shutil
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TEMPLATE = ROOT / "distribution" / "airflow-workbench"
PLUGIN = ROOT / "orchestration" / "airflow" / "plugins"
TEMPLATE_FILES = (
    ".dockerignore", ".gitignore", "Dockerfile", "Dockerfile.worker",
    "README.md", "WORKER.md", "RELEASE.md", "VALIDATION.md", "WALKTHROUGH.md", "CHANGELOG.md",
    "compose.yaml", "compose.worker.yaml", "compose.gpu.yaml",
    "requirements.txt", "worker-requirements.txt", "worker-requirements-training.txt",
    "config/bootstrap.json", "config/worker.json",
    "scripts/configure.py", "scripts/initialize.py", "scripts/verify_installation.py",
    "scripts/verify_source.py", "tests/test_distribution_tools.py",
)
TESTS = (
    "workbench_test_db.py", "test_airflow_workbench.py", "test_model_lab_workflow.py",
    "test_workbench_portable.py", "test_workbench_batch.py",
    "test_model_lab_connections.py", "test_workbench_workflows.py",
)


def sources():
    # An allowlist is deliberate: never export the repository or deployment DAG tree.
    for name in TEMPLATE_FILES:
        yield TEMPLATE / name, Path(name)
    for name in ("LICENSE", "NOTICE"):
        if (TEMPLATE / name).is_file():
            yield TEMPLATE / name, Path(name)
    for name in ("workbench_plugin.py", ".airflowignore"):
        yield PLUGIN / name, Path("plugins") / name
    for path in sorted((PLUGIN / "airflow_workbench").rglob("*")):
        relative = path.relative_to(PLUGIN)
        if any(p in {"node_modules", "__pycache__"} for p in relative.parts):
            continue
        if path.is_file() and path.suffix in {".py", ".js", ".cjs", ".css", ".html", ".svg", ".txt", ".json"}:
            yield path, Path("plugins") / relative
    for name in ("worker.py", "state_client.py", "driver.py", "train.py", "cuda_probe.py", "batch_entrypoint.py", "test_worker.py"):
        yield ROOT / "orchestration" / "model_worker" / name, Path("worker") / name
    for name in TESTS:
        yield ROOT / "orchestration" / "airflow" / "tests" / name, Path("tests") / name


def export(destination, archive=False):
    destination = Path(destination).absolute()
    archive_path = destination.parent / (destination.name + ".zip")
    checksum_path = destination.parent / (destination.name + ".zip.sha256")
    if destination.exists():
        raise SystemExit("Destination already exists; choose a new release directory.")
    if archive and (archive_path.exists() or checksum_path.exists()):
        raise SystemExit("Archive or checksum already exists; choose a new release name.")
    entries = list(sources())
    for source, relative in entries:
        if source.is_symlink() or any(p.is_symlink() for p in source.parents if p != ROOT):
            raise SystemExit("Symlink is not allowed in an export: " + str(relative))
        if not source.is_file():
            raise SystemExit("Missing source: " + str(relative))
    destination.mkdir(parents=True)
    manifest = {}
    for source, relative in entries:
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        manifest[relative.as_posix()] = hashlib.sha256(target.read_bytes()).hexdigest()
    (destination / "SOURCE_MANIFEST.json").write_text(
        json.dumps({"format": 1, "files": manifest}, indent=2) + "\n", encoding="utf-8"
    )
    print(f"Exported {len(manifest)} files to {destination}")
    if archive:
        # Fixed metadata makes the same source bytes produce the same archive.
        # Append the extension: with_suffix() would truncate a dotted version.
        with zipfile.ZipFile(archive_path, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as output:
            for relative in sorted([*manifest, "SOURCE_MANIFEST.json"]):
                item = zipfile.ZipInfo(destination.name + "/" + relative, date_time=(1980, 1, 1, 0, 0, 0))
                item.create_system = 3
                item.external_attr = 0o100644 << 16
                output.writestr(item, (destination / relative).read_bytes(), compress_type=zipfile.ZIP_DEFLATED, compresslevel=9)
        checksum = hashlib.sha256(archive_path.read_bytes()).hexdigest()
        with checksum_path.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(checksum + "  " + archive_path.name + "\n")
        print(f"Archive: {archive_path}\nSHA-256: {checksum}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination")
    parser.add_argument("--archive", action="store_true", help="Also create a reproducible ZIP and SHA-256 file")
    args = parser.parse_args()
    export(args.destination, archive=args.archive)
