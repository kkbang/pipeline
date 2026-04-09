import errno
import json
import os
import re
import tempfile
import time
from pathlib import Path
from typing import Callable, TypeVar


T = TypeVar("T")
RETRYABLE_IO_ERRNOS = {errno.EDEADLK}


class LocalJsonStore:
    def __init__(self, base_dir: str | Path | None = None) -> None:
        configured_dir = os.getenv("LOCAL_DATA_DIR", "").strip()

        if base_dir is None:
            if configured_dir:
                base_dir = configured_dir
            else:
                base_dir = Path(__file__).resolve().parents[2] / "local_data"

        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _resolve_path(self, relative_path: str) -> Path:
        return self.base_dir / relative_path.lstrip("/")

    def _document_path(self, collection_name: str, doc_id: str) -> Path:
        safe_doc_id = doc_id.replace(":", "__").replace("/", "__")
        safe_doc_id = re.sub(r"[^A-Za-z0-9._-]", "_", safe_doc_id)
        return self.base_dir / collection_name / f"{safe_doc_id}.json"

    def _run_io_with_retry(
        self,
        operation: Callable[[], T],
        retries: int = 5,
        delay_seconds: float = 0.05,
    ) -> T:
        last_error: OSError | None = None

        for attempt in range(retries):
            try:
                return operation()
            except OSError as exc:
                last_error = exc
                if exc.errno not in RETRYABLE_IO_ERRNOS or attempt == retries - 1:
                    raise
                time.sleep(delay_seconds * (attempt + 1))

        raise RuntimeError("unreachable") from last_error

    def _read_json(self, path: Path, default: dict | None = None) -> dict:
        if not path.exists():
            return {} if default is None else default

        payload = self._run_io_with_retry(
            lambda: path.read_text(encoding="utf-8"),
        ).strip()
        if not payload:
            return {} if default is None else default

        return json.loads(payload)

    def _write_json(self, path: Path, payload: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        serialized = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            suffix=".tmp",
            delete=False,
        ) as tmp_file:
            tmp_file.write(serialized)
            tmp_path = Path(tmp_file.name)

        try:
            self._run_io_with_retry(lambda: os.replace(tmp_path, path))
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise

    def put_json(self, relative_path: str, payload: dict) -> str:
        path = self._resolve_path(relative_path)
        self._write_json(path, payload)
        return relative_path

    def get_json(self, relative_path: str) -> dict:
        path = self._resolve_path(relative_path)
        return self._read_json(path)

    def get_document(self, collection_name: str, doc_id: str) -> dict:
        path = self._document_path(collection_name, doc_id)
        return self._read_json(path)

    def upsert_document(self, collection_name: str, doc_id: str, body: dict) -> None:
        path = self._document_path(collection_name, doc_id)
        existing = self._read_json(path, default={"_id": doc_id, "_source": {}})
        existing_source = existing.get("_source", {})
        payload = {
            "_id": doc_id,
            "_source": {**existing_source, **body},
        }
        self._write_json(path, payload)

    def update_document(self, collection_name: str, doc_id: str, body: dict) -> None:
        path = self._document_path(collection_name, doc_id)
        existing = self._read_json(path)
        if not existing:
            raise KeyError(f"Document '{doc_id}' does not exist in collection '{collection_name}'")

        existing_source = existing.get("_source", {})
        payload = {
            "_id": doc_id,
            "_source": {**existing_source, **body},
        }
        self._write_json(path, payload)

    def replace_document(self, collection_name: str, doc_id: str, source: dict) -> None:
        path = self._document_path(collection_name, doc_id)
        payload = {
            "_id": doc_id,
            "_source": source,
        }
        self._write_json(path, payload)

    def delete_document(self, collection_name: str, doc_id: str) -> None:
        path = self._document_path(collection_name, doc_id)
        if path.exists():
            path.unlink()

    def list_documents(
        self,
        collection_name: str,
        size: int = 1000,
    ) -> list[dict]:
        collection_dir = self.base_dir / collection_name
        if not collection_dir.exists():
            return []

        hits = []
        for path in sorted(collection_dir.glob("*.json")):
            hits.append(self._read_json(path))
            if len(hits) >= size:
                break

        return hits

    def find_documents_by_field(
        self,
        collection_name: str,
        field_name: str,
        value: str,
        size: int = 1000,
    ) -> list[dict]:
        collection_dir = self.base_dir / collection_name
        if not collection_dir.exists():
            return []

        hits = []
        for path in sorted(collection_dir.glob("*.json")):
            payload = self._read_json(path)
            if payload.get("_source", {}).get(field_name) == value:
                hits.append(payload)

            if len(hits) >= size:
                break

        return hits

    def find_documents_by_status(
        self,
        collection_name: str,
        status: str,
        size: int = 1000,
    ) -> list[dict]:
        return self.find_documents_by_field(
            collection_name=collection_name,
            field_name="status",
            value=status,
            size=size,
        )
