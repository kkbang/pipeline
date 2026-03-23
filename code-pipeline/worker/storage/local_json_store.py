import json
import os
import re
from pathlib import Path


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

    def _read_json(self, path: Path, default: dict | None = None) -> dict:
        if not path.exists():
            return {} if default is None else default

        payload = path.read_text(encoding="utf-8").strip()
        if not payload:
            return {} if default is None else default

        return json.loads(payload)

    def _write_json(self, path: Path, payload: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def put_json(self, relative_path: str, payload: dict) -> str:
        path = self._resolve_path(relative_path)
        self._write_json(path, payload)
        return relative_path

    def get_json(self, relative_path: str) -> dict:
        path = self._resolve_path(relative_path)
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

    def find_documents_by_status(
        self,
        collection_name: str,
        status: str,
        size: int = 1000,
    ) -> list[dict]:
        collection_dir = self.base_dir / collection_name
        if not collection_dir.exists():
            return []

        hits = []
        for path in sorted(collection_dir.glob("*.json")):
            payload = self._read_json(path)
            if payload.get("_source", {}).get("status") == status:
                hits.append(payload)

            if len(hits) >= size:
                break

        return hits
