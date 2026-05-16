# `worker/repo` package layout

- `chunking/`: code chunk document generation, chunking, embedding, local query repo prep
- `common/`: shared repo-processing helpers for time parsing, repo identity, and OpenSearch query snippets
- `snapshot/`: repo snapshot download and local snapshot path/cleanup utilities
- `extraction/`: extracted repo file indexing
- `pipeline/`: stage manifests, shard planning, retry/recovery, stage selection
- `validation/`: repo processing validation
- `registration/`: direct GitHub URL registration and single-repo processing flow

The root package is intentionally kept thin so domain code lives under a single responsibility boundary.
