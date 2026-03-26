# Benchmark Data

이 디렉토리는 benchmark dataset artifact를 두는 위치입니다.

호스트 기준 경로:

```text
code-pipeline/benchmark_data/
```

컨테이너 내부 경로:

```text
/opt/airflow/benchmark_data/
```

현재 benchmark ingestion은 benchmark artifact를 자동 다운로드할 수 있도록 `artifact fetch` 단계를 지원하지만, 실제 source URL은 dataset별로 따로 설정해야 합니다.

예시 배치 구조:

```text
benchmark_data/
├── swe_bench/
│   └── test-00000-of-00001.parquet
├── codesearchnet/
│   └── python/
│       └── final/
│           └── jsonl/
│               └── train/
│                   ├── *.jsonl
│                   └── ...
├── codexglue/
│   └── clone_detection/
│       └── *.jsonl
└── coir/
    └── **/*.jsonl
```

연결 설정은 다음 파일에서 관리합니다.

- `/Users/xxuchan/Desktop/kkbang/code-pipeline/airflow/config/benchmark_datasets.json`

일반적인 사용 순서:

1. benchmark artifact source URL 또는 local file 경로를 `benchmark_datasets.json`에 설정
2. 해당 dataset의 `enabled=true` 설정
3. 필요하면 `artifact_fetch.enabled=true` 설정
4. Airflow가 `benchmark_artifact_fetch_*` 후 `seed_ingestion_benchmark_*` 태스크를 동적으로 생성

현재 기본 예시로 채워진 항목:

- `swe_bench_verified_v1`
- artifact source: official Hugging Face parquet artifact
- target path: `benchmark_data/swe_bench/test-00000-of-00001.parquet`
- `codesearchnet_python_train_v1`
- artifact source: official CodeSearchNet S3 `python.zip`
- extract path: `benchmark_data/codesearchnet/python/final/jsonl/train/*.jsonl`
