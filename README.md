# Code Pipeline

라이선스 파생 코드 탐지용 데이터셋을 만들기 위한 파이프라인 프로젝트입니다.

이 저장소의 최종 목표는 공개 코드 저장소를 대규모로 수집한 뒤, 코드와 라이선스 문맥을 함께 보존하고, 이후 function-level chunking, feature/embedding 생성, 유사도 검색, license-aware 판단까지 이어지는 데이터 파이프라인을 구축하는 것입니다.

현재 구현은 `Seed Source -> Repo Discovery -> Repo Registry`를 넘어, `Repository Snapshot Download -> File Extraction -> Tree-sitter Symbol Chunking -> Normalization / Anonymization / Summary Feature Extraction -> Optional Embedding Enrichment -> Validation`까지 기본 동작이 들어와 있습니다.

## 한눈에 보기

- 최종 목표: 라이선스 리스크가 있는 유사 코드 탐지를 위한 학습/검증 데이터셋 구축
- 현재 구현 범위: seed source 수집, GitHub 저장소 식별, repo registry 구축, repo snapshot download, file extraction, tree-sitter 기반 function/class chunking, normalization, anonymization, summary feature 추출, optional embedding enrichment, validation
- 현재 seed source:
  - package registry repo: `PyPI`, `npm`, `nuget`, `maven`, `crates.io` 등을 포함한 다중 registry adapter
  - curated repo list: 정적 GitHub repo URL 목록
  - benchmark dataset source: benchmark dataset artifact에서 추출한 공개 repo
  - GitHub org / search / topic seed: 코드 구현 완료, config 기반 활성화
- 현재 기본 저장소: `OpenSearch`
- 장기 설계 저장소: `OpenSearch` 중심, object storage 확장 가능
- 실행 환경: `Airflow + Docker Compose`
- 운영 보조 스크립트: OpenSearch 문서와 local snapshot cleanup 상태를 맞추는 스크립트 포함

## 왜 이 프로젝트가 필요한가

LLM이 생성한 코드나 대규모 코드 코퍼스 안의 유사 코드를 라이선스 관점에서 판단하려면, 단순히 "코드가 비슷하다"는 사실만으로는 부족합니다. 실제로는 아래 정보가 함께 필요합니다.

- 원본 저장소가 어디인지
- 그 저장소가 어떤 배포 생태계와 연결되는지
- 라이선스가 무엇인지
- 어떤 seed source가 그 저장소를 발견했는지
- 나중에 어떤 파일/함수/청크가 유사하게 매칭되는지

즉, 이 프로젝트의 첫 단계는 "의미 있는 공개 저장소 후보를 설득력 있는 근거와 함께 모으는 것"입니다. 현재 저장소는 바로 그 첫 단계를 구현합니다.

## 프로젝트 전체 그림

장기적으로는 아래와 같은 파이프라인을 목표로 합니다.

```text
[Seed Sources]
  -> [Repo Discovery]
  -> [Repo Registry]
  -> [Repository Snapshot Download]
  -> [File Extraction / Validation]
  -> [Code Chunking]
  -> [Normalization / Anonymization]
  -> [Feature / Embedding Generation]
  -> [Similarity Retrieval]
  -> [License-aware Judgement]
  -> [Dataset Export]
```

설계 문서 기준으로 이후 단계에서 중요하게 다뤄야 할 주제는 다음과 같습니다.

- function / class 단위 tree-sitter chunking
- AST / DFG 기반 feature
- embedding 기반 검색
- dedup 및 leakage 방지
- 대규모 처리와 확장성

## 현재 저장소가 담당하는 범위

현재 코드가 실제로 하는 일은 아래와 같습니다.

1. seed source에서 후보를 가져옵니다.
2. 후보 안에 들어 있는 repo URL을 수집합니다.
3. 여러 URL 중 GitHub canonical repo를 식별합니다.
4. GitHub API로 저장소 메타데이터를 확인합니다.
5. 등록 가능한 repo만 `repo_registry_index`에 올립니다.
6. 발견 경로와 provenance를 함께 남깁니다.
7. registered repo의 snapshot tarball을 내려받습니다.
8. snapshot에서 파일을 추출합니다.
9. 코드 파일을 tree-sitter 기반 `function/class` chunk로 분할합니다.
10. 각 chunk에 `raw_code`, `normalized_code`, `anonymized_code`, summary feature, optional embedding을 생성합니다.
11. chunk 결과를 검증하고 다음 crawl 배치를 이어갈지 판단합니다.

즉, 현재 저장소의 직접적인 산출물은 `repo registry`뿐 아니라, 그 이후 단계의 snapshot download, file extraction, `code_chunk_index` 생성, validation 결과까지 포함합니다.

현재 repo 처리 계층의 운영 구조와 문제 해결 내역은 아래 문서를 참고합니다.

- [repo-manifest-pipeline-architecture.md](/Users/xxuchan/Desktop/kkbang/docs/repo-manifest-pipeline-architecture.md)
- [repo-processing-current-state.md](/Users/xxuchan/Desktop/kkbang/docs/repo-processing-current-state.md)
- [large-scale-collection-current-state.md](/Users/xxuchan/Desktop/kkbang/docs/large-scale-collection-current-state.md)

## Seed Source 전략

이 프로젝트는 seed source를 한 번에 다 붙이기보다, 설명력이 높은 source부터 차례대로 확장하는 방식으로 설계되어 있습니다.

초기 seed와 후속 확장을 구분하면 현재 상태는 아래와 같습니다.

### 1. 초기 Seed Source

| Source | 의미 | 상태 |
| --- | --- | --- |
| Package Registry Repo | package registry 메타데이터에서 공식 repo 후보 추출 | 구현됨 |
| Curated Repo List | 사람이 직접 고른 공개 repo 목록 | 구현됨 |
| Benchmark Dataset Source | benchmark dataset artifact에서 repo를 추출해 seed로 등록 | 구현됨 |
| GitHub Org Seed | GitHub org 단위로 공개 repo 수집 | 구현됨 |
| GitHub Search Seed | query 기반 GitHub search 결과를 seed로 등록 | 구현됨 |
| GitHub Topic Seed | GitHub topic 기반 search 결과를 seed로 등록 | 구현됨 |
| StackOverflow Repo Source | StackOverflow 질문/답변 문맥에서 언급된 공개 repo를 seed로 등록 | 예정 |
| Direct Repo Seed | 사용자가 repo URL을 직접 지정 | 예정 |
| GitHub User Seed | GitHub user 아래 repo 일괄 수집 | 예정 |

### 2. 후속 Expansion / Relation

| 기능 | 의미 | 상태 |
| --- | --- | --- |
| README External Link Expansion | README 안의 외부 GitHub 링크를 새로운 seed로 확장 | 구현됨 |
| Dependency Manifest Expansion | manifest/lockfile 안의 repo 힌트를 새로운 seed로 확장 | 구현됨 |
| Same Owner Relation | 같은 owner 아래 repo 간 관계 생성 | 구현됨 |
| Same Project Family Relation | 이름 패턴이 유사한 repo 간 관계 생성 | 구현됨 |
| Fork Relation | fork 관계를 relation graph로 기록 | 구현됨 |
| Near-Duplicate Relation | 유사 repo 후보를 relation graph로 연결 | 예정 |

현재 `seed_discovery_dag`는 package registry, curated repo, benchmark dataset, GitHub org/search/topic source를 모두 포함할 수 있도록 연결되어 있습니다. 실제 태스크 생성 여부는 config 파일과 `enabled` 플래그에 따라 결정됩니다.

현재 운영 원칙은 다음과 같습니다.

- `curated repo list`: 사람이 명시적으로 넣는 고정밀 단일 repo seed
- `benchmark dataset source`: benchmark record에서 repo provenance를 함께 가져오는 고신뢰 seed

즉, 같은 repo를 여러 source에 무작정 중복으로 넣기보다, curated는 "꼭 포함해야 하는 대표 repo", benchmark dataset은 "평가/연구 문맥이 명확한 dataset provenance를 가진 repo"로 분리해서 관리하는 것을 원칙으로 합니다.

탐지 단계에서의 `query expansion`은 seed discovery와는 별도 개념으로 보고 있습니다. 이 기능은 아직 구현 전이며, repo를 더 모으기 위한 search query 확장이 아니라, **이미 수집한 코드 조각을 다양한 변형 형태로 다시 검색하는 탐지용 query 확장**을 의미합니다. 자세한 설계 메모는 [docs/query-expansion.md](docs/query-expansion.md)에 정리했습니다.

현재 `code_chunk_index`와 그 위에 올라갈 탐지 설계 관련 문서는 아래를 참고합니다.

- [code-chunk-implementation.md](/Users/xxuchan/Desktop/kkbang/docs/code-chunk-implementation.md)
- [query-expansion.md](/Users/xxuchan/Desktop/kkbang/docs/query-expansion.md)
- [query-expansion-design.md](/Users/xxuchan/Desktop/kkbang/docs/query-expansion-design.md)
- [code-chunk-embedding.md](/Users/xxuchan/Desktop/kkbang/docs/code-chunk-embedding.md)

## 현재 아키텍처

현재 구현 범위만 놓고 보면 흐름은 아래와 같습니다.

```text
[Package Registry / Curated Repo / Benchmark Dataset / GitHub Org / GitHub Search / GitHub Topic]
  -> [Seed Ingestion]
  -> [seed_item_index]
  -> [Seed Normalization]
  -> [GitHub Repo Qualification]
  -> [repo_registry_index]
  -> [Repository Snapshot Download]
  -> [File Extraction]
  -> [Code Chunking]
  -> [Validation]
```

핵심 역할은 다음처럼 나뉩니다.

- Airflow: orchestration과 실행 순서 제어
- worker: seed 수집, URL 추출, normalize, qualification 로직 수행
- OpenSearch: 현재 기본 저장소
- Backblaze B2: 장기 설계상 아카이브 저장소 인터페이스

현재 구현 관점에서 보면 구조는 크게 두 층으로 나뉩니다.

- Seed discovery 레이어: 여러 source에서 공개 repo 후보를 수집하고 canonical repo registry를 구축
- Repo processing 레이어: registry에 등록된 repo를 snapshot/file/chunk 단위 데이터로 확장

## 현재 구현된 DAG

현재 seed DAG는 역할별로 분리되어 있습니다.

- [seed_discovery_dag.py](/Users/xxuchan/Desktop/kkbang/airflow/dags/seed_discovery_dag.py)
  - DAG ID: `seed_discovery_dag`
  - 스케줄: 수동/trigger 전용
  - 역할: base seed ingestion -> normalization -> qualification
- [seed_expansion_dag.py](/Users/xxuchan/Desktop/kkbang/airflow/dags/seed_expansion_dag.py)
  - DAG ID: `seed_expansion_dag`
  - 스케줄: 수동/trigger 전용
  - 역할: README / dependency expansion -> normalization -> qualification
- [repo_relation_dag.py](/Users/xxuchan/Desktop/kkbang/airflow/dags/repo_relation_dag.py)
  - DAG ID: `repo_relation_dag`
  - 스케줄: 수동/trigger 전용
  - 역할: registered repo 간 관계 그래프 생성
- [seed_pipeline_dag.py](/Users/xxuchan/Desktop/kkbang/airflow/dags/seed_pipeline_dag.py)
  - DAG ID: `seed_pipeline_dag`
  - 스케줄: 수동/compatibility 전용
  - 역할: `seed_discovery_dag` trigger
- [code_pipeline_dag.py](/Users/xxuchan/Desktop/kkbang/airflow/dags/code_pipeline_dag.py)
  - DAG ID: `code_pipeline_dag`
  - 스케줄: 수동/trigger 전용
  - 역할: registered repo snapshot download 시작
- [repo_extract_dag.py](/Users/xxuchan/Desktop/kkbang/airflow/dags/repo_extract_dag.py)
  - DAG ID: `repo_extract_dag`
  - 스케줄: `code_pipeline_dag`에서 trigger
  - 역할: snapshot에서 텍스트/코드 파일 추출
- [repo_chunk_dag.py](/Users/xxuchan/Desktop/kkbang/airflow/dags/repo_chunk_dag.py)
  - DAG ID: `repo_chunk_dag`
  - 스케줄: `repo_extract_dag`에서 trigger
  - 역할: 추출된 코드를 chunk 단위로 분할
- [repo_validation_dag.py](/Users/xxuchan/Desktop/kkbang/airflow/dags/repo_validation_dag.py)
  - DAG ID: `repo_validation_dag`
  - 스케줄: `repo_chunk_dag`에서 trigger
  - 역할: repo 처리 결과 검증 및 다음 crawl 필요 여부 판단

현재 seed discovery DAG는 config-driven 구조입니다.

- package registry ingestion: `seed_ingestion_<registry>`
- curated repo ingestion: `seed_ingestion_curated_<list_name>`
- benchmark dataset ingestion: `seed_ingestion_benchmark_<dataset_name>`
- GitHub org ingestion: `seed_ingestion_github_org_<list_name>`
- GitHub search ingestion: `seed_ingestion_github_search_<list_name>`
- GitHub topic ingestion: `seed_ingestion_github_topic_<list_name>`
- normalization: dynamic task mapping 기반 `seed_normalization_shard`
- qualification: dynamic task mapping 기반 `seed_qualification_shard`

benchmark dataset source는 [`benchmark_datasets.json`](/Users/xxuchan/Desktop/kkbang/airflow/config/benchmark_datasets.json) 에서 `enabled=true` 인 항목만 실제 태스크가 생성됩니다. `artifact_fetch.enabled=true` 인 경우에는 `benchmark_artifact_fetch_*` 태스크가 먼저 실행되어 dataset artifact를 `benchmark_data/` 아래에 가져온 뒤 ingestion 단계로 넘깁니다.

기본 실행 순서는 다음과 같습니다.

```text
[seed_ingestion_package_registry_*]
[seed_ingestion_curated_*]
[seed_ingestion_benchmark_*]
[seed_ingestion_github_org_*]
[seed_ingestion_github_search_*]
[seed_ingestion_github_topic_*]
          -> [seed_normalization_shard[*]]
          -> [seed_qualification_shard[*]]
```

repo processing 기본 실행 순서는 다음과 같습니다.

```text
[code_pipeline_dag]
  -> [repo_snapshot_download]
  -> [repo_extract_dag]
  -> [repo_chunk_dag]
  -> [repo_validation_dag]
```

현재 repo processing DAG는 각 단계가 `batch_id`를 넘기며 연결되고, extract/chunk/validation 단계는 shard fan-out 방식으로 병렬 처리됩니다. `repo_validation_dag`가 끝난 뒤 pending crawl 대상이 남아 있으면 `code_pipeline_dag`를 다시 trigger 하는 self-draining 구조로 이어집니다.

## 단계별 역할

### 1. Seed Ingestion

파일:

- [seed_ingest_service.py](/Users/xxuchan/Desktop/kkbang/worker/seed/services/seed_ingest_service.py)
- [dataset.py](/Users/xxuchan/Desktop/kkbang/worker/seed/adapters/benchmark/dataset.py)
- [pypi.py](/Users/xxuchan/Desktop/kkbang/worker/seed/adapters/package_registry/pypi.py)
- [npm.py](/Users/xxuchan/Desktop/kkbang/worker/seed/adapters/package_registry/npm.py)
- [repo_list.py](/Users/xxuchan/Desktop/kkbang/worker/seed/adapters/curated/repo_list.py)

역할:

- package registry, curated list, benchmark dataset source에서 입력을 가져옵니다.
- raw metadata를 `seed_item_index.raw_metadata`로 저장합니다.
- `candidate_repo_urls`를 미리 계산해 `seed_item_index`에 함께 저장합니다.
- 문서 상태를 `ingested`로 기록합니다.

특징:

- `PyPI`, `npm`은 compact raw만 기본 저장합니다.
- full raw 전체 응답은 `KEEP_FULL_PACKAGE_REGISTRY_RAW=true` 일 때만 별도 저장합니다.
- curated repo list는 repo URL 문자열만으로도 ingest 가능합니다.
- benchmark dataset source는 local artifact를 읽고, record에서 repo를 추출한 뒤 dataset provenance와 함께 기록합니다.
- benchmark dataset source는 필요할 경우 `artifact fetch -> dataset ingest` 2단계로 동작합니다.

### 2. Seed Normalization

파일:

- [seed_normalize_service.py](/Users/xxuchan/Desktop/kkbang/worker/seed/services/seed_normalize_service.py)
- [repo_url_extractor.py](/Users/xxuchan/Desktop/kkbang/worker/seed/extractors/repo_url_extractor.py)
- [github_url_canonicalizer.py](/Users/xxuchan/Desktop/kkbang/worker/seed/resolvers/github_url_canonicalizer.py)

역할:

- `ingested` 상태 문서를 읽습니다.
- `candidate_repo_urls` 또는 raw metadata에서 repo 후보를 가져옵니다.
- GitHub repo URL을 canonical form으로 정규화합니다.
- 성공 시 `normalized`, 실패 시 `invalid`로 상태를 갱신합니다.

이 단계는 "후보 URL 수집"과 "실제 공식 repo 식별"을 분리해 주는 역할을 합니다.

### 3. Seed Qualification

파일:

- [seed_qualify_service.py](/Users/xxuchan/Desktop/kkbang/worker/seed/services/seed_qualify_service.py)
- [repo_metadata.py](/Users/xxuchan/Desktop/kkbang/worker/seed/adapters/github/repo_metadata.py)
- [repo_qualifier.py](/Users/xxuchan/Desktop/kkbang/worker/seed/qualifiers/repo_qualifier.py)

역할:

- `normalized` 상태 문서를 읽습니다.
- GitHub REST API로 실제 repo 메타데이터를 조회합니다.
- public/private, archived, fork 여부 등을 기준으로 등록 가능 여부를 판단합니다.
- 통과한 repo를 `repo_registry_index`에 upsert 합니다.
- 실패 시 `rejected` 또는 `qualification_failed`로 남깁니다.

특징:

- GitHub token 없이도 동작하도록 무인증 요청 기반으로 구성되어 있습니다.
- 단, GitHub API rate limit에 걸릴 수 있습니다.
- 같은 repo를 여러 source가 발견하면 `discovery_sources`를 누적해 provenance를 보존합니다.

### 4. Repository Snapshot Download

파일:

- [repo_crawl_service.py](/Users/xxuchan/Desktop/kkbang/worker/repo/repo_crawl_service.py)
- [repo_snapshot_local_paths.py](/Users/xxuchan/Desktop/kkbang/worker/repo/repo_snapshot_local_paths.py)

역할:

- `repo_registry_index`에서 `crawl_status`가 `scheduled`, `crawl_failed`, 또는 stale `downloading`인 repo를 선택합니다.
- `repo_size_kb` 기반 `normal / whale_hint / giant_hint` tier를 참고해 crawl batch를 구성합니다.
- `normal` repo를 먼저 처리하고, 그 다음 `whale/giant hint` repo를 처리하는 2-phase scheduling을 사용합니다.
- download concurrency와 extract concurrency를 분리해 큰 repo의 압축 해제가 download 슬롯을 오래 점유하지 않게 합니다.
- GitHub snapshot tarball을 내려받고 `local_data/raw/repo_snapshot_download/`와 `local_data/raw/repo_snapshot/` 아래에 저장합니다.
- 성공 시 `crawl_status=downloaded`로 갱신해 다음 extract 단계가 이어서 처리할 수 있게 합니다.

### 5. File Extraction

파일:

- [repo_file_extract_service.py](/Users/xxuchan/Desktop/kkbang/worker/repo/repo_file_extract_service.py)
- [repo_stage_service.py](/Users/xxuchan/Desktop/kkbang/worker/repo/repo_stage_service.py)

역할:

- `crawl_status=downloaded` 인 repo 중 아직 추출되지 않은 대상을 고릅니다.
- snapshot 내부를 순회하면서 텍스트/코드 파일을 분류하고 메타데이터를 집계합니다.
- `file_extract_total_code_bytes`, `file_extract_code_files_count` 같은 chunk planning용 통계를 함께 저장합니다.
- 성공 시 `file_extract_status=extracted` 로 갱신합니다.

### 6. Code Chunking

파일:

- [repo_chunk_service.py](/Users/xxuchan/Desktop/kkbang/worker/repo/repo_chunk_service.py)
- [repo_chunk_planning_service.py](/Users/xxuchan/Desktop/kkbang/worker/repo/repo_chunk_planning_service.py)

역할:

- `file_extract_status=extracted` 인 repo를 읽어 code chunk를 생성합니다.
- extract 결과의 `file_extract_total_code_bytes`를 기준으로 shard를 다시 배분합니다.
- tree-sitter 기반 `function/class` symbol chunk를 만들고, 각 chunk에 `raw_code`, `normalized_code`, `anonymized_code`를 저장합니다.
- `identifier_tokens`, `call_tokens`, `operator_tokens`, `control_flow_tags`, `structure_signature`, `ast_node_sequence` 같은 검색용 summary feature를 함께 생성합니다.
- `raw_hash`, `normalized_hash`, `anonymized_hash`와 stable `chunk_id`를 생성합니다.
- 설정이 켜져 있으면 `raw_embedding`, `anonymized_embedding`도 저장 직전에 채웁니다.
- whale repo는 repo 내부 file-level parallel chunking으로 처리합니다.
- 결과는 `code_chunk_index` alias에 저장되고, repo 상태는 `chunk_status`로 관리합니다.
- 성공 시 로컬 snapshot cleanup을 시도합니다.

### 7. Validation

파일:

- [repo_validation_service.py](/Users/xxuchan/Desktop/kkbang/worker/repo/repo_validation_service.py)

역할:

- chunk 결과가 실제로 유효한지 확인하고 validation 상태를 갱신합니다.
- embedding이 켜져 있으면 `raw_embedding`, `anonymized_embedding` 누락 개수도 warning 성격으로 집계합니다.
- `REPO_PIPELINE_SELF_LOOP_ENABLED=true`일 때만, pending crawl work가 남아 있으면 `code_pipeline_dag`를 다시 trigger 해 다음 배치를 이어갑니다.

## 현재 데이터 모델

### 1. Source Metadata

기본 저장 위치:

- `seed_item_index.raw_metadata`
- `package_registry_full_metadata_index.full_raw_metadata` (옵션)

특징:

- source raw metadata는 `seed_item_index` 문서 내부 필드로 직접 저장합니다.
- 너무 큰 registry 응답을 그대로 저장하지 않도록 compact format이 기본입니다.
- full raw 전체 응답이 필요하면 `KEEP_FULL_PACKAGE_REGISTRY_RAW=true` 로 `package_registry_full_metadata_index`에 저장합니다.

### 2. `seed_item_index`

역할:

- source observation 단위의 중간 문서 저장
- 상태 관리와 provenance 기록

대표 필드:

- `source_type`
- `source_name`
- `source_item_id`
- `raw_metadata`
- `candidate_repo_urls`
- `selected_repository_url`
- `canonical_repo_url`
- `owner`
- `repo`
- `status`
- `reason`

대표 상태:

```text
ingested -> normalized -> registered
                   \-> invalid
                   \-> rejected
                   \-> qualification_failed
```

### 3. `repo_registry_index`

역할:

- 최종 repo registry
- 이후 snapshot 수집, 파일 추출, chunking 단계의 입력

대표 필드:

- `canonical_repo_url`
- `hosting_platform`
- `owner`
- `repo_name`
- `crawl_status`
- `discovery_source_count`
- `source_types`
- `discovery_sources`

중요한 점:

- 이 문서는 단순 repo 목록이 아닙니다.
- "어떤 source가 이 repo를 발견했는가"를 최소 provenance 형태로 함께 남기는 registry입니다.

### 4. `code_chunk_index`

역할:

- 검색 중심의 derived code chunk index
- repo/file source code에서 뽑은 `function/class` 단위 symbol chunk 저장
- 이후 retrieval, query expansion, embedding search의 공통 기반

대표 필드:

- `chunk_id`
- `repo_id`
- `repo_url`
- `language`
- `file_path`
- `chunk_type`
- `start_line`, `end_line`
- `raw_code`
- `normalized_code`
- `anonymized_code`
- `symbol_type`
- `symbol_name`
- `identifier_tokens`
- `call_tokens`
- `operator_tokens`
- `control_flow_tags`
- `structure_signature`
- `ast_node_sequence`
- `raw_hash`
- `normalized_hash`
- `anonymized_hash`
- `validation_status`
- `raw_embedding` (옵션)
- `anonymized_embedding` (옵션)
- `chunker_version`
- `normalization_version`
- `anonymization_version`
- `feature_version`

중요한 점:

- `code_chunk_index`는 system of record가 아니라 drop-and-rebuild 가능한 derived index입니다.
- full AST JSON, full DFG JSON은 저장하지 않고 retrieval용 summary feature만 저장합니다.
- 현재 physical index는 `code_chunk_index_v1`, logical alias는 `code_chunk_index` 방식으로 운영합니다.
- embedding 설명은 [code-chunk-embedding.md](/Users/xxuchan/Desktop/kkbang/docs/code-chunk-embedding.md)를 참고합니다.

## 스키마 설명

이 프로젝트는 `seed_item_index`와 `repo_registry_index`를 분리해, source observation과 최종 canonical repo 상태를 섞지 않도록 설계했습니다. 이렇게 해야 ingestion 실패, 중복 source, provenance 누적은 `seed_item_index`에서 다루고, 실제 downstream 처리 기준이 되는 repo 상태는 `repo_registry_index`에서 안정적으로 관리할 수 있습니다.

또한 `repo_registry_index`에는 `discovery_sources`, `source_types`, `discovery_source_count`를 함께 저장합니다. 나중에 라이선스 유사 코드 탐지에서 "왜 이 repo를 수집했는가"를 설명할 수 있어야 하므로, 최종 registry에도 provenance를 남기는 방향을 선택했습니다.

## 현재 기본 저장 모드

현재 기본 저장 모드는 `OpenSearchStore` 입니다.

파일:

- [opensearch_store.py](/Users/xxuchan/Desktop/kkbang/worker/storage/opensearch_store.py)

이 모드의 장점:

- `seed_item_index`, `repo_registry_index` 등 인덱스 단위 조회/갱신이 바로 가능
- Airflow/worker에서 동일한 저장소 인터페이스로 운영 환경과 개발 환경을 맞출 수 있음
- 이후 검색/유사도/통계 파이프라인을 붙이기 쉬움
- shard recovering 시점의 짧은 `503`은 저장소 레이어에서 짧게 재시도해 task-level failure를 줄입니다.
- 대량 iteration은 OpenSearch-safe한 `_doc` 정렬과 scroll 기반 조회로 처리합니다.

## 설정 파일

주요 설정 파일은 아래와 같습니다.

- [seed_packages.json](/Users/xxuchan/Desktop/kkbang/airflow/config/seed_packages.json)
  - `pypi`, `npm` seed 패키지 목록
- [curated_repo_lists.json](/Users/xxuchan/Desktop/kkbang/airflow/config/curated_repo_lists.json)
  - curated repo URL 목록
- [benchmark_datasets.json](/Users/xxuchan/Desktop/kkbang/airflow/config/benchmark_datasets.json)
  - benchmark dataset artifact 입력 설정
- [airflow.env](/Users/xxuchan/Desktop/kkbang/airflow/config/airflow.env)
  - Airflow executor, timezone, metadata DB 설정
  - 기본값: `LocalExecutor` + `PostgreSQL` (`postgres` 서비스)
- [.env](/Users/xxuchan/Desktop/kkbang/.env)
  - 앱 환경변수, OpenSearch 연결, timeout 등

benchmark dataset 파일은 기본적으로 아래 경로에 마운트해서 읽습니다.

- `/opt/airflow/benchmark_data`
- 호스트 기준으로는 [benchmark_data](/Users/xxuchan/Desktop/kkbang/benchmark_data) 디렉토리입니다.

운영 중 로컬 snapshot cleanup 상태를 OpenSearch 문서와 맞추고 싶다면 아래 스크립트를 사용할 수 있습니다.

- [reconcile_snapshot_cleanup_state.py](/Users/xxuchan/Desktop/kkbang/scripts/reconcile_snapshot_cleanup_state.py)
  - `snapshot_artifacts_missing` 상태 문서를 찾아 실제 파일이 없는 경우 `missing_confirmed`로 정리

## 실행 방법

필요한 도구:

- Docker
- Docker Compose
- Python 3 (`scripts/` 유틸 실행 시)

### 1. 컨테이너 실행

```bash
cd /Users/xxuchan/Desktop/kkbang
docker compose up --build -d airflow
```

초기 실행 시 `postgres`, `tor-proxy`, `airflow` 순서로 올라오며,
Airflow 메타데이터 DB는 PostgreSQL(`postgresql+psycopg2://airflow:airflow@postgres:5432/airflow`)를 사용합니다.

Airflow UI:

```text
http://localhost:8080
```

기본 계정:

```text
admin / admin
```

버전 확인 예시:

```bash
python3 --version
docker --version
docker compose version
```

### 2. 코드나 설정을 바꾼 뒤 반영

현재 구조는 DAG와 worker 코드를 Docker 이미지에 복사해서 사용합니다.

즉, 코드나 설정 파일을 수정한 뒤에는 아래처럼 다시 빌드해야 합니다.

```bash
cd /Users/xxuchan/Desktop/kkbang
docker compose up --build -d --force-recreate airflow
```

### 3. 전체 DAG 실행

seed discovery만 실행:

```bash
docker compose exec airflow airflow dags unpause seed_discovery_dag
docker compose exec airflow airflow dags trigger seed_discovery_dag
```

seed discovery 이후 repo processing까지 이어서 실행:

```bash
docker compose exec airflow airflow dags trigger code_pipeline_dag
```

`code_pipeline_dag`는 한 번만 trigger 하면 되고, 이후에는 `repo_validation_dag`가 pending crawl 대상이 남아 있는 동안 다음 배치를 자동으로 이어서 실행합니다.
단, 이 동작은 `REPO_PIPELINE_SELF_LOOP_ENABLED=true`일 때만 활성화됩니다.

### 4. 특정 태스크만 테스트 실행

예를 들어 curated repo ingestion 태스크만 보고 싶다면:

```bash
docker compose exec airflow airflow tasks test seed_discovery_dag seed_ingestion_curated_llm_license_focus_v1 2026-03-24
```

예를 들어 benchmark artifact fetch 태스크만 보고 싶다면:

```bash
docker compose exec airflow airflow tasks test seed_discovery_dag benchmark_artifact_fetch_swe_bench_verified_v1 2026-03-24
```

## 결과 확인 위치

OpenSearch 주요 인덱스:

- `seed_item_index`
- `repo_registry_index`
- `code_chunk_index`
- `repo_relation_index`
- `seed_qualification_failure_index`
- `package_registry_full_metadata_index` (옵션)
- `benchmark_artifact_fetch_index`
- repo processing 관련 상태는 주로 `repo_registry_index`에 누적 저장됩니다.

## 디렉토리 구조

```text
.
├── airflow/
│   ├── config/
│   │   ├── airflow.env
│   │   ├── benchmark_datasets.json
│   │   ├── curated_repo_lists.json
│   │   └── seed_packages.json
│   ├── dags/
│   │   ├── code_pipeline_dag.py
│   │   ├── repo_chunk_dag.py
│   │   ├── repo_extract_dag.py
│   │   ├── repo_relation_dag.py
│   │   ├── repo_validation_dag.py
│   │   ├── seed_dag_support.py
│   │   ├── seed_discovery_dag.py
│   │   ├── seed_expansion_dag.py
│   │   └── seed_pipeline_dag.py
│   ├── plugins/
│   ├── Dockerfile
│   └── requirements.txt
├── infra/
│   └── opensearch_index/
├── local_data/
├── scripts/
├── worker/
│   ├── common/
│   ├── repo/
│   ├── seed/
│   │   ├── adapters/
│   │   │   ├── benchmark/
│   │   │   ├── curated/
│   │   │   ├── github/
│   │   │   └── package_registry/
│   │   ├── extractors/
│   │   ├── qualifiers/
│   │   ├── resolvers/
│   │   └── services/
│   └── storage/
├── docker-compose.yml
└── README.md
```

## 현재 한계

- 현재 저장소는 OpenSearch 중심이며, repo snapshot은 local scratch 기반으로 처리 후 정리합니다.
- GitHub qualification은 GitHub REST API 무인증 호출에 의존합니다.
- repo snapshot download, 파일 수집, tree-sitter symbol chunking, normalization/anonymization, summary feature 저장, optional embedding enrichment까지는 구현되어 있습니다.
- 다만 embedding 경로는 현재 config-gated 기능이며, 실제 운영 환경에서의 end-to-end smoke와 retrieval 품질 검증은 아직 남아 있습니다.
- seed source coverage는 아직 초기 단계입니다.
- 현재 qualification 로직은 단순하며, 장기적으로 더 정교한 정책이 필요합니다.
- repo processing DAG는 self-loop를 켤 수 있는 구조라서 병렬도 설정이 높으면 load spike가 생길 수 있습니다.
- whale repo는 task 내부 file-level parallelism으로 완화하고 있지만, giant repo를 여러 Airflow task로 다시 쪼개는 전용 lane은 아직 없습니다.
- local snapshot cleanup과 OpenSearch 문서 상태가 항상 완벽히 동기화되지는 않아, 운영 보조 스크립트가 필요합니다.

## 탐지 Query Expansion 방향

현재는 아직 구현 전이지만, 이후 탐지 단계에서는 하나의 코드 조각을 여러 방향으로 확장해 검색할 계획입니다. 아래 표는 요약본이고, 설계 배경과 단계별 제안은 [docs/query-expansion.md](docs/query-expansion.md)를 참고하면 됩니다.

| 방향 | 의미 | 구현 계획 |
| --- | --- | --- |
| Lexical Variation | 변수명, 함수명, import alias, 문자열/숫자 literal이 조금 바뀐 코드 대응 | 예정 |
| Structural Variation | 포맷팅, block 구조, 조건문/반복문 형태가 일부 달라진 코드 대응 | 예정 |
| Tree-sitter 기반 Query | AST 단위로 함수/호출/제어흐름 패턴을 정규화한 탐지 query 생성 | 예정 |
| LLM-generated Variation | 원본 코드를 조금씩 바꾼 변형 예시를 생성해 semantic retrieval recall 보강 | 예정 |
| Adversarial Rewrite | 의도적으로 탐지를 피하려고 identifier 치환, helper 함수 분리, 문장 재배치한 변형 대응 | 예정 |
| Multi-query Retrieval | 하나의 원본 chunk에서 여러 query를 만들고 결과를 합치는 방식 | 예정 |

핵심 아이디어는 "원래 코드가 그대로 복사되지 않았더라도 잡을 수 있어야 한다"는 점입니다. 예를 들어 함수명 치환, 주석 제거, 상수 값 변경, helper 함수 추출, formatting 변경 정도는 실제 생성 코드나 수작업 수정 코드에서 흔히 발생하므로, 단일 exact query만으로는 recall이 낮아질 수 있습니다.

이 단계에서 `tree-sitter`는 코드 구조를 안정적으로 뽑아내는 역할을 하고, LLM 기반 variation은 사람이 직접 열거하기 어려운 의미 보존 변형을 넓히는 역할을 하게 됩니다. 즉 장기적으로는 lexical query, structural query, AST query, semantic variation query를 함께 쓰는 다중 탐지 전략을 목표로 합니다.

## 구현하면서 고민한 점

- seed source를 한 번에 많이 붙이기보다, `package registry -> curated repo -> benchmark dataset` 순서로 신뢰도 높은 source부터 확장했습니다. 초기에 coverage보다 provenance를 먼저 안정화하는 쪽이 이후 라이선스 판단에 더 유리하다고 봤습니다.
- package registry raw를 얼마나 저장할지도 고민이 컸습니다. 전체 응답을 저장하면 디버깅은 쉽지만 저장 용량이 너무 커져서, 기본은 compact raw만 남기고 full raw는 옵션으로 분리했습니다.
- `seed_item_index`와 `repo_registry_index`를 나눈 이유도 운영 편의 때문입니다. source record와 canonical repo를 섞으면 dedupe, qualification 재시도, provenance 누적이 모두 복잡해지기 때문입니다.
- benchmark source는 수동 repo 목록이 아니라, 실제 dataset artifact에서 repo를 추출하는 구조로 바꿨습니다. 그래야 논문/benchmark에 등장한 repo라는 근거가 코드와 데이터에 함께 남습니다.
- repo processing 단계에서는 처리량과 서버 안정성의 균형이 중요했습니다. 실제 운영 중 load spike가 있었기 때문에, 현재는 shard 수와 Airflow parallelism을 다소 보수적으로 낮추고 안정적인 fan-out 범위를 먼저 찾는 방향으로 조정했습니다.
- crawl 단계는 repo 크기 편차가 커서, `repo_size_kb` 기반 `normal / whale_hint / giant_hint` 분류와 `normal -> whale/giant hint` 2-phase scheduling, download/extract concurrency 분리를 추가했습니다.
- chunk 단계는 해시 기반 shard fan-out만으로는 tail latency가 커졌기 때문에, extract 이후 `file_extract_total_code_bytes` 기반 재분배와 whale repo file-level parallel chunking을 추가했습니다.
- code chunk는 단순 텍스트 보관보다 검색 중심이 중요하다고 봤기 때문에, `repo_chunk_index` 성격에서 `code_chunk_index` 성격으로 옮겨 왔습니다. 현재는 symbol-level chunk, normalization/anonymization, summary feature, optional embedding enrich를 한 문서에 모아 retrieval-ready 형태로 저장합니다.
- OpenSearch 쪽도 운영 중 race가 있었기 때문에, shard recovering 시점의 짧은 503 재시도와 `_id` 정렬 회피를 저장소 레이어에 넣어 seed/repo 파이프라인이 덜 깨지게 조정했습니다.
- local snapshot cleanup이 항상 문서 상태와 동시에 맞춰지지 않는 문제도 있었습니다. 그래서 cleanup 자체뿐 아니라, 나중에 OpenSearch 문서를 reconcile하는 운영 스크립트까지 같이 두는 방향을 선택했습니다.

## 다음 단계

현재 다음과 같은 확장을 염두에 두고 있습니다.

1. direct repo seed 추가
2. GitHub user seed 추가
3. StackOverflow repo source 추가
4. qualification 전 canonical repo dedupe 강화
5. GitHub API rate limit 대응 강화
6. near-duplicate relation 추가
7. giant repo 전용 별도 처리 lane 또는 내부 재분배 추가
8. embedding 모델/차원 확정 및 운영 smoke
9. similarity retrieval 추가
10. 탐지 query expansion 추가
11. license-aware similarity pipeline 연결

## 요약

이 저장소는 라이선스 파생 코드 탐지용 전체 시스템의 첫 단계를 구현합니다.

현재는 `PyPI`, `npm`, curated repo list, benchmark dataset source 로부터 설득력 있는 공개 GitHub 저장소를 찾아 `repo_registry_index`를 만들고, 이어서 size-aware snapshot download, 파일 추출, extract-aware chunk shard planning, tree-sitter 기반 symbol chunk 생성, normalization/anonymization, summary feature 저장, optional embedding enrich, validation까지 연결하는 데 초점을 맞추고 있습니다.
