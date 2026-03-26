# Code Pipeline

라이선스 파생 코드 탐지용 데이터셋을 만들기 위한 파이프라인 프로젝트입니다.

이 저장소의 최종 목표는 공개 코드 저장소를 대규모로 수집한 뒤, 코드와 라이선스 문맥을 함께 보존하고, 이후 function-level chunking, feature/embedding 생성, 유사도 검색, license-aware 판단까지 이어지는 데이터 파이프라인을 구축하는 것입니다.

현재 구현은 그 전체 그림 중에서도 가장 앞단인 `Seed Source -> Repo Discovery -> Repo Registry` 구간에 집중되어 있습니다.

## 한눈에 보기

- 최종 목표: 라이선스 리스크가 있는 유사 코드 탐지를 위한 학습/검증 데이터셋 구축
- 현재 구현 범위: seed source 수집, GitHub 저장소 식별, repo registry 구축
- 현재 seed source:
  - package registry repo: `PyPI`, `npm`
  - curated repo list: 정적 GitHub repo URL 목록
  - benchmark dataset source: benchmark dataset artifact에서 추출한 공개 repo
- 현재 기본 저장소: 로컬 JSON 파일
- 장기 설계 저장소: `OpenSearch`, `S3`
- 실행 환경: `Airflow + Docker Compose`

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

- function / class / fallback 단위 chunking
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

즉, 현재 저장소의 직접적인 산출물은 "라이선스 탐지 데이터셋"이 아니라, 그 다음 단계의 입력이 되는 `repo registry`입니다.

## Seed Source 전략

이 프로젝트는 seed source를 한 번에 다 붙이기보다, 설명력이 높은 source부터 차례대로 확장하는 방식으로 설계되어 있습니다.

| Seed Source                    | 의미                                                         | 상태   |
| ------------------------------ | ------------------------------------------------------------ | ------ |
| Package Registry Repo          | PyPI, npm 같은 레지스트리 메타데이터에서 공식 repo 후보 추출 | 구현됨 |
| Curated Repo List              | 사람이 직접 고른 공개 repo 목록                              | 구현됨 |
| Direct Repo Seed               | 사용자가 repo URL을 직접 지정                                | 예정   |
| Org / User Seed                | GitHub org/user 아래 repo 일괄 수집                          | 예정   |
| Benchmark Dataset Source       | benchmark dataset artifact에서 repo를 추출해 seed로 등록    | 구현됨 |
| GitHub Search                  | query 기반 repo 수집                                         | 예정   |
| Topic 기반 Seed                | GitHub topic/tag 기반 repo 수집                              | 예정   |
| Dependency Expansion           | package dependency/lockfile 기반 확장                        | 예정   |
| Fork Network Seed              | fork graph 기반 확장                                         | 예정   |
| README External Link Expansion | README, docs 외부 링크 기반 repo 확장                        | 예정   |

현재는 `PyPI`, `npm`, curated repo list가 기본 DAG에 연결되어 있고, benchmark dataset source는 dataset config를 활성화했을 때 동적으로 추가됩니다.

현재 운영 원칙은 다음과 같습니다.

- `curated repo list`: 사람이 명시적으로 넣는 고정밀 단일 repo seed
- `benchmark dataset source`: benchmark record에서 repo provenance를 함께 가져오는 고신뢰 seed

즉, 같은 repo를 여러 source에 무작정 중복으로 넣기보다, curated는 "꼭 포함해야 하는 대표 repo", benchmark dataset은 "평가/연구 문맥이 명확한 dataset provenance를 가진 repo"로 분리해서 관리하는 것을 원칙으로 합니다.

## 현재 아키텍처

현재 구현 범위만 놓고 보면 흐름은 아래와 같습니다.

```text
[PyPI / npm / Curated Repo List / Benchmark Dataset Source]
  -> [Seed Ingestion]
  -> [Local JSON Raw Store]
  -> [seed_item_index]
  -> [Seed Normalization]
  -> [GitHub Repo Qualification]
  -> [repo_registry_index]
```

핵심 역할은 다음처럼 나뉩니다.

- Airflow: orchestration과 실행 순서 제어
- worker: seed 수집, URL 추출, normalize, qualification 로직 수행
- local JSON store: 현재 기본 저장소
- OpenSearch / S3: 장기 설계상 저장소 인터페이스, 현재 기본 경로는 아님

## 현재 구현된 DAG

현재 DAG 파일은 [seed_pipeline_dag.py](/Users/xxuchan/Desktop/kkbang/code-pipeline/airflow/dags/seed_pipeline_dag.py) 입니다.

- DAG ID: `seed_pipeline_dag`
- 스케줄: `@daily`
- `catchup=False`
- Airflow 태그: `seed`, `package-registry`

현재 기본 구성 태스크는 아래와 같습니다.

1. `seed_ingestion_pypi`
2. `seed_ingestion_npm`
3. `seed_ingestion_curated_llm_license_focus_v1`
4. `seed_normalization`
5. `seed_qualification`

benchmark dataset source는 [`benchmark_datasets.json`](/Users/xxuchan/Desktop/kkbang/code-pipeline/airflow/config/benchmark_datasets.json) 에서 `enabled=true` 인 항목이 있을 때만 `seed_ingestion_benchmark_*` 태스크가 동적으로 생성됩니다.

또한 `artifact_fetch.enabled=true` 인 경우, `benchmark_artifact_fetch_*` 태스크가 먼저 실행되어 dataset artifact를 `benchmark_data/` 아래에 가져온 뒤 ingestion 단계로 넘깁니다.

기본 실행 순서는 다음과 같습니다.

```text
[seed_ingestion_pypi]
          \
[seed_ingestion_npm] ----> [seed_normalization] -> [seed_qualification]
          /
[seed_ingestion_curated_llm_license_focus_v1]
```

## 단계별 역할

### 1. Seed Ingestion

파일:

- [seed_ingest_service.py](/Users/xxuchan/Desktop/kkbang/code-pipeline/worker/seed/services/seed_ingest_service.py)
- [benchmark_dataset.py](/Users/xxuchan/Desktop/kkbang/code-pipeline/worker/seed/adapters/benchmark_dataset.py)
- [pypi.py](/Users/xxuchan/Desktop/kkbang/code-pipeline/worker/seed/adapters/pypi.py)
- [npm.py](/Users/xxuchan/Desktop/kkbang/code-pipeline/worker/seed/adapters/npm.py)
- [curated_repo.py](/Users/xxuchan/Desktop/kkbang/code-pipeline/worker/seed/adapters/curated_repo.py)

역할:

- package registry, curated list, benchmark dataset source에서 입력을 가져옵니다.
- raw metadata를 로컬 JSON으로 저장합니다.
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

- [seed_normalize_service.py](/Users/xxuchan/Desktop/kkbang/code-pipeline/worker/seed/services/seed_normalize_service.py)
- [repo_url_extractor.py](/Users/xxuchan/Desktop/kkbang/code-pipeline/worker/seed/extractors/repo_url_extractor.py)
- [github_url_canonicalizer.py](/Users/xxuchan/Desktop/kkbang/code-pipeline/worker/seed/resolvers/github_url_canonicalizer.py)

역할:

- `ingested` 상태 문서를 읽습니다.
- `candidate_repo_urls` 또는 raw metadata에서 repo 후보를 가져옵니다.
- GitHub repo URL을 canonical form으로 정규화합니다.
- 성공 시 `normalized`, 실패 시 `invalid`로 상태를 갱신합니다.

이 단계는 "후보 URL 수집"과 "실제 공식 repo 식별"을 분리해 주는 역할을 합니다.

### 3. Seed Qualification

파일:

- [seed_qualify_service.py](/Users/xxuchan/Desktop/kkbang/code-pipeline/worker/seed/services/seed_qualify_service.py)
- [github_repo_resolver.py](/Users/xxuchan/Desktop/kkbang/code-pipeline/worker/seed/resolvers/github_repo_resolver.py)
- [repo_qualifier.py](/Users/xxuchan/Desktop/kkbang/code-pipeline/worker/seed/qualifiers/repo_qualifier.py)

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

## 현재 데이터 모델

### 1. Raw Data

기본 저장 위치:

- `local_data/raw/package_registry/pypi/*.json`
- `local_data/raw/package_registry/npm/*.json`
- `local_data/raw/curated_repo_list/<list_name>/*.json`
- `local_data/raw/benchmark_dataset/<dataset_name>/*.json`

특징:

- package registry raw는 compact format이 기본입니다.
- 너무 큰 registry 응답을 그대로 저장하지 않도록 최소 필드만 남깁니다.
- full raw 전체 응답이 필요하면 `KEEP_FULL_PACKAGE_REGISTRY_RAW=true` 로 켤 수 있습니다.

### 2. `seed_item_index`

역할:

- source observation 단위의 중간 문서 저장
- 상태 관리와 provenance 기록

대표 필드:

- `source_type`
- `source_name`
- `source_item_id`
- `source_context`
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
- `primary_language`
- `repo_license_spdx`
- `stars`
- `forks_count`
- `default_branch`
- `is_public`
- `is_archived`
- `is_fork`
- `priority_score`
- `discovery_source_count`
- `source_types`
- `discovery_sources`

중요한 점:

- 이 문서는 단순 repo 목록이 아닙니다.
- "어떤 source가 왜 이 repo를 발견했는가"까지 함께 남기는 provenance registry입니다.

## 현재 기본 저장 모드

현재 기본 경로는 `LocalJsonStore` 입니다.

파일:

- [local_json_store.py](/Users/xxuchan/Desktop/kkbang/code-pipeline/worker/storage/local_json_store.py)

이 모드의 장점:

- OpenSearch와 S3 없이도 로컬에서 빠르게 테스트 가능
- 산출물을 JSON 파일로 직접 확인 가능
- seed/source/provenance 구조를 디버깅하기 쉬움

장기적으로는 OpenSearch와 S3를 다시 활성화할 수 있도록 저장소 인터페이스 파일을 함께 유지하고 있습니다.

## 설정 파일

주요 설정 파일은 아래와 같습니다.

- [seed_packages.json](/Users/xxuchan/Desktop/kkbang/code-pipeline/airflow/config/seed_packages.json)
  - `pypi`, `npm` seed 패키지 목록
- [curated_repo_lists.json](/Users/xxuchan/Desktop/kkbang/code-pipeline/airflow/config/curated_repo_lists.json)
  - curated repo URL 목록
- [benchmark_datasets.json](/Users/xxuchan/Desktop/kkbang/code-pipeline/airflow/config/benchmark_datasets.json)
  - benchmark dataset artifact 입력 설정
- [airflow.env](/Users/xxuchan/Desktop/kkbang/code-pipeline/airflow/config/airflow.env)
  - Airflow executor, timezone, metadata DB 설정
- [.env](/Users/xxuchan/Desktop/kkbang/code-pipeline/.env)
  - 앱 환경변수, local data 위치, timeout 등

benchmark dataset 파일은 기본적으로 아래 경로에 마운트해서 읽습니다.

- `/opt/airflow/benchmark_data`
- 호스트 기준으로는 [benchmark_data](/Users/xxuchan/Desktop/kkbang/code-pipeline/benchmark_data) 디렉토리입니다.

## 실행 방법

### 1. 컨테이너 실행

```bash
cd /Users/xxuchan/Desktop/kkbang/code-pipeline
docker compose up --build -d airflow
```

Airflow UI:

```text
http://localhost:8080
```

기본 계정:

```text
admin / admin
```

### 2. 코드나 설정을 바꾼 뒤 반영

현재 구조는 DAG와 worker 코드를 Docker 이미지에 복사해서 사용합니다.

즉, 코드나 설정 파일을 수정한 뒤에는 아래처럼 다시 빌드해야 합니다.

```bash
cd /Users/xxuchan/Desktop/kkbang/code-pipeline
docker compose up --build -d --force-recreate airflow
```

### 3. 전체 DAG 실행

```bash
docker compose exec airflow airflow dags unpause seed_pipeline_dag
docker compose exec airflow airflow dags trigger seed_pipeline_dag
```

### 4. 특정 태스크만 테스트 실행

예를 들어 qualification 단계만 보고 싶다면:

```bash
docker compose exec airflow airflow tasks test seed_pipeline_dag seed_qualification 2026-03-24
```

## 결과 확인 위치

기본 결과 위치:

- [local_data](/Users/xxuchan/Desktop/kkbang/code-pipeline/local_data)

대표 경로:

- [local_data/raw/package_registry](/Users/xxuchan/Desktop/kkbang/code-pipeline/local_data/raw/package_registry)
- [local_data/raw/curated_repo_list](/Users/xxuchan/Desktop/kkbang/code-pipeline/local_data/raw/curated_repo_list)
- [local_data/raw/benchmark_dataset](/Users/xxuchan/Desktop/kkbang/code-pipeline/local_data/raw/benchmark_dataset)
- [local_data/seed_item_index](/Users/xxuchan/Desktop/kkbang/code-pipeline/local_data/seed_item_index)
- [local_data/repo_registry_index](/Users/xxuchan/Desktop/kkbang/code-pipeline/local_data/repo_registry_index)

## 디렉토리 구조

```text
code-pipeline/
├── airflow/
│   ├── config/
│   │   ├── airflow.env
│   │   ├── benchmark_datasets.json
│   │   ├── curated_repo_lists.json
│   │   └── seed_packages.json
│   ├── dags/
│   │   └── seed_pipeline_dag.py
│   ├── plugins/
│   ├── Dockerfile
│   └── requirements.txt
├── infra/
│   └── opensearch_index/
├── local_data/
├── worker/
│   ├── common/
│   ├── seed/
│   │   ├── adapters/
│   │   ├── extractors/
│   │   ├── qualifiers/
│   │   ├── resolvers/
│   │   └── services/
│   └── storage/
├── docker-compose.yml
└── README.md
```

## 현재 한계

- 현재 기본 저장소는 OpenSearch/S3가 아니라 local JSON입니다.
- GitHub qualification은 GitHub REST API 무인증 호출에 의존합니다.
- repo snapshot, 파일 수집, chunking, embedding, retrieval 단계는 아직 구현되지 않았습니다.
- seed source coverage는 아직 초기 단계입니다.
- 현재 qualification 로직은 단순하며, 장기적으로 더 정교한 정책이 필요합니다.

## 다음 단계

현재 다음과 같은 확장을 염두에 두고 있습니다.

1. direct repo seed 추가
2. GitHub search / topic 기반 자동 확장 추가
3. README / dependency link expansion 추가
4. snapshot download 계층 구현
5. file extraction / validation 추가
6. function-level chunking 추가
7. feature / embedding 생성 추가
8. license-aware similarity pipeline 연결

## 요약

이 저장소는 라이선스 파생 코드 탐지용 전체 시스템의 첫 단계를 구현합니다.

현재는 `PyPI`, `npm`, curated repo list, benchmark dataset source 로부터 설득력 있는 공개 GitHub 저장소를 찾아 `repo_registry_index`를 만드는 데 초점을 맞추고 있으며, 이 registry는 이후 snapshot 수집, 코드 청크 생성, 유사도 검색, 라이선스 판단 단계의 기반 데이터가 됩니다.
