# Code Pipeline

공개 GitHub 저장소를 수집하고 정제해, 라이선스를 고려한 코드 파생 탐지 학습 데이터를 만들기 위한 파이프라인의 초기 구현입니다.

현재 저장소는 전체 비전 중에서도 `Seed Source -> Repo Discovery -> Repo Registry` 구간을 먼저 구현한 상태이며, 첫 번째 seed source 로 `PyPI`를 사용합니다.

## 프로젝트 목표

이 프로젝트의 장기 목표는 단순 크롤링이 아니라, 다음과 같은 학습 데이터를 안정적으로 만드는 것입니다.

- 단일 코드 레코드
  - 원본 코드
  - 정규화 / 익명화 코드
  - 라이선스, 저장소, 파일, provenance 메타데이터
- 코드 쌍 레코드
  - positive pair
  - hard negative pair
  - 변형 난이도 메타데이터

즉, 이 저장소는 장기적으로 contrastive learning 기반의 코드 파생 탐지 데이터셋 파이프라인을 목표로 합니다.

## 현재 구현 범위

현재 구현된 범위는 seed pipeline 입니다.

1. PyPI 패키지 메타데이터 수집
2. 패키지 메타데이터에서 저장소 URL 후보 추출
3. GitHub 저장소 URL 정규화
4. GitHub 저장소 메타데이터 조회
5. 간단한 qualification 규칙 적용
6. OpenSearch repo registry 등록

아직 구현되지 않은 영역은 다음과 같습니다.

- 저장소 snapshot 수집
- 파일 필터링 / 검증
- 함수 / 클래스 단위 chunking
- 정규화 / 익명화
- feature extraction
- dedup / leakage validation
- embedding 생성
- pair generation

## 핵심 설계 원칙

### Airflow는 control plane

Airflow는 파이프라인 순서와 실행 시점을 관리합니다.

- seed 수집 작업 스케줄링
- 단계별 태스크 오케스트레이션
- 상태 기반 후속 처리 연결

현재는 하나의 DAG에서 seed pipeline 을 직렬로 실행합니다.

### Worker는 compute plane

실제 비즈니스 로직은 `worker/` 아래에서 처리합니다.

- 패키지 메타데이터 수집
- URL 후보 추출
- GitHub URL 정규화
- 저장소 qualification
- 저장소 등록

### 스토리지는 역할별로 분리

- `S3`: raw metadata 와 대용량 산출물 저장
- `OpenSearch`: 상태, 메타데이터, 검색용 문서 저장

이 저장소는 현재 파이프라인의 주 저장소로 PostgreSQL 을 사용하지 않습니다. Airflow 메타데이터 DB 는 별도이며, 현재는 SQLite 기반입니다.

## 현재 아키텍처

```text
[PyPI Package Metadata]
    -> [Seed Ingestion]
    -> [Raw Metadata in S3]
    -> [seed_item_index in OpenSearch]
    -> [Seed Normalization]
    -> [GitHub Repo Qualification]
    -> [repo_registry_index in OpenSearch]
```

장기적으로는 아래 흐름으로 확장할 계획입니다.

```text
[Seed Sources]
    -> [Repo Discovery]
    -> [Repo Registry]
    -> [Snapshot Collector]
    -> [File Filtering / Validation]
    -> [Chunking]
    -> [Normalization / Anonymization]
    -> [Feature Extraction]
    -> [Dedup / Leakage Validation]
    -> [Embedding / Retrieval Index]
    -> [Pair Dataset Generation]
```

## 현재 DAG 동작

현재 DAG 는 [seed_pipeline_dag.py](./airflow/dags/seed_pipeline_dag.py) 하나입니다.

- DAG ID: `seed_pipeline_dag`
- 스케줄: `@daily`
- `catchup=False`
- 기본 실행 순서:
  1. `seed_ingestion`
  2. `seed_normalization`
  3. `seed_qualification`

기본 seed package 목록은 코드에 고정되어 있습니다.

- `requests`
- `flask`
- `django`
- `fastapi`
- `numpy`
- `pandas`

### 단계별 역할

#### 1. `seed_ingestion`

- PyPI 에서 패키지 메타데이터 조회
- raw JSON 을 S3 에 저장
- `seed_item_index` 에 `ingested` 상태로 upsert

예시 S3 key:

```text
raw/package_registry/pypi/{package_name}.json
```

#### 2. `seed_normalization`

- `seed_item_index` 에서 `ingested` 문서 조회
- S3 raw metadata 재조회
- 저장소 URL 후보 추출
- GitHub URL 정규화
- 성공 시 `normalized`
- 실패 시 `invalid`

#### 3. `seed_qualification`

- `seed_item_index` 에서 `normalized` 문서 조회
- GitHub API 로 저장소 메타데이터 조회
- qualification 규칙 적용
- 통과 시 `repo_registry_index` 에 등록 후 `registered`
- 탈락 시 `rejected`

### 상태 전이

```text
ingested -> normalized -> registered
                   \-> invalid
                   \-> rejected
```

## 저장 모델

### S3

현재 구현에서는 PyPI raw metadata 저장 용도로 사용합니다.

- `raw/package_registry/pypi/{package_name}.json`

향후에는 다음 산출물도 S3 에 저장할 수 있습니다.

- repository archive
- raw source files
- chunk 결과물
- normalized / anonymized code
- feature JSON
- 학습 데이터셋 export

### OpenSearch

현재 코드에서 실제 사용하는 인덱스는 다음 두 개입니다.

- `seed_item_index`
- `repo_registry_index`

추가로 `seed_quarantine_index` 파일이 존재하지만, 현재 코드에는 아직 연결되어 있지 않습니다.

### 문서 의도

`seed_item_index`

- 패키지 이름과 버전
- raw metadata 위치
- URL 후보 목록
- 선택된 저장소 URL
- canonical repo URL
- owner / repo
- 현재 처리 상태
- invalid / rejected 사유

`repo_registry_index`

- canonical repo URL
- owner / repo name
- primary language
- license 정보
- stars / forks
- default branch
- public / archived / fork 여부
- priority score
- discovery source
- crawl status

## 디렉토리 구조

```text
code-pipeline/
├── airflow/
│   ├── dags/
│   │   └── seed_pipeline_dag.py
│   ├── config/
│   │   └── airflow.env
│   ├── logs/
│   ├── plugins/
│   ├── Dockerfile
│   └── requirements.txt
├── worker/
│   ├── common/
│   │   └── config.py
│   ├── seed/
│   │   ├── adapters/
│   │   │   └── pypi.py
│   │   ├── extractors/
│   │   │   └── repo_url_extractor.py
│   │   ├── qualifiers/
│   │   │   └── repo_qualifier.py
│   │   ├── resolvers/
│   │   │   ├── github_repo_resolver.py
│   │   │   └── github_url_canonicalizer.py
│   │   └── services/
│   │       ├── seed_ingest_service.py
│   │       ├── seed_normalize_service.py
│   │       └── seed_qualify_service.py
│   └── storage/
│       ├── opensearch_store.py
│       ├── quarantine_store.py
│       └── s3_store.py
├── infra/
│   └── opensearch_index/
├── docker-compose.yml
├── .env
└── README.md
```

## 실행 환경 가정

현재 저장소는 아래 환경을 전제로 합니다.

- Airflow 는 Docker Compose 로 실행
- OpenSearch 는 외부 환경에 이미 준비되어 있음
- S3 접근 가능한 AWS 자격 증명이 존재함
- GitHub API 호출 가능
- Airflow 메타데이터 DB 는 SQLite 사용
- Airflow executor 는 `SequentialExecutor`

즉, 지금은 대규모 분산 환경보다는 초기 개발과 단일 서버 실험에 맞춘 구성입니다.

## 환경 변수

프로젝트 루트의 `.env` 에 아래 값을 설정합니다.

```env
APP_ENV=local
LOG_LEVEL=INFO

AWS_REGION=ap-northeast-2
S3_BUCKET=your-bucket-name

OPENSEARCH_HOST=your-opensearch-host
OPENSEARCH_PORT=9200
OPENSEARCH_USER=admin
OPENSEARCH_PASSWORD=your-password
OPENSEARCH_USE_SSL=false

GITHUB_TOKEN=your-github-token
REQUEST_TIMEOUT_SECONDS=20
```

Airflow 전용 설정은 `airflow/config/airflow.env` 에 있습니다.

현재 주요 값은 다음과 같습니다.

- `AIRFLOW__CORE__EXECUTOR=SequentialExecutor`
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db`
- `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True`

## 로컬 실행 방법

### 1. Airflow 시작

```bash
docker compose up --build
```

### 2. Airflow UI 접속

```text
http://localhost:8080
```

기본 로컬 계정은 현재 아래 값으로 생성됩니다.

- username: `admin`
- password: `admin`

### 3. DAG 실행

- Airflow UI 에서 `seed_pipeline_dag` 활성화
- 필요 시 수동 trigger 실행

## 현재 한계

현재 코드는 bootstrap 단계이기 때문에 다음 한계가 있습니다.

- PyPI seed 만 구현되어 있음
- seed package 목록이 DAG 코드에 고정되어 있음
- qualification 규칙이 아직 단순함
- 예외 처리와 재시도 전략이 제한적임
- quarantine 저장 경로는 아직 실제로 연결되지 않음
- OpenSearch index mapping 파일은 아직 placeholder 상태임
- 테스트 코드가 없음
- 로깅 유틸과 일부 모델 파일이 비어 있음

문서를 읽을 때는 "장기 비전"과 "현재 구현"을 구분해서 보는 것이 중요합니다. 현재 실제 동작 범위는 seed pipeline 까지입니다.

## 가까운 다음 작업

우선순위가 높은 다음 단계는 아래와 같습니다.

1. OpenSearch index mapping 구체화
2. 예외 처리 / 로깅 / 재시도 정책 보강
3. invalid seed 에 대한 quarantine 흐름 연결
4. npm, crates.io, Maven 등 seed source 확장
5. repository snapshot 수집 단계 추가
6. 테스트 코드 추가

## 편집 가이드

이 저장소를 수정할 때는 다음 원칙을 유지하는 것이 좋습니다.

1. Airflow 오케스트레이션과 worker 비즈니스 로직을 분리한다.
2. raw artifact 는 OpenSearch 가 아니라 S3 에 저장한다.
3. 상태 전이 기반 흐름을 유지한다.
4. 새로운 seed source 는 adapter / service 로 확장한다.
5. 장기적으로 snapshot, chunking, feature extraction 단계와 자연스럽게 이어지도록 설계한다.

## 한 줄 요약

이 저장소는 PyPI 를 시작점으로 공개 GitHub 저장소를 발견하고 선별해 OpenSearch repo registry 에 등록하는 초기 seed pipeline 이며, 이후 snapshot 수집과 코드 데이터셋 생성 단계로 확장될 예정입니다.
