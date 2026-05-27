# Code Pipeline

공개 저장소를 수집하고, snapshot을 파일/코드 청크 단위로 가공한 뒤, 유사 코드 검색과 라이선스 검토에 필요한 데이터를 만드는 파이프라인입니다.

이 프로젝트의 목적은 단순한 코드 수집이 아니라, 나중에 "어떤 공개 코드와 어떤 코드 조각이 유사한가"를 검색하고 검토할 수 있도록, repo 수준 정보와 chunk 수준 정보를 함께 쌓는 것입니다. 즉 최종 산출물은 단순 raw archive가 아니라, retrieval과 review에 바로 쓸 수 있는 structured code corpus입니다.

현재 이 저장소는 `license violation detector`를 직접 구현하는 단계는 아닙니다. 현재 초점은 아래 두 가지입니다.

- 공개 repo를 provenance와 함께 안정적으로 수집
- 수집한 repo를 file/chunk/feature 단위로 가공해서 OpenSearch에 적재

## 한눈에 보기

- 배치 파이프라인
  - 공개 저장소를 수집하고, snapshot -> file -> chunk -> feature / embedding 순으로 가공합니다.
- 온라인 retrieval API
  - GitHub repo URL을 입력받아, 그 repo의 코드 청크와 유사한 외부 후보 코드를 검색합니다.
- 결과의 의미
  - 법적 위반 판정이 아니라 `license review candidate report`입니다.
  - 즉 "검토가 필요한 유사 코드 후보"를 우선순위와 함께 보여주는 시스템입니다.

장기적으로 의도하는 흐름은 아래와 같습니다.

```text
Seed Source
-> Repo Registry
-> Snapshot Download
-> File Extraction
-> Code Chunking
-> Feature / Embedding
-> Similar Code Retrieval
-> License Review
```

현재 이 저장소에서 실제로 돌아가는 범위는 아래입니다.

- seed 수집 및 GitHub repo 등록
- repo snapshot 다운로드
- 파일 추출
- tree-sitter 기반 code chunk 생성
- normalization / anonymization / feature 생성
- validation
- 별도 embedding index backfill

## 현재 포함 범위

- seed source 수집과 GitHub repo 등록
- repo snapshot 다운로드와 로컬 임시 보관
- 텍스트 / 코드 파일 추출
- tree-sitter 기반 청크 생성
- 코드 normalization / anonymization / feature 생성
- OpenSearch 인덱싱
- 별도 embedding index backfill
- repo URL 기반 hybrid retrieval API

현재 포함되지 않는 것은 아래입니다.

- 법적 의미의 infringement / derivation 자동 판정
- 사용자 facing 최종 리뷰 UI 자체
- 비동기 job queue 기반 대규모 온라인 분석 orchestration

현재 결과물은 크게 세 층으로 나뉩니다.

- repo registry
  - 어떤 repo를 왜 수집했는지 기록
- extracted files
  - repo snapshot에서 실제 처리 가능한 텍스트/코드 파일 메타데이터 기록
- code chunks
  - function/class 중심 청크, normalized/anonymized 코드, summary feature, optional embedding 기록

이 저장소 바깥에서 직접 판단해야 하는 것은 아래입니다.

- retrieval 결과를 어떻게 사용자에게 보여줄지
- 유사 코드 후보를 어떤 기준으로 검토 우선순위화할지
- 법적 의미의 위반 판정을 어떻게 분리할지

## 요청 흐름

현재 `POST /retrieve/hybrid/by-repo-url` 요청은 대략 아래 순서로 처리됩니다.

```text
GitHub repo URL 입력
-> URL canonicalization / validation
-> 임시 snapshot 다운로드
-> 로컬 query repo chunking
-> 10줄 이상 source chunk 전체 선택
-> query chunk embedding precompute
-> source chunk별 hybrid retrieval
   -> rule-based search
   -> kNN search
   -> merge / license review candidate scoring
-> 사용자용 report payload 조립
```

현재 timing 출력도 이 단계에 맞춰 노출됩니다.

## 현재 스택

- Airflow
- Postgres
- OpenSearch
- OpenSearch Dashboards
- Tor proxy
- FastAPI retrieval API

## 시작 전 확인

1. Docker / Docker Compose가 설치되어 있어야 합니다.
2. Linux에서는 OpenSearch 실행 전에 아래 값을 설정해야 합니다.

```bash
sudo sysctl -w vm.max_map_count=262144
```

3. `.env`를 현재 환경에 맞게 채워야 합니다.
   - 최소한 `OPENSEARCH_*`, `GITHUB_TOKEN` 또는 `GITHUB_TOKENS`
   - embedding을 쓸 경우 `CODE_CHUNK_EMBEDDING_*`

## 실행

전체 스택을 올립니다.

```bash
docker compose up -d
```

상태 확인:

```bash
docker compose ps
```

기본 접속 포트:

- Airflow: `http://127.0.0.1:8080`
- OpenSearch: `http://127.0.0.1:9200`
- OpenSearch Dashboards: `http://127.0.0.1:5601`
- Retrieval API: `http://127.0.0.1:18000`

Airflow 기본 계정:

- ID: `admin`
- PW: `admin`

## 코드 반영 방식

이 저장소는 `worker/`, `airflow/dags/`, `scripts/` 등을 컨테이너에 bind mount 합니다.

- 일반적인 Python 코드, DAG, 스크립트 수정은 보통 이미지 rebuild가 필요 없습니다.
- `Dockerfile`, Python 의존성, compose 레벨 설정을 바꿨을 때만 rebuild를 고려하면 됩니다.

Airflow 컨테이너 재생성이 필요할 때:

```bash
docker compose up -d --build --force-recreate airflow
```

OpenSearch / Dashboards 설정을 바꿨을 때:

```bash
docker compose up -d --force-recreate opensearch opensearch-dashboards
```

Retrieval API만 다시 올릴 때:

```bash
docker compose up -d --build --no-deps retrieval-api
```

## 핵심 DAG

- `seed_discovery_dag`
  - seed source에서 repo 후보를 수집하고 `repo_registry_index`까지 등록
- `seed_expansion_dag`
  - README / dependency manifest 기반 확장 seed 처리
- `code_pipeline_dag`
  - registered repo의 snapshot download 시작
- `repo_extract_dag`
  - snapshot에서 텍스트/코드 파일 추출
- `repo_chunk_dag`
  - 추출된 코드에서 chunk 생성
- `repo_validation_dag`
  - repo 처리 결과 검증 및 다음 batch continuation 판단
- `repo_retry_dag`
  - 실패 / stale 상태 repo 재처리
- `code_chunk_embedding_backfill_dag`
  - 기존 chunk 문서에 대해 embedding을 별도 index로 backfill

예시:

```bash
docker compose exec airflow airflow dags trigger seed_discovery_dag
docker compose exec airflow airflow dags trigger code_pipeline_dag
docker compose exec airflow airflow dags trigger code_chunk_embedding_backfill_dag
```

repo 하나만 embedding backfill:

```bash
docker compose exec airflow airflow dags trigger code_chunk_embedding_backfill_dag --conf '{"repo_id":"github:owner/repo","limit":500}'
```

## 주요 OpenSearch 인덱스

- `seed_item_index`
- `repo_registry_index`
- `repo_file_index`
- `code_chunk_index`
  - alias 이름입니다. 실제 저장은 versioned physical index에 들어갑니다.
- `code_chunk_embedding_index_v1`
  - embedding backfill의 별도 저장 대상입니다.

주의:

- main chunk pipeline은 `code_chunk_index`에 chunk 문서를 저장합니다.
- embedding backfill DAG는 별도 index인 `code_chunk_embedding_index_v1`에 저장합니다.
- 현재 retrieval 경로는 query chunk 임베딩을 사용해 kNN을 시도하지만, corpus 쪽 separate embedding index를 직접 읽는 구조는 아닙니다. 필요하면 retrieval 쪽을 별도로 연결해야 합니다.

## 현재 retrieval API

FastAPI 모듈은 아래에 있습니다.

- `worker/retrieval/api.py`

주요 endpoint:

- `POST /retrieve/hybrid/by-repo-url`

이 API의 의미는 `violation detector`가 아니라 `license review candidate retrieval`입니다. 즉 결과는 법적 위반 판정이 아니라, 검토가 필요한 유사 코드 후보 목록입니다.

응답의 핵심 구조는 아래와 같습니다.

- `report_kind`
- `interpretation`
- `repo_id`, `repo_url`
- `analyzed_source_count`
- `summary`
- `findings`
- `timings`

여기서:

- `findings[].source`
  - 요청한 repo의 source chunk
- `findings[].candidates`
  - 외부에서 검색된 유사 후보 chunk
- `timings`
  - download / chunk / retrieval / payload 단계 시간

즉 source 코드가 응답에 포함되는 것은 정상이며, 그것은 "비교 기준"입니다.

jump / proxy 서버에서 프론트와 API를 함께 붙일 때는 nginx reverse proxy를 두고 브라우저에서는 same-origin `/api/...`만 호출하는 구성이 가장 단순합니다. 예시 설정은 [jump_frontend_proxy.conf](/Users/xxuchan/Desktop/kkbang/infra/nginx/jump_frontend_proxy.conf)에 있습니다. 이 설정은 jump 서버의 `127.0.0.1:5173` 프론트를 프록시하고, `/api/`는 `ngseo-ubuntu:18000`으로 넘기면서 nginx 앞단 rate limit도 같이 적용합니다.

## 현재 운영 가드레일

- API middleware 동시 처리 제한
- API middleware rate limit
- request body size 제한
- 기본 보안 헤더 부여
- GitHub canonical URL 검증
- jump nginx 앞단 rate limit 예시 제공

다만 아래는 아직 더 보강할 여지가 있습니다.

- 인증 / API key
- restrictive CORS 운영값
- async job queue 기반 대용량 요청 처리
- retrieval 결과 캐시
- separate embedding index를 직접 읽는 retrieval 경로

## 참고 문서

- `docs/repo-manifest-pipeline-architecture.md`
- `docs/repo-processing-current-state.md`
- `docs/large-scale-collection-current-state.md`
- `docs/code-chunk-implementation.md`
- `docs/code-chunk-embedding.md`
- `docs/query-expansion.md`
- `docs/query-expansion-design.md`
