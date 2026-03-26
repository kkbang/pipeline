import time
import random
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from worker.common.config import settings
from stem import Signal
from stem.control import Controller
class GitHubRepoResolver:
    # GitHub API 기본 URL 수정 (repos가 중복되지 않도록 설정)
    BASE_URL = "https://api.github.com"

    def __init__(self) -> None:
        self.headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        # Docker 서비스 이름 'tor-proxy'를 호스트로 설정
        self.proxies = {
            'http': 'socks5h://tor-proxy:9050',
            'https': 'socks5h://tor-proxy:9050'
        }
        
        # HTTP 재시도 전략 설정
        # 403(Forbidden/Rate Limit), 429(Too Many Requests) 및 서버 에러 발생 시 자동 재시도
        retry_strategy = Retry(
            total=3,
            status_forcelist=[ 500, 502, 503, 504],
            backoff_factor=1,
        )
        # Session 객체 생성 및 어댑터 연결
        self.session = requests.Session()
        self.session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
    def renew_tor_ip(self):
        try:
            # Docker 서비스 이름 'tor-proxy'의 9051 포트 사용
            with Controller.from_port(address="tor-proxy", port=9051) as controller:
                controller.authenticate() 
                controller.signal(Signal.NEWNYM)
                print("[Tor] IP 교체 신호를 보냈습니다. 10초 대기...")
                time.sleep(10) # IP가 완전히 바뀔 때까지 충분히 대기
        except Exception as e:
            print(f"⚠️ [Tor] IP 교체 실패: {e}")

    def fetch_repo_metadata(self, owner: str, repo: str,retry_count=0) -> dict | None:
        # 요청 전 랜덤 딜레이 (1초 ~ 3초 사이)
        time.sleep(random.uniform(1.0, 3.0)) 
        
        try:
            url = f"{self.BASE_URL}/repos/{owner}/{repo}"
            
            response = self.session.get(
                url,
                headers=self.headers,
                proxies=self.proxies,
                timeout=settings.request_timeout_seconds,
            )
            # 403(Rate Limit) 또는 429가 발생한 경우
            if response.status_code in [403, 429]:
                if retry_count < 3: # 최대 3번까지 IP 바꿔서 재시도
                    print(f"🚫 [403/429] Limit 도달! IP 교체 후 다시 시도합니다. (시도 {retry_count+1}/3)")
                    self.renew_tor_ip()
                    return self.fetch_repo_metadata(owner, repo, retry_count + 1)
                else:
                    print("❌ IP 교체를 3번 시도했으나 계속 실패했습니다.")
                    response.raise_for_status()

            # 리포지토리가 없는 경우
            if response.status_code == 404:
                return None
            
            # 재시도 끝에 여전히 403/429라면 예외 발생 (Airflow Task 실패 처리 목적)
            response.raise_for_status()
            
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"Error fetching metadata for {owner}/{repo}: {e}")
            raise
