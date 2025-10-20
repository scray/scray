import json
import logging
from typing import Callable, Optional, Dict, Any

logger = logging.getLogger(__name__)

class HttpClient:
    def __init__(self, token_provider: Callable[[], str]):
        self.token_provider = token_provider

    def _auth_headers(self, extra: Optional[Dict[str, str]] = None):
        headers = {
            "Authorization": f"Bearer {self.token_provider()}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if extra:
            headers.update(extra)
        return headers

    def _handle(self, response):
        if 200 <= response.status_code < 300:
            try:
                return response.json()
            except Exception:
                return None
        logger.error(f"API error {response.status_code}: {response.text}")
        return None

    def get(self, conn, method, url):
        resp = conn.request(method, url, headers=self._auth_headers(), timeout=15)
        if resp.status_code == 401:
            resp = conn.request(method, url, headers=self._auth_headers(), timeout=15)
        return self._handle(resp)

    def put(self, conn, url, data: Any):
        resp = conn.put(url, data=data, headers=self._auth_headers(), timeout=15)
        if resp.status_code == 401:
            resp = conn.put(url, data=data, headers=self._auth_headers(), timeout=15)
        return self._handle(resp)

    def post(self, conn, url, data: Any):
        resp = conn.post(url, data=data, headers=self._auth_headers(), timeout=15)
        if resp.status_code == 401:
            resp = conn.post(url, data=data, headers=self._auth_headers(), timeout=15)
        return self._handle(resp)
