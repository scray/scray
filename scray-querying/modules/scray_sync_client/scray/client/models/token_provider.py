import time
import requests

class TokenProvider:
    def __init__(self, client_id, client_secret, token_url, access_token):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url
        self.access_token = access_token
        self.expires_at = 0

    def __call__(self):
        if not self.access_token or time.time() > self.expires_at:
            self._refresh()
        return self.access_token

    def _refresh(self):
        resp = requests.post(self.token_url, data={
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        })
        data = resp.json()
        self.access_token = data["access_token"]
        self.expires_at = time.time() + data.get("expires_in", 3600) - 30