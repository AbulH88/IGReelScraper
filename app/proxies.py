import random
import os
import time
from datetime import datetime, timedelta

class ProxyManager:
    def __init__(self, proxy_file="proxy.txt"):
        self.proxies = []
        self.bad_proxies = {}  # proxy_url -> expiry_time
        self.proxy_file = proxy_file
        self.load_proxies()

    def load_proxies(self):
        if not os.path.exists(self.proxy_file):
            print(f"DEBUG: Proxy file {self.proxy_file} not found.")
            return

        try:
            with open(self.proxy_file, "r") as f:
                lines = f.readlines()
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    # Format: host:port:user:pass
                    parts = line.split(":")
                    if len(parts) == 4:
                        host, port, user, password = parts
                        proxy_url = f"http://{user}:{password}@{host}:{port}"
                        self.proxies.append(proxy_url)
            print(f"DEBUG: Loaded {len(self.proxies)} proxies from {self.proxy_file}")
        except Exception as e:
            print(f"DEBUG: Error loading proxies: {e}")

    def get_random_proxy(self):
        if not self.proxies:
            return None
            
        # Filter out bad proxies
        now = datetime.now()
        available_proxies = [
            p for p in self.proxies 
            if p not in self.bad_proxies or self.bad_proxies[p] < now
        ]
        
        if not available_proxies:
            # If all proxies are "bad", clear the blacklist and try again
            self.bad_proxies.clear()
            available_proxies = self.proxies

        return random.choice(available_proxies)

    def mark_bad(self, proxy_url, duration_minutes=15):
        """Temporarily blacklist a proxy that failed or was rate limited."""
        if proxy_url:
            self.bad_proxies[proxy_url] = datetime.now() + timedelta(minutes=duration_minutes)
            print(f"DEBUG: Proxy marked as bad: {proxy_url} (until {self.bad_proxies[proxy_url]})")

    def get_requests_proxy(self):
        proxy_url = self.get_random_proxy()
        if not proxy_url:
            return None
        return {
            "http": proxy_url,
            "https": proxy_url
        }

# Global instance
proxy_manager = ProxyManager()
