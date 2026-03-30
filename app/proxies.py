import random
import os
import time
from datetime import datetime, timedelta
import threading

class ProxyManager:
    def __init__(self, proxy_file="proxy.txt"):
        self.proxies = []
        self.bad_proxies = {}  # proxy_url -> (expiry_time, fail_count)
        self.proxy_file = proxy_file
        self._lock = threading.Lock()
        self.load_proxies()

    def load_proxies(self):
        if not os.path.exists(self.proxy_file):
            print(f"DEBUG: Proxy file {self.proxy_file} not found.")
            return

        new_proxies = []
        try:
            with open(self.proxy_file, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    
                    # Support multiple formats
                    # 1. host:port:user:pass
                    # 2. user:pass@host:port
                    # 3. host:port
                    parts = line.split(":")
                    proxy_url = None
                    
                    if len(parts) == 4:
                        host, port, user, password = parts
                        proxy_url = f"http://{user}:{password}@{host}:{port}"
                    elif "@" in line:
                        proxy_url = f"http://{line}" if not line.startswith("http") else line
                    elif len(parts) == 2:
                        host, port = parts
                        proxy_url = f"http://{host}:{port}"
                    
                    if proxy_url:
                        new_proxies.append(proxy_url)
            
            with self._lock:
                self.proxies = new_proxies
            print(f"DEBUG: Loaded {len(self.proxies)} proxies from {self.proxy_file}")
        except Exception as e:
            print(f"DEBUG: Error loading proxies: {e}")

    def get_random_proxy(self):
        with self._lock:
            if not self.proxies:
                return None
                
            now = datetime.now()
            # Efficiently filter out bad proxies
            # Since we have 10k, even if 5k are "bad", we have plenty left.
            # We'll just pick a few random ones and check if they are bad to avoid full list iteration
            
            for _ in range(10): # Try 10 random picks
                p = random.choice(self.proxies)
                if p not in self.bad_proxies:
                    return p
                
                expiry, _ = self.bad_proxies[p]
                if expiry < now:
                    del self.bad_proxies[p]
                    return p
            
            # Fallback to full filtering if we're unlucky or many are bad
            available = [p for p in self.proxies if p not in self.bad_proxies]
            if not available:
                # If everything is bad, reset half of the oldest bad proxies
                sorted_bad = sorted(self.bad_proxies.items(), key=lambda x: x[1][0])
                for i in range(len(sorted_bad) // 2):
                    del self.bad_proxies[sorted_bad[i][0]]
                return random.choice(self.proxies)
                
            return random.choice(available)

    def mark_bad(self, proxy_url, is_rate_limit=False):
        """Blacklist a proxy. Rate limits get longer bans."""
        if not proxy_url:
            return
            
        duration = 60 if is_rate_limit else 15 # 1 hour for 429, 15m for connection errors
        
        with self._lock:
            expiry = datetime.now() + timedelta(minutes=duration)
            _, count = self.bad_proxies.get(proxy_url, (None, 0))
            self.bad_proxies[proxy_url] = (expiry, count + 1)
            
            # If a proxy fails 5 times, we could potentially remove it, 
            # but with 10k, simple timed blacklisting is usually enough.

    def get_requests_proxy(self):
        proxy_url = self.get_random_proxy()
        return {"http": proxy_url, "https": proxy_url} if proxy_url else None

# Global instance
proxy_manager = ProxyManager()
