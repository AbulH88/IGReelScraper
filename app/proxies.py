import random
from datetime import datetime, timezone
from sqlalchemy import func
from .models import db, ProxyRecord

class ProxyManager:
    """Database-backed proxy manager with rotation and status tracking."""

    def get_random_proxy(self, group_name=None):
        """Pick an active proxy from the database, optionally filtered by group."""
        query = ProxyRecord.query.filter_by(is_active=True)
        if group_name:
            query = query.filter_by(group_name=group_name)
        
        # Simple random selection via SQL
        proxy = query.order_by(func.random()).first()
        if proxy:
            # Update last used time (optional, can be slow if done too often)
            # proxy.last_used_at = datetime.now(timezone.utc)
            # db.session.commit()
            return proxy.url
        return None

    def report_fail(self, proxy_url):
        """Mark a proxy as failed and deactivate if too many failures occur."""
        if not proxy_url:
            return
        
        proxy = ProxyRecord.query.filter_by(url=proxy_url).first()
        if proxy:
            proxy.fail_count += 1
            # If a proxy fails 5 times consecutively (or in total), deactivate it
            if proxy.fail_count >= 5:
                proxy.is_active = False
            db.session.commit()

    def report_success(self, proxy_url):
        """Reset failure count on successful use."""
        if not proxy_url:
            return
            
        proxy = ProxyRecord.query.filter_by(url=proxy_url).first()
        if proxy:
            if proxy.fail_count > 0:
                proxy.fail_count = 0
                db.session.commit()

    def get_requests_proxy(self, group_name=None):
        """Return a proxy dictionary formatted for the 'requests' or 'curl_cffi' library."""
        proxy_url = self.get_random_proxy(group_name)
        if not proxy_url:
            return None
        return {"http": proxy_url, "https": proxy_url}

# Global instance
proxy_manager = ProxyManager()
