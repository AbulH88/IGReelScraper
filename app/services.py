import json
import math
import re
import time
import random
from datetime import datetime, timezone
from typing import Iterable
from urllib.parse import quote_plus, urljoin

from curl_cffi import requests
from bs4 import BeautifulSoup

from ddgs import DDGS
from concurrent.futures import ThreadPoolExecutor
from .models import InstagramSession, Reel, ReelSnapshot, db
from .proxies import proxy_manager

# Global thread pool for highly concurrent media downloading and enrichment
worker_pool = ThreadPoolExecutor(max_workers=50)

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
REQUEST_TIMEOUT = 30
INSTAGRAM_BASE = "https://www.instagram.com"
INSTAGRAM_APP_ID = "936619743392459"


class InstagramLoginRequiredError(Exception):
    pass


def normalize_hashtags(raw: str) -> list[str]:
    seen = set()
    tags = []
    for part in re.split(r"[\s,]+", raw or ""):
        # Remove all special characters (keep letters, numbers, underscores)
        cleaned = re.sub(r"[^\w]", "", part.lower())
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            tags.append(cleaned)
    return tags


def shortcode_from_url(url: str | None) -> str | None:
    if not url:
        return None
    match = re.search(r"/reel/([^/?#]+)/?", url)
    return match.group(1) if match else None


def parse_metric(value: str | None) -> int | None:
    if not value:
        return None
    text = value.strip().lower().replace(",", "")
    multiplier = 1
    if text.endswith("k"):
        multiplier = 1_000
        text = text[:-1]
    elif text.endswith("m"):
        multiplier = 1_000_000
        text = text[:-1]
    elif text.endswith("b"):
        multiplier = 1_000_000_000
        text = text[:-1]
    text = re.sub(r"[^0-9.]", "", text)
    if not text:
        return None
    return int(float(text) * multiplier)


def metric_score(views: int | None, likes: int | None, comments: int | None) -> float:
    values = [
        math.log10((views or 0) + 1) * 45,
        math.log10((likes or 0) + 1) * 35,
        math.log10((comments or 0) + 1) * 20,
    ]
    return round(sum(values), 2)


def build_chart_points(snapshots: Iterable[ReelSnapshot]) -> list[dict]:
    points = []
    first = None
    for snapshot in snapshots:
        if first is None:
            first = snapshot.captured_at
        days = (snapshot.captured_at - first).days if first else 0
        points.append(
            {
                "label": f"Day {days}",
                "views": snapshot.views or 0,
                "likes": snapshot.likes or 0,
                "comments": snapshot.comments or 0,
                "captured_at": snapshot.captured_at.isoformat(),
            }
        )
    return points


def _public_get(url: str) -> requests.Response:
    session_config = get_instagram_session()
    headers = _instagram_headers(session_config)
    cookies = _instagram_cookies(session_config)
    proxy = proxy_manager.get_random_proxy()
    
    response = requests.get(
        url,
        headers=headers,
        cookies=cookies,
        proxy=proxy,
        impersonate="chrome131",
        timeout=REQUEST_TIMEOUT,
    )

    response.raise_for_status()
    return response


def _instagram_headers(session_config: InstagramSession | None, *, referer: str | None = None) -> dict:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Referer": referer or INSTAGRAM_BASE + "/",
    }
    if session_config and session_config.is_active:
        headers["X-Requested-With"] = "XMLHttpRequest"
        headers["X-IG-App-ID"] = INSTAGRAM_APP_ID
        headers["X-ASBD-ID"] = "129477"
        headers["X-IG-WWW-Claim"] = "0"
        if session_config.csrftoken:
            headers["X-CSRFToken"] = session_config.csrftoken
    return headers


def _instagram_cookies(session_config: InstagramSession | None) -> dict:
    cookies = {}
    if session_config and session_config.is_active:
        if session_config.sessionid:
            cookies["sessionid"] = session_config.sessionid
        if session_config.csrftoken:
            cookies["csrftoken"] = session_config.csrftoken
        if session_config.ds_user_id:
            cookies["ds_user_id"] = session_config.ds_user_id
    return cookies


def _instagram_api_get(path: str, *, referer: str | None = None, retries: int = 3) -> dict:
    session_config = get_instagram_session()
    last_error = None
    
    for attempt in range(retries):
        proxy = proxy_manager.get_random_proxy()
        try:
            response = requests.get(
                path,
                headers=_instagram_headers(session_config, referer=referer),
                cookies=_instagram_cookies(session_config),
                proxy=proxy,
                impersonate="chrome131",
                timeout=REQUEST_TIMEOUT,
            )
            
            if response.status_code == 429:
                proxy_manager.mark_bad(proxy)
                time.sleep(random.uniform(3, 7))
                continue
                
            response.raise_for_status()
            payload = response.json()
            if payload.get("status") == "fail":
                last_error = payload.get("message", "Instagram API request failed.")
                continue
            return payload
        except Exception as e:
            proxy_manager.mark_bad(proxy)
            last_error = str(e)
            time.sleep(random.uniform(2, 4))
            
    raise Exception(f"API failed after {retries} attempts. Last error: {last_error}")


def get_instagram_session() -> InstagramSession | None:
    return db.session.execute(
        db.select(InstagramSession).order_by(InstagramSession.id.desc())
    ).scalar_one_or_none()


def has_instagram_session() -> bool:
    session_config = get_instagram_session()
    return bool(session_config and session_config.is_active and session_config.sessionid)


def save_instagram_session(
    sessionid: str,
    csrftoken: str | None = None,
    ds_user_id: str | None = None,
    user_agent: str | None = None,
    is_active: bool = True,
) -> InstagramSession:
    session_config = get_instagram_session() or InstagramSession()
    session_config.sessionid = sessionid.strip()
    session_config.csrftoken = (csrftoken or "").strip() or None
    session_config.ds_user_id = (ds_user_id or "").strip() or None
    session_config.user_agent = (user_agent or "").strip() or None
    session_config.is_active = is_active
    db.session.add(session_config)
    db.session.commit()
    return session_config


def clear_instagram_session() -> None:
    session_config = get_instagram_session()
    if not session_config:
        return
    session_config.sessionid = None
    session_config.csrftoken = None
    session_config.ds_user_id = None
    session_config.user_agent = None
    session_config.is_active = False
    db.session.add(session_config)
    db.session.commit()


def discover_reels_for_hashtag(hashtag: str, max_id: str | None = None, app_context=None) -> tuple[int, list[str], list[Reel], str | None]:
    """
    Fetch all media for a hashtag using the modern 'sections' API for deeper results.
    Uses 'triangulation' strategy: rotating proxies every single batch to evade detection.
    """
    import threading
    errors = []
    imported = 0
    new_reels = []
    current_max_id = max_id
    
    tag_name = hashtag.strip().lstrip('#')
    source_tag = f"#{tag_name}"
    session_config = get_instagram_session()
    if not session_config or not session_config.is_active:
        errors.append("No active Instagram session.")
        return 0, errors, [], None

    headers = _instagram_headers(session_config, referer=f"{INSTAGRAM_BASE}/explore/tags/{tag_name}/")
    cookies = _instagram_cookies(session_config)
    
    # Modern Sections API (Used by mobile and web for deeper pagination)
    url = f"{INSTAGRAM_BASE}/api/v1/tags/{tag_name}/sections/"

    from .models import HashtagSearchState
    
    # Deeper crawling: up to 50 batches (Approx 2000-3000 items)
    for page_num in range(50):
        db.session.expire_all()
        state = HashtagSearchState.query.filter_by(hashtag=source_tag).first()
        if state and state.status == 'cancelled':
            return imported, errors, new_reels, None

        # Triangulation: Mandatory proxy rotation for every single page
        data = {
            "tab": "top",
            "count": 50,
        }
        if current_max_id:
            data["max_id"] = str(current_max_id)
        
        # Sections API usually requires POST
        payload, exc = _make_ig_request(url, headers, cookies, data=data, method="POST")
        
        if not payload:
            if exc: errors.append(f"Batch failed: {exc}")
            break

        sections = payload.get("sections", [])
        if not sections:
            break
            
        found_in_batch = 0
        for section in sections:
            layout_content = section.get("layout_content", {})
            # Also check 'fill_items' which sometimes contains reels in grid view
            medias = layout_content.get("medias", []) + layout_content.get("fill_items", [])
            for wrap in medias:
                media = wrap.get("media")
                if not media: continue
                
                code = media.get("code")
                if not code: continue
                
                m_type = _extract_media_type(media)
                
                # USER REQUEST: Hashtag search should only include videos
                if m_type != "video":
                    continue

                full_url = f"{INSTAGRAM_BASE}/reel/{code}/"
                m_type = "video"
                
                views = media.get("play_count") or media.get("view_count")
                likes = media.get("like_count")
                comments = media.get("comment_count")
                
                existing = Reel.query.filter_by(url=full_url).first()
                
                # Extract carousel images
                carousel_urls = []
                if m_type == "carousel" and media.get("carousel_media"):
                    for sub in media.get("carousel_media"):
                        c_thumb = _thumbnail_from_media(sub)
                        if c_thumb: carousel_urls.append(c_thumb)

                if existing:
                    if source_tag not in existing.hashtag_list:
                        merged = existing.hashtag_list + [source_tag]
                        existing.hashtags = ", ".join(dict.fromkeys(merged))
                    apply_metrics(existing, views, likes, comments)
                    existing.media_type = m_type
                    if carousel_urls:
                        existing.carousel_json = json.dumps(carousel_urls)
                    db.session.add(existing)
                    new_reels.append(existing)
                    continue
                    
                reel = Reel(
                    source_hashtag=source_tag,
                    url=full_url,
                    shortcode=code,
                    hashtags=source_tag,
                    media_type=m_type,
                    carousel_json=json.dumps(carousel_urls) if carousel_urls else None,
                    title=media.get("caption", {}).get("text") if media.get("caption") else None,
                    creator=media.get("user", {}).get("username"),
                    thumbnail_url=_thumbnail_from_media(media),
                    video_url=_video_url_from_media(media) if m_type == "video" else None,
                    enrichment_status="ok",
                )
                apply_metrics(reel, views, likes, comments)
                db.session.add(reel)
                db.session.flush()
                
                if app_context:
                    worker_pool.submit(download_media, reel.id, app_context)
                    # If views are missing, queue for deep enrichment
                    if (reel.last_views is None or reel.last_views == 0) and reel.media_type == 'video':
                        worker_pool.submit(_deep_enrich_task, reel.id, app_context)

                new_reels.append(reel)
                imported += 1
                found_in_batch += 1
        
        db.session.commit()
        
        # Paging for Sections API
        current_max_id = payload.get("next_max_id")
        more = payload.get("more_available", False)
        
        if not current_max_id or not more: 
            break
            
        # Fast pagination using rotating proxies
        time.sleep(random.uniform(0.5, 1.5))
            
    return imported, errors, new_reels, current_max_id


def enrich_reel(reel: Reel, manual_metrics: dict | None = None) -> Reel:
    payload = manual_metrics or {}
    try:
        session_config = get_instagram_session()
        headers = _instagram_headers(session_config, referer=reel.url)
        cookies = _instagram_cookies(session_config)
        proxy = proxy_manager.get_random_proxy()
        
        response = requests.get(
            reel.url, 
            headers=headers, 
            cookies=cookies, 
            proxy=proxy,
            impersonate="chrome131",
            timeout=REQUEST_TIMEOUT
        )
        if response.status_code in (403, 429):
            proxy_manager.mark_bad(proxy)
            
        response.raise_for_status()
        
        html = response.text
        soup = BeautifulSoup(html, "html.parser")

        title = None
        og_title = soup.find("meta", attrs={"property": "og:title"})
        if og_title:
            title = og_title.get("content")
        description = soup.find("meta", attrs={"property": "og:description"})
        description_text = description.get("content") if description else None
        og_video = soup.find("meta", attrs={"property": "og:video:secure_url"}) or soup.find(
            "meta", attrs={"property": "og:video"}
        )
        og_image = (
            soup.find("meta", attrs={"property": "og:image:secure_url"}) or 
            soup.find("meta", attrs={"property": "og:image"}) or
            soup.find("meta", attrs={"name": "twitter:image"})
        )

        reel.shortcode = reel.shortcode or shortcode_from_url(reel.url)
        reel.title = reel.title or title
        reel.caption = reel.caption or description_text
        reel.creator = reel.creator or _extract_creator(title)
        
        # Always update video and thumbnail if we found fresh ones, as they expire
        if og_video and og_video.get("content"):
            reel.video_url = og_video.get("content")
        if og_image and og_image.get("content"):
            reel.thumbnail_url = og_image.get("content")

        metrics = extract_metrics_from_html(html, description_text)
        views = payload.get("views") if payload.get("views") is not None else metrics.get("views")
        likes = payload.get("likes") if payload.get("likes") is not None else metrics.get("likes")
        comments = payload.get("comments") if payload.get("comments") is not None else metrics.get("comments")

        apply_metrics(reel, views, likes, comments)
        reel.enrichment_status = "ok"
        reel.last_error = None
    except Exception as exc:
        reel.enrichment_status = "error"
        reel.last_error = str(exc)
        if payload:
            apply_metrics(reel, payload.get("views"), payload.get("likes"), payload.get("comments"))
    reel.last_checked_at = datetime.now(timezone.utc)
    db.session.add(reel)
    db.session.commit()
    return reel


def validate_instagram_session() -> tuple[bool, str]:
    if not has_instagram_session():
        return False, "No active Instagram session is configured."
    try:
        payload = _instagram_api_get(
            f"{INSTAGRAM_BASE}/api/v1/tags/web_info/?tag_name=instagram",
            referer=f"{INSTAGRAM_BASE}/explore/tags/instagram/",
        )
    except Exception as exc:
        return False, f"Instagram request failed: {exc}"
    if not payload.get("data", {}).get("name"):
        return False, "Instagram session responded, but hashtag discovery data was missing."
    return True, "Instagram session looks usable for authenticated discovery."


def _extract_creator(title: str | None) -> str | None:
    if not title:
        return None
    parts = re.split(r" on Instagram:|•|\(@", title)
    first = parts[0].strip()
    return first or None


def extract_metrics_from_html(html: str, description: str | None = None) -> dict:
    matches = {"views": None, "likes": None, "comments": None}
    metric_patterns = {
        "views": [r"([\d.,]+[KMBkmb]?)\s+views", r'viewCount"\s*:?\s*"?(\d+)'],
        "likes": [r"([\d.,]+[KMBkmb]?)\s+likes", r"edge_media_preview_like[^\d]+(\d+)"],
        "comments": [r"([\d.,]+[KMBkmb]?)\s+comments", r"edge_media_to_parent_comment[^\d]+(\d+)"],
    }
    blob = f"{html}\n{description or ''}"
    for name, patterns in metric_patterns.items():
        for pattern in patterns:
            match = re.search(pattern, blob, re.I)
            if match:
                matches[name] = parse_metric(match.group(1))
                break
    return matches


def _looks_like_login_page(html: str, final_url: str) -> bool:
    login_markers = (
        "/accounts/login",
        'href="https://www.instagram.com/accounts/login/',
        "Log in",
        "loginForm",
    )
    lowered = html.lower()
    return "/accounts/login" in final_url or any(marker.lower() in lowered for marker in login_markers)


def apply_metrics(reel: Reel, views: int | None, likes: int | None, comments: int | None) -> Reel:
    values = (views, likes, comments)
    if all(value is None for value in values):
        return reel
    reel.last_views = views if views is not None else reel.last_views
    reel.last_likes = likes if likes is not None else reel.last_likes
    reel.last_comments = comments if comments is not None else reel.last_comments
    reel.viral_score = metric_score(reel.last_views, reel.last_likes, reel.last_comments)
    latest_snapshot = reel.snapshots[-1] if reel.snapshots else None
    if latest_snapshot and (
        latest_snapshot.views == reel.last_views
        and latest_snapshot.likes == reel.last_likes
        and latest_snapshot.comments == reel.last_comments
    ):
        return reel
    snapshot = ReelSnapshot(
        reel=reel,
        views=reel.last_views,
        likes=reel.last_likes,
        comments=reel.last_comments,
    )
    db.session.add(snapshot)
    return reel


def import_discovered_reels(hashtags: list[str], max_id_by_tag: dict[str, str] | None = None, depth: int = 1) -> tuple[int, list[str], dict[str, dict]]:
    total_imported = 0
    errors = []
    search_state = {}
    for tag in hashtags:
        current_max_id = (max_id_by_tag or {}).get(tag)
        for d in range(depth):
            try:
                discovered, pagination = discover_reels_for_hashtag(tag, max_id=current_max_id)
            except Exception as exc:
                errors.append(f"#{tag} (page {d+1}): {exc}")
                break
            
            search_state[tag] = pagination
            page_imported = 0
            for item in discovered:
                url = item["url"]
                existing = Reel.query.filter_by(url=url).first()
                if existing:
                    existing.source_hashtag = existing.source_hashtag or tag
                    if tag not in existing.hashtag_list:
                        merged = existing.hashtag_list + [tag]
                        existing.hashtags = ", ".join(dict.fromkeys(merged))
                    
                    # Always refresh these as they expire
                    if item.get("thumbnail_url"):
                        existing.thumbnail_url = item["thumbnail_url"]
                    if item.get("video_url"):
                        existing.video_url = item["video_url"]
                    
                    db.session.add(existing)
                    apply_metrics(existing, item.get("views"), item.get("likes"), item.get("comments"))
                    continue
                
                reel = Reel(
                    source_hashtag=tag,
                    url=url,
                    shortcode=shortcode_from_url(url),
                    hashtags=tag,
                    thumbnail_url=item.get("thumbnail_url"),
                    video_url=item.get("video_url"),
                    creator=item.get("creator"),
                    title=item.get("title"),
                    caption=item.get("caption"),
                    enrichment_status="pending",
                )
                db.session.add(reel)
                apply_metrics(reel, item.get("views"), item.get("likes"), item.get("comments"))
                page_imported += 1
            
            total_imported += page_imported
            db.session.commit()
            
            current_max_id = pagination.get("next_max_id")
            if not pagination.get("more_available") or not current_max_id:
                break
                
    return total_imported, errors, search_state


def _extract_reels_from_tag_payload(payload: dict, hashtag: str) -> tuple[list[dict], dict]:
    data = payload.get("data", {})
    candidates = []
    seen = set()
    
    # Instagram often puts next_max_id in different locations depending on the endpoint
    next_max_id = data.get("next_max_id")
    more_available = bool(data.get("more_available"))
    
    # Try alternate location if not found
    top_section = data.get("top", {}) or {}
    if not next_max_id:
        next_max_id = top_section.get("next_max_id")
        more_available = more_available or bool(top_section.get("more_available"))
    
    pagination = {
        "next_max_id": next_max_id,
        "more_available": more_available,
    }

    for bucket in ("top", "recent"):
        section_data = data.get(bucket, {})
        for media in _iter_media(section_data):
            code = media.get("code")
            if not code:
                continue
            url = f"{INSTAGRAM_BASE}/reel/{code}/"
            if url in seen:
                continue
            seen.add(url)
            caption = ((media.get("caption") or {}).get("text") or "").strip() or None
            user = media.get("user") or {}
            candidates.append(
                {
                    "url": url,
                    "source_hashtag": hashtag,
                    "title": caption[:255] if caption else f"#{hashtag} reel {code}",
                    "creator": user.get("username") or user.get("full_name"),
                    "caption": caption,
                    "views": media.get("play_count") or media.get("view_count"),
                    "likes": media.get("like_count"),
                    "comments": media.get("comment_count"),
                    "thumbnail_url": _thumbnail_from_media(media),
                    "video_url": _video_url_from_media(media),
                }
            )
    return candidates, pagination


def _iter_media(node):
    if isinstance(node, dict):
        if "media" in node and isinstance(node["media"], dict):
            yield node["media"]
        elif {"code", "pk"} & set(node.keys()):
            yield node
        for value in node.values():
            yield from _iter_media(value)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_media(item)


def _thumbnail_from_media(media: dict) -> str | None:
    image_versions = media.get("image_versions2") or {}
    candidates = image_versions.get("candidates") or []
    if candidates:
        return candidates[0].get("url")
    additional = image_versions.get("additional_candidates") or {}
    for item in additional.values():
        if isinstance(item, dict) and item.get("url"):
            return item["url"]
    return None


def _video_url_from_media(media: dict) -> str | None:
    versions = media.get("video_versions") or []
    for version in versions:
        if isinstance(version, dict) and version.get("url"):
            return version["url"]
    return None


def refresh_reel(reel: Reel) -> Reel:
    return enrich_reel(reel)


def refresh_all_reels() -> tuple[int, list[str]]:
    refreshed = 0
    errors = []
    for reel in Reel.query.order_by(Reel.created_at.desc()).all():
        try:
            refresh_reel(reel)
            refreshed += 1
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{reel.url}: {exc}")
            db.session.rollback()
    return refreshed, errors


def get_user_info(username: str) -> dict | None:
    """Get full profile info for a user and update CreatorStats."""
    from .models import CreatorStats
    
    data = None
    # Method 1: web_profile_info (Most detailed)
    try:
        url = f"{INSTAGRAM_BASE}/api/v1/users/web_profile_info/?username={username}"
        payload = _instagram_api_get(url, referer=f"{INSTAGRAM_BASE}/{username}/")
        user_data = payload.get("data", {}).get("user", {})
        if user_data:
            data = {
                "id": user_data.get("id"),
                "username": user_data.get("username"),
                "full_name": user_data.get("full_name"),
                "profile_pic_url": user_data.get("profile_pic_url_hd") or user_data.get("profile_pic_url"),
                "biography": user_data.get("biography"),
                "external_url": user_data.get("external_url"),
                "followers_count": user_data.get("edge_followed_by", {}).get("count"),
                "following_count": user_data.get("edge_follow", {}).get("count"),
                "posts_count": user_data.get("edge_owner_to_timeline_media", {}).get("count"),
                "is_verified": user_data.get("is_verified"),
            }
    except Exception:
        pass
        
    if not data:
        # Fallback to HTML scraping via proxy
        try:
            response = _public_get(f"{INSTAGRAM_BASE}/{username}/")
            html = response.text
            # Simple regex extraction for ID if API fails
            match = re.search(r'"(?:profile|user)_id":"(\d+)"', html)
            uid = match.group(1) if match else None
            if uid:
                data = {"id": uid, "username": username}
        except Exception:
            pass

    if data and data.get("username"):
        # Update or create CreatorStats
        stats = CreatorStats.query.filter_by(username=data["username"]).first() or CreatorStats(username=data["username"])
        stats.full_name = data.get("full_name")
        stats.profile_pic_url = data.get("profile_pic_url")
        stats.biography = data.get("biography")
        stats.external_url = data.get("external_url")
        stats.followers_count = data.get("followers_count")
        stats.following_count = data.get("following_count")
        stats.posts_count = data.get("posts_count")
        stats.is_verified = data.get("is_verified", False)
        db.session.add(stats)
        db.session.commit()
        
    return data


import os
from pathlib import Path

# Media storage configuration
MEDIA_DIR = Path("instance") / "media"
MEDIA_DIR.mkdir(parents=True, exist_ok=True)

def download_media(reel_id: int, app_context):
    """Download thumbnail and video for a reel to local storage."""
    with app_context.app_context():
        reel = db.session.get(Reel, reel_id)
        if not reel: return

        # 1. Download Thumbnail
        if reel.thumbnail_url and not reel.local_thumb_path:
            ext = "jpg"
            filename = f"thumb_{reel.shortcode}_{reel.id}.{ext}"
            filepath = MEDIA_DIR / filename
            if not filepath.exists():
                try:
                    # Reuse raw request logic
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                        'Referer': 'https://www.instagram.com/',
                    }
                    proxies = proxy_manager.get_requests_proxy()
                    resp = requests.get(reel.thumbnail_url, headers=headers, proxies=proxies, timeout=15)
                    if resp.status_code == 200:
                        filepath.write_bytes(resp.content)
                        reel.local_thumb_path = f"media/{filename}"
                        db.session.commit()
                except Exception as e:
                    print(f"Failed to download thumb for {reel.shortcode}: {e}")

        # 2. Download Video (if applicable)
        if reel.media_type == 'video' and reel.video_url and not reel.local_video_path:
            filename = f"video_{reel.shortcode}_{reel.id}.mp4"
            filepath = MEDIA_DIR / filename
            if not filepath.exists():
                try:
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                        'Referer': 'https://www.instagram.com/',
                    }
                    proxies = proxy_manager.get_requests_proxy()
                    resp = requests.get(reel.video_url, headers=headers, proxies=proxies, timeout=30)
                    if resp.status_code == 200:
                        filepath.write_bytes(resp.content)
                        reel.local_video_path = f"media/{filename}"
                        db.session.commit()
                except Exception as e:
                    print(f"Failed to download video for {reel.shortcode}: {e}")

        # 3. Download Carousel Images
        if reel.media_type == 'carousel' and reel.carousel_json:
            try:
                urls = json.loads(reel.carousel_json)
                local_paths = []
                updated = False
                
                for i, url in enumerate(urls):
                    if url.startswith('media/'): # Already local
                        local_paths.append(url)
                        continue
                        
                    filename = f"carousel_{reel.shortcode}_{reel.id}_{i}.jpg"
                    filepath = MEDIA_DIR / filename
                    
                    if not filepath.exists():
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                            'Referer': 'https://www.instagram.com/',
                        }
                        proxies = proxy_manager.get_requests_proxy()
                        resp = requests.get(url, headers=headers, proxies=proxies, timeout=15)
                        if resp.status_code == 200:
                            filepath.write_bytes(resp.content)
                            local_paths.append(f"media/{filename}")
                            updated = True
                        else:
                            local_paths.append(url) # Keep original if failed
                    else:
                        local_paths.append(f"media/{filename}")
                    
                    time.sleep(0.5) # Anti-ban
                
                if updated:
                    reel.carousel_json = json.dumps(local_paths)
                    db.session.commit()
            except Exception as e:
                print(f"Carousel download failed for {reel.shortcode}: {e}")

def _deep_enrich_task(reel_id: int, app_context):
    """Background task to fetch missing views/likes for a specific item."""
    with app_context.app_context():
        reel = db.session.get(Reel, reel_id)
        if not reel: return
        
        # Only enrich if metrics are missing
        if reel.last_views is not None and reel.last_views > 0:
            return
            
        try:
            # reuse enrich_reel logic
            enrich_reel(reel)
            print(f"Deep enriched {reel.shortcode}: {reel.last_views} views found.")
        except Exception as e:
            print(f"Deep enrichment failed for {reel.shortcode}: {e}")

def _extract_media_type(media: dict) -> str:
    """Determine if media is a video, image, or carousel."""
    m_type = media.get("media_type")
    if m_type == 1:
        return "image"
    if m_type == 2:
        return "video"
    if m_type == 8:
        return "carousel"
    
    # Fallback/alternative
    if media.get("video_versions"):
        return "video"
    if media.get("carousel_media"):
        return "carousel"
    return "image"

def _make_ig_request(url, headers, cookies, params=None, data=None, method="GET"):
    """Internal helper to make IG requests with retries and proxy rotation."""
    payload = None
    last_exc = None
    for retry in range(3):
        proxy = None
        try:
            proxy = proxy_manager.get_random_proxy()
            if method == "GET":
                resp = requests.get(url, headers=headers, cookies=cookies, params=params, proxy=proxy, impersonate="chrome131", timeout=15)
            else:
                resp = requests.post(url, headers=headers, cookies=cookies, data=data, proxy=proxy, impersonate="chrome131", timeout=15)
            
            if resp.status_code == 200:
                print(f"SUCCESS: {method} {url.split('/v1/')[1]} via {proxy.split('@')[1] if proxy else 'direct'}")
                return resp.json(), None

            if resp.status_code == 429:
                proxy_manager.mark_bad(proxy)
                print(f"RETRY: Rate limited (429) on proxy {proxy.split('@')[1] if proxy else 'direct'}. Retry {retry+1}/3")
                time.sleep(1)
                continue
            
            resp.raise_for_status()
            return resp.json(), None
        except Exception as e:
            if proxy: proxy_manager.mark_bad(proxy)
            print(f"RETRY: Request failed: {e}. Retry {retry+1}/3")
            last_exc = e
            time.sleep(0.5)
    return None, last_exc

def discover_reels_direct(username: str, max_id: str | None = None, app_context=None) -> tuple[int, list[str], list[Reel], str | None]:
    """
    Fetch all media (images, reels, carousels) directly from Instagram API.
    Uses both feed and clips endpoints to ensure 100% coverage.
    """
    import threading
    errors = []
    imported = 0
    new_reels = []
    current_max_id = max_id
    
    # 1. Clean username
    username = username.strip().strip('/').split('/')[-1].lstrip('@').split('?')[0]
    
    # 2. Get User Info & ID
    user_info = get_user_info(username)
    if not user_info or not user_info.get("id"):
        errors.append(f"Could not find ID for @{username}. Profile may be private.")
        return 0, errors, [], None
        
    user_id = user_info["id"]
    tag = f"creator:{username}"
    session_config = get_instagram_session()
    if not session_config or not session_config.is_active:
        errors.append("No active Instagram session.")
        return 0, errors, [], None

    headers = _instagram_headers(session_config, referer=f"{INSTAGRAM_BASE}/{username}/")
    cookies = _instagram_cookies(session_config)
    
    # We will alternate or fetch from both to be sure. 
    # Feed is better for images, Clips is better for reels.
    endpoints = [
        f"{INSTAGRAM_BASE}/api/v1/feed/user/{user_id}/",
        f"{INSTAGRAM_BASE}/api/v1/clips/user/"
    ]

    from .models import HashtagSearchState
    
    for url in endpoints:
        current_max_id = max_id if url == endpoints[0] else None # Reset max_id for second endpoint
        is_clips = "clips" in url
        
        for page_num in range(50): # 50 batches per endpoint = 100 total
            db.session.expire_all()
            state = HashtagSearchState.query.filter_by(hashtag=tag).first()
            if state and state.status == 'cancelled':
                return imported, errors, new_reels, None

            params = {"count": 24} if not is_clips else {"target_user_id": str(user_id), "page_size": 24}
            if current_max_id:
                params["max_id"] = str(current_max_id)
            
            data = None
            method = "GET"
            if is_clips:
                method = "POST"
                data = params
                params = None

            payload, exc = _make_ig_request(url, headers, cookies, params=params, data=data, method=method)
            
            if not payload:
                if exc: errors.append(f"Batch failed: {exc}")
                break

            items = payload.get("items", [])
            if not items: break
                
            for item in items:
                media = item.get("media", item) 
                code = media.get("code")
                if not code: continue
                
                m_type = _extract_media_type(media)
                # If it's a video, check if it's a Reel (clips)
                is_reel = is_clips or (m_type == "video" and media.get("is_dash_eligible"))
                
                if is_reel:
                    full_url = f"{INSTAGRAM_BASE}/reel/{code}/"
                    m_type = "video"
                else:
                    full_url = f"{INSTAGRAM_BASE}/p/{code}/"
                
                views = media.get("play_count") or media.get("view_count")
                likes = media.get("like_count")
                comments = media.get("comment_count")
                
                existing = Reel.query.filter_by(url=full_url).first()
                
                # Extract carousel images if present
                carousel_urls = []
                if m_type == "carousel" and media.get("carousel_media"):
                    for sub in media.get("carousel_media"):
                        # Get best thumbnail/image for this slide
                        c_thumb = _thumbnail_from_media(sub)
                        if c_thumb: carousel_urls.append(c_thumb)
                
                if existing:
                    if tag not in existing.hashtag_list:
                        merged = existing.hashtag_list + [tag]
                        existing.hashtags = ", ".join(dict.fromkeys(merged))
                    apply_metrics(existing, views, likes, comments)
                    existing.media_type = m_type
                    if carousel_urls:
                        existing.carousel_json = json.dumps(carousel_urls)
                    db.session.add(existing)
                    new_reels.append(existing)
                    continue
                    
                reel = Reel(
                    source_hashtag=tag,
                    url=full_url,
                    shortcode=code,
                    hashtags=tag,
                    media_type=m_type,
                    carousel_json=json.dumps(carousel_urls) if carousel_urls else None,
                    title=media.get("caption", {}).get("text") if media.get("caption") else None,
                    creator=username,
                    thumbnail_url=_thumbnail_from_media(media),
                    video_url=_video_url_from_media(media) if m_type == "video" else None,
                    enrichment_status="ok",
                )
                apply_metrics(reel, views, likes, comments)
                db.session.add(reel)
                db.session.flush() # Get ID for thread
                
                if app_context:
                    worker_pool.submit(download_media, reel.id, app_context)
                    # If views are missing, queue for deep enrichment
                    if (reel.last_views is None or reel.last_views == 0) and reel.media_type == 'video':
                        worker_pool.submit(_deep_enrich_task, reel.id, app_context)

                new_reels.append(reel)
                imported += 1
            
            db.session.commit()
            
            # Paging
            if is_clips:
                p_info = payload.get("paging_info", {})
                current_max_id = p_info.get("max_id") or payload.get("next_max_id")
                more = p_info.get("more_available", True)
            else:
                current_max_id = payload.get("next_max_id")
                more = payload.get("more_available", True)
                
            if not current_max_id or not more: break
            time.sleep(random.uniform(0.5, 1.5)) # Accelerated pagination
            
    return imported, errors, new_reels, current_max_id




def discover_reels_from_web(keyword: str, limit: int = 50, tag_prefix: str = "web") -> tuple[int, list[str], list[Reel], str | None]:
    errors = []
    imported = 0
    new_reels = []
    query = f'site:instagram.com/reel/ "{keyword}"'
    tag = f"{tag_prefix}:{keyword}"
    
    try:
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=limit))
    except Exception as exc:
        errors.append(f"Web search failed: {exc}")
        return 0, errors, [], None

    for item in results:
        url = item.get('href')
        if not url or '/reel/' not in url:
            continue
        
        full_url = url.split("?")[0]
        if not full_url.endswith('/'):
            full_url += '/'
            
        # Try to parse initial likes/comments from DuckDuckGo snippet
        body = item.get('body', '')
        likes_match = re.search(r'([\d.,]+[KMBkmb]?)\s+likes', body, re.I)
        comments_match = re.search(r'([\d.,]+[KMBkmb]?)\s+comments', body, re.I)
        
        initial_likes = parse_metric(likes_match.group(1)) if likes_match else None
        initial_comments = parse_metric(comments_match.group(1)) if comments_match else None
        
        existing = Reel.query.filter_by(url=full_url).first()
        if existing:
            if tag not in existing.hashtag_list:
                merged = existing.hashtag_list + [tag]
                existing.hashtags = ", ".join(dict.fromkeys(merged))
                existing.source_hashtag = existing.source_hashtag or tag
                
                if initial_likes and not existing.last_likes:
                    existing.last_likes = initial_likes
                if initial_comments and not existing.last_comments:
                    existing.last_comments = initial_comments
                
                db.session.add(existing)
                new_reels.append(existing)
            continue
            
        reel = Reel(
            source_hashtag=tag,
            url=full_url,
            shortcode=shortcode_from_url(full_url),
            hashtags=tag,
            title=item.get('title'),
            enrichment_status="pending",
        )
        if initial_likes or initial_comments:
            apply_metrics(reel, None, initial_likes, initial_comments)
            
        db.session.add(reel)
        new_reels.append(reel)
        imported += 1
        
    db.session.commit()
    return imported, errors, new_reels, None
