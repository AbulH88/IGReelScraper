import json
import math
import re
from datetime import datetime, timezone
from typing import Iterable
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup

from ddgs import DDGS
from .models import InstagramSession, Reel, ReelSnapshot, db

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT = 15
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
    response = requests.get(
        url,
        headers=headers,
        cookies=cookies,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response


def _instagram_headers(session_config: InstagramSession | None, *, referer: str | None = None) -> dict:
    headers = {
        "User-Agent": (
            session_config.user_agent
            if session_config and session_config.user_agent
            else USER_AGENT
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": referer or INSTAGRAM_BASE + "/",
    }
    if session_config and session_config.is_active:
        headers["Accept"] = "*/*"
        headers["X-Requested-With"] = "XMLHttpRequest"
        headers["X-IG-App-ID"] = INSTAGRAM_APP_ID
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


def _instagram_api_get(path: str, *, referer: str | None = None) -> dict:
    session_config = get_instagram_session()
    response = requests.get(
        path,
        headers=_instagram_headers(session_config, referer=referer),
        cookies=_instagram_cookies(session_config),
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("status") == "fail":
        raise requests.RequestException(payload.get("message", "Instagram API request failed."))
    return payload


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


def discover_reels_for_hashtag(hashtag: str, max_id: str | None = None) -> tuple[list[dict], dict]:
    if has_instagram_session():
        path = f"{INSTAGRAM_BASE}/api/v1/tags/web_info/?tag_name={quote_plus(hashtag)}"
        if max_id:
            path += f"&max_id={quote_plus(max_id)}"
            
        payload = _instagram_api_get(
            path,
            referer=f"{INSTAGRAM_BASE}/explore/tags/{hashtag}/",
        )
        results, pagination = _extract_reels_from_tag_payload(payload, hashtag)
        if results:
            return results, pagination

    url = f"{INSTAGRAM_BASE}/explore/tags/{hashtag}/"
    response = _public_get(url)
    html = response.text
    if _looks_like_login_page(html, str(response.url)):
        raise InstagramLoginRequiredError(
            "Instagram returned a login page. Connect an Instagram session first."
        )
    soup = BeautifulSoup(html, "html.parser")
    reel_urls = []
    seen = set()

    for anchor in soup.select('a[href*="/reel/"]'):
        href = anchor.get("href")
        if not href:
            continue
        full_url = urljoin(INSTAGRAM_BASE, href.split("?")[0])
        if full_url not in seen:
            seen.add(full_url)
            reel_urls.append({"url": full_url, "source_hashtag": hashtag})

    if reel_urls:
        return reel_urls

    pattern = re.compile(r'"(\\/reel\\/[^"]+)"')
    for match in pattern.findall(html):
        full_url = urljoin(INSTAGRAM_BASE, match.replace("\\/", "/").split("?")[0])
        if full_url not in seen:
            seen.add(full_url)
            reel_urls.append({"url": full_url, "source_hashtag": hashtag})

    json_pattern = re.compile(r'<script type="application/ld\+json">(.*?)</script>', re.S)
    for block in json_pattern.findall(html):
        try:
            payload = json.loads(block)
        except json.JSONDecodeError:
            continue
        candidates = payload if isinstance(payload, list) else [payload]
        for item in candidates:
            item_url = item.get("url") if isinstance(item, dict) else None
            if item_url and "/reel/" in item_url and item_url not in seen:
                seen.add(item_url)
                reel_urls.append({"url": item_url, "source_hashtag": hashtag})

    return reel_urls, {"page": page, "next_page": None, "more_available": False}


def enrich_reel(reel: Reel, manual_metrics: dict | None = None) -> Reel:
    payload = manual_metrics or {}
    try:
        response = _public_get(reel.url)
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
    except requests.RequestException as exc:
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
    except (requests.RequestException, ValueError) as exc:
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
            except (requests.RequestException, InstagramLoginRequiredError) as exc:
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


def discover_reels_from_web(keyword: str, limit: int = 50) -> tuple[int, list[str]]:
    errors = []
    imported = 0
    query = f'site:instagram.com/reel/ "{keyword}"'
    tag = f"web:{keyword}"
    
    try:
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=limit))
    except Exception as exc:
        errors.append(f"Web search failed: {exc}")
        return 0, errors

    for item in results:
        url = item.get('href')
        if not url or '/reel/' not in url:
            continue
        
        full_url = url.split("?")[0]
        if not full_url.endswith('/'):
            full_url += '/'
            
        existing = Reel.query.filter_by(url=full_url).first()
        if existing:
            if tag not in existing.hashtag_list:
                merged = existing.hashtag_list + [tag]
                existing.hashtags = ", ".join(dict.fromkeys(merged))
                existing.source_hashtag = existing.source_hashtag or tag
                db.session.add(existing)
            continue
            
        reel = Reel(
            source_hashtag=tag,
            url=full_url,
            shortcode=shortcode_from_url(full_url),
            hashtags=tag,
            enrichment_status="pending",
        )
        db.session.add(reel)
        imported += 1
        
    db.session.commit()
    return imported, errors

