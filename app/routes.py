from __future__ import annotations

import os
import requests
import time
import threading
from flask import Blueprint, Response, abort, current_app, flash, jsonify, redirect, render_template, request, send_from_directory, stream_with_context, url_for
from sqlalchemy import or_

from sqlalchemy.exc import IntegrityError
from .models import HashtagSearchState, Reel, TaskNotification, db
from .services import (
    _instagram_cookies,
    _instagram_headers,
    build_chart_points,
    clear_instagram_session,
    discover_reels_direct,
    discover_reels_from_web,
    enrich_reel,
    get_instagram_session,
    has_instagram_session,
    import_discovered_reels,
    normalize_hashtags,
    refresh_all_reels,
    refresh_reel,
    save_instagram_session,
    validate_instagram_session,
)
from .proxies import proxy_manager

bp = Blueprint('main', __name__)


def _to_int(name: str):
    value = request.form.get(name, '').strip()
    return int(value) if value else None


def _get_or_create_hashtag_state(hashtag: str) -> HashtagSearchState:
    """Safely get or create a HashtagSearchState record to prevent race condition IntegrityErrors."""
    state = HashtagSearchState.query.filter_by(hashtag=hashtag).first()
    if not state:
        try:
            state = HashtagSearchState(hashtag=hashtag)
            db.session.add(state)
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
            state = HashtagSearchState.query.filter_by(hashtag=hashtag).first()
    return state


@bp.route('/hashtag-search', methods=['GET', 'POST'])
def hashtag_search():
    if request.method == 'POST':
        raw_hashtag = request.form.get('hashtag', '').strip()
        if not raw_hashtag:
            flash('Hashtag is required', 'danger')
            return redirect(url_for('main.hashtag_search'))
        
        hashtags = normalize_hashtags(raw_hashtag)
        if not hashtags:
            flash('Enter at least one valid hashtag (min 2 characters)', 'warning')
            return redirect(url_for('main.hashtag_search'))
            
        for tag_name in hashtags:
            hashtag = f"#{tag_name}"
                
            # Check if already scrolling
            state = _get_or_create_hashtag_state(hashtag)
            if state.status == 'scrolling':
                continue # Skip if already in progress
                
            # Start background search
            threading.Thread(
                target=async_scroll_hashtag, 
                args=(current_app._get_current_object(), hashtag)
            ).start()
        
        active_hashtag = f"#{hashtags[0]}"
        flash(f'Started search for {len(hashtags)} hashtag(s) in the background.', 'success')
        return redirect(url_for('main.hashtag_search', active_hashtag=active_hashtag))

    active_hashtag = request.args.get('active_hashtag', '')
    limit = request.args.get('limit', type=int) or 200
    sort_by = request.args.get('sort_by', 'views_desc')
    
    search_query = HashtagSearchState.query.filter(HashtagSearchState.hashtag.startswith('#')).order_by(HashtagSearchState.updated_at.desc())
    recent_searches = search_query.all()
    
    hashtag_stats_list = []
    if not active_hashtag:
        for state in recent_searches:
            total = Reel.query.filter(Reel.hashtags.like(f"%{state.hashtag}%"), Reel.media_type == 'video').count()
            processed = Reel.query.filter(Reel.hashtags.like(f"%{state.hashtag}%"), Reel.media_type == 'video', Reel.enrichment_status != 'pending').count()
            progress = int((processed / max(total, 1)) * 100)
            
            hashtag_stats_list.append({
                'hashtag': state.hashtag,
                'total_items': total,
                'progress': progress,
                'status': state.status,
                'last_updated': state.updated_at
            })

    reels = []
    has_more_local = False
    stats = {'count': 0, 'status': 'ready', 'progress': 0}
    
    if active_hashtag:
        tag = active_hashtag if active_hashtag.startswith('#') else f"#{active_hashtag}"
        state = _get_or_create_hashtag_state(tag)
            
        base_query = Reel.query.filter(or_(Reel.source_hashtag == tag, Reel.hashtags.like(f"%{tag}%")), Reel.media_type == 'video')
        total_count = base_query.count()
        processed_count = base_query.filter(Reel.enrichment_status != 'pending').count()
        
        query = base_query
        
        if sort_by == 'views_desc':
            query = query.order_by(Reel.last_views.desc().nullslast())
        elif sort_by == 'views_asc':
            query = query.order_by(Reel.last_views.asc().nullslast())
        elif sort_by == 'newest':
            query = query.order_by(Reel.created_at.desc())
        elif sort_by == 'oldest':
            query = query.order_by(Reel.created_at.asc())
            
        reels = query.limit(limit).all()
        has_more_local = total_count > len(reels)
        
        stats['count'] = total_count
        stats['progress'] = int((processed_count / max(total_count, 1)) * 100)
        stats['status'] = state.status

    return render_template(
        'hashtag_search.html',
        reels=reels,
        active_hashtag=active_hashtag,
        hashtag_stats_list=hashtag_stats_list,
        recent_searches=recent_searches,
        stats=stats,
        limit=limit,
        sort_by=sort_by,
        has_more_local=has_more_local
    )


@bp.route('/api/hashtag-status/<path:hashtag>')
def get_hashtag_status(hashtag: str):
    from .models import HashtagSearchState, Reel
    if not hashtag.startswith('#'): hashtag = f"#{hashtag}"
    state = HashtagSearchState.query.filter_by(hashtag=hashtag).first()
    if not state:
        return jsonify({'error': 'Not found'}), 404
    
    base_query = Reel.query.filter(or_(Reel.source_hashtag == hashtag, Reel.hashtags.like(f"%{hashtag}%")), Reel.media_type == 'video')
    total = base_query.count()
    processed = base_query.filter(Reel.enrichment_status != 'pending').count()
    progress = int((processed / max(total, 1)) * 100)
    
    return jsonify({
        'hashtag': hashtag,
        'status': state.status,
        'progress': progress,
        'total_items': total,
        'processed_items': processed
    })


@bp.route('/api/cancel-hashtag-search/<path:hashtag>')
def cancel_hashtag_search(hashtag: str):
    from .models import HashtagSearchState
    if not hashtag.startswith('#'): hashtag = f"#{hashtag}"
    state = HashtagSearchState.query.filter_by(hashtag=hashtag).first()
    if state and state.status == 'scrolling':
        state.status = 'cancelled'
        db.session.commit()
        return jsonify({'status': 'success'})
    return jsonify({'status': 'error'}), 400


def async_scroll_hashtag(app, hashtag):
    with app.app_context():
        from .models import db, Reel, HashtagSearchState
        from .services import discover_reels_for_hashtag
        
        state = _get_or_create_hashtag_state(hashtag)
        
        state.status = 'scrolling'
        state.last_error = None
        db.session.commit()
        
        imported, errors, new_reels, next_max_id = discover_reels_for_hashtag(hashtag, app_context=app)
        
        state.next_max_id = next_max_id
        state.more_available = bool(next_max_id)
        state.status = 'done' if not errors else 'error'
        if errors: state.last_error = "; ".join(errors)
        db.session.commit()


@bp.route('/')
def dashboard():
    from sqlalchemy import func, or_
    
    total_count = Reel.query.count()
    video_count = Reel.query.filter_by(media_type='video').count()
    image_count = Reel.query.filter_by(media_type='image').count()
    carousel_count = Reel.query.filter_by(media_type='carousel').count()
    
    total_views_res = db.session.query(func.sum(Reel.last_views)).scalar()
    total_views = int(total_views_res) if total_views_res else 0
    avg_views = int(total_views / max(total_count, 1))
    
    max_views_res = db.session.query(func.max(Reel.last_views)).scalar()
    max_views = int(max_views_res) if max_views_res else 0
    
    playable_reels = Reel.query.filter(or_(Reel.video_url.isnot(None), Reel.shortcode.isnot(None))).count()

    # Top creators by count
    top_creators = db.session.query(Reel.creator, func.count(Reel.id).label('cnt')).filter(Reel.creator.isnot(None)).group_by(Reel.creator).order_by(func.count(Reel.id).desc()).limit(5).all()
    
    # Recent searches
    recent_searches = HashtagSearchState.query.order_by(HashtagSearchState.updated_at.desc()).limit(10).all()
    
    stats = {
        'total_items': total_count,
        'video_count': video_count,
        'image_count': image_count,
        'carousel_count': carousel_count,
        'avg_views': avg_views,
        'max_views': max_views,
        'playable_reels': playable_reels
    }

    top_reels = Reel.query.order_by(Reel.last_views.desc().nullslast()).limit(6).all()

    return render_template(
        'dashboard.html',
        stats=stats,
        top_creators=top_creators,
        recent_searches=recent_searches,
        top_reels=top_reels
    )


@bp.route('/media/<path:filename>')
def serve_media(filename):
    """Serve downloaded images and videos."""
    media_path = os.path.join(current_app.instance_path, 'media')
    full_path = os.path.join(media_path, filename)
    print(f"DEBUG: Serving media from {full_path}")
    if not os.path.exists(full_path):
        print(f"DEBUG: File not found: {full_path}")
        abort(404)
    return send_from_directory(media_path, filename)

@bp.route('/proxy-image')
def proxy_image():
    url = request.args.get('url')
    if not url:
        abort(400)

    # Check if we have this cached locally
    from .models import Reel
    reel = Reel.query.filter_by(thumbnail_url=url).first()
    if reel and reel.local_thumb_path:
        media_path = os.path.join(current_app.instance_path, 'media')
        filename = reel.local_thumb_path.replace('media/', '')
        if os.path.exists(os.path.join(media_path, filename)):
            return redirect(url_for('main.serve_media', filename=filename))

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Referer': 'https://www.instagram.com/',
    }

    try:
        # Strict Proxy Enforcement: Every request must go through a proxy
        proxies = proxy_manager.get_requests_proxy()
        if not proxies:
            # If no proxies, we don't want to leak IP, but we can't show image.
            # Return a placeholder or 403.
            abort(403, description="No active proxies available to fetch image.")

        resp = requests.get(url, headers=headers, proxies=proxies, timeout=10, stream=True)
        if resp.status_code == 200:
            proxy_manager.report_success(proxies.get('http'))
            return Response(
                stream_with_context(resp.iter_content(chunk_size=10240)),
                content_type=resp.headers.get('Content-Type')
            )
        
        proxy_manager.report_fail(proxies.get('http'))
        resp.raise_for_status()
    except Exception as e:
        # If proxy fails, don't redirect to direct URL (leaks IP)
        # Instead, return a 404 or a placeholder
        abort(404, description=f"Proxy failed to fetch image: {str(e)}")

@bp.route('/library')
def library():
    from sqlalchemy import func
    search_states = HashtagSearchState.query.order_by(HashtagSearchState.updated_at.desc()).all()
    # Optimized: Count in one go or per hashtag using SQL
    counts = {}
    for state in search_states:
        counts[state.hashtag] = db.session.query(func.count(Reel.id)).filter(Reel.hashtags.like(f"%{state.hashtag}%")).scalar()
    return render_template('library.html', search_states=search_states, counts=counts)


@bp.route('/library/refresh/<string:hashtag>', methods=['POST'])
def refresh_hashtag_group(hashtag: str):
    reels = Reel.query.filter(Reel.hashtags.like(f"%{hashtag}%")).all()
    for reel in reels:
        try:
            from .services import refresh_reel
            refresh_reel(reel)
            time.sleep(1.5) # Prevent 429 rate limit
        except:
            pass
    flash(f"Refreshed {len(reels)} reels for #{hashtag}.", "success")
    return redirect(url_for('main.library'))


@bp.route('/library/delete/<string:hashtag>', methods=['POST'])
def delete_hashtag_group(hashtag: str):
    # Delete the search state
    state = HashtagSearchState.query.filter_by(hashtag=hashtag).first()
    if state:
        db.session.delete(state)
    
    # Delete associated reels
    reels_deleted = Reel.query.filter(Reel.hashtags.like(f"%{hashtag}%")).delete(synchronize_session=False)
    
    db.session.commit()
    flash(f"Removed #{hashtag} and deleted {reels_deleted} associated reels from your library.", "success")
    return redirect(url_for('main.library'))


@bp.route('/instagram-session', methods=['GET', 'POST'])
def instagram_session():
    if request.method == 'POST':
        action = request.form.get('action', 'save')
        if action == 'clear':
            clear_instagram_session()
            flash('Instagram session removed from the app.', 'success')
            return redirect(url_for('main.instagram_session'))

        sessionid = request.form.get('sessionid', '').strip()
        if not sessionid:
            flash('Session ID is required to connect Instagram discovery.', 'warning')
            return redirect(url_for('main.instagram_session'))
        save_instagram_session(
            sessionid=sessionid,
            csrftoken=request.form.get('csrftoken'),
            ds_user_id=request.form.get('ds_user_id'),
            user_agent=request.form.get('user_agent'),
            is_active=True,
        )
        is_valid, message = validate_instagram_session()
        flash(message, 'success' if is_valid else 'warning')
        return redirect(url_for('main.instagram_session'))

    return render_template(
        'instagram_session.html',
        session_config=get_instagram_session(),
        instagram_connected=has_instagram_session(),
    )


@bp.route('/proxies', methods=['GET'])
def list_proxies():
    from .models import ProxyRecord
    proxies = ProxyRecord.query.order_by(ProxyRecord.group_name, ProxyRecord.created_at.desc()).all()
    
    # Group them for the UI
    grouped = {}
    for p in proxies:
        if p.group_name not in grouped:
            grouped[p.group_name] = []
        grouped[p.group_name].append(p)
        
    return render_template('proxies.html', grouped_proxies=grouped)


@bp.route('/proxies/add', methods=['POST'])
def add_proxies():
    from .models import ProxyRecord, db
    raw_data = request.form.get('proxy_list', '').strip()
    group_name = request.form.get('group_name', 'Default').strip() or 'Default'
    
    if not raw_data:
        flash('No proxy data provided.', 'warning')
        return redirect(url_for('main.list_proxies'))
        
    added = 0
    skipped = 0
    for line in raw_data.splitlines():
        line = line.strip()
        if not line: continue
        
        # Format normalization
        proxy_url = None
        parts = line.split(':')
        if len(parts) == 4:
            host, port, user, password = parts
            proxy_url = f"http://{user}:{password}@{host}:{port}"
        elif '@' in line:
            proxy_url = f"http://{line}" if not line.startswith('http') else line
        elif len(parts) == 2:
            host, port = parts
            proxy_url = f"http://{host}:{port}"
            
        if proxy_url:
            existing = ProxyRecord.query.filter_by(url=proxy_url).first()
            if not existing:
                db.session.add(ProxyRecord(url=proxy_url, group_name=group_name))
                added += 1
            else:
                skipped += 1
                
    db.session.commit()
    flash(f"Added {added} new proxies. (Skipped {skipped} duplicates)", "success")
    return redirect(url_for('main.list_proxies'))


@bp.route('/proxies/delete/<int:proxy_id>', methods=['POST'])
def delete_proxy(proxy_id):
    from .models import ProxyRecord, db
    proxy = ProxyRecord.query.get_or_404(proxy_id)
    db.session.delete(proxy)
    db.session.commit()
    return jsonify({'status': 'success'})


@bp.route('/proxies/toggle/<int:proxy_id>', methods=['POST'])
def toggle_proxy(proxy_id):
    from .models import ProxyRecord, db
    proxy = ProxyRecord.query.get_or_404(proxy_id)
    proxy.is_active = not proxy.is_active
    db.session.commit()
    return jsonify({'status': 'success', 'is_active': proxy.is_active})


@bp.route('/proxies/clear-failed', methods=['POST'])
def clear_failed_proxies():
    from .models import ProxyRecord, db
    # Either delete them or just reactivate them
    failed = ProxyRecord.query.filter(ProxyRecord.fail_count > 0).all()
    for p in failed:
        p.fail_count = 0
        p.is_active = True
    db.session.commit()
    flash(f"Reset {len(failed)} failed proxies to active status.", "success")
    return redirect(url_for('main.list_proxies'))


@bp.route('/proxies/delete-all', methods=['POST'])
def delete_all_proxies():
    from .models import ProxyRecord, db
    num_deleted = ProxyRecord.query.delete()
    db.session.commit()
    flash(f"Successfully deleted all {num_deleted} proxies.", "success")
    return redirect(url_for('main.list_proxies'))


@bp.route('/proxies/delete-group/<string:group_name>', methods=['POST'])
def delete_proxy_group(group_name):
    from .models import ProxyRecord, db
    num_deleted = ProxyRecord.query.filter_by(group_name=group_name).delete()
    db.session.commit()
    flash(f"Deleted {num_deleted} proxies from group '{group_name}'.", "success")
    return redirect(url_for('main.list_proxies'))


@bp.route('/discover', methods=['POST'])
def discover():
    if not has_instagram_session():
        flash('Connect an Instagram session first so hashtag discovery can use authenticated requests.', 'warning')
        return redirect(url_for('main.instagram_session'))
    hashtags = normalize_hashtags(request.form.get('hashtags', ''))
    if not hashtags:
        flash('Enter at least one hashtag to discover reels.', 'warning')
        return redirect(url_for('main.dashboard'))
    
    # Use depth=5 for initial search to get ~250 reels instead of just 50
    imported, errors, search_state = import_discovered_reels(hashtags, depth=5)
    
    for tag, state in search_state.items():
        record = _get_or_create_hashtag_state(tag)
        record.next_max_id = state.get('next_max_id')
        record.more_available = bool(state.get('more_available'))
        db.session.add(record)
    db.session.commit()
    if imported:
        flash(f'Imported {imported} reel URLs from hashtag discovery.', 'success')
    else:
        flash('No new reels were imported from the requested hashtags.', 'warning')
    for error in errors:
        flash(error, 'danger')
    return redirect(url_for('main.dashboard', hashtags=','.join(hashtags), active_tag=hashtags[0]))


@bp.route('/discover/more/<string:hashtag>', methods=['POST'])
def discover_more(hashtag: str):
    if not has_instagram_session():
        flash('Connect an Instagram session first so hashtag discovery can use authenticated requests.', 'warning')
        return redirect(url_for('main.instagram_session'))

    hashtag = normalize_hashtags(hashtag)
    if not hashtag:
        flash('Invalid hashtag.', 'warning')
        return redirect(url_for('main.dashboard'))
    hashtag = hashtag[0]

    tag_state = HashtagSearchState.query.filter_by(hashtag=hashtag).first()
    next_max_id = tag_state.next_max_id if tag_state else None
    if not tag_state or not tag_state.more_available:
        flash(f'No more saved results are available for #{hashtag} right now.', 'warning')
        return redirect(url_for('main.dashboard'))

    # Fetch 2 pages at a time when clicking "Discover More"
    imported, errors, search_state = import_discovered_reels([hashtag], max_id_by_tag={hashtag: next_max_id}, depth=2)
    
    if hashtag in search_state:
        state = search_state[hashtag]
        tag_state.next_max_id = state.get('next_max_id')
        tag_state.more_available = bool(state.get('more_available'))
        db.session.add(tag_state)
        db.session.commit()

    if imported:
        flash(f'Loaded more reels for #{hashtag}. Imported {imported} new URLs.', 'success')
    else:
        flash(f'Loaded another page for #{hashtag}, but all found reels were already tracked.', 'warning')
    for error in errors:
        flash(error, 'danger')
    active_hashtags = normalize_hashtags(request.form.get('active_hashtags', ''))
    if hashtag not in active_hashtags:
        active_hashtags.append(hashtag)
    return redirect(url_for('main.dashboard', hashtags=','.join(active_hashtags), active_tag=hashtag))


@bp.route('/reels/<int:reel_id>/stream')
def stream_reel_video(reel_id: int):
    reel = db.session.get(Reel, reel_id)
    if reel is None:
        abort(404)

    # 1. Try local storage first
    if reel.local_video_path:
        filename = reel.local_video_path.replace('media/', '')
        return redirect(url_for('main.serve_media', filename=filename))

    if not reel.video_url:
        abort(404)

    def get_upstream(url):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.instagram.com/',
            'Origin': 'https://www.instagram.com',
            'Sec-Fetch-Dest': 'video',
            'Sec-Fetch-Mode': 'no-cors',
            'Sec-Fetch-Site': 'cross-site',
            'Connection': 'keep-alive',
        }
        if request.headers.get('Range'):
            headers['Range'] = request.headers['Range']
        
        proxies = proxy_manager.get_requests_proxy()
        return requests.get(url, headers=headers, proxies=proxies, stream=True, timeout=15)

    try:
        upstream = get_upstream(reel.video_url)
        upstream.raise_for_status()
    except requests.RequestException as e:
        print(f"DEBUG: Initial stream attempt failed for {reel.shortcode}: {e}")
        # If the direct URL fails (403/410), refresh the reel to get a fresh signed URL
        from .services import refresh_reel
        old_url = reel.video_url
        refresh_reel(reel)
        
        # If the URL didn't change, we might be hitting a block or login wall on the public page
        if reel.video_url == old_url:
            print(f"DEBUG: Refresh failed to provide new URL for {reel.shortcode}")
            abort(403, description="Instagram blocked the request or the session is invalid. Try refreshing the session.")
            
        try:
            upstream = get_upstream(reel.video_url)
            upstream.raise_for_status()
        except requests.RequestException as e2:
            print(f"DEBUG: Second stream attempt failed for {reel.shortcode}: {e2}")
            abort(403, description=f"Could not stream video after refresh: {e2}")

    passthrough_headers = {
        'Content-Type': upstream.headers.get('Content-Type', 'video/mp4'),
        'Content-Length': upstream.headers.get('Content-Length'),
        'Accept-Ranges': upstream.headers.get('Accept-Ranges', 'bytes'),
        'Content-Range': upstream.headers.get('Content-Range'),
    }
    clean_headers = {k: v for k, v in passthrough_headers.items() if v}
    
    return Response(
        stream_with_context(upstream.iter_content(chunk_size=256 * 1024)),
        status=upstream.status_code,
        headers=clean_headers,
        direct_passthrough=True,
    )


def async_scroll_reels(app, username, max_id=None, media_type='all'):
    with app.app_context():
        from .models import db, Reel, HashtagSearchState, TaskNotification
        from .services import discover_reels_direct
        
        tag = f"creator:{username}"
        state = _get_or_create_hashtag_state(tag)
        
        state.status = 'scrolling'
        state.last_error = None
        db.session.commit()
        
        imported, errors, new_reels, next_max_id = discover_reels_direct(username, max_id=max_id, app_context=app, media_type_filter=media_type)
        
        state.next_max_id = next_max_id
        state.more_available = bool(next_max_id)
        
        if errors:
            state.status = 'error'
            state.last_error = "; ".join(errors)
        else:
            state.status = 'ready'
            state.last_error = None
            
        db.session.add(state)
        db.session.commit()
        
        # Notify completion
        msg = f"Finished scrolling @{username}. Found {imported} new reels."
        if errors:
            msg = f"Scroll for @{username} interrupted. Found {imported} reels."
            
        notif = TaskNotification(
            message=msg,
            action_url=f"/creator-search?active_creator={username}"
        )
        db.session.add(notif)
        db.session.commit()


@bp.route('/creator-search', methods=['GET', 'POST'])
def creator_search():
    from .models import CreatorStats
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        max_id = request.form.get('max_id') # For continuing a scroll
        media_type = request.form.get('media_type', 'all')
        
        if not username:
            flash('Enter an Instagram profile URL or username.', 'warning')
            return redirect(url_for('main.creator_search'))
        
        # Require an active Instagram session
        if not has_instagram_session():
            flash('Connect an Instagram session first so creator discovery can use authenticated requests.', 'warning')
            return redirect(url_for('main.instagram_session'))
        
        # Clean username
        clean_username = username.strip().strip('/').split('/')[-1].lstrip('@').split('?')[0]
        tag = f"creator:{clean_username}"
        
        # Check if already scrolling
        state = _get_or_create_hashtag_state(tag)
        if state.status == 'scrolling':
            flash(f'Already scrolling @{clean_username}. Please wait.', 'warning')
            return redirect(url_for('main.creator_search', active_creator=clean_username))
            
        # Start background scroll
        threading.Thread(
            target=async_scroll_reels, 
            args=(current_app._get_current_object(), clean_username, max_id, media_type)
        ).start()
        
        flash(f'Started human-like scroll for @{clean_username} in the background.', 'success')
        return redirect(url_for('main.creator_search', active_creator=clean_username))

    active_creator = request.args.get('active_creator', '')
    limit = request.args.get('limit', type=int) or 100
    sort_by = request.args.get('sort_by', 'views_desc')
    
    search_query = HashtagSearchState.query.filter(HashtagSearchState.hashtag.startswith('creator:')).order_by(HashtagSearchState.updated_at.desc())
    recent_searches = search_query.all()
    
    # Build detailed stats for the creator list
    creator_stats_list = []
    if not active_creator:
        for state in recent_searches:
            uname = state.hashtag.replace('creator:', '')
            base_query = Reel.query.filter(Reel.hashtags.like(f"%{state.hashtag}%"))
            total = base_query.count()
            
            processed = base_query.filter(Reel.enrichment_status != 'pending').count()
            progress = int((processed / max(total, 1)) * 100)
            
            from sqlalchemy import func
            total_views_res = db.session.query(func.sum(Reel.last_views)).filter(Reel.hashtags.like(f"%{state.hashtag}%")).scalar()
            total_views = int(total_views_res) if total_views_res else 0
            
            # Fetch profile info if available
            profile = CreatorStats.query.filter_by(username=uname).first()

            creator_stats_list.append({
                'username': uname,
                'total_reels': total,
                'processed_reels': processed,
                'progress': progress,
                'status': state.status,
                'total_views': total_views,
                'last_updated': state.updated_at,
                'next_max_id': state.next_max_id,
                'more_available': state.more_available,
                'profile_pic': profile.profile_pic_url if profile else None,
                'followers': profile.followers_count if profile else None,
                'last_error': state.last_error
            })

    reels = []
    has_more_local = False
    profile_data = None
    stats = {
        'reel_count': 0, 
        'processed_reels': 0, 
        'progress': 0, 
        'avg_views': 0, 
        'max_views': 0, 
        'playable_reels': 0, 
        'next_max_id': None, 
        'more_available': False,
        'status': 'ready',
        'last_error': None
    }
    
    if active_creator:
        tag = f"creator:{active_creator}"
        state = _get_or_create_hashtag_state(tag)
            
        profile_data = CreatorStats.query.filter_by(username=active_creator).first()
        
        # LOCAL-FIRST: Only fetch from internet if profile is missing or older than 24h
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        should_fetch_profile = not profile_data or not profile_data.updated_at or (now - profile_data.updated_at.replace(tzinfo=timezone.utc)) > timedelta(hours=24)
        
        if should_fetch_profile:
            print(f"DEBUG: Profile data missing or old for @{active_creator}. Fetching from IG...")
            from .services import get_user_info
            get_user_info(active_creator)
            profile_data = CreatorStats.query.filter_by(username=active_creator).first()
        else:
            print(f"DEBUG: Using local profile data for @{active_creator}")
        
        from sqlalchemy import func, or_
        base_query = Reel.query.filter(or_(Reel.source_hashtag == tag, Reel.hashtags.like(f"%{tag}%")))
        total_count = base_query.count()
        
        query = base_query
        
        if sort_by == 'views_desc':
            query = query.order_by(Reel.last_views.desc().nullslast())
        elif sort_by == 'views_asc':
            query = query.order_by(Reel.last_views.asc().nullslast())
        elif sort_by == 'newest':
            query = query.order_by(Reel.created_at.desc())
        elif sort_by == 'oldest':
            query = query.order_by(Reel.created_at.asc())
            
        reels = query.limit(limit).all()
        has_more_local = total_count > len(reels)
        
        processed_count = base_query.filter(Reel.enrichment_status != 'pending').count()
        
        total_views_res = db.session.query(func.sum(Reel.last_views)).filter(or_(Reel.source_hashtag == tag, Reel.hashtags.like(f"%{tag}%"))).scalar()
        total_views = int(total_views_res) if total_views_res else 0
        
        max_views_res = db.session.query(func.max(Reel.last_views)).filter(or_(Reel.source_hashtag == tag, Reel.hashtags.like(f"%{tag}%"))).scalar()
        max_views = int(max_views_res) if max_views_res else 0
        
        playable_count = base_query.filter(or_(Reel.video_url.isnot(None), Reel.shortcode.isnot(None))).count()
        
        stats['reel_count'] = total_count
        stats['processed_reels'] = len(reels) # Showing only processed
        stats['progress'] = int((processed_count / max(total_count, 1)) * 100)
        
        stats['avg_views'] = int(total_views / max(total_count, 1))
        stats['max_views'] = max_views
        stats['playable_reels'] = playable_count
        stats['status'] = state.status
        stats['last_error'] = state.last_error
        stats['next_max_id'] = state.next_max_id
        stats['more_available'] = state.more_available

    any_scrolling = any(c['status'] == 'scrolling' for c in creator_stats_list)

    return render_template(
        'creator_search.html',
        reels=reels,
        active_creator=active_creator,
        profile_data=profile_data,
        recent_searches=recent_searches,
        creator_stats_list=creator_stats_list,
        any_scrolling=any_scrolling,
        stats=stats,
        limit=limit,
        sort_by=sort_by,
        has_more_local=has_more_local
    )


@bp.route('/reels/<int:reel_id>/download')
def download_reel(reel_id: int):
    reel = db.session.get(Reel, reel_id)
    if reel is None or not reel.video_url:
        abort(404)

    if reel.local_video_path:
        filename = reel.local_video_path.replace('media/', '')
        return send_from_directory(
            os.path.join(current_app.instance_path, 'media'),
            filename,
            as_attachment=True,
            download_name=f"{reel.creator or 'instagram'}_{reel.shortcode or reel_id}.mp4"
        )

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Referer': 'https://www.instagram.com/',
    }
    
    try:
        proxies = proxy_manager.get_requests_proxy()
        resp = requests.get(reel.video_url, headers=headers, proxies=proxies, stream=True, timeout=15)
        resp.raise_for_status()
        
        filename = f"{reel.creator or 'instagram'}_{reel.shortcode or reel_id}.mp4"
        
        return Response(
            stream_with_context(resp.iter_content(chunk_size=1024 * 1024)),
            content_type=resp.headers.get('Content-Type', 'video/mp4'),
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Length": resp.headers.get('Content-Length')
            }
        )
    except:
        # If the direct URL fails, try to refresh once
        from .services import refresh_reel
        refresh_reel(reel)
        try:
            proxies = proxy_manager.get_requests_proxy()
            resp = requests.get(reel.video_url, headers=headers, proxies=proxies, stream=True, timeout=15)
            resp.raise_for_status()
            filename = f"{reel.creator or 'instagram'}_{reel.shortcode or reel_id}.mp4"
            return Response(
                stream_with_context(resp.iter_content(chunk_size=1024 * 1024)),
                content_type=resp.headers.get('Content-Type', 'video/mp4'),
                headers={
                    "Content-Disposition": f"attachment; filename={filename}"
                }
            )
        except:
            abort(403)


@bp.route('/reels/<int:reel_id>', methods=['GET', 'POST'])
def reel_detail(reel_id: int):
    reel = db.session.get(Reel, reel_id)
    if reel is None:
        abort(404)
    if request.method == 'POST':
        reel.creator = request.form.get('creator') or None
        reel.niche = request.form.get('niche') or None
        reel.hashtags = ', '.join(normalize_hashtags(request.form.get('hashtags', '')))
        reel.hook = request.form.get('hook') or None
        reel.cta = request.form.get('cta') or None
        reel.format = request.form.get('format') or None
        reel.notes = request.form.get('notes') or None
        db.session.add(reel)
        db.session.commit()
        enrich_reel(
            reel,
            {
                'views': _to_int('last_views'),
                'likes': _to_int('last_likes'),
                'comments': _to_int('last_comments'),
            },
        )
        flash('Reel updated.', 'success')
        return redirect(url_for('main.reel_detail', reel_id=reel.id))
    chart_points = build_chart_points(reel.snapshots)
    return render_template('reel_detail.html', reel=reel, chart_points=chart_points)


@bp.route('/reels/<int:reel_id>/refresh', methods=['POST'])
def refresh_one(reel_id: int):
    reel = db.session.get(Reel, reel_id)
    if reel is None:
        abort(404)
    refresh_reel(reel)
    flash('Reel refreshed from the public page when available.', 'success')
    return redirect(url_for('main.reel_detail', reel_id=reel.id))


@bp.route('/refresh-all', methods=['POST'])
def refresh_all():
    refreshed, errors = refresh_all_reels()
    flash(f'Refreshed {refreshed} tracked reels.', 'success')
    for error in errors:
        flash(error, 'danger')
    return redirect(url_for('main.dashboard'))


@bp.route('/insights')
def insights():
    # Only pull the hashtags column to save memory
    all_hashtags = db.session.query(Reel.hashtags).filter(Reel.hashtags.isnot(None), Reel.hashtags != '').all()
    tag_counts = {}
    for (tags_str,) in all_hashtags:
        for tag in dict.fromkeys([t.strip() for t in tags_str.split(',') if t.strip()]):
            tag_counts[tag] = tag_counts.get(tag, 0) + 1
            
    top_tags = sorted(tag_counts.items(), key=lambda item: item[1], reverse=True)[:10]
    
    # Only pull top 5 reels for idea generation
    top_viral_reels = Reel.query.order_by(Reel.viral_score.desc().nullslast()).limit(5).all()
    idea_prompts = []
    for reel in top_viral_reels:
        parts = [part for part in [reel.niche, reel.hook, reel.cta, reel.format] if part]
        if parts:
            idea_prompts.append(' / '.join(parts))
            
    return render_template('insights.html', reels=top_viral_reels, top_tags=top_tags, idea_prompts=idea_prompts)


def async_enrich_reels(app, reel_ids, keyword):
    with app.app_context():
        from .models import db, Reel, TaskNotification
        from .services import refresh_reel
        success_count = 0
        for rid in reel_ids:
            reel = db.session.get(Reel, rid)
            if reel and reel.enrichment_status != 'ok':
                try:
                    refresh_reel(reel)
                    success_count += 1
                    # Pause to avoid Instagram 429 Too Many Requests
                    time.sleep(2.5)
                except Exception:
                    pass
        
        # Create a notification when finished
        if keyword.startswith('@'):
            clean_name = keyword.lstrip('@')
            action_url = f"/creator-search?active_creator={clean_name}"
        else:
            action_url = f"/web-search?active_keyword={keyword}"

        notif = TaskNotification(
            message=f"Background fetch complete for '{keyword}'. {success_count} reels analyzed.",
            action_url=action_url
        )
        db.session.add(notif)
        db.session.commit()

@bp.route('/api/notifications')
def get_notifications():
    notifications = TaskNotification.query.filter_by(is_read=False).order_by(TaskNotification.created_at.asc()).all()
    results = []
    for notif in notifications:
        results.append({
            'id': notif.id,
            'message': notif.message,
            'action_url': notif.action_url
        })
        notif.is_read = True
    
    if notifications:
        db.session.commit()
        
    return jsonify(results)

@bp.route('/api/cancel-creator-search/<string:username>')
def cancel_creator_search(username: str):
    from .models import HashtagSearchState
    tag = f"creator:{username}"
    state = HashtagSearchState.query.filter_by(hashtag=tag).first()
    if state and state.status == 'scrolling':
        state.status = 'cancelled'
        db.session.commit()
        return jsonify({'status': 'success', 'message': 'Search cancellation requested.'})
    return jsonify({'status': 'error', 'message': 'Search not in progress.'}), 400


@bp.route('/api/creator-status/<string:username>')
def get_creator_status(username: str):
    from .models import HashtagSearchState, Reel, CreatorStats
    tag = f"creator:{username}"
    state = HashtagSearchState.query.filter_by(hashtag=tag).first()
    if not state:
        return jsonify({'error': 'Not found'}), 404
    
    all_group_reels = Reel.query.filter(Reel.hashtags.like(f"%{tag}%")).all()
    total = len(all_group_reels)
    processed = sum(1 for r in all_group_reels if r.enrichment_status != 'pending')
    progress = int((processed / max(total, 1)) * 100)
    
    profile = CreatorStats.query.filter_by(username=username).first()
    
    return jsonify({
        'username': username,
        'status': state.status,
        'progress': progress,
        'total_reels': total,
        'processed_reels': processed,
        'more_available': state.more_available,
        'next_max_id': state.next_max_id,
        'last_error': state.last_error,
        'followers': profile.followers_count if profile else None,
        'profile_pic': profile.profile_pic_url if profile else None
    })


@bp.route('/web-search', methods=['GET', 'POST'])
def web_search():
    if request.method == 'POST':
        keyword = request.form.get('keyword', '').strip()
        limit = request.form.get('limit', type=int) or 50
        if not keyword:
            flash('Enter a keyword to search.', 'warning')
            return redirect(url_for('main.web_search'))
        
        imported, errors, new_reels, _ = discover_reels_from_web(keyword, limit=limit)
        
        if new_reels:
            # Kick off automatic enrichment in the background
            reel_ids = [r.id for r in new_reels]
            threading.Thread(target=async_enrich_reels, args=(current_app._get_current_object(), reel_ids, keyword)).start()
            flash(f'Imported {imported} URLs. The app is now automatically fetching their data in the background (we will notify you when it is complete).', 'success')
        elif imported:
            flash(f'Imported {imported} reel URLs from web search.', 'success')
        else:
            flash('No new reels were imported from the web search.', 'warning')
            
        for error in errors:
            flash(error, 'danger')
            
        return redirect(url_for('main.web_search', active_keyword=keyword))

    # GET Request
    active_keyword = request.args.get('active_keyword', '')
    limit = request.args.get('limit', type=int) or 20
    sort_by = request.args.get('sort_by', 'views_desc')
    
    # We prefix web searches with "web:" to separate them, or we just look for source_hashtag matching the keyword if they are the same.
    # We used tag = f"web:{keyword}" in services.py
    search_query = HashtagSearchState.query.filter(HashtagSearchState.hashtag.startswith('web:')).order_by(HashtagSearchState.updated_at.desc())
    recent_searches = search_query.limit(8).all()
    
    reels = []
    has_more_local = False
    stats = {'reel_count': 0, 'avg_views': 0, 'max_views': 0, 'playable_reels': 0}
    
    if active_keyword:
        tag = f"web:{active_keyword}"
        query = Reel.query.filter(Reel.source_hashtag == tag)
        
        if sort_by == 'views_desc':
            query = query.order_by(Reel.last_views.desc().nullslast())
        elif sort_by == 'views_asc':
            query = query.order_by(Reel.last_views.asc().nullslast())
        elif sort_by == 'newest':
            query = query.order_by(Reel.created_at.desc())
        elif sort_by == 'oldest':
            query = query.order_by(Reel.created_at.asc())
            
        reels = query.limit(limit).all()
        has_more_local = query.count() > len(reels)
        
        all_group_reels = Reel.query.filter(Reel.source_hashtag == tag).all()
        stats['reel_count'] = len(all_group_reels)
        stats['avg_views'] = int(sum((r.last_views or 0) for r in all_group_reels) / len(all_group_reels)) if all_group_reels else 0
        stats['max_views'] = max((r.last_views or 0) for r in all_group_reels) if all_group_reels else 0
        stats['playable_reels'] = sum(1 for r in all_group_reels if r.playable_url)

        # Ensure search state exists
        state = _get_or_create_hashtag_state(tag)

    return render_template(
        'web_search.html',
        reels=reels,
        active_keyword=active_keyword,
        recent_searches=recent_searches,
        stats=stats,
        limit=limit,
        sort_by=sort_by,
        has_more_local=has_more_local
    )
