from app import create_app
from app import routes
from app.models import HashtagSearchState, InstagramSession, Reel
from app import services


def make_app():
    app = create_app(
        {
            'TESTING': True,
            'SQLALCHEMY_DATABASE_URI': 'sqlite:///:memory:',
            'WTF_CSRF_ENABLED': False,
        }
    )
    return app


def test_dashboard_renders():
    app = make_app()
    client = app.test_client()
    response = client.get('/')
    assert response.status_code == 200
    assert b'Discover reels by hashtag' in response.data


def test_instagram_session_can_be_saved():
    app = make_app()
    client = app.test_client()
    original = routes.validate_instagram_session
    routes.validate_instagram_session = lambda: (True, 'Instagram session looks usable for authenticated discovery.')

    response = client.post(
        '/instagram-session',
        data={
            'sessionid': 'session-value',
            'csrftoken': 'csrf-value',
            'ds_user_id': '1234',
        },
        follow_redirects=True,
    )
    routes.validate_instagram_session = original
    assert response.status_code == 200
    with app.app_context():
        session_config = InstagramSession.query.one()
        assert session_config.sessionid == 'session-value'
        assert session_config.is_active is True


def test_discover_redirects_without_session():
    app = make_app()
    client = app.test_client()
    response = client.post(
        '/discover',
        data={'hashtags': 'fitness'},
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert b'Connect an Instagram session first' in response.data


def test_discover_imports_reels_from_authenticated_json():
    app = make_app()
    client = app.test_client()
    with app.app_context():
        services.save_instagram_session('session-value', 'csrf-value', '1234')

    original = services._instagram_api_get
    services._instagram_api_get = lambda *_args, **_kwargs: {
        'status': 'ok',
        'data': {
            'name': 'fitness',
            'top': {
                'sections': [
                    {
                        'layout_content': {
                            'one_by_two_item': {
                                'clips': {
                                    'items': [
                                        {
                                            'media': {
                                                'code': 'ABC123',
                                                'play_count': 51000,
                                                'like_count': 1200,
                                                'comment_count': 45,
                                                'video_versions': [{'url': 'https://cdn.example/video.mp4'}],
                                                'caption': {'text': 'Strong hook #fitness'},
                                                'user': {'username': 'creator_one'},
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    }
                ]
            },
            'recent': {'sections': []},
        },
    }
    response = client.post(
        '/discover',
        data={'hashtags': 'fitness'},
        follow_redirects=True,
    )
    services._instagram_api_get = original
    assert response.status_code == 200
    assert b'#fitness' in response.data
    with app.app_context():
        reel = Reel.query.one()
        assert reel.url.endswith('/ABC123/')
        assert reel.last_views == 51000
        assert reel.creator == 'creator_one'
        assert reel.video_url == 'https://cdn.example/video.mp4'


def test_discover_more_uses_saved_page_state():
    app = make_app()
    client = app.test_client()
    with app.app_context():
        services.save_instagram_session('session-value', 'csrf-value', '1234')
        routes.db.session.add(HashtagSearchState(hashtag='fitness', page=1, next_page=2, more_available=True))
        routes.db.session.commit()

    original = services._instagram_api_get

    def fake_api(url, **_kwargs):
        if 'page=2' in url:
            return {
                'status': 'ok',
                'data': {
                    'name': 'fitness',
                    'top': {
                        'more_available': False,
                        'sections': [
                            {
                                'layout_content': {
                                    'fill_items': [
                                        {
                                            'media': {
                                                'code': 'PAGE2',
                                                'play_count': 120000,
                                                'like_count': 3000,
                                                'comment_count': 88,
                                                'caption': {'text': 'Second page reel'},
                                                'user': {'username': 'creator_two'},
                                            }
                                        }
                                    ]
                                }
                            }
                        ],
                    },
                    'recent': {'sections': []},
                },
            }
        raise AssertionError('unexpected page request')

    services._instagram_api_get = fake_api
    response = client.post('/discover/more/fitness', follow_redirects=True)
    services._instagram_api_get = original
    assert response.status_code == 200
    with app.app_context():
        reel = Reel.query.one()
        assert reel.url.endswith('/PAGE2/')
        assert reel.last_views == 120000
        state = HashtagSearchState.query.filter_by(hashtag='fitness').one()
        assert state.page == 2


def test_dashboard_shows_saved_search_state():
    app = make_app()
    client = app.test_client()
    with app.app_context():
        routes.db.session.add(HashtagSearchState(hashtag='fitness', page=2, next_page=3, more_available=True))
        routes.db.session.commit()
    response = client.get('/')
    assert response.status_code == 200
    assert b'Current page 2' in response.data
    assert b'Load page 3' in response.data
    assert b'Page 1' in response.data
    assert b'Page 2' in response.data
    assert b'Open grouped view' in response.data


def test_dashboard_groups_results_by_selected_hashtags():
    app = make_app()
    client = app.test_client()
    with app.app_context():
        routes.db.session.add_all(
            [
                Reel(url='https://www.instagram.com/reel/fit-one/', shortcode='fit-one', source_hashtag='fitness', title='Fitness Reel'),
                Reel(url='https://www.instagram.com/reel/biz-one/', shortcode='biz-one', source_hashtag='business', title='Business Reel'),
                Reel(url='https://www.instagram.com/reel/old-one/', shortcode='old-one', source_hashtag='oldtag', title='Old Reel'),
            ]
        )
        routes.db.session.add_all(
            [
                HashtagSearchState(hashtag='fitness', page=1, next_page=2, more_available=True),
                HashtagSearchState(hashtag='business', page=1, next_page=2, more_available=True),
                HashtagSearchState(hashtag='oldtag', page=1, next_page=2, more_available=True),
            ]
        )
        routes.db.session.commit()
    response = client.get('/?hashtags=fitness,business')
    assert response.status_code == 200
    assert b'#fitness' in response.data
    assert b'#business' in response.data
    assert b'Fitness Reel' in response.data
    assert b'Business Reel' not in response.data
    assert b'Old Reel' not in response.data
    assert b'Clear search' in response.data


def test_dashboard_tab_switches_visible_group():
    app = make_app()
    client = app.test_client()
    with app.app_context():
        routes.db.session.add_all(
            [
                Reel(url='https://www.instagram.com/reel/fit-two/', shortcode='fit-two', source_hashtag='fitness', title='Fitness Tab Reel'),
                Reel(url='https://www.instagram.com/reel/biz-two/', shortcode='biz-two', source_hashtag='business', title='Business Tab Reel'),
            ]
        )
        routes.db.session.add_all(
            [
                HashtagSearchState(hashtag='fitness', page=1, next_page=2, more_available=True),
                HashtagSearchState(hashtag='business', page=1, next_page=2, more_available=True),
            ]
        )
        routes.db.session.commit()
    response = client.get('/?hashtags=fitness,business&active_tag=business')
    assert response.status_code == 200
    assert b'Business Tab Reel' in response.data
    assert b'Fitness Tab Reel' not in response.data


def test_clear_search_hides_reel_results():
    app = make_app()
    client = app.test_client()
    with app.app_context():
        routes.db.session.add(Reel(url='https://www.instagram.com/reel/clear-one/', shortcode='clear-one', source_hashtag='fitness', title='Clear Me'))
        routes.db.session.add(HashtagSearchState(hashtag='fitness', page=1, next_page=2, more_available=True))
        routes.db.session.commit()
    response = client.get('/?clear=1')
    assert response.status_code == 200
    assert b'Clear Me' not in response.data
    assert b'Search cleared.' in response.data
    assert b'Open grouped view' in response.data


def test_dashboard_bootstraps_state_from_existing_reels():
    app = make_app()
    client = app.test_client()
    with app.app_context():
        routes.db.session.add(Reel(url='https://www.instagram.com/reel/one/', shortcode='one', source_hashtag='egirl'))
        routes.db.session.commit()
    response = client.get('/')
    assert response.status_code == 200
    assert b'Load page 2' in response.data
    with app.app_context():
        state = HashtagSearchState.query.filter_by(hashtag='egirl').one()
        assert state.page == 1
        assert state.next_page == 2


def test_stream_route_uses_proxy_video():
    app = make_app()
    client = app.test_client()
    with app.app_context():
        reel = Reel(url='https://www.instagram.com/reel/test-one/', shortcode='test-one', video_url='https://cdn.example/video.mp4')
        routes.db.session.add(reel)
        routes.db.session.commit()

    class DummyStreamResponse:
        status_code = 200
        headers = {'Content-Type': 'video/mp4', 'Content-Length': '4', 'Accept-Ranges': 'bytes'}

        def raise_for_status(self):
            return None

        def iter_content(self, chunk_size=1024):
            yield b'test'

    original_get = routes.requests.get
    routes.requests.get = lambda *args, **kwargs: DummyStreamResponse()
    response = client.get('/reels/1/stream')
    routes.requests.get = original_get
    assert response.status_code == 200
    assert response.data == b'test'


def test_manual_reel_creation():
    app = make_app()
    client = app.test_client()

    class DummyResponse:
        text = '<html><head><meta property="og:title" content="Test Creator on Instagram: &quot;Example&quot;" /></head><body>1,000 views 100 likes 10 comments</body></html>'

    def fake_public_get(_url):
        return DummyResponse()

    original = services._public_get
    services._public_get = fake_public_get
    response = client.post(
        '/reels/new',
        data={
            'url': 'https://www.instagram.com/reel/test-one/',
            'hashtags': 'fitness, growth',
            'last_views': '1000',
            'last_likes': '100',
            'last_comments': '10',
        },
        follow_redirects=True,
    )
    services._public_get = original
    assert response.status_code == 200
    with app.app_context():
        reel = Reel.query.one()
        assert reel.last_views == 1000
        assert reel.snapshots[0].views == 1000
