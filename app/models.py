from datetime import datetime, timezone

from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


def utcnow():
    return datetime.now(timezone.utc)


class Reel(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    source_hashtag = db.Column(db.String(120), index=True)
    discovery_page = db.Column(db.Integer, default=1)
    url = db.Column(db.String(500), unique=True, nullable=False)
    shortcode = db.Column(db.String(120), index=True)
    thumbnail_url = db.Column(db.Text)
    video_url = db.Column(db.Text)
    title = db.Column(db.String(255))
    creator = db.Column(db.String(120), index=True)
    niche = db.Column(db.String(120), index=True)
    caption = db.Column(db.Text)
    hashtags = db.Column(db.Text)
    hook = db.Column(db.String(255))
    cta = db.Column(db.String(120))
    format = db.Column(db.String(120))
    notes = db.Column(db.Text)
    last_views = db.Column(db.Integer)
    last_likes = db.Column(db.Integer)
    last_comments = db.Column(db.Integer)
    viral_score = db.Column(db.Float, default=0)
    enrichment_status = db.Column(db.String(40), default='pending')
    last_error = db.Column(db.Text)
    discovered_at = db.Column(db.DateTime(timezone=True), default=utcnow, nullable=False)
    last_checked_at = db.Column(db.DateTime(timezone=True))
    created_at = db.Column(db.DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at = db.Column(db.DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    snapshots = db.relationship(
        'ReelSnapshot',
        back_populates='reel',
        cascade='all, delete-orphan',
        order_by='ReelSnapshot.captured_at.asc()',
    )

    @property
    def hashtag_list(self):
        if not self.hashtags:
            return []
        return [tag.strip() for tag in self.hashtags.split(',') if tag.strip()]

    @property
    def embed_url(self):
        if not self.shortcode:
            return None
        return f"https://www.instagram.com/reel/{self.shortcode}/embed/captioned/"

    @property
    def playable_url(self):
        return self.video_url or self.embed_url


class ReelSnapshot(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    reel_id = db.Column(db.Integer, db.ForeignKey('reel.id'), nullable=False, index=True)
    views = db.Column(db.Integer)
    likes = db.Column(db.Integer)
    comments = db.Column(db.Integer)
    captured_at = db.Column(db.DateTime(timezone=True), default=utcnow, nullable=False, index=True)
    source = db.Column(db.String(40), default='public-page')

    reel = db.relationship('Reel', back_populates='snapshots')


class InstagramSession(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sessionid = db.Column(db.Text)
    csrftoken = db.Column(db.String(255))
    ds_user_id = db.Column(db.String(120))
    user_agent = db.Column(db.String(255))
    is_active = db.Column(db.Boolean, default=False, nullable=False)
    updated_at = db.Column(db.DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)


class HashtagSearchState(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    hashtag = db.Column(db.String(120), unique=True, nullable=False, index=True)
    page = db.Column(db.Integer, default=1, nullable=False)
    next_page = db.Column(db.Integer)
    next_max_id = db.Column(db.Text)
    more_available = db.Column(db.Boolean, default=False, nullable=False)
    status = db.Column(db.String(20), default='ready') # scrolling, done, error
    last_error = db.Column(db.Text)
    updated_at = db.Column(db.DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)


class TaskNotification(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    message = db.Column(db.String(255), nullable=False)
    action_url = db.Column(db.String(500))
    is_read = db.Column(db.Boolean, default=False, nullable=False)
    created_at = db.Column(db.DateTime(timezone=True), default=utcnow, nullable=False)
