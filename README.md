# IGReelScraper

Local Flask app for discovering Instagram reels by hashtag, saving them for analysis, and browsing them in a grouped dashboard.

## Features

- Search one or more hashtags with an authenticated Instagram session
- Save discovered reels into a local SQLite database
- Group results by hashtag with tabbed browsing and page loading
- Play saved reels inside the app through a same-origin video proxy
- Track visible metrics like views, likes, and comments
- Review reel details and simple insights pages

## Requirements

- Python 3
- A virtual environment in `.venv`
- Packages from `requirements.txt`
- Instagram session cookies for authenticated hashtag discovery

## Setup

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

## Run the app

Use the startup script:

```bash
./start.sh
```

Default URL:

```text
http://127.0.0.1:5000
```

Optional custom port:

```bash
PORT=5050 ./start.sh
```

## Instagram session

Open the app, then go to the Instagram session page and enter:

- `sessionid`
- `csrftoken`
- `ds_user_id`
- optional `user_agent`

These values are stored locally for hashtag discovery requests.

## Tests

```bash
. .venv/bin/activate
pytest -q
```

## Project structure

- `app/` Flask application package
- `tests/` test suite
- `start.sh` local startup script
- `instance/` local SQLite database files

