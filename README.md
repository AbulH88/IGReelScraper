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

If port `5000` is already busy, the script automatically moves to the next free port and prints the URL.

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

## How the Data Fetching Works

Instagram fights very hard to stop bots from downloading their data, so the app uses a combination of clever tricks to get the info. 

### 1. Bypassing the Login Wall (The "Session" Trick)
If a bot just asks Instagram for a page, Instagram immediately blocks it and shows a "Log In to Continue" screen. 
* To fix this, you provide your `sessionid` in the **Instagram Session** page.
* When the app requests data from Instagram, it attaches your `sessionid` to the request as a "Cookie". 
* Instagram's servers look at the request, see your Cookie, and think it's a real user browsing the web, bypassing the login wall.

### 2. How it "Reads" the Data (Scraping & Open Graph)
Once the app gets past the login wall and loads the Reel's webpage, it doesn't look at the page like a human does. It looks at the hidden **HTML Source Code**.
* **Thumbnails & Video Links:** When you share a link on iMessage or WhatsApp, a preview card pops up. Instagram builds these using hidden `<meta>` tags called "Open Graph" (e.g., `og:image` and `og:video`). The app specifically hunts for these hidden tags to grab the raw MP4 video file and the high-quality JPG thumbnail.
* **Views, Likes, and Comments:** Instagram hides the exact numbers inside massive blocks of JavaScript on the page. The app uses **Regex** (Regular Expressions) to scan thousands of lines of code in a fraction of a second, hunting for specific patterns like `"viewCount":1500000` or text that says `"1.5M views"`.

### 3. How it Finds Reels in the First Place (The APIs)
Depending on how you search, it uses two different methods:
* **Hashtag Search:** It secretly talks to Instagram's hidden internal API (`/api/v1/tags/web_info/`). This is the exact same API the mobile app uses when you click a hashtag. It returns a neat JSON dictionary containing the top reels for that tag.
* **Creator Search (Direct):** For specific profiles, the app uses the **User Clips API**. It mimics a real person scrolling through a profile's Reels tab. This method is incredibly fast because it fetches the thumbnails, views, and likes for the entire batch in a single request.
* **Web Search (Fallback):** If Instagram blocks the direct APIs, the app uses DuckDuckGo to find public Reel links as a safety backup. 

### 4. How the Video Player Works (The Proxy)
Instagram CDN (Content Delivery Network) has "Hotlinking Protection." If you try to put an Instagram MP4 link directly into your own website, Instagram blocks it with a `403 Forbidden` error because it knows the video isn't being played on Instagram.com.
* **The Fix:** The app uses a **Proxy**. When you click "Watch," your browser asks the Python app for the video. The Python app adds a header saying `Referer: https://www.instagram.com/`, asks Instagram for the video, and then smoothly hands the video chunks back to you. Instagram thinks the video is being watched on their own site!
