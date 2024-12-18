from datetime import datetime, timedelta
import sqlite3
from time import sleep
import jwt
import requests
from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message, CAR, models
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Configuration from .env
BLUESKY_HANDLE = os.getenv('BLUESKY_HANDLE')
APP_PASSWORD = os.getenv('APP_PASSWORD')
BSKY_API_BASE = os.getenv('BSKY_API_BASE')

# Global variables
access_token = None
token_expiry = None  # Tracks the expiry of the current access token


def initialize_db():
    """Create SQLite database and table if not exists"""
    conn = sqlite3.connect('bluesky_replies.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS reply_log (
            repo TEXT PRIMARY KEY,
            last_reply_timestamp DATETIME
        )
    ''')
    conn.commit()
    conn.close()

def update_reply_log(repo):
    """Update or insert reply timestamp for a repo"""
    conn = sqlite3.connect('bluesky_replies.db')
    cursor = conn.cursor()

    cursor.execute('''
        INSERT OR REPLACE INTO reply_log (repo, last_reply_timestamp) 
        VALUES (?, ?)
    ''', (repo, datetime.now().isoformat()))

    conn.commit()
    conn.close()

def should_send_reply(repo):
    """Check if a reply can be sent based on last reply timestamp"""
    conn = sqlite3.connect('bluesky_replies.db')
    cursor = conn.cursor()

    cursor.execute('SELECT last_reply_timestamp FROM reply_log WHERE repo = ?', (repo,))
    result = cursor.fetchone()

    current_time = datetime.now()
    can_reply = False

    if result is None:
        # No previous reply for this repo
        can_reply = True
    else:
        last_reply_time = datetime.fromisoformat(result[0])
        if current_time - last_reply_time > timedelta(days=7):
            can_reply = True

    conn.close()
    return can_reply


def get_access_token(handle, app_password):
    """Obtain an access token for authentication, handling rate limits"""
    global access_token, token_expiry
    auth_url = f'{BSKY_API_BASE}/com.atproto.server.createSession'

    while True:
        try:
            response = requests.post(auth_url, json={
                'identifier': handle,
                'password': app_password
            })

            # Check if we're being rate limited
            if response.status_code == 429:  # HTTP 429 Too Many Requests
                reset_time = response.headers.get('RateLimit-Reset')
                limit = response.headers.get('RateLimit-Limit')
                remaining = response.headers.get('RateLimit-Remaining')

                print(f"Rate limited! Limit: {limit}, Remaining: {remaining}, Reset at: {reset_time}")

                # Wait until the rate limit resets
                if reset_time:
                    wait_time = max(0, int(reset_time) - int(datetime.utcnow().timestamp()))
                    print(f"Waiting for {wait_time} seconds...")
                    sleep(wait_time)
                else:
                    print("No RateLimit-Reset header found. Waiting for 60 seconds...")
                    sleep(60)
                continue

            # Raise other HTTP errors if any
            response.raise_for_status()

            # Parse response and store token
            data = response.json()
            access_token = data['accessJwt']

            # Decode the token to calculate expiration time
            decoded = jwt.decode(access_token, options={"verify_signature": False})
            expiry_timestamp = decoded.get('exp', 0)
            token_expiry = datetime.utcfromtimestamp(expiry_timestamp)

            return access_token

        except requests.RequestException as e:
            print(f"Authentication error: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response content: {e.response.text}")
            raise

def ensure_valid_token():
    """Ensure a valid access token is available, refreshing it if expired."""
    global access_token, token_expiry
    if access_token is None or token_expiry is None or datetime.utcnow() >= token_expiry:
        print("Access token expired or unavailable, refreshing...")
        get_access_token(BLUESKY_HANDLE, APP_PASSWORD)


def create_reply(handle, post_uri, post_cid):
    """Create a reply post using the Bluesky API"""
    ensure_valid_token()  # Ensure we have a valid token before proceeding

    create_url = f'{BSKY_API_BASE}/com.atproto.repo.createRecord'
    created_at = datetime.utcnow().isoformat() + 'Z'

    reply_record = {
        "$type": "app.bsky.feed.post",
        "text": "Thank you for using BlueSky but can you please add alt text to your images so that everyone can enjoy your posts.\n\n If this image is just decorative or the description is in the text, please accept my apologises, I'm just a bot that scans the firehose for missing alt text.",
        "reply": {
            "root": {
                "uri": post_uri,
                "cid": str(post_cid)
            },
            "parent": {
                "uri": post_uri,
                "cid": str(post_cid)
            }
        },
        "facets": [
            {
                "index": {
                    "byteStart": 46,
                    "byteEnd": 59
                },
                "features": [
                    {
                        "$type": "app.bsky.richtext.facet#link",
                        "uri": "https://later.com/blog/alt-text/"
                    }
                ]
            }
        ],
        "createdAt": created_at
    }

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    try:
        sleep(5)
        response = requests.post(create_url,
                                 headers=headers,
                                 json={
                                     'repo': handle,
                                     'collection': 'app.bsky.feed.post',
                                     'record': reply_record
                                 })

        reset_time = response.headers.get('RateLimit-Reset')
        limit = response.headers.get('RateLimit-Limit')
        remaining = response.headers.get('RateLimit-Remaining')
        print(f"Rate limiting details for reply's: {limit}, Remaining: {remaining}, Reset at: {reset_time}")

        if response.status_code == 429:  # HTTP 429 Too Many Requests
            if reset_time:
                wait_time = max(0, int(reset_time) - int(datetime.utcnow().timestamp()))
                print(f"RATE LIMITED - Waiting for {wait_time} seconds...")
                sleep(wait_time)
            else:
                print("No RateLimit-Reset header found. Waiting for 60 seconds...")
                sleep(60)

        if response.status_code >= 400:
            print(f"Error response: {response.status_code}")
            print(f"Error content: {response.text}")

        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error creating reply: {e}")
        print(f"Response content: {e.response.text if hasattr(e, 'response') else 'No response'}")
        raise


client = FirehoseSubscribeReposClient()


def on_message_handler(message) -> None:
    commit = parse_subscribe_repos_message(message)
    if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
        return

    if not commit.blocks:
        return

    car = CAR.from_bytes(commit.blocks)
    for op in commit.ops:
        if op.action in ["create"] and op.cid:
            try:
                raw = car.blocks.get(op.cid)
                skeet = raw

                if skeet.get("$type") == "app.bsky.feed.post":
                    embed = skeet.get("embed")

                    if embed:
                        embed_type = embed.get("$type")

                        if embed_type == "app.bsky.embed.images":
                            images = embed.get("images", [])
                            for image in images:
                                if image.get('alt') == '':
                                    post_cid = op.cid
                                    post_uri = f"at://{commit.repo}/{op.path}"
                                    tell_off(post_uri, post_cid, commit.repo)

            except Exception as e:
                print(f"Error processing message: {e}")


def tell_off(post_uri, post_cid, repo):
    """Send a reply to a post without alt text"""
    try:
        if should_send_reply(repo):
            reply = create_reply(BLUESKY_HANDLE, post_uri, post_cid)
            # Update the reply log after successful reply
            update_reply_log(repo)
    except Exception as e:
        print(f"Error replying to post: {e}")


def on_error_handler(error):
    print(f"Error in firehose: {error}")


def main():

    initialize_db()
    while True:
        try:
            # Start the firehose client
            client.start(on_message_handler)
        except Exception as e:
            print(f"Error replying to post: {e}")
        sleep(10)   # Give us 10 seconds for whatever happened to pass.


if __name__ == "__main__":
    main()
