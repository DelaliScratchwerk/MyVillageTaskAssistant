import os
import time
import hmac
import hashlib
import json
from pathlib import Path
from urllib import request
from urllib.error import HTTPError
from dotenv import load_dotenv

load_dotenv(Path('.env'))
secret = os.environ['SLACK_SIGNING_SECRET']
url = 'https://colby-completive-phyllis.ngrok-free.dev/slack/events'
body = json.dumps({'type': 'url_verification', 'challenge': 'test'}).encode('utf-8')
timestamp = str(int(time.time()))
basestring = f'v0:{timestamp}:{body.decode("utf-8")}'.encode('utf-8')
sig = 'v0=' + hmac.new(secret.encode('utf-8'), basestring, hashlib.sha256).hexdigest()
headers = {
    'Content-Type': 'application/json',
    'X-Slack-Request-Timestamp': timestamp,
    'X-Slack-Signature': sig,
}
req = request.Request(url, data=body, headers=headers, method='POST')
print('headers:', headers)
try:
    with request.urlopen(req) as resp:
        print('status', resp.status)
        print('text', resp.read().decode('utf-8'))
except HTTPError as e:
    print('status', e.code)
    print('text', e.read().decode('utf-8'))
