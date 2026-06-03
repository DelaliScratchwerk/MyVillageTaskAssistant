
# Slack Task App (FastAPI + Slack Python SDK)

## Run locally
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# fill in real values

# export env vars however you prefer, then:
uvicorn slack_task_app:app --reload --port 8000

## Read-only task API
The app now exposes the following endpoints:

- `GET /tasks`
- `GET /tasks/{task_id}`
- `POST /tasks`

Example usage:

```bash
curl http://127.0.0.1:8000/tasks
curl "http://127.0.0.1:8000/tasks?status=In%20progress"
curl "http://127.0.0.1:8000/tasks?assignee=U03JL8L7GJ0"
curl http://127.0.0.1:8000/tasks/DEV-0022
```

Create a new task with JSON:

```bash
curl -X POST http://127.0.0.1:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Build Slack task API",
    "description": "Add POST /tasks endpoint and API docs.",
    "assignee": "delali",
    "due_date": "next friday",
    "priority": "high",
    "status": "Not started"
  }'
```

If `TASK_API_KEY` is enabled in `.env`:

```bash
curl -H "X-API-Key: your_key" http://127.0.0.1:8000/tasks
```

## Due-date reminders
The app sends a morning DM to task assignees for open tasks that are due tomorrow or overdue.

Optional `.env` values:

```bash
APP_TIMEZONE=America/New_York
TASK_REMINDER_HOUR=8
TASK_REMINDER_MINUTE=0
```

## Invoice reminders
The app sends invoice reminders at 8:00 AM Eastern on the 1st, 14th, 15th, and last day of each month. A one-off test reminder is also scheduled for June 3, 2026. It sends to every public or private channel the bot is a member of. If the app was down at the scheduled time, it catches up for the most recent missed invoice reminder within 48 hours. Missed 1st and 15th reminders use a follow-up message asking people to send invoices if they have not already.

Optional `.env` values:

```bash
INVOICE_REMINDER_CATCH_UP_HOURS=48
```

## Slack DM commands
Create a task by DMing the app:

```text
Create task: build Slack tool, due Friday
Create task: fix API auth. Priority high. Due tomorrow.
```

Update task status by task ID:

```text
done DEV-0038
start DEV-0034
block DEV-0031 waiting on API token
```

Review huddle transcript proposals:

```text
Process huddle notes:
[paste huddle notes here]

Or paste Slack AI huddle notes, a huddle transcript, or both directly.

approve all
approve 1,3,5
reject 2
edit 1 assignee Delali due Friday priority high
```

## Team channel huddle notes
Map each Slack channel to the Slack List that should receive that channel's tasks:

```bash
CHANNEL_LIST_MAP_JSON='{"C_TEAM_CHANNEL_ID":"F_TEAM_LIST_ID"}'
```

Mention the bot in the team channel and paste the Slack AI huddle notes:

```text
@My Village Task Assistant Process huddle notes:
[paste huddle notes here]
```

Or mention the bot and attach a `.txt`, `.md`, `.vtt`, or `.srt` file that contains the Slack AI huddle notes/canvas text:

```text
@My Village Task Assistant Process huddle notes:
```

Raw huddle transcripts are too noisy for the rule-based parser; use the Slack AI huddle notes with the Action items section.

The app replies in a thread with proposed tasks. Mention the bot again in the same channel to approve:

```text
@My Village Task Assistant approve all
```

## Slack setup
- Event Subscriptions: ON
- Request URL: https://your-public-url/slack/events
- Bot events:
  - message.im
  - app_mention
  - message.channels (optional, only needed for transcript file shares that do not emit app_mention)
- Scopes:
  - app_mentions:read
  - chat:write
  - files:read
  - im:history
  - lists:write
  - users:read (optional, only if using dynamic user lookup)
