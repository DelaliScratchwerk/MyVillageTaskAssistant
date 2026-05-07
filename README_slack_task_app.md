
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
approve all
approve 1,3,5
reject 2
edit 1 assignee Delali due Friday priority high
```

## Slack setup
- Event Subscriptions: ON
- Request URL: https://your-public-url/slack/events
- Bot events: message.im
- Scopes:
  - chat:write
  - im:history
  - lists:write
  - users:read (optional, only if using dynamic user lookup)
