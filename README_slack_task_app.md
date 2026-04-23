
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

## Slack setup
- Event Subscriptions: ON
- Request URL: https://your-public-url/slack/events
- Bot events: message.im
- Scopes:
  - chat:write
  - im:history
  - lists:write
  - users:read (optional, only if using dynamic user lookup)
