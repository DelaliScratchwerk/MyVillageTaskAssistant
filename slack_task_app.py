
import asyncio
import json
import logging
import os
import re
from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import Any, Dict, Optional

from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel
from fastapi.responses import JSONResponse
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.signature import SignatureVerifier

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("slack-task-app")

app = FastAPI(title="Slack Task App")
TASK_ID_LOCK = asyncio.Lock()
PROCESSED_EVENT_IDS = set()
PENDING_TRANSCRIPT_ACTIONS = {}

STATUS_OPTION_TO_LABEL = {
    "Opt7MNHB19N": "Not started",
    "OptXBPNOYKC": "In progress",
    "OptEY5M00J3": "Blocked",
    "OptTR35W8NA": "Done",
}

PRIORITY_RATING_TO_LABEL = {
    1: "low",
    2: "medium",
    3: "high",
}

# Required environment values are loaded lazily so helper functions can be imported
# without requiring secrets to be set at import time.

def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

@lru_cache()
def get_settings() -> Dict[str, str]:
    return {
        "SLACK_SIGNING_SECRET": get_required_env("SLACK_SIGNING_SECRET"),
        "SLACK_BOT_TOKEN": get_required_env("SLACK_BOT_TOKEN"),
        "SLACK_USER_TOKEN": get_required_env("SLACK_USER_TOKEN"),
        "SLACK_LIST_ID": get_required_env("SLACK_LIST_ID"),
        "COL_TASK": get_required_env("COL_TASK"),
        "COL_TASK_ID": get_required_env("COL_TASK_ID"),
        "COL_DESCRIPTION": get_required_env("COL_DESCRIPTION"),
        "COL_CREATED_BY": get_required_env("COL_CREATED_BY"),
        "COL_ASSIGNEE": get_required_env("COL_ASSIGNEE"),
        "COL_STATUS": get_required_env("COL_STATUS"),
        "COL_PRIORITY": get_required_env("COL_PRIORITY"),
        "COL_DUE_DATE": get_required_env("COL_DUE_DATE"),
        "TASK_ID_PREFIX": os.getenv("TASK_ID_PREFIX", "DEV"),
        "OPT_STATUS_NOT_STARTED": get_required_env("OPT_STATUS_NOT_STARTED"),
        "OPT_STATUS_IN_PROGRESS": get_required_env("OPT_STATUS_IN_PROGRESS"),
        "OPT_STATUS_BLOCKED": get_required_env("OPT_STATUS_BLOCKED"),
        "OPT_STATUS_DONE": get_required_env("OPT_STATUS_DONE"),
    }

@lru_cache()
def get_verifier() -> SignatureVerifier:
    return SignatureVerifier(signing_secret=get_settings()["SLACK_SIGNING_SECRET"])

@lru_cache()
def get_bot_client() -> WebClient:
    return WebClient(token=get_settings()["SLACK_BOT_TOKEN"])

@lru_cache()
def get_lists_client() -> WebClient:
    return WebClient(token=get_settings()["SLACK_USER_TOKEN"])

@lru_cache()
def get_team_user_map() -> Dict[str, str]:
    raw = os.getenv("TEAM_USER_MAP_JSON", "{}")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Invalid TEAM_USER_MAP_JSON, falling back to empty map")
        return {}

def is_dynamic_user_lookup_enabled() -> bool:
    return os.getenv("ENABLE_DYNAMIC_USER_LOOKUP", "false").lower() == "true"

# Optional map of display names to Slack IDs.
TEAM_USER_MAP = get_team_user_map()
ENABLE_DYNAMIC_USER_LOOKUP = is_dynamic_user_lookup_enabled()


def build_rich_text(text: str) -> list:
    return [
        {
            "type": "rich_text",
            "elements": [
                {
                    "type": "rich_text_section",
                    "elements": [
                        {
                            "type": "text",
                            "text": text,
                        }
                    ],
                }
            ],
        }
    ]


def rich_text_field(column_id: str, text: str) -> Dict[str, Any]:
    return {
        "column_id": column_id,
        "rich_text": build_rich_text(text),
    }


def extract_field_text(field: dict) -> str:
    if field.get("text"):
        return str(field["text"]).strip()

    rich_text = field.get("rich_text") or []
    if rich_text:
        parts = []
        for block in rich_text:
            for element in block.get("elements", []):
                for child in element.get("elements", []):
                    if child.get("type") == "text":
                        parts.append(child.get("text", ""))
        text = "".join(parts).strip()
        if text:
            return text

    value = field.get("value")
    if value is None:
        return ""
    return str(value).strip()


def fetch_list_rows(list_id: str) -> list:
    response = get_lists_client().api_call(
        api_method="slackLists.items.list",
        json={"list_id": list_id},
    )
    response.validate()
    data = response.data
    return data.get("records") or data.get("items") or []


def fetch_all_slack_list_rows() -> list:
    settings = get_settings()
    response = get_lists_client().api_call(
        api_method="slackLists.items.list",
        json={"list_id": settings["SLACK_LIST_ID"]},
    )
    response.validate()
    data = response.data
    return data.get("records") or data.get("items") or []


def extract_user_field_value(field: dict) -> str:
    if not isinstance(field, dict):
        return ""
    users = field.get("user") or field.get("users") or []
    if isinstance(users, list) and users:
        return str(users[0]).strip()
    return str(field.get("value") or "").strip()


def normalize_task_row(row: dict, settings: dict) -> dict:
    row_id = row.get("id") or row.get("row_id") or ""
    task_id = ""
    title = ""
    description = ""
    created_by = ""
    assignee = ""
    status = ""
    priority = ""
    priority_rating = None
    due_date = None

    for field in row.get("fields", []):
        if not isinstance(field, dict):
            continue
        column_id = field.get("column_id")
        if column_id == settings["COL_TASK_ID"]:
            task_id = extract_field_text(field)
        elif column_id == settings["COL_TASK"]:
            title = extract_field_text(field)
        elif column_id == settings["COL_DESCRIPTION"]:
            description = extract_field_text(field)
        elif column_id == settings["COL_CREATED_BY"]:
            created_by = extract_user_field_value(field)
        elif column_id == settings["COL_ASSIGNEE"]:
            assignee = extract_user_field_value(field)
        elif column_id == settings["COL_STATUS"]:
            select_values = field.get("select") or []
            if select_values:
                first = select_values[0]
                option_id = ""
                if isinstance(first, dict):
                    option_id = first.get("id") or first.get("value") or ""
                else:
                    option_id = str(first)
                status = STATUS_OPTION_TO_LABEL.get(option_id, "")
        elif column_id == settings["COL_PRIORITY"]:
            rating_values = field.get("rating") or []
            if rating_values:
                first = rating_values[0]
                try:
                    priority_rating = int(first)
                except (TypeError, ValueError):
                    priority_rating = None
                if priority_rating is not None:
                    priority = PRIORITY_RATING_TO_LABEL.get(priority_rating, "")
        elif column_id == settings["COL_DUE_DATE"]:
            date_values = field.get("date") or []
            if date_values:
                first = date_values[0]
                if isinstance(first, str):
                    due_date = first
                elif isinstance(first, dict):
                    due_date = first.get("start_date") or first.get("date") or None

    if due_date is not None and due_date == "":
        due_date = None

    updated_timestamp = (
        row.get("updated_timestamp")
        or row.get("date_updated")
        or ""
    )

    return {
        "row_id": row_id,
        "task_id": task_id,
        "title": title,
        "description": description,
        "created_by": created_by,
        "assignee": assignee,
        "status": status,
        "priority": priority,
        "priority_rating": priority_rating,
        "due_date": due_date,
        "updated_timestamp": updated_timestamp,
        "raw": row,
    }


def filter_tasks(tasks: list[dict], assignee: Optional[str], status: Optional[str], q: Optional[str]) -> list[dict]:
    filtered = []
    normalized_status = status.strip().lower() if status else None
    normalized_q = q.strip().lower() if q else None

    for task in tasks:
        if assignee and task.get("assignee") != assignee:
            continue
        if normalized_status and task.get("status", "").strip().lower() != normalized_status:
            continue
        if normalized_q:
            haystack = " ".join(
                [
                    str(task.get("task_id", "")),
                    str(task.get("title", "")),
                    str(task.get("description", "")),
                ]
            ).lower()
            if normalized_q not in haystack:
                continue
        filtered.append(task)
    return filtered


def require_api_key(request: Request) -> None:
    api_key = os.getenv("TASK_API_KEY", "").strip()
    if not api_key:
        return

    provided_key = request.headers.get("X-API-Key", "")
    if provided_key != api_key:
        raise HTTPException(status_code=401, detail="Unauthorized")


def is_slack_user_id(value: str) -> bool:
    return bool(re.fullmatch(r"U[A-Z0-9]{8,}", value.strip()))


def resolve_slack_user(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    normalized = value.strip()
    mention_match = re.fullmatch(r"<@([A-Z0-9]+)>", normalized)
    if mention_match:
        return mention_match.group(1)
    if is_slack_user_id(normalized):
        return normalized
    return resolve_user_id(normalized, "", default_to_sender=False)


class TaskCreateRequest(BaseModel):
    title: str
    description: Optional[str] = None
    assignee: Optional[str] = None
    due_date: Optional[str] = None
    priority: Optional[str] = None
    status: Optional[str] = None
    created_by: Optional[str] = None


def get_next_task_id() -> str:
    settings = get_settings()
    prefix = settings.get("TASK_ID_PREFIX", "DEV")
    escaped_prefix = re.escape(prefix)
    pattern = re.compile(rf"^{escaped_prefix}-(\d{{4}})$")
    highest = 0

    for row in fetch_list_rows(settings["SLACK_LIST_ID"]):
        for field in row.get("fields", []):
            if field.get("column_id") != settings["COL_TASK_ID"]:
                continue
            text = extract_field_text(field)
            if not text:
                continue
            match = pattern.match(text)
            if not match:
                continue
            number = int(match.group(1))
            if number > highest:
                highest = number

    return f"{prefix}-{highest + 1:04d}"


def parse_huddle_transcript(transcript: str) -> dict:
    """Parse a Slack huddle transcript into structured actions."""
    lines = transcript.split('\n')
    project_name = ""
    attendees = []
    tasks_to_create = []
    tasks_to_update = []
    milestones = []
    recurring_meetings = []
    ignored_notes = []

    # Extract project name
    for line in lines:
        if 'project kickoff meeting for' in line.lower():
            match = re.search(r'new SaaS product "([^"]+)"', line)
            if match:
                project_name = match.group(1)
                break

    # Extract attendees
    in_attendees = False
    for line in lines:
        if ':handshake:' in line or 'Attendees' in line:
            in_attendees = True
            continue
        if in_attendees and line.strip().startswith('-'):
            attendees.append(line.strip()[1:].strip())
        elif in_attendees and line.strip() and not line.startswith(' '):
            in_attendees = False

    # Extract action items
    in_action_items = False
    for line in lines:
        if ':white_check_mark:' in line or 'Action items' in line:
            in_action_items = True
            continue
        if in_action_items and line.strip().startswith('-'):
            item_text = line.strip()[1:].strip()
            if any(word in item_text.lower() for word in ['product overview', 'technical features', 'administrative functionality']):
                ignored_notes.append(item_text)
                continue
            # Check for recurring
            if any(word in item_text.lower() for word in ['weekly', 'every', 'meeting on', 'sundays at']):
                recurring_meetings.append(item_text)
                continue
            # Check for milestone
            if 'by' in item_text.lower() and any(word in item_text.lower() for word in ['week', 'month', 'ready']):
                milestones.append(item_text)
                continue
            # Extract assignee
            assignee_name = ""
            match = re.search(r'@(\w+)', item_text)
            if match:
                assignee_name = match.group(1)
            elif 'will' in item_text:
                assignee_name = item_text.split(' will')[0].strip()
            # Create task
            tasks_to_create.append({
                "title": item_text,
                "description": f"Source: Slack huddle notes\nProject: {project_name}\n\nOriginal action item:\n{item_text}",
                "assignee_name": assignee_name,
                "assignee_user_id": None,
                "due_date": None,
                "priority": "medium",
                "source": "huddle_notes",
                "source_excerpt": item_text,
                "project_name": project_name
            })
        elif in_action_items and line.strip() and not line.startswith(' '):
            in_action_items = False

    return {
        "project_name": project_name,
        "attendees": attendees,
        "tasks_to_create": tasks_to_create,
        "tasks_to_update": tasks_to_update,
        "milestones": milestones,
        "recurring_meetings": recurring_meetings,
        "ignored_notes": ignored_notes
    }


def handle_transcript_approval(channel_id: str, text: str) -> Optional[str]:
    """Handle approval commands for pending transcript actions."""
    if channel_id not in PENDING_TRANSCRIPT_ACTIONS:
        return None

    pending = PENDING_TRANSCRIPT_ACTIONS[channel_id]
    lower_text = text.lower().strip()

    if lower_text == "approve all":
        approved_tasks = pending["tasks_to_create"]
        # For now, only handle tasks_to_create
        return approved_tasks

    if lower_text.startswith("approve "):
        indexes = lower_text.replace("approve ", "").split(",")
        approved_tasks = []
        for idx in indexes:
            try:
                idx = int(idx.strip()) - 1
                if 0 <= idx < len(pending["tasks_to_create"]):
                    approved_tasks.append(pending["tasks_to_create"][idx])
            except ValueError:
                pass
        return approved_tasks

    return None


def normalize_name(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


def resolve_user_id(name: str, sender_user_id: str, default_to_sender: bool = True) -> Optional[str]:
    """Resolve an assignee name to a Slack user ID.
    Defaults to sender if blank when default_to_sender is True.
    """
    if not name or not name.strip():
        return sender_user_id if default_to_sender else None

    normalized = normalize_name(name)

    # 1) Static map wins
    if normalized in TEAM_USER_MAP:
        return TEAM_USER_MAP[normalized]

    # 2) Optional dynamic lookup via users.list
    if ENABLE_DYNAMIC_USER_LOOKUP:
        try:
            cursor = None
            while True:
                resp = get_bot_client().users_list(limit=200, cursor=cursor)
                for member in resp.get("members", []):
                    if member.get("deleted") or member.get("is_bot"):
                        continue
                    profile = member.get("profile", {})
                    candidates = {
                        normalize_name(member.get("real_name", "")),
                        normalize_name(member.get("name", "")),
                        normalize_name(profile.get("display_name", "")),
                        normalize_name(profile.get("real_name", "")),
                    }
                    if normalized in candidates:
                        return member["id"]

                cursor = resp.get("response_metadata", {}).get("next_cursor")
                if not cursor:
                    break
        except SlackApiError as e:
            logger.warning("users.list lookup failed: %s", e.response.get("error"))

    return sender_user_id if default_to_sender else None


def next_weekday(base_date: date, weekday: int, force_next_week: bool = False) -> date:
    days_ahead = weekday - base_date.weekday()
    if days_ahead < 0:
        days_ahead += 7
    if days_ahead == 0 and force_next_week:
        days_ahead = 7
    return base_date + timedelta(days=days_ahead)


def parse_due_date(raw: str) -> Optional[str]:
    """Parse a few friendly date phrases into YYYY-MM-DD.
    Supports:
      - 2026-03-27
      - today / tomorrow
      - monday, tuesday, ...
      - next monday, next tuesday, ...
      - friday
    """
    if not raw:
        return None

    s = raw.strip().lower()
    today = date.today()

    # ISO date
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s

    if s == "today":
        return today.isoformat()
    if s == "tomorrow":
        return (today + timedelta(days=1)).isoformat()

    weekdays = {
        "monday": 0,
        "tuesday": 1,
        "wednesday": 2,
        "thursday": 3,
        "friday": 4,
        "saturday": 5,
        "sunday": 6,
    }

    for label, idx in weekdays.items():
        if s == label:
            return next_weekday(today, idx, force_next_week=False).isoformat()
        if s == f"next {label}":
            return next_weekday(today, idx, force_next_week=True).isoformat()

    return None


def parse_priority(raw: Optional[str]) -> int:
    if not raw:
        return 2  # medium
    value = raw.strip().lower()
    mapping = {
        "low": 1,
        "medium": 2,
        "med": 2,
        "normal": 2,
        "high": 3,
        "urgent": 3,
    }
    return mapping.get(value, 2)


def parse_status(raw: Optional[str]) -> str:
    default_status = get_settings()["OPT_STATUS_NOT_STARTED"]
    if not raw:
        return default_status
    value = raw.strip().lower()
    mapping = {
        "not started": default_status,
        "todo": default_status,
        "in progress": get_settings()["OPT_STATUS_IN_PROGRESS"],
        "progress": get_settings()["OPT_STATUS_IN_PROGRESS"],
        "blocked": get_settings()["OPT_STATUS_BLOCKED"],
        "done": get_settings()["OPT_STATUS_DONE"],
    }
    return mapping.get(value, default_status)


def parse_create_task_message(text: str) -> Dict[str, Any]:
    """Simple rule-based parser.

    Supported examples:
      Create task: build Slack tool, due Friday
      Create task: fix API auth. Priority high. Due tomorrow.
      Create task: update ticket workflow, assign to Ethan, due next Tuesday
      Create task: build Slack tool. Description: wire up Slack DMs to task list.
    """
    original = text.strip()
    lower = original.lower()

    if not lower.startswith("create task"):
        return {
            "ok": False,
            "error": "This message does not look like a create-task request.",
        }

    # Strip the leading command
    body = re.sub(r"^create task\s*:?\s*", "", original, flags=re.I).strip()

    # Extract explicit fields
    due_match = re.search(r"\bdue\s+([A-Za-z]+(?:\s+[A-Za-z]+)?|\d{4}-\d{2}-\d{2})", body, flags=re.I)
    assign_match = re.search(r"\bassign\s+to\s+([A-Za-z][A-Za-z .'-]*)", body, flags=re.I)
    priority_match = re.search(r"\bpriority\s+(low|medium|med|normal|high|urgent)\b", body, flags=re.I)
    description_match = re.search(r"\bdescription\s*:\s*(.+)$", body, flags=re.I)

    due_raw = due_match.group(1).strip() if due_match else ""
    assignee_name = assign_match.group(1).strip() if assign_match else ""
    priority_raw = priority_match.group(1).strip() if priority_match else ""
    description = description_match.group(1).strip() if description_match else ""

    # Remove extracted phrases from title candidate
    title_candidate = body
    title_candidate = re.sub(r"\bdescription\s*:\s*.+$", "", title_candidate, flags=re.I)
    title_candidate = re.sub(r"\bassign\s+to\s+[A-Za-z][A-Za-z .'-]*", "", title_candidate, flags=re.I)
    title_candidate = re.sub(r"\bdue\s+[A-Za-z]+(?:\s+[A-Za-z]+)?", "", title_candidate, flags=re.I)
    title_candidate = re.sub(r"\bdue\s+\d{4}-\d{2}-\d{2}", "", title_candidate, flags=re.I)
    title_candidate = re.sub(r"\bpriority\s+(low|medium|med|normal|high|urgent)\b", "", title_candidate, flags=re.I)
    title_candidate = re.sub(r"\s*,\s*", " ", title_candidate)
    title_candidate = re.sub(r"\s{2,}", " ", title_candidate).strip(" .,-")

    if not title_candidate:
        return {
            "ok": False,
            "error": "I could not find a task title. Try: Create task: build Slack tool, due Friday",
        }

    due_date = parse_due_date(due_raw)

    return {
        "ok": True,
        "title": title_candidate,
        "description": description,
        "due_date": due_date,
        "assignee_name": assignee_name,
        "priority_rating": parse_priority(priority_raw),
        "priority_label": priority_raw or "medium",
        "status_option_id": None,
    }


def create_slack_list_item(
    *,
    title: str,
    description: str,
    sender_user_id: str,
    assignee_user_id: str,
    due_date: Optional[str],
    priority_rating: int,
    status_option_id: str,
    task_id: Optional[str] = None,
) -> Dict[str, Any]:
    settings = get_settings()
    initial_fields = [
        rich_text_field(settings["COL_TASK"], title),
        {
            "column_id": settings["COL_CREATED_BY"],
            "user": [sender_user_id],
        },
        {
            "column_id": settings["COL_ASSIGNEE"],
            "user": [assignee_user_id],
        },
        {
            "column_id": settings["COL_PRIORITY"],
            "rating": [priority_rating],
        },
        {
            "column_id": settings["COL_STATUS"],
            "select": [status_option_id],
        },
    ]

    if description:
        initial_fields.append(rich_text_field(settings["COL_DESCRIPTION"], description))

    if task_id:
        initial_fields.append(
            {
                "column_id": settings["COL_TASK_ID"],
                "rich_text": build_rich_text(task_id),
            }
        )

    if due_date:
        initial_fields.append(
            {
                "column_id": settings["COL_DUE_DATE"],
                "date": [due_date],
            }
        )

    response = get_lists_client().api_call(
        api_method="slackLists.items.create",
        json={
            "list_id": settings["SLACK_LIST_ID"],
            "initial_fields": initial_fields,
        },
        headers={"Content-Type": "application/json; charset=utf-8"},
    )
    response.validate()
    return response.data


def post_dm(channel_id: str, text: str) -> None:
    try:
        get_bot_client().chat_postMessage(channel=channel_id, text=text)
    except SlackApiError as e:
        logger.error("Unable to send DM to Slack: %s", e.response.get('error'))
    except Exception as e:
        logger.error("Unexpected error sending DM to Slack: %s", e)


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


def _parse_task_sort_key(task: dict):
    task_id = str(task.get("task_id", "") or "")
    match = re.search(r"(\d+)$", task_id)
    if match:
        return (0, int(match.group(1)), task_id.lower())
    return (1, 0, str(task.get("title", "") or "").lower())


@app.get("/tasks")
async def get_tasks(
    assignee: Optional[str] = None,
    status: Optional[str] = None,
    q: Optional[str] = None,
    _api_key: None = Depends(require_api_key),
):
    settings = get_settings()
    rows = fetch_all_slack_list_rows()
    tasks = [normalize_task_row(row, settings) for row in rows]
    tasks = filter_tasks(tasks, assignee, status, q)
    tasks.sort(key=_parse_task_sort_key)
    tasks = [
        {
            "row_id": task["row_id"],
            "task_id": task["task_id"],
            "title": task["title"],
            "description": task["description"],
            "created_by": task["created_by"],
            "assignee": task["assignee"],
            "status": task["status"],
            "priority": task["priority"],
            "priority_rating": task["priority_rating"],
            "due_date": task["due_date"],
            "updated_timestamp": task["updated_timestamp"],
        }
        for task in tasks
    ]
    return {"count": len(tasks), "tasks": tasks}


@app.get("/tasks/{task_id}")
async def get_task_by_task_id(task_id: str, _api_key: None = Depends(require_api_key)):
    settings = get_settings()
    rows = fetch_all_slack_list_rows()
    tasks = [normalize_task_row(row, settings) for row in rows]
    target = next(
        (task for task in tasks if task.get("task_id", "").lower() == task_id.lower()),
        None,
    )
    if not target:
        raise HTTPException(status_code=404, detail="Task not found")
    result = dict(target)
    result.pop("raw", None)
    return result


@app.post("/tasks")
async def create_task(task: TaskCreateRequest, _api_key: None = Depends(require_api_key)):
    title = task.title.strip()
    if not title:
        raise HTTPException(status_code=400, detail="Task title is required.")

    assignee_user_id = resolve_slack_user(task.assignee)
    if not assignee_user_id:
        raise HTTPException(
            status_code=400,
            detail="Assignee is required and must be a valid Slack user ID, mention, or team member name.",
        )

    created_by_user_id = resolve_slack_user(task.created_by) or assignee_user_id
    due_date = parse_due_date(task.due_date) if task.due_date else None
    priority_rating = parse_priority(task.priority)
    status_option_id = parse_status(task.status)

    try:
        async with TASK_ID_LOCK:
            next_task_id = get_next_task_id()
            created = create_slack_list_item(
                title=title,
                description=task.description or "",
                sender_user_id=created_by_user_id,
                assignee_user_id=assignee_user_id,
                due_date=due_date,
                priority_rating=priority_rating,
                status_option_id=status_option_id,
                task_id=next_task_id,
            )
    except SlackApiError as e:
        error = e.response.get("error", "unknown_error")
        logger.exception("Slack Lists create failed")
        raise HTTPException(status_code=502, detail=f"Slack error: {error}")

    item_id = (
        created.get("item", {}).get("id")
        or created.get("id")
        or created.get("row_id")
        or "unknown"
    )

    return {
        "ok": True,
        "task_id": next_task_id,
        "row_id": item_id,
        "title": title,
        "assignee": assignee_user_id,
        "created_by": created_by_user_id,
        "due_date": due_date,
        "priority_rating": priority_rating,
        "status_option_id": status_option_id,
    }


@app.post("/slack/events")
async def slack_events(request: Request):
    raw_body = await request.body()
    headers = dict(request.headers)

    if not get_verifier().is_valid_request(raw_body, headers):
        raise HTTPException(status_code=401, detail="Invalid Slack signature")

    payload = await request.json()

    # Slack Events API URL verification handshake
    if payload.get("type") == "url_verification":
        return JSONResponse({"challenge": payload["challenge"]})

    if payload.get("type") != "event_callback":
        return JSONResponse({"ok": True})

    event_id = payload.get("event_id")
    if event_id:
        logger.info("Received Slack event_id=%s", event_id)
        if event_id in PROCESSED_EVENT_IDS:
            return JSONResponse({"ok": True})
        PROCESSED_EVENT_IDS.add(event_id)

    # Slack may retry requests; you can choose to ignore duplicates here if you persist event_ids.
    event = payload.get("event", {})
    if not event:
        return JSONResponse({"ok": True})

    # Only handle new DMs to the app
    if event.get("type") != "message" or event.get("channel_type") != "im":
        return JSONResponse({"ok": True})

    # Ignore bot messages, edits, and messages without a user
    if event.get("bot_id") or event.get("subtype") or not event.get("user"):
        return JSONResponse({"ok": True})

    sender_user_id = event["user"]
    dm_channel_id = event["channel"]
    text = (event.get("text") or "").strip()

    # Check for transcript approval commands
    approved_tasks = handle_transcript_approval(dm_channel_id, text)
    if approved_tasks is not None:
        if not approved_tasks:
            post_dm(dm_channel_id, "No tasks approved.")
            del PENDING_TRANSCRIPT_ACTIONS[dm_channel_id]
            return JSONResponse({"ok": True})
        # Create approved tasks
        created_count = 0
        for task in approved_tasks:
            assignee_user_id = resolve_user_id(task["assignee_name"], sender_user_id)
            if assignee_user_id is None:
                assignee_user_id = sender_user_id
            priority_rating = parse_priority(task["priority"])
            status_option_id = get_settings()["OPT_STATUS_NOT_STARTED"]
            try:
                async with TASK_ID_LOCK:
                    next_task_id = get_next_task_id()
                    create_slack_list_item(
                        title=task["title"],
                        description=task["description"],
                        sender_user_id=sender_user_id,
                        assignee_user_id=assignee_user_id,
                        due_date=task["due_date"],
                        priority_rating=priority_rating,
                        status_option_id=status_option_id,
                        task_id=next_task_id,
                    )
                created_count += 1
            except SlackApiError as e:
                logger.exception("Failed to create task from transcript")
        post_dm(dm_channel_id, f"✅ Created {created_count} tasks from transcript.")
        del PENDING_TRANSCRIPT_ACTIONS[dm_channel_id]
        return JSONResponse({"ok": True})

    # Check for transcript processing requests
    transcript_triggers = ["Process huddle notes:", "Process transcript:", "Read these notes and make tasks:"]
    is_transcript_request = any(trigger.lower() in text.lower() for trigger in transcript_triggers)
    if is_transcript_request:
        transcript_body = text
        for trigger in transcript_triggers:
            if trigger.lower() in transcript_body.lower():
                transcript_body = transcript_body.lower().replace(trigger.lower(), "").strip()
                break
        parsed_transcript = parse_huddle_transcript(transcript_body)
        PENDING_TRANSCRIPT_ACTIONS[dm_channel_id] = parsed_transcript
        # Build proposal message
        proposal = f"I found:\n- {len(parsed_transcript['tasks_to_create'])} tasks to create\n- {len(parsed_transcript['milestones'])} milestones\n- {len(parsed_transcript['recurring_meetings'])} recurring meetings\n\nProposed actions:\n"
        idx = 1
        for task in parsed_transcript['tasks_to_create']:
            proposal += f"{idx}. Create task for {task['assignee_name'] or 'Team'}: {task['title']}\n"
            idx += 1
        for milestone in parsed_transcript['milestones']:
            proposal += f"{idx}. Milestone: {milestone}\n"
            idx += 1
        for meeting in parsed_transcript['recurring_meetings']:
            proposal += f"{idx}. Recurring meeting: {meeting}\n"
            idx += 1
        proposal += "\nReply with:\n- approve all\n- approve 1,2,3"
        post_dm(dm_channel_id, proposal)
        return JSONResponse({"ok": True})

    # Normal task creation
    parsed = parse_create_task_message(text)
    if not parsed["ok"]:
        post_dm(
            dm_channel_id,
            f"⚠️ {parsed['error']}\n\nTry something like:\n"
            "Create task: build Slack tool, due Friday\n"
            "Create task: fix API auth. Priority high. Due tomorrow.",
        )
        return JSONResponse({"ok": True})

    assignee_user_id = resolve_user_id(parsed["assignee_name"], sender_user_id)
    if assignee_user_id is None:
        post_dm(
            dm_channel_id,
            f'⚠️ I could not find a team member named "{parsed["assignee_name"]}". '
            "Please try again with a valid team member name or omit the assignee to assign it to yourself.",
        )
        return JSONResponse({"ok": True})

    if not parsed["status_option_id"]:
        parsed["status_option_id"] = get_settings()["OPT_STATUS_NOT_STARTED"]

    try:
        async with TASK_ID_LOCK:
            next_task_id = get_next_task_id()
            created = create_slack_list_item(
                title=parsed["title"],
                description=parsed["description"],
                sender_user_id=sender_user_id,
                assignee_user_id=assignee_user_id,
                due_date=parsed["due_date"],
                priority_rating=parsed["priority_rating"],
                status_option_id=parsed["status_option_id"],
                task_id=next_task_id,
            )
    except SlackApiError as e:
        error = e.response.get("error", "unknown_error")
        logger.exception("Slack Lists create failed")
        post_dm(
            dm_channel_id,
            f"❌ I couldn't create the task. Slack returned: {error}",
        )
        return JSONResponse({"ok": True})

    row_id = created.get("item", {}).get("id") or created.get("id") or created.get("row_id") or "unknown"
    confirmation_text = (
        f"✅ Task created successfully. Task ID: {next_task_id}\n\n"
        f"Title: {parsed['title']}\n"
        f"Assignee: <@{assignee_user_id}>\n"
        f"Due: {parsed['due_date'] or 'No due date'}\n"
        f"Priority: {parsed['priority_label']}\n"
        f"Row ID: {row_id}"
    )
    post_dm(dm_channel_id, confirmation_text)

    return JSONResponse({"ok": True})
