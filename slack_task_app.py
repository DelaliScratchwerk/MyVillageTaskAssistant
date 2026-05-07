
import asyncio
import json
import logging
import os
import re
from datetime import date, datetime, timedelta, time
import calendar
from contextlib import asynccontextmanager
from zoneinfo import ZoneInfo
from functools import lru_cache
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel
from fastapi.responses import JSONResponse
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.signature import SignatureVerifier

# Load environment variables from .env file
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("slack-task-app")

@asynccontextmanager
async def lifespan(app: FastAPI):
    background_tasks = [
        asyncio.create_task(invoice_reminder_loop()),
        asyncio.create_task(task_due_reminder_loop()),
    ]
    try:
        yield
    finally:
        for task in background_tasks:
            task.cancel()
        for task in background_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass


app = FastAPI(title="Slack Task App", lifespan=lifespan)
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
    raw = raw.strip()
    if raw.startswith("'") and raw.endswith("'"):
        raw = raw[1:-1].strip()
    if raw.startswith('"') and raw.endswith('"'):
        raw = raw[1:-1].strip()
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


def clean_huddle_action_item_line(line: str) -> Optional[str]:
    item_text = line.strip()
    if not item_text:
        return None

    if item_text.startswith("-"):
        item_text = item_text[1:].strip()

    item_text = re.sub(r"^\[\s*[-xX]?\s*\]\s*", "", item_text).strip()
    item_text = item_text.strip("- ").strip()

    if not item_text:
        return None

    ignored_prefixes = (
        "this tool uses ai",
        "huddle transcript",
        "thread in ",
        "view message",
        "canvas",
    )
    if item_text.lower().startswith(ignored_prefixes):
        return None

    if not item_text.startswith("@") and " to " not in item_text.lower():
        return None

    return item_text


def extract_assignee_name_from_action_item(item_text: str) -> str:
    mention_match = re.search(r"@([^,\[]+?)(?:\s+to\s+|\s+will\s+|$)", item_text)
    if mention_match:
        return mention_match.group(1).strip()

    if " will " in item_text:
        return item_text.split(" will ", 1)[0].strip()

    if " to " in item_text:
        return item_text.split(" to ", 1)[0].strip()

    return ""


def is_likely_huddle_text(text: str) -> bool:
    lower_text = text.lower()
    if "huddle notes:" in lower_text or "huddle transcript" in lower_text:
        return True
    if "slack ai took notes" in lower_text and "action items" in lower_text:
        return True
    if "action items" in lower_text and "attendees" in lower_text:
        return True
    return False


def clean_huddle_transcript_line(line: str) -> Optional[str]:
    item_text = line.strip()
    if not item_text:
        return None

    item_text = re.sub(r"^\[[0-9:]+\]\s*", "", item_text).strip()
    speaker_match = re.match(r"^([A-Za-z][A-Za-z .'-]{1,80}):\s+(.+)$", item_text)
    if speaker_match:
        speaker = speaker_match.group(1).strip()
        spoken_text = speaker_match.group(2).strip()
        first_person_match = re.match(r"^(?:i|we)\s+(will|can|should|need to|have to|am going to|are going to)\s+(.+)$", spoken_text, flags=re.I)
        if first_person_match:
            item_text = f"{speaker} will {first_person_match.group(2).strip()}"
        else:
            item_text = spoken_text
    item_text = item_text.strip("- ").strip()

    if not item_text:
        return None

    ignored_fragments = (
        "huddle notes:",
        "huddle transcript",
        "slack ai took notes",
        "view huddle",
        "attendees",
        "summary",
        "action items",
        "this tool uses ai",
    )
    lowered = item_text.lower()
    if any(fragment in lowered for fragment in ignored_fragments):
        return None

    action_patterns = (
        r"@[^,\[]+?\s+will\s+",
        r"@[^,\[]+?\s+to\s+",
        r"\b[A-Z][A-Za-z .'-]{1,60}\s+will\s+",
        r"\b[A-Z][A-Za-z .'-]{1,60}\s+to\s+",
    )
    if not any(re.search(pattern, item_text) for pattern in action_patterns):
        return None

    return item_text


def add_huddle_task(
    tasks_to_create: list[dict],
    item_text: str,
    project_name: str,
    source: str,
) -> None:
    if any(task.get("source_excerpt") == item_text for task in tasks_to_create):
        return

    tasks_to_create.append({
        "title": item_text,
        "description": f"Source: Slack huddle notes\nProject: {project_name}\n\nOriginal action item:\n{item_text}",
        "assignee_name": extract_assignee_name_from_action_item(item_text),
        "assignee_user_id": None,
        "due_date": None,
        "priority": "medium",
        "source": source,
        "source_excerpt": item_text,
        "project_name": project_name
    })


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
        if in_action_items:
            item_text = clean_huddle_action_item_line(line)
            if item_text is None:
                if line.strip().lower().startswith("this tool uses ai"):
                    in_action_items = False
                continue

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
            add_huddle_task(tasks_to_create, item_text, project_name, "huddle_notes")

    if not tasks_to_create:
        for line in lines:
            item_text = clean_huddle_transcript_line(line)
            if item_text is not None:
                add_huddle_task(tasks_to_create, item_text, project_name, "huddle_transcript")

    return {
        "project_name": project_name,
        "attendees": attendees,
        "tasks_to_create": tasks_to_create,
        "tasks_to_update": tasks_to_update,
        "milestones": milestones,
        "recurring_meetings": recurring_meetings,
        "ignored_notes": ignored_notes
    }


def parse_transcript_action_indexes(raw_indexes: str, item_count: int) -> list[int]:
    indexes = []
    for raw_index in raw_indexes.split(","):
        try:
            index = int(raw_index.strip()) - 1
        except ValueError:
            continue
        if 0 <= index < item_count and index not in indexes:
            indexes.append(index)
    return indexes


def build_transcript_proposal_message(parsed_transcript: dict) -> str:
    proposal = (
        f"I found:\n"
        f"- {len(parsed_transcript['tasks_to_create'])} tasks to create\n"
        f"- {len(parsed_transcript['milestones'])} milestones\n"
        f"- {len(parsed_transcript['recurring_meetings'])} recurring meetings\n\n"
        "Proposed actions:\n"
    )

    if parsed_transcript["tasks_to_create"]:
        for idx, task in enumerate(parsed_transcript["tasks_to_create"], start=1):
            due_text = f", due {task['due_date']}" if task.get("due_date") else ""
            priority_text = f", priority {task.get('priority') or 'medium'}"
            proposal += (
                f"{idx}. Create task for {task['assignee_name'] or 'Team'}: "
                f"{task['title']}{due_text}{priority_text}\n"
            )
    else:
        proposal += "No tasks are currently queued.\n"

    proposal += (
        "\nReply with:\n"
        "- approve all\n"
        "- approve 1,2,3\n"
        "- reject 2\n"
        "- edit 1 assignee Delali due Friday priority high"
    )
    return proposal


def extract_transcript_edit_field(body: str, field_name: str) -> Optional[str]:
    match = re.search(
        rf"\b{field_name}\s+(.+?)(?=\s+(?:assignee|due|priority|title)\b|$)",
        body,
        flags=re.I,
    )
    if not match:
        return None
    value = match.group(1).strip(" .,")
    return value or None


def apply_transcript_task_edit(task: dict, edit_body: str) -> list[str]:
    changes = []

    assignee = extract_transcript_edit_field(edit_body, "assignee")
    if assignee is not None:
        task["assignee_name"] = assignee
        task["assignee_user_id"] = None
        changes.append(f"assignee {assignee}")

    due_raw = extract_transcript_edit_field(edit_body, "due")
    if due_raw is not None:
        due_date = parse_due_date(due_raw)
        task["due_date"] = due_date
        changes.append(f"due {due_date or 'No due date'}")

    priority = extract_transcript_edit_field(edit_body, "priority")
    if priority is not None:
        task["priority"] = PRIORITY_RATING_TO_LABEL.get(parse_priority(priority), "medium")
        changes.append(f"priority {task['priority']}")

    title = extract_transcript_edit_field(edit_body, "title")
    if title is not None:
        task["title"] = title
        changes.append("title")

    return changes


def handle_transcript_approval(channel_id: str, text: str) -> Optional[dict]:
    """Handle approval, reject, and edit commands for pending transcript actions."""
    if channel_id not in PENDING_TRANSCRIPT_ACTIONS:
        return None

    pending = PENDING_TRANSCRIPT_ACTIONS[channel_id]
    lower_text = text.lower().strip()
    tasks = pending["tasks_to_create"]

    if lower_text == "approve all":
        return {"action": "approve", "tasks": list(tasks)}

    if lower_text.startswith("approve "):
        raw_indexes = text.strip()[len("approve "):]
        indexes = parse_transcript_action_indexes(raw_indexes, len(tasks))
        if not indexes:
            return {"action": "message", "text": "I could not find any matching tasks to approve."}
        approved_tasks = [tasks[index] for index in indexes]
        return {"action": "approve", "tasks": approved_tasks}

    if lower_text.startswith("reject "):
        raw_indexes = text.strip()[len("reject "):]
        indexes = parse_transcript_action_indexes(raw_indexes, len(tasks))
        if not indexes:
            return {"action": "message", "text": "I could not find any matching tasks to reject."}

        rejected = [tasks[index]["title"] for index in indexes]
        pending["tasks_to_create"] = [
            task for index, task in enumerate(tasks) if index not in indexes
        ]
        return {
            "action": "message",
            "text": "Rejected:\n- " + "\n- ".join(rejected) + "\n\n" + build_transcript_proposal_message(pending),
        }

    edit_match = re.fullmatch(r"edit\s+(\d+)\s+(.+)", text.strip(), flags=re.I | re.S)
    if edit_match:
        index = int(edit_match.group(1)) - 1
        if not 0 <= index < len(tasks):
            return {"action": "message", "text": f"I could not find task {index + 1} to edit."}

        changes = apply_transcript_task_edit(tasks[index], edit_match.group(2))
        if not changes:
            return {
                "action": "message",
                "text": "I did not find anything to edit. Try: edit 1 assignee Delali due Friday priority high",
            }

        return {
            "action": "message",
            "text": f"Updated task {index + 1}: {', '.join(changes)}\n\n" + build_transcript_proposal_message(pending),
        }

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

    # 1) Static map exact match wins
    if normalized in TEAM_USER_MAP:
        return TEAM_USER_MAP[normalized]

    # 1b) Static map partial or sub-name match
    for key, user_id in TEAM_USER_MAP.items():
        if key in normalized or normalized in key:
            return user_id

    # 2) Optional dynamic lookup via users.list
    if ENABLE_DYNAMIC_USER_LOOKUP:
        user_id = find_slack_user_id_by_name(normalized)
        if user_id:
            return user_id

    return sender_user_id if default_to_sender else None


def find_slack_user_id_by_name(name: str) -> Optional[str]:
    normalized = normalize_name(name)
    if not normalized:
        return None

    exact_matches = []
    loose_matches = []

    try:
        cursor = None
        while True:
            resp = get_bot_client().users_list(limit=200, cursor=cursor)
            for member in resp.get("members", []):
                if member.get("deleted") or member.get("is_bot"):
                    continue

                profile = member.get("profile", {})
                candidates = {
                    normalize_name(member.get("name", "")),
                    normalize_name(member.get("real_name", "")),
                    normalize_name(profile.get("display_name", "")),
                    normalize_name(profile.get("real_name", "")),
                }
                candidates = {c for c in candidates if c}

                if normalized in candidates:
                    exact_matches.append(member["id"])
                    continue

                for candidate in candidates:
                    if normalized in candidate or candidate in normalized:
                        loose_matches.append(member["id"])
                        break

            cursor = resp.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break
    except SlackApiError as e:
        logger.warning("bot users.list lookup failed: %s", e.response.get("error"))
        # Fall back to the user token client if available.
        try:
            cursor = None
            while True:
                resp = get_lists_client().users_list(limit=200, cursor=cursor)
                for member in resp.get("members", []):
                    if member.get("deleted") or member.get("is_bot"):
                        continue

                    profile = member.get("profile", {})
                    candidates = {
                        normalize_name(member.get("name", "")),
                        normalize_name(member.get("real_name", "")),
                        normalize_name(profile.get("display_name", "")),
                        normalize_name(profile.get("real_name", "")),
                    }
                    candidates = {c for c in candidates if c}

                    if normalized in candidates:
                        exact_matches.append(member["id"])
                        continue

                    for candidate in candidates:
                        if normalized in candidate or candidate in normalized:
                            loose_matches.append(member["id"])
                            break

                cursor = resp.get("response_metadata", {}).get("next_cursor")
                if not cursor:
                    break
        except SlackApiError as e2:
            logger.warning("user token users.list lookup failed: %s", e2.response.get("error"))
            return None

    if len(exact_matches) == 1:
        return exact_matches[0]
    if len(exact_matches) > 1:
        return exact_matches[0]
    if len(loose_matches) == 1:
        return loose_matches[0]
    if len(loose_matches) > 1:
        return loose_matches[0]

    return None


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


def get_status_label_for_option_id(option_id: str) -> str:
    settings = get_settings()
    mapping = {
        settings["OPT_STATUS_NOT_STARTED"]: "Not started",
        settings["OPT_STATUS_IN_PROGRESS"]: "In progress",
        settings["OPT_STATUS_BLOCKED"]: "Blocked",
        settings["OPT_STATUS_DONE"]: "Done",
    }
    return mapping.get(option_id, STATUS_OPTION_TO_LABEL.get(option_id, option_id))


def find_task_by_task_id(task_id: str) -> Optional[dict]:
    settings = get_settings()
    normalized_task_id = task_id.strip().lower()
    rows = fetch_all_slack_list_rows()
    tasks = [normalize_task_row(row, settings) for row in rows]
    return next(
        (task for task in tasks if task.get("task_id", "").strip().lower() == normalized_task_id),
        None,
    )


def parse_status_update_message(text: str) -> Optional[Dict[str, Any]]:
    match = re.fullmatch(
        r"(done|complete|completed|start|started|block|blocked)\s+([A-Za-z]+-\d+)(?:\s+(.+))?",
        text.strip(),
        flags=re.I,
    )
    if not match:
        return None

    command = match.group(1).lower()
    task_id = match.group(2).upper()
    note = (match.group(3) or "").strip()

    if command in {"done", "complete", "completed"}:
        status_label = "done"
    elif command in {"start", "started"}:
        status_label = "in progress"
    else:
        status_label = "blocked"

    return {
        "task_id": task_id,
        "status_label": status_label,
        "status_option_id": parse_status(status_label),
        "note": note,
    }


def parse_status_update_messages(text: str) -> Optional[list[Dict[str, Any]]]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return None

    updates = []
    for line in lines:
        update = parse_status_update_message(line)
        if update is None:
            return None
        updates.append(update)

    return updates


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


def update_slack_list_item_cells(row_id: str, cells: list[dict]) -> Dict[str, Any]:
    settings = get_settings()
    cells_with_row_id = []
    for cell in cells:
        updated_cell = dict(cell)
        updated_cell["row_id"] = row_id
        cells_with_row_id.append(updated_cell)

    response = get_lists_client().api_call(
        api_method="slackLists.items.update",
        json={
            "list_id": settings["SLACK_LIST_ID"],
            "cells": cells_with_row_id,
        },
        headers={"Content-Type": "application/json; charset=utf-8"},
    )
    response.validate()
    return response.data


def build_blocked_description(existing_description: str, note: str, sender_user_id: str) -> str:
    if not note:
        return existing_description

    today = date.today().isoformat()
    block_note = f"Blocked by <@{sender_user_id}> on {today}: {note}"
    if not existing_description:
        return block_note
    return f"{existing_description}\n\n{block_note}"


def update_task_status(task: dict, status_option_id: str, note: str, sender_user_id: str) -> None:
    settings = get_settings()
    cells = [
        {
            "column_id": settings["COL_STATUS"],
            "select": [status_option_id],
        }
    ]

    if status_option_id == settings["OPT_STATUS_BLOCKED"] and note:
        cells.append(
            rich_text_field(
                settings["COL_DESCRIPTION"],
                build_blocked_description(task.get("description") or "", note, sender_user_id),
            )
        )

    update_slack_list_item_cells(task["row_id"], cells)


def post_dm(channel_id: str, text: str) -> None:
    try:
        get_bot_client().chat_postMessage(channel=channel_id, text=text)
    except SlackApiError as e:
        logger.error("Unable to send DM to Slack: %s", e.response.get('error'))
    except Exception as e:
        logger.error("Unexpected error sending DM to Slack: %s", e)


def post_dm_to_user(user_id: str, text: str) -> None:
    try:
        get_bot_client().chat_postMessage(channel=user_id, text=text)
    except SlackApiError as e:
        logger.error("Unable to send DM to user %s: %s", user_id, e.response.get('error'))
    except Exception as e:
        logger.error("Unexpected error sending DM to user %s: %s", user_id, e)

INVOICE_REMINDER_TEXT = "Reminder, please get your invoices submitted to Ms. Perez by tomorrow"


def get_invoice_reminder_channel_id() -> str:
    channel_id = os.getenv("INVOICE_REMINDER_CHANNEL_ID", "").strip()
    if not channel_id:
        raise RuntimeError("Missing required environment variable: INVOICE_REMINDER_CHANNEL_ID")
    return channel_id


def get_app_timezone() -> ZoneInfo:
    tz_name = os.getenv("APP_TIMEZONE", "America/New_York").strip()
    return ZoneInfo(tz_name)


def is_last_day_of_month(day: date) -> bool:
    last_day = calendar.monthrange(day.year, day.month)[1]
    return day.day == last_day


def should_send_invoice_reminder(day: date) -> bool:
    return day.day == 14 or is_last_day_of_month(day)


def next_invoice_reminder_time(now: datetime) -> datetime:
    tz = get_app_timezone()

    # Start checking from today's 8 AM
    candidate_day = now.date()
    while True:
        candidate = datetime.combine(candidate_day, time(hour=8, minute=0), tzinfo=tz)

        if candidate > now and should_send_invoice_reminder(candidate_day):
            return candidate

        candidate_day = candidate_day + timedelta(days=1)


def get_task_reminder_hour() -> int:
    raw_hour = os.getenv("TASK_REMINDER_HOUR", "8").strip()
    try:
        hour = int(raw_hour)
    except ValueError:
        logger.warning("Invalid TASK_REMINDER_HOUR=%s, falling back to 8", raw_hour)
        return 8
    if 0 <= hour <= 23:
        return hour
    logger.warning("Invalid TASK_REMINDER_HOUR=%s, falling back to 8", raw_hour)
    return 8


def get_task_reminder_minute() -> int:
    raw_minute = os.getenv("TASK_REMINDER_MINUTE", "0").strip()
    try:
        minute = int(raw_minute)
    except ValueError:
        logger.warning("Invalid TASK_REMINDER_MINUTE=%s, falling back to 0", raw_minute)
        return 0
    if 0 <= minute <= 59:
        return minute
    logger.warning("Invalid TASK_REMINDER_MINUTE=%s, falling back to 0", raw_minute)
    return 0


def next_daily_task_reminder_time(now: datetime) -> datetime:
    candidate = datetime.combine(
        now.date(),
        time(hour=get_task_reminder_hour(), minute=get_task_reminder_minute()),
        tzinfo=now.tzinfo,
    )
    if candidate <= now:
        candidate = candidate + timedelta(days=1)
    return candidate


def parse_task_due_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        logger.warning("Skipping task with invalid due_date=%s", value)
        return None


def is_done_task(task: dict) -> bool:
    return str(task.get("status") or "").strip().lower() == "done"


def get_task_due_reminder_label(due_date: date, today: date) -> Optional[str]:
    if due_date == today + timedelta(days=1):
        return "is due tomorrow"
    if due_date < today:
        return f"was due {due_date.isoformat()}"
    return None


def build_task_due_reminder_message(task: dict, due_date: date, today: date) -> Optional[str]:
    reminder_label = get_task_due_reminder_label(due_date, today)
    if reminder_label is None:
        return None

    task_id = str(task.get("task_id") or "Task").strip()
    title = str(task.get("title") or "Untitled task").strip()
    return f"Reminder: {task_id} {reminder_label} - {title}"


def get_tasks_due_for_reminder(today: date) -> list[tuple[dict, date]]:
    settings = get_settings()
    rows = fetch_all_slack_list_rows()
    tasks = [normalize_task_row(row, settings) for row in rows]
    reminders = []

    for task in tasks:
        if is_done_task(task):
            continue
        if not task.get("assignee"):
            continue

        due_date = parse_task_due_date(task.get("due_date"))
        if due_date is None:
            continue
        if get_task_due_reminder_label(due_date, today) is None:
            continue

        reminders.append((task, due_date))

    reminders.sort(key=lambda item: (item[1], _parse_task_sort_key(item[0])))
    return reminders


def send_task_due_reminders(today: date) -> int:
    sent_count = 0
    for task, due_date in get_tasks_due_for_reminder(today):
        message = build_task_due_reminder_message(task, due_date, today)
        if message is None:
            continue
        post_dm_to_user(str(task["assignee"]), message)
        sent_count += 1
    return sent_count


async def task_due_reminder_loop() -> None:
    while True:
        try:
            tz = get_app_timezone()
            now = datetime.now(tz)
            next_run = next_daily_task_reminder_time(now)

            sleep_seconds = (next_run - now).total_seconds()
            logger.info("Next task due reminder scheduled for %s", next_run.isoformat())

            await asyncio.sleep(sleep_seconds)

            reminder_day = datetime.now(tz).date()
            sent_count = send_task_due_reminders(reminder_day)

            logger.info("Sent %s task due reminders", sent_count)

            # Prevent accidental double-send if the loop wakes up again within the same minute
            await asyncio.sleep(60)

        except asyncio.CancelledError:
            logger.info("Task due reminder loop cancelled")
            raise
        except Exception:
            logger.exception("Task due reminder loop failed")
            await asyncio.sleep(60)


async def invoice_reminder_loop() -> None:
    while True:
        try:
            tz = get_app_timezone()
            now = datetime.now(tz)
            next_run = next_invoice_reminder_time(now)

            sleep_seconds = (next_run - now).total_seconds()
            logger.info("Next invoice reminder scheduled for %s", next_run.isoformat())

            await asyncio.sleep(sleep_seconds)

            channel_id = get_invoice_reminder_channel_id()
            post_dm(channel_id, INVOICE_REMINDER_TEXT)

            logger.info("Invoice reminder sent to channel %s", channel_id)

            # Prevent accidental double-send if the loop wakes up again within the same minute
            await asyncio.sleep(60)

        except asyncio.CancelledError:
            logger.info("Invoice reminder loop cancelled")
            raise
        except Exception:
            logger.exception("Invoice reminder loop failed")
            await asyncio.sleep(60)


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

    # Swap: created_by is the assignee, assignee is the Task Assistant bot
    created_by_user_id = get_required_env("TASK_BOT_USER_ID")
    assignee_user_id = resolve_slack_user(task.assignee)
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

    post_dm_to_user(
        assignee_user_id,
        f"📌 A new task has been assigned to you: *{title}*\nTask ID: {next_task_id}\nDue: {due_date or 'No due date'}\nPriority: {PRIORITY_RATING_TO_LABEL.get(priority_rating, 'medium')}"
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
    transcript_action = handle_transcript_approval(dm_channel_id, text)
    if transcript_action is not None:
        if transcript_action["action"] == "message":
            post_dm(dm_channel_id, transcript_action["text"])
            return JSONResponse({"ok": True})

        approved_tasks = transcript_action["tasks"]
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
                post_dm_to_user(
                    assignee_user_id,
                    f"📌 A new task has been assigned to you: *{task['title']}*\nTask ID: {next_task_id}\nDue: {task['due_date'] or 'No due date'}\nPriority: {task['priority'] or 'medium'}"
                )
                created_count += 1
            except SlackApiError as e:
                logger.exception("Failed to create task from transcript")
        post_dm(dm_channel_id, f"✅ Created {created_count} tasks from transcript.")
        del PENDING_TRANSCRIPT_ACTIONS[dm_channel_id]
        return JSONResponse({"ok": True})

    # Check for transcript processing requests
    transcript_triggers = ["Process huddle notes:", "Process transcript:", "Read these notes and make tasks:"]
    lower_text = text.lower()
    is_explicit_transcript_request = any(trigger.lower() in lower_text for trigger in transcript_triggers)
    is_transcript_request = is_explicit_transcript_request or is_likely_huddle_text(text)
    if is_transcript_request:
        transcript_body = text
        for trigger in transcript_triggers:
            if trigger.lower() in transcript_body.lower():
                transcript_body = re.sub(re.escape(trigger), "", transcript_body, count=1, flags=re.I).strip()
                break
        parsed_transcript = parse_huddle_transcript(transcript_body)
        PENDING_TRANSCRIPT_ACTIONS[dm_channel_id] = parsed_transcript
        post_dm(dm_channel_id, build_transcript_proposal_message(parsed_transcript))
        return JSONResponse({"ok": True})

    # Check for task status update commands
    status_updates = parse_status_update_messages(text)
    if status_updates is not None:
        confirmations = []
        for status_update in status_updates:
            task = find_task_by_task_id(status_update["task_id"])
            if task is None:
                post_dm(dm_channel_id, f"⚠️ I couldn't find task {status_update['task_id']}.")
                return JSONResponse({"ok": True})

            try:
                update_task_status(
                    task=task,
                    status_option_id=status_update["status_option_id"],
                    note=status_update["note"],
                    sender_user_id=sender_user_id,
                )
            except SlackApiError as e:
                error = e.response.get("error", "unknown_error")
                logger.exception("Slack Lists status update failed")
                post_dm(dm_channel_id, f"❌ I couldn't update {status_update['task_id']}. Slack returned: {error}")
                return JSONResponse({"ok": True})

            status_label = get_status_label_for_option_id(status_update["status_option_id"])
            note_text = f" Note: {status_update['note']}" if status_update["note"] else ""
            confirmations.append(
                f"✅ Updated {status_update['task_id']} to {status_label}: {task['title']}{note_text}"
            )

        post_dm(dm_channel_id, "\n".join(confirmations))
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

    post_dm_to_user(
        assignee_user_id,
        f"📌 A new task has been assigned to you: *{parsed['title']}*\nTask ID: {next_task_id}\nDue: {parsed['due_date'] or 'No due date'}\nPriority: {PRIORITY_RATING_TO_LABEL.get(parsed['priority_rating'], 'medium')}"
    )

    return JSONResponse({"ok": True})
