"""Task Manager implemented with a list-of-dicts table.

The list-of-dicts approach keeps the code lightweight and easy to serialize to
CSV without introducing external dependencies like pandas. Each row is a task
record, and each column is a key on the dict, mirroring a spreadsheet model.
"""

from __future__ import annotations

import csv
from datetime import datetime, date
import uuid
from typing import Dict, List, Optional


# Enumerations and schema definitions
PRIORITIES = ["Low", "Medium", "High", "Urgent"]
STATUSES = ["To Do", "In Progress", "Blocked", "Completed"]
COLUMNS = [
    "task_id",
    "task_name",
    "description",
    "category",
    "priority",
    "status",
    "start_date",
    "due_date",
    "completion_date",
    "assigned_to",
    "notes",
    "last_updated",
]


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _validate_date(date_str: Optional[str], field_name: str) -> Optional[date]:
    if date_str in (None, ""):
        return None
    try:
        parsed = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError as exc:  # pragma: no cover - defensive
        raise ValueError(f"{field_name} must be in YYYY-MM-DD format") from exc
    return parsed


def _validate_task_record(task: Dict[str, Optional[str]], table: List[Dict[str, Optional[str]]]) -> None:
    if not task.get("task_name", "").strip():
        raise ValueError("task_name is required")

    if task.get("priority") not in PRIORITIES:
        raise ValueError(f"priority must be one of {PRIORITIES}")

    if task.get("status") not in STATUSES:
        raise ValueError(f"status must be one of {STATUSES}")

    start = _validate_date(task.get("start_date"), "start_date")
    due = _validate_date(task.get("due_date"), "due_date")
    completion = _validate_date(task.get("completion_date"), "completion_date")

    if start and due and start > due:
        raise ValueError("start_date must be on or before due_date")

    if task.get("completion_date") not in (None, "") and task.get("status") != "Completed":
        raise ValueError("completion_date can only be set when status is 'Completed'")

    if completion and start and completion < start:
        raise ValueError("completion_date cannot be before start_date")

    # Enforce unique task_id in the current table
    ids = [row["task_id"] for row in table]
    if ids.count(task["task_id"]) > 1:
        raise ValueError("task_id must be unique")


def _ensure_columns(task: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
    return {col: task.get(col, None) for col in COLUMNS}


def add_task(table: List[Dict[str, Optional[str]]], task_data: Dict[str, Optional[str]]) -> List[Dict[str, Optional[str]]]:
    new_table = list(table)
    task_id = task_data.get("task_id") or str(uuid.uuid4())
    if any(row["task_id"] == task_id for row in new_table):
        raise ValueError("task_id must be unique")

    base_task = _ensure_columns({
        **task_data,
        "task_id": task_id,
        "last_updated": _now_iso(),
    })
    _validate_task_record(base_task, new_table + [base_task])
    new_table.append(base_task)
    return new_table


def update_task(table: List[Dict[str, Optional[str]]], task_id: str, updates: Dict[str, Optional[str]]) -> List[Dict[str, Optional[str]]]:
    if "task_id" in updates and updates["task_id"] != task_id:
        raise ValueError("task_id is immutable")

    new_table = list(table)
    for idx, row in enumerate(new_table):
        if row["task_id"] == task_id:
            merged = _ensure_columns({**row, **{k: v for k, v in updates.items() if k in COLUMNS}})
            merged["task_id"] = task_id
            merged["last_updated"] = _now_iso()
            new_table[idx] = merged
            _validate_task_record(merged, new_table)
            return new_table
    raise ValueError(f"task_id '{task_id}' not found")


def filter_tasks(
    table: List[Dict[str, Optional[str]]],
    status: Optional[str] = None,
    category: Optional[str] = None,
    assigned_to: Optional[str] = None,
) -> List[Dict[str, Optional[str]]]:
    def _matches(row: Dict[str, Optional[str]]) -> bool:
        if status and row.get("status") != status:
            return False
        if category and row.get("category") != category:
            return False
        if assigned_to and row.get("assigned_to") != assigned_to:
            return False
        return True

    return [row for row in table if _matches(row)]


def sort_tasks_by_priority_and_due_date(table: List[Dict[str, Optional[str]]]) -> List[Dict[str, Optional[str]]]:
    priority_rank = {name: idx for idx, name in enumerate(PRIORITIES)}

    def _due_key(row: Dict[str, Optional[str]]):
        due = _validate_date(row.get("due_date"), "due_date")
        return due or date.max

    return sorted(
        table,
        key=lambda row: (priority_rank.get(row.get("priority"), len(PRIORITIES)), _due_key(row)),
    )


def get_tasks(
    table: List[Dict[str, Optional[str]]],
    filters: Optional[Dict[str, Optional[str]]] = None,
    sort_by: Optional[str] = None,
) -> List[Dict[str, Optional[str]]]:
    result = list(table)
    filters = filters or {}
    if filters:
        result = filter_tasks(
            result,
            status=filters.get("status"),
            category=filters.get("category"),
            assigned_to=filters.get("assigned_to"),
        )

    if sort_by == "priority_due":
        result = sort_tasks_by_priority_and_due_date(result)
    elif sort_by:
        result = sorted(result, key=lambda row: row.get(sort_by) or "")
    return result


def overdue_tasks(table: List[Dict[str, Optional[str]]], today: Optional[date] = None) -> List[Dict[str, Optional[str]]]:
    today = today or date.today()
    overdue = []
    for row in table:
        due = _validate_date(row.get("due_date"), "due_date")
        if not due:
            continue
        if due < today and row.get("status") != "Completed":
            overdue.append(row)
    return overdue


def progress_percentage(table: List[Dict[str, Optional[str]]]) -> float:
    if not table:
        return 0.0
    completed = sum(1 for row in table if row.get("status") == "Completed")
    return round((completed / len(table)) * 100, 2)


def export_to_csv(table: List[Dict[str, Optional[str]]], path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        for row in table:
            writer.writerow({col: row.get(col, "") or "" for col in COLUMNS})


def import_from_csv(path: str) -> List[Dict[str, Optional[str]]]:
    new_table: List[Dict[str, Optional[str]]] = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != COLUMNS:
            raise ValueError("CSV columns do not match expected schema")
        for row in reader:
            cleaned = {col: (row.get(col) or None) for col in COLUMNS}
            if any(existing["task_id"] == cleaned["task_id"] for existing in new_table):
                raise ValueError("Duplicate task_id found in CSV")
            cleaned["last_updated"] = cleaned.get("last_updated") or _now_iso()
            _validate_task_record(cleaned, new_table + [cleaned])
            new_table.append(cleaned)
    return new_table


# --------------------------- CLI Utilities ---------------------------
def _print_tasks(table: List[Dict[str, Optional[str]]]) -> None:
    if not table:
        print("No tasks to display.")
        return
    columns_to_show = [
        "task_id",
        "task_name",
        "priority",
        "status",
        "start_date",
        "due_date",
        "completion_date",
        "assigned_to",
        "category",
        "last_updated",
    ]
    widths = {col: max(len(col), *(len(str(row.get(col, ""))) for row in table)) for col in columns_to_show}
    header = " | ".join(col.ljust(widths[col]) for col in columns_to_show)
    print(header)
    print("-" * len(header))
    for row in table:
        line = " | ".join(str(row.get(col, "") or "").ljust(widths[col]) for col in columns_to_show)
        print(line)


def _input_date(prompt: str) -> Optional[str]:
    value = input(prompt).strip()
    if not value:
        return None
    _validate_date(value, prompt.strip(": "))
    return value


def cli_loop() -> None:
    table: List[Dict[str, Optional[str]]] = []
    while True:
        print(
            """
Task Manager
1) Add task
2) Update task
3) List tasks (with optional filter)
4) Show overdue tasks
5) Show progress %
6) Export to CSV
7) Import from CSV
0) Exit
"""
        )
        choice = input("Select an option: ").strip()

        try:
            if choice == "1":
                task_data = {
                    "task_name": input("Task name: ").strip(),
                    "description": input("Description: ").strip(),
                    "category": input("Category: ").strip() or None,
                    "priority": input(f"Priority {PRIORITIES}: ").strip(),
                    "status": input(f"Status {STATUSES}: ").strip(),
                    "start_date": _input_date("Start date (YYYY-MM-DD, optional): "),
                    "due_date": _input_date("Due date (YYYY-MM-DD, optional): "),
                    "completion_date": _input_date("Completion date (YYYY-MM-DD, optional): "),
                    "assigned_to": input("Assigned to (optional): ").strip() or None,
                    "notes": input("Notes: ").strip(),
                }
                table = add_task(table, task_data)
                print("Task added.\n")

            elif choice == "2":
                task_id = input("Task ID to update: ").strip()
                print("Leave a field blank to skip updating it.")
                updates = {}
                for field in ["task_name", "description", "category", "priority", "status", "assigned_to", "notes"]:
                    val = input(f"{field}: ").strip()
                    if val:
                        updates[field] = val
                for field in ["start_date", "due_date", "completion_date"]:
                    val = _input_date(f"{field} (YYYY-MM-DD): ")
                    if val is not None:
                        updates[field] = val
                table = update_task(table, task_id, updates)
                print("Task updated.\n")

            elif choice == "3":
                print("Apply filters (press Enter to skip):")
                filters = {
                    "status": input("Status filter: ").strip() or None,
                    "category": input("Category filter: ").strip() or None,
                    "assigned_to": input("Assigned_to filter: ").strip() or None,
                }
                sort_choice = input("Sort by priority & due date? (y/N): ").strip().lower()
                sort_key = "priority_due" if sort_choice == "y" else None
                filtered = get_tasks(table, filters={k: v for k, v in filters.items() if v}, sort_by=sort_key)
                _print_tasks(filtered)

            elif choice == "4":
                overdue = overdue_tasks(table)
                _print_tasks(overdue)

            elif choice == "5":
                print(f"Progress: {progress_percentage(table)}%\n")

            elif choice == "6":
                path = input("Export CSV path: ").strip()
                export_to_csv(table, path)
                print(f"Exported to {path}.\n")

            elif choice == "7":
                path = input("Import CSV path: ").strip()
                table = import_from_csv(path)
                print("Imported tasks.\n")

            elif choice == "0":
                print("Goodbye!")
                break

            else:
                print("Invalid option.\n")
        except Exception as exc:  # pragma: no cover - CLI guardrail
            print(f"Error: {exc}\n")


def main() -> None:
    cli_loop()


if __name__ == "__main__":
    main()
