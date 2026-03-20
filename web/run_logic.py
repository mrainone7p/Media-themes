"""Run orchestration and task history for Media Tracks.

Extracted from web/logic.py to keep logic.py focused on ledger operations,
scheduling, health checks, and golden source/theme handling.

This module owns: RunManager, task recording, run history parsing,
and the global RUN_MANAGER singleton.
"""

from __future__ import annotations

import atexit
import json
import os
import queue
import re
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from shared.storage import CONFIG_PATH, now_str

LOGS_DIR = Path("/app/logs")
RUNS_DIR = LOGS_DIR / "runs"
TASKS_FILE = LOGS_DIR / "task_history.jsonl"
SCRIPT_MODULE = "script.media_tracks"

for path in (RUNS_DIR,):
    path.mkdir(parents=True, exist_ok=True)


def record_task(task_name, status="success", scope="", summary="", details=None, duration_seconds=None):
    entry = {
        "time": now_str(),
        "task": str(task_name or "Task"),
        "status": str(status or "success"),
        "outcome": str(status or "success"),
        "scope": str(scope or ""),
        "summary": str(summary or ""),
        "details": details or {},
        "duration_seconds": float(duration_seconds or 0),
    }
    try:
        with open(TASKS_FILE, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


RUN_TASK_NAMES = {
    "Run Pipeline",
    "Run Scan Now",
    "Run Find Sources Now",
    "Run Download Themes Now",
}


def _task_name_for_pass(run_pass: int) -> str:
    return {
        1: "Scan Libraries",
        2: "Find Sources",
        3: "Download Themes",
    }.get(run_pass, "Pipeline Run")


def _run_history_entry(run: dict) -> dict:
    run_pass = int(run.get("pass") or 0)
    run_status = str(run.get("status") or run.get("outcome") or "success")
    libraries = [str(name).strip() for name in (run.get("libraries") or []) if str(name).strip()]
    return {
        "time": run.get("time", ""),
        "task": _task_name_for_pass(run_pass),
        "status": run_status,
        "outcome": run_status,
        "scope": str(run.get("scope") or ""),
        "summary": run.get("summary", ""),
        "details": {
            "pass": run_pass,
            "stats": run.get("stats", {}),
            "return_code": run.get("return_code"),
            "stop_requested": bool(run.get("stop_requested")),
            "libraries": libraries,
        },
        "duration_seconds": run.get("duration_seconds") or 0,
        "is_run_history": True,
    }


def _is_legacy_run_task_entry(entry: dict) -> bool:
    details = entry.get("details")
    return (
        isinstance(details, dict)
        and int(details.get("pass") or 0) > 0
        and str(entry.get("task") or "") in RUN_TASK_NAMES
    )


def load_task_entries(limit=250):
    entries = []
    if TASKS_FILE.exists():
        for line in TASKS_FILE.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                if _is_legacy_run_task_entry(entry):
                    continue
                entries.append(entry)
            except Exception:
                continue
    run_entries = []
    for run_file in sorted(RUNS_DIR.glob("*.json")):
        try:
            run = json.loads(run_file.read_text(encoding="utf-8"))
            run_entries.append(_run_history_entry(run))
        except Exception:
            continue
    return sorted(entries + run_entries, key=lambda entry: entry.get("time", ""), reverse=True)[: max(1, int(limit or 250))]


def parse_run_stats(lines: Iterable[str]) -> dict:
    stats = {}
    for line in lines:
        if "Pass 1 complete" in line:
            match = re.search(r"Total:\s*(\d+)\s*Have theme:\s*(\d+)\s*Missing:\s*(\d+)\s*Staged:\s*(\d+)\s*Approved:\s*(\d+)\s*New:\s*(\d+)\s*Removed:\s*(\d+)", line)
            if match:
                stats["pass1"] = {"total": int(match.group(1)), "has_theme": int(match.group(2)), "missing": int(match.group(3)), "staged": int(match.group(4)), "approved": int(match.group(5)), "new": int(match.group(6)), "removed": int(match.group(7))}
        if "Pass 2 complete" in line:
            match = re.search(r"Staged:\s*(\d+)\s*Missing:\s*(\d+)\s*Failed:\s*(\d+)", line)
            if match:
                stats["pass2"] = {"staged": int(match.group(1)), "missing": int(match.group(2)), "failed": int(match.group(3))}
        if "Pass 3 complete" in line:
            match = re.search(r"Available:\s*(\d+)\s*Failed:\s*(\d+)\s*Skipped:\s*(\d+)", line)
            if match:
                stats["pass3"] = {"available": int(match.group(1)), "failed": int(match.group(2)), "skipped": int(match.group(3))}
    return stats


# ── Run orchestration ────────────────────────────────────────────────────────

@dataclass
class RunManager:
    lock: threading.Lock = field(default_factory=threading.Lock)
    active: bool = False
    clients: list = field(default_factory=list)
    proc: subprocess.Popen | None = None
    stop_requested: bool = False
    started_at: float | None = None
    last_line: str = ""
    current_pass: int = 0
    scope_label: str = ""
    libraries: list[str] = field(default_factory=list)
    last_outcome: str | None = None
    last_return_code: int | None = None
    last_summary: str = ""
    completed_at: float | None = None

    def broadcast(self, message: str):
        dead = []
        for client in self.clients:
            try:
                try:
                    client.put_nowait(message)
                except Exception:
                    try:
                        client.get_nowait()
                        client.put_nowait(message)
                    except Exception:
                        dead.append(client)
                        continue
            except Exception:
                dead.append(client)
        for client in dead:
            try:
                self.clients.remove(client)
            except Exception:
                pass

    def start(self, *, force_pass: int = 0, explicit_libraries=None, scope_label: str = "", allow_schedule_disabled: bool = False):
        explicit_libraries = [str(name).strip() for name in (explicit_libraries or []) if str(name).strip()]
        with self.lock:
            if self.active:
                return False
            self.active = True
        thread = threading.Thread(
            target=self._do_run,
            kwargs={
                "force_pass": force_pass,
                "explicit_libraries": explicit_libraries,
                "scope_label": scope_label,
                "allow_schedule_disabled": allow_schedule_disabled,
            },
            daemon=True,
        )
        thread.start()
        return True

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.stop_requested = True
            try:
                self.proc.send_signal(signal.SIGTERM)
                self.broadcast("[STOP] Graceful stop requested…")
            except Exception:
                self.proc.kill()
            return True
        return False

    def event_stream(self):
        client = queue.Queue(maxsize=8000)
        self.clients.append(client)
        try:
            while True:
                try:
                    message = client.get(timeout=5)
                except queue.Empty:
                    if self.active:
                        yield ": heartbeat\n\n"
                        continue
                    break
                yield f"data: {message}\n\n"
                if message == "__DONE__":
                    break
        finally:
            try:
                self.clients.remove(client)
            except Exception:
                pass

    def history(self):
        runs = []
        for run_file in sorted(RUNS_DIR.glob("*.json")):
            try:
                runs.append(json.loads(run_file.read_text()))
            except Exception:
                pass
        return runs

    def status(self):
        return {
            "active": self.active,
            "started_at": self.started_at,
            "pass": self.current_pass,
            "last_line": self.last_line,
            "scope": self.scope_label,
            "libraries": self.libraries,
            "outcome": self.last_outcome,
            "return_code": self.last_return_code,
            "summary": self.last_summary,
            "completed_at": self.completed_at,
        }

    def _do_run(self, *, force_pass=0, explicit_libraries=None, scope_label="", allow_schedule_disabled=False):
        run_log = []
        timestamp = now_str()
        run_pass = force_pass or 0
        proc = None
        return_code = None
        explicit_libraries = [str(name).strip() for name in (explicit_libraries or []) if str(name).strip()]
        resolved_scope = scope_label or (
            explicit_libraries[0] if len(explicit_libraries) == 1 else f"{len(explicit_libraries)} selected libraries" if explicit_libraries else "scheduled libraries"
        )
        try:
            self.started_at = time.time()
            self.completed_at = None
            self.last_line = ""
            self.current_pass = run_pass
            self.stop_requested = False
            self.scope_label = resolved_scope
            self.libraries = list(explicit_libraries)
            self.last_outcome = None
            self.last_return_code = None
            self.last_summary = ""
            env = {**os.environ, "CONFIG_PATH": CONFIG_PATH}
            if force_pass:
                env["FORCE_PASS"] = str(force_pass)
            else:
                env.pop("FORCE_PASS", None)
            if allow_schedule_disabled:
                env["RUN_SCHEDULE_NOW"] = "1"
            else:
                env.pop("RUN_SCHEDULE_NOW", None)
            if explicit_libraries:
                env["RUN_LIBRARIES"] = json.dumps(explicit_libraries)
            else:
                env.pop("RUN_LIBRARIES", None)
            proc = subprocess.Popen([sys.executable, "-m", SCRIPT_MODULE], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, env=env)
            self.proc = proc
            for line in proc.stdout:
                line = line.rstrip()
                run_log.append(line)
                self.broadcast(line)
                self.last_line = line
                if "Pass 1" in line:
                    run_pass = max(run_pass, 1)
                if "Pass 2" in line:
                    run_pass = max(run_pass, 2)
                if "Pass 3" in line:
                    run_pass = max(run_pass, 3)
                self.current_pass = run_pass
            return_code = proc.wait()
        except Exception as exc:
            message = f"[ERROR] {exc}"
            run_log.append(message)
            self.broadcast(message)
        finally:
            stop_requested = self.stop_requested
            if return_code == 0 and not stop_requested:
                outcome = "success"
            elif stop_requested:
                outcome = "stopped"
            else:
                outcome = "error"
            self.proc = None
            duration = time.time() - self.started_at if self.started_at else None
            summary = next((line for line in reversed(run_log) if any(marker in line for marker in ["complete", "caught up", "nothing to do", "processed", "STOP", "ERROR"])), "No output")
            self.last_outcome = outcome
            self.last_return_code = return_code
            self.last_summary = summary
            self.completed_at = time.time()
            self.stop_requested = False
            self.broadcast("__DONE__")
            self.active = False
            stats = parse_run_stats(run_log)
            run_file = RUNS_DIR / (timestamp.replace(":", "-").replace(" ", "_") + ".json")
            run_file.write_text(json.dumps({
                "time": timestamp,
                "pass": run_pass,
                "scope": resolved_scope,
                "libraries": explicit_libraries,
                "summary": summary,
                "status": outcome,
                "outcome": outcome,
                "log": "\n".join(run_log),
                "duration_seconds": duration,
                "stats": stats,
                "return_code": return_code,
                "stop_requested": stop_requested,
            }))
            self.started_at = None
            self.scope_label = ""
            self.libraries = []

    def cleanup(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except Exception:
                self.proc.kill()


RUN_MANAGER = RunManager()
atexit.register(RUN_MANAGER.cleanup)
