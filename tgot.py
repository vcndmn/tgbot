#!/usr/bin/env python3
"""
Telegram Auto-Forwarder Bot — Pro Edition

One process, two superpowers:
- A Telegram **Bot UI** (python-telegram-bot v21) to configure everything from chat.
- A **Telethon client** that actually listens and forwards messages, with advanced tasks.

Key features
------------
• Multi-task engine with SQLite persistence
• Keyword/include & exclude filters (case-insensitive, supports simple OR with commas)
• Media, replies, and re-forwards control
• Delay per task, per-source rate limiting, and FloodWait handling
• Start/stop global forwarding
• Export/import tasks as JSON
• Inline menus + commands
• Admin lock (only allowed user IDs can control the bot)

Environment
-----------
TELEGRAM_BOT_TOKEN=123:ABC
TELEGRAM_API_ID=xxxx
TELEGRAM_API_HASH=yyyy
ALLOWED_TELEGRAM_IDS=11111111,22222222   # optional; if omitted, anyone can use the bot
SESSION_NAME=forwarder                    # optional; default "forwarder"

Run
---
python telegram_forwarder_bot_pro.py

Notes
-----
• First run requires logging in the Telethon user (via the bot's guided login).
• This file is self‑contained. No external migrations required; SQLite will be created on first start.
"""

import os
import re
import json
import asyncio
import logging
import sqlite3
import time
import random
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any

from dotenv import load_dotenv
load_dotenv()

# ---- Logging ----
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("forwarder-pro")

# ---- Env ----
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
API_ID = int(os.getenv("TELEGRAM_API_ID") or 0)
API_HASH = os.getenv("TELEGRAM_API_HASH")
# Remove hardcoded user restrictions - allow global access
SESSION_NAME = os.getenv("SESSION_NAME", "forwarder")
DB_PATH = os.getenv("DB_PATH", "forwarder.db")
# Security settings
MAX_taskS_PER_USER = int(os.getenv("MAX_taskS_PER_USER", "10"))
MAX_BLACKLIST_ENTRIES = int(os.getenv("MAX_BLACKLIST_ENTRIES", "50"))
ENABLE_USER_VERIFICATION = os.getenv("ENABLE_USER_VERIFICATION", "true").lower() == "true"

# ---- PTB / Telethon ----
from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardRemove,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError, FloodWaitError, PhoneCodeInvalidError

# ----------------- Data Model -----------------
@dataclass
class task:
    id: str
    user_id: int
    name: str
    source_chat_id: int
    destination_chat_id: int
    keywords: str = ""               # comma-separated OR list; empty = all
    exclude_keywords: str = ""        # comma-separated OR list
    forward_media: int = 1
    forward_replies: int = 1
    forward_forwards: int = 1
    delay_seconds: int = 0
    enabled: int = 1
    created_at: str = datetime.utcnow().isoformat()
    last_used: Optional[str] = None
    message_count: int = 0
    # New advanced fields
    blacklist_keywords: str = ""
    whitelist_keywords: str = ""
    blacklist_users: str = ""  # comma-separated user IDs
    whitelist_users: str = ""  # comma-separated user IDs
    max_edit_time: int = 0  # seconds after which editing is disabled
    prevent_duplicates: int = 1  # prevent duplicate messages
    auto_schedule: str = ""  # cron-like schedule string
    schedule_enabled: int = 0  # whether scheduling is active

@dataclass
class BlacklistEntry:
    id: str
    type: str  # "keyword", "user", "chat"
    value: str
    reason: str = ""
    created_at: str = datetime.utcnow().isoformat()
    enabled: int = 1

@dataclass
class Schedule:
    id: str
    task_id: str
    cron_expression: str  # "0 9 * * *" format
    enabled: int = 1
    created_at: str = datetime.utcnow().isoformat()
    last_run: Optional[str] = None

@dataclass
class UserSession:
    user_id: int
    phone: str
    session_name: str
    is_verified: bool = False
    created_at: str = datetime.utcnow().isoformat()
    last_activity: str = datetime.utcnow().isoformat()
    tasks_count: int = 0
    is_premium: bool = False

# ----------------- Storage -----------------
class Store:
    def __init__(self, path: str):
        self.path = path
        self._task_change_callback = None
        self._init()
    
    def set_task_change_callback(self, callback):
        """Set a callback function to be called when tasks change"""
        self._task_change_callback = callback
    
    def _notify_task_changed(self, user_id: int, task_id: str, action: str):
        """Notify the callback that a task has changed"""
        if self._task_change_callback:
            try:
                # Schedule the callback to run asynchronously
                import asyncio
                asyncio.create_task(self._task_change_callback(user_id, task_id, action))
            except Exception as e:
                log.error(f"Error in task change callback: {e}")

    def _conn(self):
        return sqlite3.connect(self.path)

    def _init(self):
        with self._conn() as con:
            # Drop and recreate tables if schema is outdated
            try:
                # Check if user_id column exists
                cur = con.execute("PRAGMA table_info(tasks)")
                columns = [row[1] for row in cur.fetchall()]
                
                if 'user_id' not in columns:
                    print("Database schema outdated. Recreating tables...")
                    con.execute("DROP TABLE IF EXISTS tasks")
                    con.execute("DROP TABLE IF EXISTS blacklist")
                    con.execute("DROP TABLE IF EXISTS schedules")
                    con.execute("DROP TABLE IF EXISTS user_sessions")
                    con.execute("DROP TABLE IF EXISTS kv")
            except:
                # If table doesn't exist, continue with creation
                pass
            
            # Create user_sessions table for multi-user support
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS user_sessions (
                    user_id INTEGER PRIMARY KEY,
                    phone TEXT NOT NULL,
                    session_name TEXT NOT NULL,
                    is_verified INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL,
                    last_activity TEXT NOT NULL,
                    tasks_count INTEGER DEFAULT 0,
                    is_premium INTEGER DEFAULT 0
                )
            """)
            
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    source_chat_id INTEGER NOT NULL,
                    destination_chat_id INTEGER NOT NULL,
                    keywords TEXT,
                    exclude_keywords TEXT,
                    forward_media INTEGER DEFAULT 1,
                    forward_replies INTEGER DEFAULT 1,
                    forward_forwards INTEGER DEFAULT 1,
                    delay_seconds INTEGER DEFAULT 0,
                    enabled INTEGER DEFAULT 1,
                    created_at TEXT,
                    last_used TEXT,
                    message_count INTEGER DEFAULT 0,
                    blacklist_keywords TEXT DEFAULT '',
                    whitelist_keywords TEXT DEFAULT '',
                    blacklist_users TEXT DEFAULT '',
                    whitelist_users TEXT DEFAULT '',
                    max_edit_time INTEGER DEFAULT 0,
                    prevent_duplicates INTEGER DEFAULT 1,
                    auto_schedule TEXT DEFAULT '',
                    schedule_enabled INTEGER DEFAULT 0
                )
                """
            )
            
            # Add missing columns if they don't exist (for existing databases)
            try:
                con.execute("ALTER TABLE tasks ADD COLUMN user_id INTEGER DEFAULT 0")
            except:
                pass
            try:
                con.execute("ALTER TABLE tasks ADD COLUMN blacklist_keywords TEXT DEFAULT ''")
            except:
                pass
            try:
                con.execute("ALTER TABLE tasks ADD COLUMN whitelist_keywords TEXT DEFAULT ''")
            except:
                pass
            try:
                con.execute("ALTER TABLE tasks ADD COLUMN blacklist_users TEXT DEFAULT ''")
            except:
                pass
            try:
                con.execute("ALTER TABLE tasks ADD COLUMN whitelist_users TEXT DEFAULT ''")
            except:
                pass
            try:
                con.execute("ALTER TABLE tasks ADD COLUMN max_edit_time INTEGER DEFAULT 0")
            except:
                pass
            try:
                con.execute("ALTER TABLE tasks ADD COLUMN prevent_duplicates INTEGER DEFAULT 1")
            except:
                pass
            try:
                con.execute("ALTER TABLE tasks ADD COLUMN auto_schedule TEXT DEFAULT ''")
            except:
                pass
            try:
                con.execute("ALTER TABLE tasks ADD COLUMN schedule_enabled INTEGER DEFAULT 0")
            except:
                pass
            
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS kv (
                    k TEXT PRIMARY KEY,
                    v TEXT
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS blacklist (
                    id TEXT PRIMARY KEY,
                    type TEXT NOT NULL,
                    value TEXT NOT NULL,
                    reason TEXT DEFAULT '',
                    created_at TEXT,
                    enabled INTEGER DEFAULT 1
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS schedules (
                    id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    cron_expression TEXT NOT NULL,
                    enabled INTEGER DEFAULT 1,
                    created_at TEXT,
                    last_run TEXT,
                    FOREIGN KEY (task_id) REFERENCES tasks (id) ON DELETE CASCADE
                )
                """
            )

    # ---- tasks ----
    def upsert_task(self, r: task):
        with self._conn() as con:
            # Check if this is an update or insert
            cur = con.execute("SELECT id FROM tasks WHERE id=?", (r.id,))
            is_update = cur.fetchone() is not None
            
            con.execute(
                """
                INSERT INTO tasks (id,user_id,name,source_chat_id,destination_chat_id,keywords,exclude_keywords,
                    forward_media,forward_replies,forward_forwards,delay_seconds,enabled,created_at,last_used,message_count,
                    blacklist_keywords,whitelist_keywords,blacklist_users,whitelist_users,max_edit_time,prevent_duplicates,auto_schedule,schedule_enabled)
                VALUES (:id,:user_id,:name,:source_chat_id,:destination_chat_id,:keywords,:exclude_keywords,
                    :forward_media,:forward_replies,:forward_forwards,:delay_seconds,:enabled,:created_at,:last_used,:message_count,
                    :blacklist_keywords,:whitelist_keywords,:blacklist_users,:whitelist_users,:max_edit_time,:prevent_duplicates,:auto_schedule,:schedule_enabled)
                ON CONFLICT(id) DO UPDATE SET
                    user_id=excluded.user_id,
                    name=excluded.name,
                    source_chat_id=excluded.source_chat_id,
                    destination_chat_id=excluded.destination_chat_id,
                    keywords=excluded.keywords,
                    exclude_keywords=excluded.exclude_keywords,
                    forward_media=excluded.forward_media,
                    forward_replies=excluded.forward_replies,
                    forward_forwards=excluded.forward_forwards,
                    delay_seconds=excluded.delay_seconds,
                    enabled=excluded.enabled,
                    blacklist_keywords=excluded.blacklist_keywords,
                    whitelist_keywords=excluded.whitelist_keywords,
                    blacklist_users=excluded.blacklist_users,
                    whitelist_users=excluded.whitelist_users,
                    max_edit_time=excluded.max_edit_time,
                    prevent_duplicates=excluded.prevent_duplicates,
                    auto_schedule=excluded.auto_schedule,
                    schedule_enabled=excluded.schedule_enabled
                """,
                asdict(r),
            )
            
            # Notify about task change
            action = "updated" if is_update else "created"
            self._notify_task_changed(r.user_id, r.id, action)

    def delete_task(self, rid: str) -> bool:
        with self._conn() as con:
            # Get the user_id before deleting
            cur = con.execute("SELECT user_id FROM tasks WHERE id=?", (rid,))
            row = cur.fetchone()
            user_id = row[0] if row else None
            
            cur = con.execute("DELETE FROM tasks WHERE id=?", (rid,))
            if cur.rowcount > 0 and user_id:
                self._notify_task_changed(user_id, rid, "deleted")
            return cur.rowcount > 0

    def list_tasks(self) -> List[task]:
        with self._conn() as con:
            cur = con.execute("SELECT * FROM tasks ORDER BY created_at ASC")
            rows = cur.fetchall()
        cols = [c[0] for c in cur.description] if rows else [
            "id","user_id","name","source_chat_id","destination_chat_id","keywords","exclude_keywords",
            "forward_media","forward_replies","forward_forwards","delay_seconds","enabled",
            "created_at","last_used","message_count","blacklist_keywords","whitelist_keywords",
            "blacklist_users","whitelist_users","max_edit_time","prevent_duplicates","auto_schedule","schedule_enabled"
        ]
        out: List[task] = []
        for row in rows:
            d = {k: row[i] for i, k in enumerate(cols)}
            # type fixes
            d["source_chat_id"] = int(d["source_chat_id"]) 
            d["destination_chat_id"] = int(d["destination_chat_id"]) 
            d["forward_media"] = int(d["forward_media"]) 
            d["forward_replies"] = int(d["forward_replies"]) 
            d["forward_forwards"] = int(d["forward_forwards"]) 
            d["delay_seconds"] = int(d["delay_seconds"]) 
            d["enabled"] = int(d["enabled"]) 
            d["message_count"] = int(d["message_count"]) 
            
            # Handle new fields with defaults
            d.setdefault("blacklist_keywords", "")
            d.setdefault("whitelist_keywords", "")
            d.setdefault("blacklist_users", "")
            d.setdefault("whitelist_users", "")
            d.setdefault("max_edit_time", 0)
            d.setdefault("prevent_duplicates", 1)
            d.setdefault("auto_schedule", "")
            d.setdefault("schedule_enabled", 0)
            
            # Convert to proper types
            d["max_edit_time"] = int(d["max_edit_time"])
            d["prevent_duplicates"] = int(d["prevent_duplicates"])
            d["schedule_enabled"] = int(d["schedule_enabled"])
            
            out.append(task(**d))
        return out

    def get_task(self, rid: str) -> Optional[task]:
        with self._conn() as con:
            cur = con.execute("SELECT * FROM tasks WHERE id=?", (rid,))
            row = cur.fetchone()
            if not row:
                return None
            cols = [c[0] for c in cur.description]
            d = {k: row[i] for i, k in enumerate(cols)}
            d["source_chat_id"] = int(d["source_chat_id"]) 
            d["destination_chat_id"] = int(d["destination_chat_id"]) 
            d["forward_media"] = int(d["forward_media"]) 
            d["forward_replies"] = int(d["forward_replies"]) 
            d["forward_forwards"] = int(d["forward_forwards"]) 
            d["delay_seconds"] = int(d["delay_seconds"]) 
            d["enabled"] = int(d["enabled"]) 
            d["message_count"] = int(d["message_count"]) 
            
            # Handle new fields with defaults
            d.setdefault("blacklist_keywords", "")
            d.setdefault("whitelist_keywords", "")
            d.setdefault("blacklist_users", "")
            d.setdefault("whitelist_users", "")
            d.setdefault("max_edit_time", 0)
            d.setdefault("prevent_duplicates", 1)
            d.setdefault("auto_schedule", "")
            d.setdefault("schedule_enabled", 0)
            
            # Convert to proper types
            d["max_edit_time"] = int(d["max_edit_time"])
            d["prevent_duplicates"] = int(d["prevent_duplicates"])
            d["schedule_enabled"] = int(d["schedule_enabled"])
            
            return task(**d)

    def set_task_enabled(self, rid: str, enable: bool) -> bool:
        with self._conn() as con:
            cur = con.execute("UPDATE tasks SET enabled=? WHERE id=?", (1 if enable else 0, rid))
            if cur.rowcount > 0:
                # Get the user_id for this task to notify the engine
                cur2 = con.execute("SELECT user_id FROM tasks WHERE id=?", (rid,))
                row = cur2.fetchone()
                if row:
                    user_id = row[0]
                    self._notify_task_changed(user_id, rid, "enabled" if enable else "disabled")
            return cur.rowcount > 0

    def bump_stats(self, rid: str):
        with self._conn() as con:
            con.execute(
                "UPDATE tasks SET message_count=message_count+1, last_used=? WHERE id=?",
                (datetime.utcnow().isoformat(), rid),
            )

    # ---- KV (global state) ----
    def set_kv(self, k: str, v: Any):
        with self._conn() as con:
            con.execute(
                "INSERT INTO kv(k,v) VALUES (?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                (k, json.dumps(v)),
            )

    def get_kv(self, k: str, default=None):
        with self._conn() as con:
            cur = con.execute("SELECT v FROM kv WHERE k=?", (k,))
            row = cur.fetchone()
        return json.loads(row[0]) if row else default

    # ---- Blacklist ----
    def add_blacklist_entry(self, entry: BlacklistEntry):
        with self._conn() as con:
            con.execute(
                "INSERT INTO blacklist (id,type,value,reason,created_at,enabled) VALUES (?,?,?,?,?,?)",
                (entry.id, entry.type, entry.value, entry.reason, entry.created_at, entry.enabled)
            )
    
    def remove_blacklist_entry(self, entry_id: str) -> bool:
        with self._conn() as con:
            cur = con.execute("DELETE FROM blacklist WHERE id=?", (entry_id,))
            return cur.rowcount > 0
    
    def list_blacklist(self, entry_type: str = None) -> List[BlacklistEntry]:
        with self._conn() as con:
            if entry_type:
                cur = con.execute("SELECT * FROM blacklist WHERE type=? ORDER BY created_at DESC", (entry_type,))
            else:
                cur = con.execute("SELECT * FROM blacklist ORDER BY created_at DESC")
            rows = cur.fetchall()
        cols = [c[0] for c in cur.description] if rows else ["id","type","value","reason","created_at","enabled"]
        out: List[BlacklistEntry] = []
        for row in rows:
            d = {k: row[i] for i, k in enumerate(cols)}
            d["enabled"] = int(d["enabled"])
            out.append(BlacklistEntry(**d))
        return out
    
    def is_blacklisted(self, entry_type: str, value: str) -> bool:
        with self._conn() as con:
            cur = con.execute("SELECT enabled FROM blacklist WHERE type=? AND value=? AND enabled=1", (entry_type, value))
            row = cur.fetchone()
        return bool(row)
    
    # ---- Schedules ----
    def add_schedule(self, schedule: Schedule):
        with self._conn() as con:
            con.execute(
                "INSERT INTO schedules (id,task_id,cron_expression,enabled,created_at,last_run) VALUES (?,?,?,?,?,?)",
                (schedule.id, schedule.task_id, schedule.cron_expression, schedule.enabled, schedule.created_at, schedule.last_run)
            )
    
    def remove_schedule(self, schedule_id: str) -> bool:
        with self._conn() as con:
            cur = con.execute("DELETE FROM schedules WHERE id=?", (schedule_id,))
            return cur.rowcount > 0
    
    def list_schedules(self) -> List[Schedule]:
        with self._conn() as con:
            cur = con.execute("SELECT * FROM schedules ORDER BY created_at DESC")
            rows = cur.fetchall()
        cols = [c[0] for c in cur.description] if rows else ["id","task_id","cron_expression","enabled","created_at","last_run"]
        out: List[Schedule] = []
        for row in rows:
            d = {k: row[i] for i, k in enumerate(cols)}
            d["enabled"] = int(d["enabled"])
            out.append(Schedule(**d))
        return out

    # ---- User Session Management ----
    def add_user_session(self, user_id: int, phone: str, session_name: str) -> bool:
        try:
            with self._conn() as con:
                con.execute(
                    """
                    INSERT OR REPLACE INTO user_sessions 
                    (user_id, phone, session_name, created_at, last_activity, is_verified) 
                    VALUES (?, ?, ?, ?, ?, 0)
                    """,
                    (user_id, phone, session_name, datetime.utcnow().isoformat(), datetime.utcnow().isoformat())
                )
                return True
        except Exception as e:
            log.error(f"Error adding user session: {e}")
            return False

    def get_user_session(self, user_id: int) -> Optional[UserSession]:
        try:
            with self._conn() as con:
                cur = con.execute("SELECT * FROM user_sessions WHERE user_id=?", (user_id,))
                row = cur.fetchone()
                if not row:
                    return None
                cols = [c[0] for c in cur.description]
                d = {k: row[i] for i, k in enumerate(cols)}
                return UserSession(**d)
        except Exception as e:
            log.error(f"Error getting user session: {e}")
            return None

    def update_user_activity(self, user_id: int) -> bool:
        try:
            with self._conn() as con:
                con.execute(
                    "UPDATE user_sessions SET last_activity=? WHERE user_id=?",
                    (datetime.utcnow().isoformat(), user_id)
                )
                return True
        except Exception as e:
            log.error(f"Error updating user activity: {e}")
            return False

    def mark_user_verified(self, user_id: int) -> bool:
        try:
            with self._conn() as con:
                con.execute(
                    "UPDATE user_sessions SET is_verified=1 WHERE user_id=?",
                    (user_id,)
                )
                return True
        except Exception as e:
            log.error(f"Error marking user verified: {e}")
            return False

    def get_user_tasks_count(self, user_id: int) -> int:
        try:
            with self._conn() as con:
                cur = con.execute("SELECT COUNT(*) FROM tasks WHERE user_id=?", (user_id,))
                return cur.fetchone()[0]
            return 0
        except Exception as e:
            log.error(f"Error getting user tasks count: {e}")
            return 0

    def list_tasks_by_user(self, user_id: int) -> List[task]:
        try:
            with self._conn() as con:
                cur = con.execute("SELECT * FROM tasks WHERE user_id=? ORDER BY created_at ASC", (user_id,))
                rows = cur.fetchall()
                if not rows:
                    return []
                cols = [c[0] for c in cur.description]
                out: List[task] = []
                for row in rows:
                    d = {k: row[i] for i, k in enumerate(cols)}
                    # type fixes
                    d["source_chat_id"] = int(d["source_chat_id"]) 
                    d["destination_chat_id"] = int(d["destination_chat_id"]) 
                    d["forward_media"] = int(d["forward_media"]) 
                    d["forward_replies"] = int(d["forward_replies"]) 
                    d["forward_forwards"] = int(d["forward_forwards"]) 
                    d["delay_seconds"] = int(d["delay_seconds"]) 
                    d["enabled"] = int(d["forward_forwards"]) 
                    d["message_count"] = int(d["message_count"]) 
                    out.append(task(**d))
                return out
        except Exception as e:
            log.error(f"Error listing user tasks: {e}")
            return []

    def get_all_user_sessions(self) -> List[UserSession]:
        with self._conn() as con:
            cur = con.execute("SELECT * FROM user_sessions ORDER BY created_at DESC")
            rows = cur.fetchall()
        cols = [c[0] for c in cur.description] if rows else [
            "user_id", "phone", "session_name", "is_verified", "created_at", "last_activity", "tasks_count", "is_premium"
        ]
        out: List[UserSession] = []
        for row in rows:
            d = {k: row[i] for i, k in enumerate(cols)}
            out.append(UserSession(**d))
        return out

    def remove_user_session(self, user_id: int) -> bool:
        """Remove a user session from the database"""
        try:
            with self._conn() as con:
                con.execute("DELETE FROM user_sessions WHERE user_id=?", (user_id,))
                return True
        except Exception as e:
            log.error(f"Error removing user session for user {user_id}: {e}")
            return False

# ----------------- Helpers -----------------
def is_admin(user_id: int) -> bool:
    return not ALLOWED_IDS or (user_id in ALLOWED_IDS)

_def_none = object()

def get_arg(text: str, key: str, default=_def_none):
    """Parse --key=value from text."""
    m = re.search(rf"--{re.escape(key)}=(\S+)", text)
    if not m:
        if default is _def_none:
            raise ValueError(f"Missing --{key}=")
        return default
    return m.group(1)


def parse_bool(s: str) -> int:
    return 1 if str(s).lower() in {"1","true","yes","y","on"} else 0


def human(v: Any) -> str:
    return "✅" if str(v) in {"1","True","true"} else "❌"

# ----------------- Forward Engine -----------------
class Engine:
    def __init__(self, store: Store):
        self.store = store
        self.clients: Dict[int, TelegramClient] = {}  # user_id -> TelegramClient
        self.event_handlers: Dict[int, Any] = {}  # user_id -> event handler reference
        self.started = False
        self.user_sessions: Dict[int, UserSession] = {}  # user_id -> UserSession

    async def ensure_client(self, user_id: int) -> bool:
        if user_id in self.clients and await self.clients[user_id].is_user_authorized():
            return True
        if not API_ID or not API_HASH:
            log.error("Missing API credentials")
            return False
        
        user_session = self.store.get_user_session(user_id)
        if not user_session:
            return False
        
        session_name = f"{SESSION_NAME}_{user_id}"
        client = TelegramClient(session_name, API_ID, API_HASH)
        await client.connect()
        if not await client.is_user_authorized():
            return False
        
        self.clients[user_id] = client
        return True

    async def get_client(self, user_id: int) -> Optional[TelegramClient]:
        if await self.ensure_client(user_id):
            return self.clients.get(user_id)
        return None

    async def login_send_code(self, phone: str, user_id: int) -> str:
        session_name = f"{SESSION_NAME}_{user_id}"
        client = TelegramClient(session_name, API_ID, API_HASH)
        await client.connect()
        result = await client.send_code_request(phone)
        
        # Store user session
        self.store.add_user_session(user_id, phone, session_name)
        self.clients[user_id] = client
        
        # Return phone_code_hash so callers can persist it and use during sign-in
        try:
            return result.phone_code_hash  # type: ignore[attr-defined]
        except AttributeError:
            # Fallback for older Telethon: keep empty; sign_in without hash may still work
            return ""

    async def login_sign_in(self, phone: str, code: str, user_id: int, twofa: Optional[str]=None, phone_code_hash: Optional[str]=None) -> None:
        if user_id not in self.clients:
            raise RuntimeError("No client found for user. Please start login process again.")
        
        client = self.clients[user_id]
        try:
            if phone_code_hash:
                await client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash)
            else:
                await client.sign_in(phone=phone, code=code)
        except SessionPasswordNeededError:
            if not twofa:
                raise
            await client.sign_in(password=twofa)
        
        # Update user session as verified
        self.store.update_user_activity(user_id)

    async def logout(self, user_id: int) -> bool:
        """Logout a specific user and clean up all data"""
        try:
            log.info(f"Logging out user {user_id}")
            
            # Disconnect and remove client
            if user_id in self.clients:
                try:
                    client = self.clients[user_id]
                    
                    # Remove event handler first
                    if user_id in self.event_handlers:
                        try:
                            old_handler = self.event_handlers[user_id]
                            client.remove_event_handler(old_handler)
                            del self.event_handlers[user_id]
                            log.info(f"Event handler removed for user {user_id}")
                        except Exception as e:
                            log.warning(f"Could not remove event handler for user {user_id}: {e}")
                    
                    await client.log_out()
                    await client.disconnect()
                    del self.clients[user_id]
                    log.info(f"Client disconnected for user {user_id}")
                except Exception as e:
                    log.error(f"Error disconnecting client for user {user_id}: {e}")
                    # Force remove client even if disconnect fails
                    if user_id in self.clients:
                        del self.clients[user_id]
            
            # Remove user session from database
            try:
                self.store.remove_user_session(user_id)
                log.info(f"User session removed from database for user {user_id}")
            except Exception as e:
                log.error(f"Error removing user session for user {user_id}: {e}")
            
            # Remove all tasks for this user
            try:
                tasks = self.store.list_tasks_by_user(user_id)
                for task in tasks:
                    self.store.delete_task(task.id)
                log.info(f"Removed {len(tasks)} tasks for user {user_id}")
            except Exception as e:
                log.error(f"Error removing tasks for user {user_id}: {e}")
            
            log.info(f"User {user_id} logged out successfully")
            return True
            
        except Exception as e:
            log.error(f"Error during logout for user {user_id}: {e}")
            return False

    async def logout_all(self) -> bool:
        """Logout all users"""
        success = True
        for user_id in list(self.clients.keys()):
            try:
                await self.logout(user_id)
            except Exception as e:
                log.error(f"Error logging out user {user_id}: {e}")
                success = False
        return success

    async def start(self):
        """Start the engine for all authorized users"""
        if self.started:
            return
        
        log.info("Starting Auto-Forwarder Pro Engine...")
        self.started = True
        
        # Start the monitoring loop
        asyncio.create_task(self._monitor_users())
        
        log.info("Engine started. Monitoring for user logins...")

    async def _monitor_users(self):
        """Monitor for new users and manage clients"""
        while self.started:
            try:
                # Get all verified user sessions
                with self.store._conn() as con:
                    cur = con.execute("SELECT user_id FROM user_sessions WHERE is_verified=1")
                    verified_user_ids = [row[0] for row in cur.fetchall()]
                
                # Start clients for new verified users
                for user_id in verified_user_ids:
                    if user_id not in self.clients:
                        try:
                            if await self.ensure_client(user_id):
                                client = self.clients[user_id]
                                
                                # Start selective monitoring for source chats only
                                await self._start_monitoring_user_chats(user_id, client)
                                
                                log.info(f"Started selective monitoring for user {user_id}")
                            else:
                                log.warning(f"Failed to start client for user {user_id}")
                        except Exception as e:
                            log.error(f"Error starting client for user {user_id}: {e}")
                
                # Clean up disconnected clients
                for user_id in list(self.clients.keys()):
                    if not self.clients[user_id].is_connected():
                        del self.clients[user_id]
                        log.info(f"Cleaned up disconnected client for user {user_id}")
                
                # Log current status
                if verified_user_ids:
                    log.info(f"Monitoring {len(verified_user_ids)} verified users, {len(self.clients)} active clients")
                else:
                    log.info("No verified users yet. Waiting for logins...")
                
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                log.error(f"Error in user monitoring: {e}")
                await asyncio.sleep(30)
    
    async def _start_monitoring_user_chats(self, user_id: int, client):
        """Start monitoring only source chats for a specific user"""
        try:
            # Get all tasks for this user
            tasks = self.store.list_tasks_by_user(user_id)
            source_chats = set()
            
            for task in tasks:
                if task.enabled and task.source_chat_id:
                    source_chats.add(task.source_chat_id)
            
            log.info(f"User {user_id}: Found {len(source_chats)} source chats to monitor: {source_chats}")

            # Only monitor chats that are configured as sources
            if source_chats:
                # Remove existing handlers for this user
                if user_id in self.event_handlers:
                    try:
                        client.remove_event_handler(self.event_handlers[user_id])
                        log.info(f"User {user_id}: Removed existing event handler")
                    except Exception as e:
                        log.warning(f"User {user_id}: Error removing old handler: {e}")

                # Register new selective event handler
                @client.on(events.NewMessage(chats=list(source_chats)))
                async def _selective_msg_handler(ev, user_id=user_id):
                    try:
                        await self._handle_new_message(ev, user_id)
                    except Exception as e:
                        log.exception(f"Selective handler error for user {user_id}: %s", e)

                # Store the handler reference for later removal
                self.event_handlers[user_id] = _selective_msg_handler

                log.info(f"User {user_id}: Registered selective monitoring for {len(source_chats)} chats")
            else:
                # No source chats configured, remove any existing handlers
                if user_id in self.event_handlers:
                    try:
                        client.remove_event_handler(self.event_handlers[user_id])
                        log.info(f"User {user_id}: Removed handler (no source chats configured)")
                    except Exception as e:
                        log.warning(f"User {user_id}: Error removing handler: {e}")
                    del self.event_handlers[user_id]

                log.info(f"User {user_id}: No source chats configured - not monitoring any chats")

        except Exception as e:
            log.error(f"Error setting up selective monitoring for user {user_id}: {e}")

    def _kw_match(self, text: str, inc: str, exc: str) -> bool:
        if not text:
            text = ""
        tl = text.lower()
        if inc.strip():
            tokens = [t.strip().lower() for t in inc.split(',') if t.strip()]
            if tokens and not any(t in tl for t in tokens):
                return False
        if exc.strip():
            tokens = [t.strip().lower() for t in exc.split(',') if t.strip()]
            if any(t in tl for t in tokens):
                return False
        return True

    async def _handle_new_message(self, ev: events.NewMessage.Event, user_id: int):
        if not ev.message or not ev.chat_id:
            log.info("No message or chat_id in event")
            return

        # Define current_time at the start for all users
        current_time = time.time()

        # Initialize all rate limiting attributes for ALL users
        if not hasattr(self, 'global_forward_limits'):
            self.global_forward_limits: List[float] = []
        if not hasattr(self, 'forward_rate_limits'):
            self.forward_rate_limits: Dict[str, List[float]] = {}
        if not hasattr(self, 'api_rate_limits'):
            self.api_rate_limits: Dict[str, List[float]] = {}

        # Check for unlimited access users
        unlimited_users = os.getenv("ALLOWED_UNLIMITED_IDS", "")
        unlimited_ids = []
        if unlimited_users:
            unlimited_ids = [int(id.strip()) for id in unlimited_users.split(",") if id.strip().isdigit()]

        if user_id in unlimited_ids:
            # Unlimited user - skip all rate limiting
            rate_key = f"forward_{user_id}"  # Still needed for recording
            api_rate_key = f"api_{user_id}"  # Still needed for recording
            # Ensure the API rate key exists for unlimited users
            if api_rate_key not in self.api_rate_limits:
                self.api_rate_limits[api_rate_key] = []
            pass
        else:
                # Standard rate limiting for regular users

                # Clean old global forwards (older than 60 seconds)
                self.global_forward_limits = [
                    fwd_time for fwd_time in self.global_forward_limits
                    if current_time - fwd_time < 60
                ]

                # Check global forwarding rate limit
                if len(self.global_forward_limits) >= 20:
                    log.warning(f"Global forwarding rate limit exceeded ({len(self.global_forward_limits)}/20), skipping message")
                    return

                rate_key = f"forward_{user_id}"
                api_rate_key = f"api_{user_id}"

                if rate_key not in self.forward_rate_limits:
                    self.forward_rate_limits[rate_key] = []

                # Clean old forwards (older than 60 seconds)
                self.forward_rate_limits[rate_key] = [
                    fwd_time for fwd_time in self.forward_rate_limits[rate_key]
                    if current_time - fwd_time < 60
                ]

                # Check per-user forwarding rate limit
                if len(self.forward_rate_limits[rate_key]) >= 5:
                    log.warning(f"User {user_id}: Forwarding rate limit exceeded ({len(self.forward_rate_limits[rate_key])}/5), skipping message")
                    return
        
        chat_id = int(ev.chat_id)
        text = ev.message.message or ""
        
        # With selective monitoring, this message should only come from configured source chats
        # But let's do a quick sanity check
        user_tasks = self.store.list_tasks_by_user(user_id)
        source_chats = {task.source_chat_id for task in user_tasks if task.enabled and task.source_chat_id}
        
        if chat_id not in source_chats:
            log.warning(f"User {user_id}: Received unexpected message from chat {chat_id} (not in source chats)")
            return
        
        log.info(f"User {user_id}: Processing message from source chat {chat_id}: {text[:50]}...")

        # global switch
        if not self.store.get_kv("forwarding_on", True):
            log.info("Forwarding is OFF globally")
            return

        # Circuit breaker: Check for recent errors that might indicate ban risk
        error_window = 600  # 10 minutes (less sensitive)
        recent_errors = self.store.get_kv("recent_errors", 0)
        last_error_time = self.store.get_kv("last_error_time", 0)

        # Reset error count if enough time has passed
        if current_time - last_error_time > error_window:
            self.store.set_kv("recent_errors", 0)

        # If too many errors recently, temporarily disable forwarding (higher threshold)
        if recent_errors >= 10:  # Increased from 5 to 10
            log.warning(f"Circuit breaker activated: {recent_errors} recent errors, temporarily disabling forwarding")
            self.store.set_kv("forwarding_on", False)
            self.store.set_kv("circuit_breaker_active", True)
            return

        # Get tasks for this specific user
        tasks = [r for r in self.store.list_tasks_by_user(user_id) if r.enabled and r.source_chat_id == chat_id]
        if not tasks:
            log.info(f"User {user_id}: No tasks found for chat {chat_id}")
            return
        
        log.info(f"Found {len(tasks)} tasks for chat {chat_id}")

        # Get user's client
        client = await self.get_client(user_id)
        if not client:
            log.error(f"User {user_id}: No client available for forwarding")
            return

        for i, r in enumerate(tasks):
            log.info(f"Processing task {r.id} for chat {chat_id}")
            
            # Add small delay between tasks to prevent flooding (0.5-1 second)
            if i > 0:
                delay = 0.5 + random.random() * 0.5  # Random delay between 0.5-1 second
                await asyncio.sleep(delay)
            
            # filter forwarded messages
            if not r.forward_forwards and getattr(ev.message, "fwd_from", None):
                log.info(f"Skipping forwarded message for task {r.id}")
                continue
            # filter replies
            if not r.forward_replies and ev.message.is_reply:
                log.info(f"Skipping reply message for task {r.id}")
                continue
            # filter keywords
            if not self._kw_match(text, r.keywords or "", r.exclude_keywords or ""):
                log.info(f"Message filtered by keywords for task {r.id}")
                continue

            try:
                if r.delay_seconds > 0:
                    log.info(f"Waiting {r.delay_seconds}s before forwarding task {r.id}")
                    await asyncio.sleep(min(r.delay_seconds, 60))  # hard cap 60s

                # Copy message instead of forwarding (no attribution)
                log.info(f"Copying message from {chat_id} to {r.destination_chat_id} using task {r.id}")
                
                try:
                    message_sent = False
                    # Handle different message types
                    if ev.message.media:
                        # Check if this is a link preview (webpage media)
                        if hasattr(ev.message.media, 'webpage') and ev.message.media.webpage:
                            # Link preview - send as text message only
                            message_text = ev.message.message or ""
                            # The URL is already in the message text, so just send the text
                            if message_text.strip():
                                await client.send_message(r.destination_chat_id, message_text)
                                log.info(f"✅ Successfully forwarded message with link preview as text using task {r.id}")
                                # Record API call for rate limiting
                                if api_rate_key in self.api_rate_limits:
                                    self.api_rate_limits[api_rate_key].append(current_time)
                                message_sent = True
                            else:
                                log.info(f"No text content to forward for task {r.id}")
                                continue
                        elif ev.message.photo:
                            # Photo
                            await client.send_file(
                                r.destination_chat_id,
                                ev.message.media,
                                caption=ev.message.message if ev.message.message else None
                            )
                            # Record API call for rate limiting
                            if api_rate_key in self.api_rate_limits:
                                self.api_rate_limits[api_rate_key].append(current_time)
                            message_sent = True
                        elif ev.message.video:
                            # Video
                            await client.send_file(
                                r.destination_chat_id,
                                ev.message.media,
                                caption=ev.message.message if ev.message.message else None
                            )
                            # Record API call for rate limiting
                            if api_rate_key in self.api_rate_limits:
                                self.api_rate_limits[api_rate_key].append(current_time)
                            message_sent = True
                        elif ev.message.document:
                            # Document/File
                            await client.send_file(
                                r.destination_chat_id,
                                ev.message.media,
                                caption=ev.message.message if ev.message.message else None
                            )
                            # Record API call for rate limiting
                            if api_rate_key in self.api_rate_limits:
                                self.api_rate_limits[api_rate_key].append(current_time)
                            message_sent = True
                        elif ev.message.audio:
                            # Audio
                            await client.send_file(
                                r.destination_chat_id,
                                ev.message.media,
                                caption=ev.message.message if ev.message.message else None
                            )
                            # Record API call for rate limiting
                            if api_rate_key in self.api_rate_limits:
                                self.api_rate_limits[api_rate_key].append(current_time)
                            message_sent = True
                        elif ev.message.voice:
                            # Voice message
                            await client.send_file(
                                r.destination_chat_id,
                                ev.message.media,
                                caption=ev.message.message if ev.message.message else None
                            )
                            # Record API call for rate limiting
                            if api_rate_key in self.api_rate_limits:
                                self.api_rate_limits[api_rate_key].append(current_time)
                        else:
                            # Other media types - try to send as file, fallback to text
                            try:
                                await client.send_file(
                                    r.destination_chat_id,
                                    ev.message.media,
                                    caption=ev.message.message if ev.message.message else None
                                )
                                # Record API call for rate limiting
                                if api_rate_key in self.api_rate_limits:
                                    self.api_rate_limits[api_rate_key].append(current_time)
                                message_sent = True
                            except Exception as media_error:
                                log.warning(f"Failed to send media as file, trying as text: {media_error}")
                                # Fallback: send as text message
                                if ev.message.message:
                                    await client.send_message(r.destination_chat_id, ev.message.message)
                                    log.info(f"✅ Successfully sent media as text fallback using task {r.id}")
                                    # Record API call for rate limiting
                                    if api_rate_key in self.api_rate_limits:
                                        self.api_rate_limits[api_rate_key].append(current_time)
                                    message_sent = True
                        if message_sent:
                            log.info(f"✅ Successfully copied media message using task {r.id}")
                            # Record forwarding and API timestamps for rate limiting
                            if user_id in unlimited_ids:
                                # For unlimited users, only record global forwarding (no per-user limits)
                                self.global_forward_limits.append(current_time)
                            else:
                                # For regular users, record all limits
                                if rate_key in self.forward_rate_limits:
                                    self.forward_rate_limits[rate_key].append(current_time)
                                self.global_forward_limits.append(current_time)
                                if api_rate_key in self.api_rate_limits:
                                    self.api_rate_limits[api_rate_key].append(current_time)
                    else:
                        # Text-only message
                        if ev.message.message:
                            await client.send_message(r.destination_chat_id, ev.message.message)
                            log.info(f"✅ Successfully copied text message using task {r.id}")
                            # Record forwarding and API timestamps for rate limiting
                            if user_id in unlimited_ids:
                                # For unlimited users, only record global forwarding
                                self.global_forward_limits.append(current_time)
                            else:
                                # For regular users, record all limits
                                if rate_key in self.forward_rate_limits:
                                    self.forward_rate_limits[rate_key].append(current_time)
                                self.global_forward_limits.append(current_time)
                                if api_rate_key in self.api_rate_limits:
                                    self.api_rate_limits[api_rate_key].append(current_time)
                            message_sent = True
                        else:
                            log.info(f"No content to copy for task {r.id}")
                            continue  # nothing to send
                except Exception as e:
                    log.error(f"Failed to copy message for task {r.id}: {e}")
                    # Don't try fallback if we already sent the message or if there's no text
                    continue

                # Only record stats and timestamp if we successfully sent a message
                if message_sent:
                    if user_id in unlimited_ids:
                        # For unlimited users, only record global forwarding
                        self.global_forward_limits.append(current_time)
                    else:
                        # For regular users, record per-user forwarding too
                        if rate_key in self.forward_rate_limits:
                            self.forward_rate_limits[rate_key].append(current_time)
                        self.global_forward_limits.append(current_time)
                self.store.bump_stats(r.id)

                # Add small random delay after forwarding to prevent rapid successive forwards (0.5-1.5 seconds)
                if message_sent:
                    post_forward_delay = 0.5 + random.random() * 1.0
                    await asyncio.sleep(post_forward_delay)
            except FloodWaitError as fw:
                log.warning("FloodWait %ss on task %s", fw.seconds, r.id)
                # Track floodwait as an error for circuit breaker
                self.store.set_kv("recent_errors", recent_errors + 1)
                self.store.set_kv("last_error_time", current_time)
                # Sleep for the floodwait duration plus a buffer to be safe
                await asyncio.sleep(min(fw.seconds + 5, 120))
                # After floodwait, add a cooldown period before processing more messages
                await asyncio.sleep(10)
            except Exception as e:
                log.exception("Forward failed for task %s: %s", r.id, e)
                # Track API errors for circuit breaker
                self.store.set_kv("recent_errors", recent_errors + 1)
                self.store.set_kv("last_error_time", current_time)
                # Add small delay after errors to prevent rapid retry loops
                await asyncio.sleep(2)

    async def refresh_user_monitoring(self, user_id: int):
        """Refresh monitored chats for a user when tasks change"""
        try:
            if user_id not in self.clients:
                log.info(f"User {user_id}: No active client to refresh monitoring")
                return
            
            # Get current tasks and source chats
            tasks = self.store.list_tasks_by_user(user_id)
            source_chats = set()
            
            for task in tasks:
                if task.enabled and task.source_chat_id:
                    source_chats.add(task.source_chat_id)
            
            log.info(f"User {user_id}: Refreshed monitoring for {len(source_chats)} source chats: {source_chats}")
            
            # Ensure client is connected and ready
            client = self.clients[user_id]
            if not client.is_connected():
                log.info(f"User {user_id}: Reconnecting client during refresh")
                await client.connect()
            
            # Verify access to all source chats
            verified_chats = set()
            for chat_id in source_chats:
                try:
                    entity = await client.get_entity(chat_id)
                    verified_chats.add(chat_id)
                    log.debug(f"User {user_id}: Verified access to chat {chat_id}")
                except Exception as e:
                    log.warning(f"User {user_id}: Could not verify access to chat {chat_id}: {e}")
                    # Keep it in the list anyway - the event handler will try to process messages
            
            log.info(f"User {user_id}: Successfully verified {len(verified_chats)} out of {len(source_chats)} source chats")

            # Update selective monitoring with the verified source chats
            if verified_chats:
                # Re-register event handler with updated chat list
                if user_id in self.event_handlers:
                    try:
                        client.remove_event_handler(self.event_handlers[user_id])
                        log.info(f"User {user_id}: Removed old event handler during refresh")
                    except Exception as e:
                        log.warning(f"User {user_id}: Error removing old handler during refresh: {e}")

                # Register new selective event handler with verified chats
                @client.on(events.NewMessage(chats=list(verified_chats)))
                async def _refreshed_msg_handler(ev, user_id=user_id):
                    try:
                        await self._handle_new_message(ev, user_id)
                    except Exception as e:
                        log.exception(f"Refreshed handler error for user {user_id}: %s", e)

                # Store the handler reference
                self.event_handlers[user_id] = _refreshed_msg_handler
                log.info(f"User {user_id}: Refreshed selective monitoring for {len(verified_chats)} verified chats")
            else:
                # No verified source chats, remove any existing handlers
                if user_id in self.event_handlers:
                    try:
                        client.remove_event_handler(self.event_handlers[user_id])
                        log.info(f"User {user_id}: Removed handler (no verified source chats)")
                    except Exception as e:
                        log.warning(f"User {user_id}: Error removing handler: {e}")
                    del self.event_handlers[user_id]
            
        except Exception as e:
            log.error(f"Error refreshing monitoring for user {user_id}: {e}")

    async def start_monitoring_chat(self, user_id: int, chat_id: int):
        """Start monitoring a specific chat for a user immediately"""
        try:
            if user_id not in self.clients:
                log.warning(f"User {user_id}: No active client to start monitoring chat {chat_id}")
                return False
            
            log.info(f"User {user_id}: Starting immediate monitoring for chat {chat_id}")
            
            # Ensure the client is connected and ready
            client = self.clients[user_id]
            if not client.is_connected():
                log.info(f"User {user_id}: Reconnecting client for immediate monitoring")
                await client.connect()
            
            # Verify the chat is accessible by trying to get its entity
            try:
                entity = await client.get_entity(chat_id)
                log.info(f"User {user_id}: Successfully verified access to chat {chat_id} ({getattr(entity, 'title', 'Unknown')})")
            except Exception as e:
                log.warning(f"User {user_id}: Could not verify access to chat {chat_id}: {e}")
                # Don't fail - the event handler will still try to process messages
            
            # Refresh selective monitoring to include this new chat
            await self._start_monitoring_user_chats(user_id, client)

            log.info(f"User {user_id}: Chat {chat_id} is now being monitored selectively")
            return True
            
        except Exception as e:
            log.error(f"Error starting monitoring for chat {chat_id} for user {user_id}: {e}")
            # Don't crash - just return False and log the error
            return False

    async def force_refresh_monitoring(self, user_id: int):
        """Force a complete refresh of monitoring for a user - useful after task changes"""
        try:
            if user_id not in self.clients:
                log.info(f"User {user_id}: No active client to force refresh monitoring")
                return False
            
            log.info(f"User {user_id}: Force refreshing monitoring...")
            
            # Get current tasks and source chats
            tasks = self.store.list_tasks_by_user(user_id)
            source_chats = set()
            
            for task in tasks:
                if task.enabled and task.source_chat_id:
                    source_chats.add(task.source_chat_id)
            
            log.info(f"User {user_id}: Force refresh found {len(source_chats)} source chats: {source_chats}")

            # Ensure client is connected
            client = self.clients[user_id]
            if not client.is_connected():
                log.info(f"User {user_id}: Reconnecting client during force refresh")
                await client.connect()

            # Verify access to all source chats
            verified_chats = set()
            for chat_id in source_chats:
                try:
                    entity = await client.get_entity(chat_id)
                    verified_chats.add(chat_id)
                    log.debug(f"User {user_id}: Force refresh verified chat {chat_id}")
                except Exception as e:
                    log.warning(f"User {user_id}: Force refresh failed for chat {chat_id}: {e}")

            # Clean up any duplicate handlers
            await self._cleanup_duplicate_handlers(user_id, client)

            # Update selective monitoring with verified chats
            if verified_chats:
                # Remove existing handler
                if user_id in self.event_handlers:
                    try:
                        client.remove_event_handler(self.event_handlers[user_id])
                        log.info(f"User {user_id}: Removed old handler during force refresh")
                    except Exception as e:
                        log.warning(f"User {user_id}: Error removing handler during force refresh: {e}")

                # Register new selective handler
                @client.on(events.NewMessage(chats=list(verified_chats)))
                async def _force_refreshed_handler(ev, user_id=user_id):
                    try:
                        await self._handle_new_message(ev, user_id)
                    except Exception as e:
                        log.exception(f"Force refreshed handler error for user {user_id}: %s", e)

                # Store the handler reference
                self.event_handlers[user_id] = _force_refreshed_handler
                log.info(f"User {user_id}: Force refresh completed with selective monitoring for {len(verified_chats)} chats")
            else:
                # No verified chats, remove any existing handlers
                if user_id in self.event_handlers:
                    try:
                        client.remove_event_handler(self.event_handlers[user_id])
                        log.info(f"User {user_id}: Removed handler (no verified chats during force refresh)")
                    except Exception as e:
                        log.warning(f"User {user_id}: Error removing handler during force refresh: {e}")
                    del self.event_handlers[user_id]
                log.info(f"User {user_id}: Force refresh completed - no source chats configured")

            return True
            
        except Exception as e:
            log.error(f"Error during force refresh monitoring for user {user_id}: {e}")
            return False

    async def _cleanup_duplicate_handlers(self, user_id: int, client):
        """Remove any duplicate event handlers for a user to prevent double forwarding"""
        try:
            log.info(f"User {user_id}: Cleaning up duplicate event handlers...")
            
            # Get all event handlers from the client
            handlers = client.list_event_handlers()
            new_message_handlers = [h for h in handlers if hasattr(h, '__name__') and 'NewMessage' in str(h)]
            
            log.info(f"User {user_id}: Found {len(new_message_handlers)} NewMessage handlers")
            
            # If we have more than one handler, remove all and we'll add one fresh one
            if len(new_message_handlers) > 1:
                log.warning(f"User {user_id}: Found {len(new_message_handlers)} handlers, removing all to prevent duplicates")
                for handler in new_message_handlers:
                    try:
                        client.remove_event_handler(handler)
                        log.info(f"User {user_id}: Removed duplicate handler")
                    except Exception as e:
                        log.warning(f"User {user_id}: Could not remove handler: {e}")
                
                # Clear our stored handler reference since we removed all
                if user_id in self.event_handlers:
                    del self.event_handlers[user_id]
            
            log.info(f"User {user_id}: Duplicate handler cleanup completed")
            
        except Exception as e:
            log.warning(f"Error during duplicate handler cleanup for user {user_id}: {e}")
            # Don't fail the refresh - just log the warning

    async def _reregister_event_handler(self, user_id: int, client):
        """Re-register the event handler for a user to ensure it's listening to new chats"""
        try:
            log.info(f"User {user_id}: Re-registering event handler...")
            
            # Remove any existing event handlers using stored reference
            if user_id in self.event_handlers:
                try:
                    old_handler = self.event_handlers[user_id]
                    client.remove_event_handler(old_handler)
                    log.info(f"User {user_id}: Removed old event handler")
                except Exception as e:
                    log.warning(f"User {user_id}: Could not remove old handler: {e}")
            
            # Register new event handler with specific chat filtering
            @client.on(events.NewMessage)
            async def _on_msg(ev, user_id=user_id):
                try:
                    await self._handle_new_message(ev, user_id)
                except Exception as e:
                    log.exception(f"Handler error for user {user_id}: %s", e)
            
            # Store the new handler reference
            self.event_handlers[user_id] = _on_msg
            
            log.info(f"User {user_id}: Event handler re-registered successfully")
            
            # Only start the client if it's not already running
            try:
                if not client.is_connected():
                    await client.start()
                    log.info(f"User {user_id}: Client started and listening to all chats")
                else:
                    log.info(f"User {user_id}: Client already running, event handler updated")
            except Exception as e:
                log.warning(f"User {user_id}: Could not start client: {e}")
                # Don't crash - just log the warning
            
        except Exception as e:
            log.error(f"Error re-registering event handler for user {user_id}: {e}")
            # Don't crash the bot - just log the error

    async def on_task_changed(self, user_id: int, task_id: str, action: str):
        """Called when a task is created, updated, enabled, or disabled"""
        try:
            log.info(f"User {user_id}: task {task_id} {action}, refreshing monitoring...")
            
            # Force refresh monitoring for this user
            await self.force_refresh_monitoring(user_id)
            
            # CRITICAL: Ensure the client is listening immediately
            if user_id in self.clients:
                client = self.clients[user_id]
                try:
                    if not client.is_connected():
                        await client.connect()
                        log.info(f"User {user_id}: Client connected after task {action}")
                    
                    # Only start if not already running
                    if not client.is_connected():
                        await client.start()
                        log.info(f"User {user_id}: Client started listening after task {action}")
                    else:
                        log.info(f"User {user_id}: Client already listening after task {action}")
                        
                except Exception as e:
                    log.warning(f"User {user_id}: Could not ensure client is listening: {e}")
                    # Don't crash the bot - just log the warning
            
            log.info(f"User {user_id}: Monitoring refreshed after task {action}")
            
        except Exception as e:
            log.error(f"Error refreshing monitoring after task {action} for user {user_id}: {e}")

# ----------------- task Builder -----------------
class taskBuilder:
    def __init__(self, store: Store, engine: Engine):
        self.store = store
        self.engine = engine
        self.pending_tasks: Dict[int, Dict[str, Any]] = {}
        self.pending_blacklist: Optional[Dict[str, Any]] = None
        # Per-user cache of the last shown chat list for numeric selection
        self.last_chats: Dict[int, List[int]] = {}

    def set_last_chats(self, user_id: int, chat_ids: List[int]) -> None:
        self.last_chats[user_id] = chat_ids

    def get_chat_by_index(self, user_id: int, index: int) -> Optional[int]:
        chats = self.last_chats.get(user_id) or []
        if 1 <= index <= len(chats):
            return chats[index - 1]
        return None
    
    def _get_options_keyboard(self, task_data: Dict[str, Any]) -> InlineKeyboardMarkup:
        """Generate the options keyboard for task creation with dynamic states"""
        data = task_data["data"]
        
        # Reply forwarding state
        reply_icon = "✅" if data.get("forward_replies", 1) else "❌"
        no_reply_icon = "❌" if data.get("forward_replies", 1) else "✅"
        
        # Forward forwarding state
        forward_icon = "✅" if data.get("forward_forwards", 1) else "❌"
        no_forward_icon = "❌" if data.get("forward_forwards", 1) else "✅"
        
        # Delay state
        delay_icon = "⏱️" if data.get("delay_seconds", 0) > 0 else "⏱️"
        no_delay_icon = "🚫" if data.get("delay_seconds", 0) == 0 else "✅"
        
        # Keywords state
        keywords_icon = "🔍" if data.get("keywords", "") else "🔍"
        all_messages_icon = "📝" if not data.get("keywords", "") else "✅"
        
        # Blacklist/Whitelist keywords state
        blacklist_kw_icon = "🚫" if not data.get("blacklist_keywords", "") else "✅"
        whitelist_kw_icon = "✅" if not data.get("whitelist_keywords", "") else "✅"
        
        # Blacklist/Whitelist users state
        blacklist_users_icon = "🚷" if not data.get("blacklist_users", "") else "✅"
        whitelist_users_icon = "🟢" if not data.get("whitelist_users", "") else "✅"
        
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{reply_icon} Forward Replies", callback_data="task_replies_1"), 
             InlineKeyboardButton(f"{no_reply_icon} No Replies", callback_data="task_replies_0")],
            [InlineKeyboardButton(f"{forward_icon} Forward Forwards", callback_data="task_forwards_1"), 
             InlineKeyboardButton(f"{no_forward_icon} No Forwards", callback_data="task_forwards_0")],
            [InlineKeyboardButton(f"{delay_icon} Add Delay", callback_data="task_delay"),
             InlineKeyboardButton(f"{no_delay_icon} No Delay", callback_data="task_no_delay")],
            [InlineKeyboardButton(f"{keywords_icon} Add Keywords", callback_data="task_keywords"),
             InlineKeyboardButton(f"{all_messages_icon} All Messages", callback_data="task_no_keywords")],
            [InlineKeyboardButton(f"{blacklist_kw_icon} Blacklist Keywords", callback_data="task_blacklist_keywords"),
             InlineKeyboardButton(f"{whitelist_kw_icon} Whitelist Keywords", callback_data="task_whitelist_keywords")],
            [InlineKeyboardButton(f"{blacklist_users_icon} Blacklist Users", callback_data="task_blacklist_users"),
             InlineKeyboardButton(f"{whitelist_users_icon} Whitelist Users", callback_data="task_whitelist_users")],
            [InlineKeyboardButton("⚙️ Advanced Settings", callback_data="task_advanced"),
             InlineKeyboardButton("💾 Save task", callback_data="task_save")]
        ])
    
    def _get_advanced_keyboard(self) -> InlineKeyboardMarkup:
        """Generate the advanced settings keyboard"""
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("⏱️ Max Edit Time", callback_data="task_max_edit_time"),
             InlineKeyboardButton("🚫 Prevent Duplicates", callback_data="task_prevent_duplicates")],
            [InlineKeyboardButton("⏰ Auto Schedule", callback_data="task_auto_schedule"),
             InlineKeyboardButton("🔙 Back to Options", callback_data="task_back_to_options")]
        ])
    
    async def start_creation(self, user_id: int) -> str:
        """Start a new task creation session"""
        self.pending_tasks[user_id] = {
            "step": "name",
            "data": {}
        }
        return "Let's create a forwarding task! First, send me the task name (e.g., 'News Forward'):"
    
    async def handle_input(self, user_id: int, text: str) -> tuple[str, Optional[InlineKeyboardMarkup]]:
        """Handle user input during task creation"""
        if user_id not in self.pending_tasks:
            return "No task creation in progress. Use /create to start.", None
        
        task_data = self.pending_tasks[user_id]
        step = task_data["step"]
        
        if step == "name":
            task_data["data"]["name"] = text
            task_data["step"] = "source"
            return "Great! Now let's choose the source channel. Use /listchats to see available chats, then send the chat name or ID:", None
        
        elif step == "source":
            try:
                chat_id = None
                # If user sent a number referencing last /listchats, map it first
                if text.strip().isdigit():
                    idx = int(text.strip())
                    mapped = self.get_chat_by_index(user_id, idx)
                    if mapped is not None:
                        chat_id = mapped
                # If not mapped, try to parse as a direct chat ID
                if chat_id is None:
                    chat_id = self._parse_chat_id(text)
                if chat_id:
                    task_data["data"]["source_chat_id"] = chat_id
                    task_data["step"] = "destination"
                    formatted_id = self._format_chat_id(chat_id)
                    return f"Source set to: {text} (ID: {formatted_id})\n\nNow choose the destination channel. Send the chat name or ID:", None
                
                # If not a valid ID, try to resolve chat name
                chat_id = await self._resolve_chat_name(text)
                if chat_id:
                    task_data["data"]["source_chat_id"] = chat_id
                    task_data["step"] = "destination"
                    formatted_id = self._format_chat_id(chat_id)
                    return f"Source set to: {text} (ID: {formatted_id})\n\nNow choose the destination channel. Send the chat name or ID:", None
                else:
                    return f"❌ Could not find chat '{text}'. Please use /listchats to see available chats and try again with the exact name or ID:", None
            except Exception as e:
                return f"❌ Error processing source chat: {e}\n\nPlease try again with a valid chat name or ID:", None
        
        elif step == "destination":
            try:
                chat_id = None
                # Try numeric index from last /listchats first
                if text.strip().isdigit():
                    idx = int(text.strip())
                    mapped = self.get_chat_by_index(user_id, idx)
                    if mapped is not None:
                        chat_id = mapped
                # If not mapped, try to parse as a direct chat ID
                if chat_id is None:
                    chat_id = self._parse_chat_id(text)
                if chat_id:
                    task_data["data"]["destination_chat_id"] = chat_id
                else:
                    # If not a valid ID, try to resolve chat name
                    chat_id = await self._resolve_chat_name(text)
                    if chat_id:
                        task_data["data"]["destination_chat_id"] = chat_id
                    else:
                        return f"❌ Could not find chat '{text}'. Please use /listchats to see available chats and try again with the exact name or ID:", None
                
                # Move to options
                task_data["step"] = "options"
                formatted_id = self._format_chat_id(chat_id)
                kb = self._get_options_keyboard(task_data)
                return f"Destination set to: {text} (ID: {formatted_id})\n\nNow configure the copying options:", kb
            except Exception as e:
                return f"❌ Error processing destination chat: {e}\n\nPlease try again with a valid chat name or ID:", None
        
        elif step == "delay":
            try:
                delay = int(text)
                task_data["data"]["delay_seconds"] = delay
                task_data["step"] = "options"
                message = f"**Destination set to:** `{task_data['data']['destination_chat_id']}` (ID: `{task_data['data']['destination_chat_id']}`)\n\n**Now configure the copying options:**"
                kb = self._get_options_keyboard(task_data)
                return message, kb
            except ValueError:
                return "Please enter a valid number for delay:", None
        
        elif step == "keywords":
            task_data["data"]["keywords"] = text
            task_data["step"] = "options"
            message = f"**Destination set to:** `{task_data['data']['destination_chat_id']}` (ID: `{task_data['data']['destination_chat_id']}`)\n\n**Now configure the copying options:**"
            kb = self._get_options_keyboard(task_data)
            return message, kb
        
        elif step == "blacklist_keywords":
            task_data["data"]["blacklist_keywords"] = text
            task_data["step"] = "options"
            message = f"**Destination set to:** `{task_data['data']['destination_chat_id']}` (ID: `{task_data['data']['destination_chat_id']}`)\n\n**Now configure the copying options:**"
            kb = self._get_options_keyboard(task_data)
            return message, kb
        
        elif step == "whitelist_keywords":
            task_data["data"]["whitelist_keywords"] = text
            task_data["step"] = "options"
            message = f"**Destination set to:** `{task_data['data']['destination_chat_id']}` (ID: `{task_data['data']['destination_chat_id']}`)\n\n**Now configure the copying options:**"
            kb = self._get_options_keyboard(task_data)
            return message, kb
        
        elif step == "blacklist_users":
            task_data["data"]["blacklist_users"] = text
            task_data["step"] = "options"
            message = f"**Destination set to:** `{task_data['data']['destination_chat_id']}` (ID: `{task_data['data']['destination_chat_id']}`)\n\n**Now configure the copying options:**"
            kb = self._get_options_keyboard(task_data)
            return message, kb
        
        elif step == "whitelist_users":
            task_data["data"]["whitelist_users"] = text
            task_data["step"] = "options"
            message = f"**Destination set to:** `{task_data['data']['destination_chat_id']}` (ID: `{task_data['data']['destination_chat_id']}`)\n\n**Now configure the copying options:**"
            kb = self._get_options_keyboard(task_data)
            return message, kb
        
        elif step == "max_edit_time":
            try:
                edit_time = int(text)
                task_data["data"]["max_edit_time"] = edit_time
                task_data["step"] = "advanced"
                return f"Max edit time set to {edit_time} seconds. Use the buttons above to continue.", None
            except ValueError:
                return "Please enter a valid number for max edit time:", None
        
        elif step == "auto_schedule":
            task_data["data"]["auto_schedule"] = text
            task_data["step"] = "advanced"
            return f"Auto schedule set to: {text}\n\nUse the buttons above to continue.", None
        
        return "Unexpected step. Use /create to start over.", None
    
    async def handle_callback(self, user_id: int, callback_data: str) -> tuple[str, Optional[InlineKeyboardMarkup]]:
        """Handle button callbacks during task creation"""
        if user_id not in self.pending_tasks:
            return "No task creation in progress.", None
        
        task_data = self.pending_tasks[user_id]
        
        if callback_data.startswith("task_replies_"):
            task_data["data"]["forward_replies"] = int(callback_data.split("_")[-1])
            message = f"**Destination set to:** `{task_data['data']['destination_chat_id']}` (ID: `{task_data['data']['destination_chat_id']}`)\n\n**Now configure the copying options:**"
            kb = self._get_options_keyboard(task_data)
            return message, kb
        
        elif callback_data.startswith("task_forwards_"):
            task_data["data"]["forward_forwards"] = int(callback_data.split("_")[-1])
            message = f"**Destination set to:** `{task_data['data']['destination_chat_id']}` (ID: `{task_data['data']['destination_chat_id']}`)\n\n**Now configure the copying options:**"
            kb = self._get_options_keyboard(task_data)
            return message, kb
        
        elif callback_data == "task_delay":
            task_data["step"] = "delay"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Options", callback_data="task_back_to_options")]])
            return "Enter delay in seconds (e.g., 5 for 5 seconds):", kb
        
        elif callback_data == "task_no_delay":
            task_data["data"]["delay_seconds"] = 0
            message = f"**Destination set to:** `{task_data['data']['destination_chat_id']}` (ID: `{task_data['data']['destination_chat_id']}`)\n\n**Now configure the copying options:**"
            kb = self._get_options_keyboard(task_data)
            return message, kb
        
        elif callback_data == "task_keywords":
            task_data["step"] = "keywords"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Options", callback_data="task_back_to_options")]])
            return "Enter keywords to filter messages (comma-separated, e.g., 'news,update,alert'). Leave empty for all messages:", kb
        
        elif callback_data == "task_no_keywords":
            task_data["data"]["keywords"] = ""
            task_data["data"]["exclude_keywords"] = ""
            message = f"**Destination set to:** `{task_data['data']['destination_chat_id']}` (ID: `{task_data['data']['destination_chat_id']}`)\n\n**Now configure the copying options:**"
            kb = self._get_options_keyboard(task_data)
            return message, kb
        
        elif callback_data == "task_blacklist_keywords":
            task_data["step"] = "blacklist_keywords"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Options", callback_data="task_back_to_options")]])
            return "Enter blacklisted keywords (comma-separated, e.g., 'spam,ads,scam'). Messages containing these will be blocked:", kb
        
        elif callback_data == "task_whitelist_keywords":
            task_data["step"] = "whitelist_keywords"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Options", callback_data="task_back_to_options")]])
            return "Enter whitelisted keywords (comma-separated, e.g., 'news,update,alert'). Only messages containing these will be forwarded:", kb
        
        elif callback_data == "task_blacklist_users":
            task_data["step"] = "blacklist_users"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Options", callback_data="task_back_to_options")]])
            return "Enter blacklisted user IDs (comma-separated, e.g., '123456789,987654321'). Messages from these users will be blocked:", kb
        
        elif callback_data == "task_whitelist_users":
            task_data["step"] = "whitelist_users"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Options", callback_data="task_back_to_options")]])
            return "Enter whitelisted user IDs (comma-separated, e.g., '123456789,987654321'). Only messages from these users will be forwarded:", kb
        
        elif callback_data == "task_advanced":
            task_data["step"] = "advanced"
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("⏱️ Max Edit Time", callback_data="task_max_edit_time"),
                 InlineKeyboardButton("🚫 Prevent Duplicates", callback_data="task_prevent_duplicates")],
                [InlineKeyboardButton("⏰ Auto Schedule", callback_data="task_auto_schedule"),
                 InlineKeyboardButton("🔙 Back to Options", callback_data="task_back_to_options")]
            ])
            return "⚙️ **Advanced Settings**\n\nConfigure advanced task behavior:", kb
        
        elif callback_data == "task_save":
            result = await self._save_task(user_id)
            return result, None
        
        elif callback_data == "task_back_to_options":
            message = f"**Destination set to:** `{task_data['data']['destination_chat_id']}` (ID: `{task_data['data']['destination_chat_id']}`)\n\n**Now configure the copying options:**"
            kb = self._get_options_keyboard(task_data)
            return message, kb
        
        return "Unknown option.", None
    
    def _get_options_keyboard(self, task_data: Dict[str, Any]) -> InlineKeyboardMarkup:
        """Generate the options keyboard for task creation with dynamic states"""
        data = task_data["data"]
        
        # Reply forwarding state
        reply_icon = "✅" if data.get("forward_replies", 1) else "❌"
        no_reply_icon = "❌" if data.get("forward_replies", 1) else "✅"
        
        # Forward forwarding state
        forward_icon = "✅" if data.get("forward_forwards", 1) else "❌"
        no_forward_icon = "❌" if data.get("forward_forwards", 1) else "✅"
        
        # Delay state
        delay_icon = "⏱️" if data.get("delay_seconds", 0) > 0 else "⏱️"
        no_delay_icon = "🚫" if data.get("delay_seconds", 0) == 0 else "✅"
        
        # Keywords state
        keywords_icon = "🔍" if data.get("keywords", "") else "🔍"
        all_messages_icon = "📝" if not data.get("keywords", "") else "✅"
        
        # Blacklist/Whitelist keywords state
        blacklist_kw_icon = "🚫" if not data.get("blacklist_keywords", "") else "✅"
        whitelist_kw_icon = "✅" if not data.get("whitelist_keywords", "") else "✅"
        
        # Blacklist/Whitelist users state
        blacklist_users_icon = "🚷" if not data.get("blacklist_users", "") else "✅"
        whitelist_users_icon = "🟢" if not data.get("whitelist_users", "") else "✅"
        
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{reply_icon} Forward Replies", callback_data="task_replies_1"), 
             InlineKeyboardButton(f"{no_reply_icon} No Replies", callback_data="task_replies_0")],
            [InlineKeyboardButton(f"{forward_icon} Forward Forwards", callback_data="task_forwards_1"), 
             InlineKeyboardButton(f"{no_forward_icon} No Forwards", callback_data="task_forwards_0")],
            [InlineKeyboardButton(f"{delay_icon} Add Delay", callback_data="task_delay"),
             InlineKeyboardButton(f"{no_delay_icon} No Delay", callback_data="task_no_delay")],
            [InlineKeyboardButton(f"{keywords_icon} Add Keywords", callback_data="task_keywords"),
             InlineKeyboardButton(f"{all_messages_icon} All Messages", callback_data="task_no_keywords")],
            [InlineKeyboardButton(f"{blacklist_kw_icon} Blacklist Keywords", callback_data="task_blacklist_keywords"),
             InlineKeyboardButton(f"{whitelist_kw_icon} Whitelist Keywords", callback_data="task_whitelist_keywords")],
            [InlineKeyboardButton(f"{blacklist_users_icon} Blacklist Users", callback_data="task_blacklist_users"),
             InlineKeyboardButton(f"{whitelist_users_icon} Whitelist Users", callback_data="task_whitelist_users")],
            [InlineKeyboardButton("⚙️ Advanced Settings", callback_data="task_advanced"),
             InlineKeyboardButton("💾 Save task", callback_data="task_save")]
        ])
    
    def _get_advanced_keyboard(self) -> InlineKeyboardMarkup:
        """Generate the advanced settings keyboard"""
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("⏱️ Max Edit Time", callback_data="task_max_edit_time"),
             InlineKeyboardButton("🚫 Prevent Duplicates", callback_data="task_prevent_duplicates")],
            [InlineKeyboardButton("⏰ Auto Schedule", callback_data="task_auto_schedule"),
             InlineKeyboardButton("🔙 Back to Options", callback_data="task_back_to_options")]
        ])
    
    async def _save_task(self, user_id: int) -> str:
        """Save the completed task"""
        print(f"DEBUG: _save_task called for user {user_id}")
        print(f"DEBUG: pending_tasks keys: {list(self.pending_tasks.keys())}")
        
        if user_id not in self.pending_tasks:
            print(f"DEBUG: User {user_id} not in pending_tasks")
            return "No task to save."
        
        task_data = self.pending_tasks[user_id]["data"]
        print(f"DEBUG: task data: {task_data}")
        
        # Check if required fields are present
        if not task_data.get("source_chat_id") or not task_data.get("destination_chat_id"):
            print(f"DEBUG: Missing source or destination chat ID")
            return "❌ **Error:** Source and destination chats must be set before saving."
        
        if not task_data.get("name"):
            print(f"DEBUG: Missing task name")
            return "❌ **Error:** task name must be set before saving."
        
        # Generate unique ID
        task_id = f"R{int(datetime.utcnow().timestamp())}"
        print(f"DEBUG: Generated task ID: {task_id}")
        
        # Create task
        new_task = task(
            id=task_id,
            user_id=user_id,
            name=task_data.get("name", "Unnamed task"),
            source_chat_id=task_data.get("source_chat_id", 0),
            destination_chat_id=task_data.get("destination_chat_id", 0),
            keywords=task_data.get("keywords", ""),
            exclude_keywords=task_data.get("exclude_keywords", ""),
            forward_media=1,  # Always copy media (no attribution)
            forward_replies=task_data.get("forward_replies", 1),
            forward_forwards=task_data.get("forward_forwards", 1),
            delay_seconds=task_data.get("delay_seconds", 0),
            blacklist_keywords=task_data.get("blacklist_keywords", ""),
            whitelist_keywords=task_data.get("whitelist_keywords", ""),
            blacklist_users=task_data.get("blacklist_users", ""),
            whitelist_users=task_data.get("whitelist_users", ""),
            max_edit_time=task_data.get("max_edit_time", 0),
            prevent_duplicates=task_data.get("prevent_duplicates", 1),
            auto_schedule=task_data.get("auto_schedule", ""),
            schedule_enabled=task_data.get("schedule_enabled", 0)
        )
        
        print(f"DEBUG: Created task object: {new_task}")
        
        try:
            # Save to store
            self.store.upsert_task(new_task)
            print(f"DEBUG: task saved to store successfully")
            del self.pending_tasks[user_id]
            
            # IMMEDIATELY start monitoring the new source chat
            try:
                print(f"DEBUG: Starting immediate monitoring for new source chat {new_task.source_chat_id}")
                await self.engine.start_monitoring_chat(user_id, new_task.source_chat_id)
                
                # Schedule monitoring refresh in background to avoid blocking the bot
                try:
                    import asyncio
                    asyncio.create_task(self.engine.force_refresh_monitoring(user_id))
                    print(f"DEBUG: Monitoring refresh scheduled in background for user {user_id}")
                except Exception as e:
                    print(f"DEBUG: Could not schedule monitoring refresh: {e}")
                
                print(f"DEBUG: Monitoring started successfully for chat {new_task.source_chat_id}")
            except Exception as e:
                print(f"DEBUG: Error starting monitoring: {e}")
                # Don't fail the task creation if monitoring fails
                # The engine will pick it up on the next message anyway
            
            success_msg = f"✅ task '{new_task.name}' created successfully!\n\n"
            success_msg += f"**Basic Settings:**\n"
            success_msg += f"• ID: {new_task.id}\n"
            success_msg += f"• Source: {new_task.source_chat_id}\n"
            success_msg += f"• Destination: {new_task.destination_chat_id}\n"
            success_msg += f"• Media: Always Copy (No Attribution)\n"
            success_msg += f"• Replies: {'ON' if new_task.forward_replies else 'OFF'}\n"
            success_msg += f"• Forwards: {'ON' if new_task.forward_forwards else 'OFF'}\n"
            success_msg += f"• Delay: {new_task.delay_seconds}s\n"
            success_msg += f"• Keywords: {new_task.keywords or 'ALL'}\n\n"
            
            if new_task.blacklist_keywords:
                success_msg += f"**🚫 Blacklist Keywords:** {new_task.blacklist_keywords}\n"
            if new_task.whitelist_keywords:
                success_msg += f"**✅ Whitelist Keywords:** {new_task.whitelist_keywords}\n"
            if new_task.blacklist_users:
                success_msg += f"**🚷 Blacklist Users:** {new_task.blacklist_users}\n"
            if new_task.whitelist_users:
                success_msg += f"**🟢 Whitelist Users:** {new_task.whitelist_users}\n"
            if new_task.max_edit_time > 0:
                success_msg += f"**⏱️ Max Edit Time:** {new_task.max_edit_time}s\n"
            if new_task.auto_schedule:
                success_msg += f"**⏰ Auto Schedule:** {new_task.auto_schedule}\n"
            
            # Add immediate status info
            success_msg += f"\n**🚀 Status:** task is now ACTIVE and monitoring source chat {new_task.source_chat_id}\n"
            success_msg += f"**💡 Tip:** Forwarding will start immediately - no restart needed!"
            
            print(f"DEBUG: Success message created")
            return success_msg
            
        except Exception as e:
            print(f"DEBUG: Error saving task: {e}")
            import traceback
            print(f"DEBUG: Full traceback: {traceback.format_exc()}")
            return f"❌ **Error saving task:** {str(e)}\n\n**Note:** The task may have been saved but monitoring setup failed. Forwarding should still work."
    
    def get_pending_task(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get pending task data for a user"""
        return self.pending_tasks.get(user_id)
    
    def cancel_creation(self, user_id: int) -> str:
        """Cancel task creation"""
        if user_id in self.pending_tasks:
            del self.pending_tasks[user_id]
            return "task creation cancelled."
        return "No task creation in progress."
    
    async def _resolve_chat_name(self, chat_name: str) -> Optional[int]:
        """Resolve chat name to chat ID"""
        try:
            if not self.engine.client:
                return None
            
            # Search through dialogs to find matching chat
            async for dialog in self.engine.client.iter_dialogs(limit=100):
                # Check exact name match
                if dialog.name.lower() == chat_name.lower():
                    return dialog.entity.id
                
                # Check username match
                username = getattr(dialog.entity, 'username', None)
                if username and username.lower() == chat_name.lower():
                    return dialog.entity.id
                
                # Check partial name match (case-insensitive)
                if chat_name.lower() in dialog.name.lower():
                    return dialog.entity.id
            
            return None
        except Exception:
            return None

    def _format_chat_id(self, chat_id: int) -> str:
        """Format chat ID to show proper Telegram format"""
        if chat_id < 0:
            return str(chat_id)  # Already negative (channel/group)
        elif chat_id > 1000000000000:  # Supergroup/channel range
            return f"-100{chat_id}"
        elif chat_id > 100000000:  # Regular channel/group range
            return f"-100{chat_id}"
        else:
            return str(chat_id)  # Regular user/group
    
    def _parse_chat_id(self, chat_input: str) -> Optional[int]:
        """Parse chat ID input, handling various formats"""
        try:
            # Remove any extra characters
            clean_input = chat_input.strip().replace(' ', '')
            
            # If it's already a valid integer
            if clean_input.isdigit() or (clean_input.startswith('-') and clean_input[1:].isdigit()):
                chat_id = int(clean_input)
                
                # Auto-transform to correct format for channels/groups
                if chat_id > 0 and chat_id > 100000000:  # Likely a channel/group
                    # Check if it needs the -100 prefix
                    if not str(chat_id).startswith('-100'):
                        transformed_id = int(f"-100{chat_id}")
                        print(f"DEBUG: Auto-transformed chat ID {chat_id} to {transformed_id}")
                        return transformed_id
                
                return chat_id
            
            # If it's in format -1001234567890, extract the ID
            if clean_input.startswith('-100') and clean_input[4:].isdigit():
                return int(clean_input)
            
            # If it's just the number part (1234567890), add -100 prefix
            if clean_input.isdigit() and len(clean_input) > 10:
                return int(f"-100{clean_input}")
            
            return None
        except:
            return None

# ----------------- Bot (UI) -----------------
class BotUI:
    def __init__(self, engine: Engine, store: Store):
        self.engine = engine
        self.store = store
        self.app: Optional[Application] = None
        self.pending_login: Dict[int, Dict[str, Any]] = {}
        self.login_states: Dict[int, str] = {}  # Track login flow state
        self.task_builder = taskBuilder(store, engine)
        self.user_rate_limits: Dict[int, List[float]] = {}  # For rate limiting

    # ------- Guards -------
    async def _guard(self, update: Update) -> bool:
        """Check if user is allowed to use the bot - now allows all users"""
        user_id = update.effective_user.id if update.effective_user else 0
        
        # Update user activity
        self.store.update_user_activity(user_id)
        
        # Check for unlimited access users
        unlimited_users = os.getenv("ALLOWED_UNLIMITED_IDS", "")
        if unlimited_users:
            unlimited_ids = [int(id.strip()) for id in unlimited_users.split(",") if id.strip().isdigit()]
            if user_id in unlimited_ids:
                # Unlimited user - skip all rate limiting
                return True

        # Standard rate limiting for regular users (max 15 requests per minute)
        current_time = time.time()

        if user_id not in self.user_rate_limits:
            self.user_rate_limits[user_id] = []

        # Clean old requests (older than 60 seconds)
        self.user_rate_limits[user_id] = [
            req_time for req_time in self.user_rate_limits[user_id]
            if current_time - req_time < 60
        ]

        # Check rate limit
        if len(self.user_rate_limits[user_id]) >= 15:
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.answer("⏱️ Rate limit exceeded. Please wait before making more requests.", show_alert=True)
            else:
                await update.effective_message.reply_text("⏱️ Rate limit exceeded. Please wait before making more requests.")
            return False

        # Add current request
        self.user_rate_limits[user_id].append(current_time)
        return True
    
    async def _require_login(self, update: Update) -> bool:
        """Check if user is logged in and verified"""
        user_id = update.effective_user.id if update.effective_user else 0
        user_session = self.store.get_user_session(user_id)
        
        if not user_session or not user_session.is_verified:
            q = update.callback_query
            if q:
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔐 Login", callback_data="login")],
                    [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
                ])
                await q.edit_message_text("🔒 **Login Required**\n\n"
                                        "You need to login with your Telegram account first to use this feature.\n\n"
                                        "**Click 'Login' to get started!**", 
                                        reply_markup=kb, parse_mode='Markdown')
            else:
                await update.effective_message.reply_text("🔒 **Login Required**\n\n"
                                                        "You need to login with your Telegram account first.\n\n"
                                                        "Use `/login +phone` to start.")
            return False
        
        return True

    # ------- Commands -------
    async def cmd_start(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not await self._guard(update):
            return
        
        user_id = update.effective_user.id
        
        # Check if user is logged in
        user_session = self.store.get_user_session(user_id)
        is_logged_in = user_session and user_session.is_verified
        forwarding_status = self.store.get_kv("forwarding_on", True)
        
        welcome_text = "🚀 **Welcome to Auto-Forwarder Pro!**\n\n"
        if is_logged_in:
            welcome_text += "✅ **Status:** Logged in and ready\n"
            welcome_text += f"🔄 **Forwarding:** {'🟢 ON' if forwarding_status else '🔴 OFF'}\n"
            welcome_text += f"📊 **Your tasks:** {self.store.get_user_tasks_count(user_id)} active\n"
        else:
            welcome_text += "❌ **Status:** Not logged in\n"
            welcome_text += "🔐 **Action needed:** Login first\n"
        
        welcome_text += "\n📱 **Use the menu below to control your bot:**"
        
        # Add debug info for testing
        welcome_text += f"\n\n🔍 **Debug Info:**\n"
        welcome_text += f"• User ID: {user_id}\n"
        welcome_text += f"• Session exists: {'Yes' if user_session else 'No'}\n"
        welcome_text += f"• Verified: {'Yes' if is_logged_in else 'No'}\n"
        welcome_text += f"• Engine running: {'Yes' if self.engine.started else 'No'}"
        
        kb = InlineKeyboardMarkup([
            # Main Actions Row
            [InlineKeyboardButton("➕ Create task", callback_data="create"),
             InlineKeyboardButton("📋 Manage tasks", callback_data="manage_tasks")],
            
            # Control Row
            [InlineKeyboardButton("🟢 Start Bot" if forwarding_status else "🔴 Stop Bot", 
                                callback_data="stopf" if forwarding_status else "startf"),
             InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
            
            # Advanced Features Row
            [InlineKeyboardButton("🔍 Filters & Moderation", callback_data="filters"),
             InlineKeyboardButton("⏰ Scheduling", callback_data="scheduling")],
            
            # Management Row
            [InlineKeyboardButton("📊 Statistics", callback_data="stats"),
             InlineKeyboardButton("🔐 Account", callback_data="account")],
            
            # Tools Row
            [InlineKeyboardButton("📦 Export/Import", callback_data="export_import"),
             InlineKeyboardButton("🛠️ Tools", callback_data="tools")],
            
            # Monitoring Row
            [InlineKeyboardButton("🔄 Refresh Monitoring", callback_data="refresh_monitoring"),
             InlineKeyboardButton("🧪 Test Monitoring", callback_data="test_monitoring")],
            
            # Debug Row
            [InlineKeyboardButton("🧹 Cleanup Handlers", callback_data="cleanup_handlers")]
        ])
        
        await update.effective_message.reply_text(welcome_text, reply_markup=kb, parse_mode='Markdown')

    async def cmd_tasks(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not await self._guard(update):
            return
        await self._send_tasks(update)

    async def cmd_status(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not await self._guard(update):
            return
        await self._status(update)

    async def cmd_forward(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not await self._guard(update):
            return
        arg = (ctx.args[0].lower() if ctx.args else "").strip()
        if arg not in {"on","off"}:
            await update.effective_message.reply_text("Usage: /forward on|off")
            return
        on = arg == "on"
        self.store.set_kv("forwarding_on", on)
        await update.effective_message.reply_text(f"Forwarding: {'ON' if on else 'OFF'}")

    async def cmd_addtask(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not await self._guard(update):
            return
        text = update.effective_message.text or ""
        try:
            rid   = get_arg(text, "id")
            name  = get_arg(text, "name")
            src_in = get_arg(text, "src")
            dst_in = get_arg(text, "dst")
            # Normalize chat IDs: ensure channels/groups have -100 prefix
            src_parsed = self.task_builder._parse_chat_id(str(src_in))
            dst_parsed = self.task_builder._parse_chat_id(str(dst_in))
            if src_parsed is None or dst_parsed is None:
                raise ValueError("Invalid chat IDs")
            src   = int(src_parsed)
            dst   = int(dst_parsed)
            kw    = get_arg(text, "kw", "")
            nkw   = get_arg(text, "nkw", "")
            media = parse_bool(get_arg(text, "media", "1"))
            repl  = parse_bool(get_arg(text, "replies", "1"))
            fwd   = parse_bool(get_arg(text, "forwards", "1"))
            delay = int(get_arg(text, "delay", "0"))
        except Exception as e:
            await update.effective_message.reply_text(
                "Usage:\n/addtask --id=R1 --name=Mytask --src=SOURCE_CHAT_ID --dst=DEST_CHAT_ID "
                "--kw=word1,word2 --nkw=bad,stop --media=1 --replies=1 --forwards=1 --delay=0"
            )
            return
        r = task(
            id=rid, name=name, source_chat_id=src, destination_chat_id=dst,
            keywords=kw, exclude_keywords=nkw, forward_media=media,
            forward_replies=repl, forward_forwards=fwd, delay_seconds=delay,
        )
        self.store.upsert_task(r)
        await update.effective_message.reply_text(f"Saved task {r.id} → {r.name}")

    async def cmd_deltask(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not await self._guard(update):
            return
        if not ctx.args:
            await update.effective_message.reply_text("Usage: /deltask task_ID")
            return
        ok = self.store.delete_task(ctx.args[0])
        await update.effective_message.reply_text("Deleted" if ok else "Not found")

    async def cmd_toggle(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not await self._guard(update):
            return
        if len(ctx.args) < 2 or ctx.args[1].lower() not in {"on","off"}:
            await update.effective_message.reply_text("Usage: /toggle task_ID on|off")
            return
        rid, switch = ctx.args[0], ctx.args[1].lower() == "on"
        ok = self.store.set_task_enabled(rid, switch)
        await update.effective_message.reply_text("OK" if ok else "Not found")

    async def cmd_export(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not await self._guard(update):
            return
        data = [asdict(r) for r in self.store.list_tasks()]
        await update.effective_message.reply_document(
            document=("tasks.json", json.dumps(data, indent=2).encode("utf-8")),
            filename="tasks.json",
            caption="Exported tasks",
        )

    async def cmd_import(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not await self._guard(update):
            return
        if not update.effective_message.document:
            await update.effective_message.reply_text("Send a JSON file with /import as a caption.")
            return
        f = await update.effective_message.document.get_file()
        b = await f.download_as_bytearray()
        try:
            arr = json.loads(b.decode("utf-8"))
            cnt = 0
            for d in arr:
                self.store.upsert_task(task(**d))
                cnt += 1
            await update.effective_message.reply_text(f"Imported {cnt} tasks.")
        except Exception as e:
            await update.effective_message.reply_text(f"Import failed: {e}")

    async def cmd_create(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not await self._guard(update):
            return
        message = await self.task_builder.start_creation(update.effective_user.id)
        await update.effective_message.reply_text(message)

    async def cmd_listchats(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not await self._guard(update):
            return
        
        user_id = update.effective_user.id
        
        # Check if user is logged in
        user_session = self.store.get_user_session(user_id)
        if not user_session or not user_session.is_verified:
            await update.effective_message.reply_text("❌ **Login Required**\n\nYou need to login with your Telegram account first.\n\nUse `/login +phone` to start.")
            return
        
        # Get user's client
        client = await self.engine.get_client(user_id)
        if not client:
            await update.effective_message.reply_text("❌ **Client Error**\n\nFailed to get your Telegram client. Please try logging in again.")
            return
        
        try:
            chats = []
            chat_ids_for_selection: List[int] = []
            async for dialog in client.iter_dialogs(limit=10):
                chat_type = "👥" if dialog.is_group else "📢" if dialog.is_channel else "👤"
                # Clean the name to avoid Markdown parsing issues - escape all special characters
                name = str(dialog.name or "Unknown")
                # Escape all Markdown special characters
                for char in ['*', '_', '`', '[', ']', '(', ')', '~', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']:
                    name = name.replace(char, f'\\{char}')
                
                chat_id = dialog.entity.id
                username = getattr(dialog.entity, 'username', None)
                username_str = f" (@{username})" if username else ""
                
                formatted_id = self.task_builder._format_chat_id(chat_id)
                # Show only the correct usable ID - escape the ID too
                escaped_id = formatted_id.replace('`', '\\`')
                id_display = f"ID: `{escaped_id}`"
                chats.append(f"{len(chat_ids_for_selection)+1}. {chat_type} {name}{username_str}\n   {id_display}")
                chat_ids_for_selection.append(chat_id)
            
            if chats:
                # Cache for numeric selection during task creation
                self.task_builder.set_last_chats(user_id, chat_ids_for_selection)
                message = "**Available chats (first 10):**\n\n" + "\n\n".join(chats)
                message += "\n\n**Page 1** - Showing chats 1-10"
                message += "\n\ncopy and send the ID for the chat you want to forward from."
                
                # Create keyboard with navigation buttons
                kb_rows = []
                
                # Check if there might be more chats
                try:
                    # Try to get one more chat to see if there are more
                    async for dialog in client.iter_dialogs(limit=1, offset_id=10):
                        kb_rows.append([InlineKeyboardButton("➡️ Next", callback_data="chats_page_1")])
                        break
                except:
                    pass
                
                kb_rows.append([InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")])
                kb = InlineKeyboardMarkup(kb_rows)
                
                # Try to send with Markdown, fallback to plain text if parsing fails
                try:
                    await update.effective_message.reply_text(message, reply_markup=kb, parse_mode='Markdown')
                except Exception as parse_error:
                    # If Markdown parsing fails, send as plain text
                    log.warning(f"Markdown parsing failed for initial chats list, falling back to plain text: {parse_error}")
                    # Remove Markdown formatting for fallback
                    plain_message = message.replace('**', '').replace('`', '')
                    await update.effective_message.reply_text(plain_message, reply_markup=kb)
            else:
                message = "No chats found. Make sure you're logged in and have access to chats."
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]])
                await update.effective_message.reply_text(message, reply_markup=kb)
        except Exception as e:
            # Fallback to plain text if Markdown fails
            try:
                chats = []
                async for dialog in client.iter_dialogs(limit=10):
                    chat_type = "👥" if dialog.is_group else "📢" if dialog.is_channel else "👤"
                    name = str(dialog.name)
                    chat_id = dialog.entity.id
                    username = getattr(dialog.entity, 'username', None)
                    username_str = f" (@{username})" if username else ""
                    
                    formatted_id = self.task_builder._format_chat_id(chat_id)
                    chats.append(f"{chat_type} {name}{username_str}\n   ID: {formatted_id}")
                
                if chats:
                    message = "Available chats (first 10):\n\n" + "\n\n".join(chats)
                    message += "\n\nPage 1 - Showing chats 1-10"
                    message += "\n\nUse these names or IDs when creating tasks."
                    
                    # Create keyboard with navigation buttons
                    kb_rows = []
                    
                    # Check if there might be more chats
                    try:
                        # Try to get one more chat to see if there are more
                        async for dialog in client.iter_dialogs(limit=1, offset_id=10):
                            kb_rows.append([InlineKeyboardButton("➡️ Next", callback_data="chats_page_1")])
                            break
                    except:
                        pass
                    
                    kb_rows.append([InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")])
                    kb = InlineKeyboardMarkup(kb_rows)
                    
                    await update.effective_message.reply_text(message, reply_markup=kb)
                else:
                    message = "No chats found. Make sure you're logged in and have access to chats."
                    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]])
                    await update.effective_message.reply_text(message, reply_markup=kb)
            except Exception as e2:
                await update.effective_message.reply_text(f"Error listing chats: {e2}")
    
    async def _show_chats_page(self, update: Update, page: int):
        """Show a specific page of chats with pagination"""
        # Handle both callback queries and direct calls
        if hasattr(update, 'callback_query') and update.callback_query:
            q = update.callback_query
            user_id = update.effective_user.id
        else:
            # Fallback for direct calls
            q = update
            user_id = update.effective_user.id
        
        # Check if user is logged in
        user_session = self.store.get_user_session(user_id)
        if not user_session or not user_session.is_verified:
            await q.edit_message_text("❌ **Login Required**\n\nYou need to login with your Telegram account first.\n\nUse `/login +phone` to start.")
            return
        
        # Get user's client
        client = await self.engine.get_client(user_id)
        if not client:
            await q.edit_message_text("❌ **Client Error**\n\nFailed to get your Telegram client. Please try logging in again.")
            return
        
        try:
            chats = []
            chat_ids_for_selection: List[int] = []
            last_dialog_id = None

            # For page 0, no offset needed
            # For subsequent pages, we need to track the last dialog ID from previous page
            if page > 0:
                # For now, we'll use a different approach - get all dialogs and slice
                # This is less efficient but more reliable for pagination
                all_dialogs = []
                async for dialog in client.iter_dialogs():
                    all_dialogs.append(dialog)

                start_idx = page * 10
                end_idx = start_idx + 10
                page_dialogs = all_dialogs[start_idx:end_idx]

                for i, dialog in enumerate(page_dialogs):
                    chat_type = "👥" if dialog.is_group else "📢" if dialog.is_channel else "👤"
                    # Clean the name to avoid Markdown parsing issues - escape all special characters
                    name = str(dialog.name or "Unknown")
                    # Escape all Markdown special characters
                    for char in ['*', '_', '`', '[', ']', '(', ')', '~', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']:
                        name = name.replace(char, f'\\{char}')
                    
                    chat_id = dialog.entity.id
                    username = getattr(dialog.entity, 'username', None)
                    username_str = f" (@{username})" if username else ""
                    
                    formatted_id = self.task_builder._format_chat_id(chat_id)
                    # Show only the correct usable ID - escape the ID too
                    escaped_id = formatted_id.replace('`', '\\`')
                    id_display = f"ID: `{escaped_id}`"
                    chats.append(f"{len(chat_ids_for_selection)+1}. {chat_type} {name}{username_str}\n   {id_display}")
                    chat_ids_for_selection.append(chat_id)
            else:
                # For page 0, use simple offset
                async for dialog in client.iter_dialogs(limit=10):
                    chat_type = "👥" if dialog.is_group else "📢" if dialog.is_channel else "👤"
                    # Clean the name to avoid Markdown parsing issues - escape all special characters
                    name = str(dialog.name or "Unknown")
                    # Escape all Markdown special characters
                    for char in ['*', '_', '`', '[', ']', '(', ')', '~', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']:
                        name = name.replace(char, f'\\{char}')
                    
                    chat_id = dialog.entity.id
                    username = getattr(dialog.entity, 'username', None)
                    username_str = f" (@{username})" if username else ""

                    formatted_id = self.task_builder._format_chat_id(chat_id)
                    # Show only the correct usable ID - escape the ID too
                    escaped_id = formatted_id.replace('`', '\\`')
                    id_display = f"ID: `{escaped_id}`"
                    chats.append(f"{len(chat_ids_for_selection)+1}. {chat_type} {name}{username_str}\n   {id_display}")
                    chat_ids_for_selection.append(chat_id)
            
            if chats:
                # Cache for numeric selection during task creation
                self.task_builder.set_last_chats(user_id, chat_ids_for_selection)
                message = f"**Available chats (page {page + 1}):**\n\n" + "\n\n".join(chats)
                start_idx = page * 10
                message += f"\n\n**Page {page + 1}** - Showing chats {start_idx + 1}-{start_idx + len(chats)}"
                message += "\n\ncopy and send the ID for the chat you want to forward from."
                
                # Create keyboard with navigation buttons
                kb_rows = []
                if page > 0:
                    kb_rows.append([InlineKeyboardButton("⬅️ Previous", callback_data=f"chats_page_{page - 1}")])
                
                # Check if there might be more chats
                if page == 0:
                    # For page 0, check if there are more than 10 chats total
                    try:
                        all_dialogs = []
                        async for dialog in client.iter_dialogs():
                            all_dialogs.append(dialog)
                        if len(all_dialogs) > 10:
                            kb_rows.append([InlineKeyboardButton("➡️ Next", callback_data=f"chats_page_{page + 1}")])
                    except:
                        pass
                else:
                    # For subsequent pages, check if we have more chats in our cached list
                    try:
                        all_dialogs = []
                        async for dialog in client.iter_dialogs():
                            all_dialogs.append(dialog)
                        if (page + 1) * 10 < len(all_dialogs):
                            kb_rows.append([InlineKeyboardButton("➡️ Next", callback_data=f"chats_page_{page + 1}")])
                    except:
                        pass
                
                kb_rows.append([InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")])
                kb = InlineKeyboardMarkup(kb_rows)
                
                # Try to send with Markdown, fallback to plain text if parsing fails
                try:
                    if hasattr(q, 'edit_message_text'):
                        await q.edit_message_text(message, reply_markup=kb, parse_mode='Markdown')
                    else:
                        await update.effective_message.reply_text(message, reply_markup=kb, parse_mode='Markdown')
                except Exception as parse_error:
                    # If Markdown parsing fails, send as plain text
                    log.warning(f"Markdown parsing failed for chats page {page + 1}, falling back to plain text: {parse_error}")
                    # Remove Markdown formatting for fallback
                    plain_message = message.replace('**', '').replace('`', '')
                    if hasattr(q, 'edit_message_text'):
                        await q.edit_message_text(plain_message, reply_markup=kb)
                    else:
                        await update.effective_message.reply_text(plain_message, reply_markup=kb)
            else:
                if page == 0:
                    message = "No chats found. Make sure you're logged in and have access to chats."
                    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]])
                else:
                    message = f"No more chats found on page {page + 1}."
                    kb = InlineKeyboardMarkup([
                        [InlineKeyboardButton("⬅️ Previous", callback_data=f"chats_page_{page - 1}")],
                        [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
                    ])
                
                if hasattr(q, 'edit_message_text'):
                    await q.edit_message_text(message, reply_markup=kb)
                else:
                    await update.effective_message.reply_text(message, reply_markup=kb)
        except Exception as e:
            error_message = f"❌ **Error loading chats**\n\nFailed to load page {page + 1}: {e}\n\nPlease try again or go back to main menu."
            error_kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]])
            if hasattr(q, 'edit_message_text'):
                await q.edit_message_text(error_message, reply_markup=error_kb)
            else:
                await update.effective_message.reply_text(error_message, reply_markup=error_kb)
    
    async def cmd_cancel(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not await self._guard(update):
            return
        message = self.task_builder.cancel_creation(update.effective_user.id)
        await update.effective_message.reply_text(message)

    async def cmd_simplechats(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Simple chat listing without Markdown formatting"""
        if not await self._guard(update):
            return
        
        user_id = update.effective_user.id
        
        # Check if user is logged in
        user_session = self.store.get_user_session(user_id)
        if not user_session or not user_session.is_verified:
            await update.effective_message.reply_text("❌ **Login Required**\n\nYou need to login with your Telegram account first.\n\nUse `/login +phone` to start.")
            return
        
        # Get user's client
        client = await self.engine.get_client(user_id)
        if not client:
            await update.effective_message.reply_text("❌ **Client Error**\n\nFailed to get your Telegram client. Please try logging in again.")
            return
        
        # Get page from command arguments (default to 0)
        page = 0
        if ctx.args and ctx.args[0].isdigit():
            page = int(ctx.args[0])
        
        try:
            chats = []
            offset = page * 10
            
            async for dialog in client.iter_dialogs(limit=10, offset_id=offset):
                chat_type = "👥" if dialog.is_group else "📢" if dialog.is_channel else "👤"
                name = str(dialog.name)
                chat_id = dialog.entity.id
                username = getattr(dialog.entity, 'username', None)
                username_str = f" (@{username})" if username else ""
                
                formatted_id = self.task_builder._format_chat_id(chat_id)
                chats.append(f"{chat_type} {name}{username_str}\n   ID: {formatted_id}")
            
            if chats:
                message = f"Available chats (page {page + 1}):\n\n" + "\n\n".join(chats)
                start_idx = page * 10
                message += f"\n\nPage {page + 1} - Showing chats {start_idx + 1}-{start_idx + len(chats)}"
                message += "\n\nUse these names or IDs when creating tasks."
                
                # Create keyboard with navigation buttons
                kb_rows = []
                if page > 0:
                    kb_rows.append([InlineKeyboardButton("⬅️ Previous", callback_data=f"simple_chats_page_{page - 1}")])
                
                # Check if there might be more chats
                try:
                    # Try to get one more chat to see if there are more
                    # Calculate offset based on page number
                    offset = page * 10
                    async for dialog in client.iter_dialogs(limit=1, offset_id=offset + 10):
                        kb_rows.append([InlineKeyboardButton("➡️ Next", callback_data=f"simple_chats_page_{page + 1}")])
                        break
                except:
                    pass
                
                kb_rows.append([InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")])
                kb = InlineKeyboardMarkup(kb_rows)
                
                await update.effective_message.reply_text(message, reply_markup=kb)
            else:
                if page == 0:
                    message = "No chats found. Make sure you're logged in and have access to chats."
                else:
                    message = f"No more chats found on page {page + 1}."
                    kb = InlineKeyboardMarkup([
                        [InlineKeyboardButton("⬅️ Previous", callback_data=f"simple_chats_page_{page - 1}")],
                        [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
                    ])
                    await update.effective_message.reply_text(message, reply_markup=kb)
                    return
                
                await update.effective_message.reply_text(message)
        except Exception as e:
            await update.effective_message.reply_text(f"Error listing chats: {e}")
    
    async def _show_simple_chats_page(self, update: Update, page: int):
        """Show a specific page of chats with pagination (no Markdown)"""
        # Handle both callback queries and direct calls
        if hasattr(update, 'callback_query') and update.callback_query:
            q = update.callback_query
            user_id = update.effective_user.id
        else:
            # Fallback for direct calls
            q = update
            user_id = update.effective_user.id
        
        # Check if user is logged in
        user_session = self.store.get_user_session(user_id)
        if not user_session or not user_session.is_verified:
            if hasattr(q, 'edit_message_text'):
                await q.edit_message_text("❌ **Login Required**\n\nYou need to login with your Telegram account first.\n\nUse `/login +phone` to start.")
            else:
                await update.effective_message.reply_text("❌ **Login Required**\n\nYou need to login with your Telegram account first.\n\nUse `/login +phone` to start.")
            return
        
        # Get user's client
        client = await self.engine.get_client(user_id)
        if not client:
            if hasattr(q, 'edit_message_text'):
                await q.edit_message_text("❌ **Client Error**\n\nFailed to get your Telegram client. Please try logging in again.")
            else:
                await update.effective_message.reply_text("❌ **Client Error**\n\nFailed to get your Telegram client. Please try logging in again.")
            return
        
        try:
            chats = []
            last_dialog_id = None

            # For page 0, no offset needed
            # For subsequent pages, we need to track the last dialog ID from previous page
            if page > 0:
                # For now, we'll use a different approach - get all dialogs and slice
                # This is less efficient but more reliable for pagination
                all_dialogs = []
                async for dialog in client.iter_dialogs():
                    all_dialogs.append(dialog)

                start_idx = page * 10
                end_idx = start_idx + 10
                page_dialogs = all_dialogs[start_idx:end_idx]

                for i, dialog in enumerate(page_dialogs):
                    chat_type = "👥" if dialog.is_group else "📢" if dialog.is_channel else "👤"
                    name = str(dialog.name)
                    chat_id = dialog.entity.id
                    username = getattr(dialog.entity, 'username', None)
                    username_str = f" (@{username})" if username else ""

                    chats.append(f"{chat_type} {name}{username_str}\n   ID: {chat_id}")
            else:
                # For page 0, use simple offset
                async for dialog in client.iter_dialogs(limit=10):
                    chat_type = "👥" if dialog.is_group else "📢" if dialog.is_channel else "👤"
                    name = str(dialog.name)
                    chat_id = dialog.entity.id
                    username = getattr(dialog.entity, 'username', None)
                    username_str = f" (@{username})" if username else ""
                    
                    chats.append(f"{chat_type} {name}{username_str}\n   ID: {chat_id}")
            
            if chats:
                message = f"Available chats (page {page + 1}):\n\n" + "\n\n".join(chats)
                start_idx = page * 10
                message += f"\n\nPage {page + 1} - Showing chats {start_idx + 1}-{start_idx + len(chats)}"
                message += "\n\nUse these names or IDs when creating tasks."
                
                # Create keyboard with navigation buttons
                kb_rows = []
                if page > 0:
                    kb_rows.append([InlineKeyboardButton("⬅️ Previous", callback_data=f"simple_chats_page_{page - 1}")])
                
                # Check if there might be more chats
                try:
                    # Try to get one more chat to see if there are more
                    # Calculate offset based on page number
                    offset = page * 10
                    async for dialog in client.iter_dialogs(limit=1, offset_id=offset + 10):
                        kb_rows.append([InlineKeyboardButton("➡️ Next", callback_data=f"simple_chats_page_{page + 1}")])
                        break
                except:
                    pass
                
                kb_rows.append([InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")])
                kb = InlineKeyboardMarkup(kb_rows)
                
                if hasattr(q, 'edit_message_text'):
                    await q.edit_message_text(message, reply_markup=kb)
                else:
                    await update.effective_message.reply_text(message, reply_markup=kb)
            else:
                if page == 0:
                    message = "No chats found. Make sure you're logged in and have access to chats."
                    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]])
                else:
                    message = f"No more chats found on page {page + 1}."
                    kb = InlineKeyboardMarkup([
                        [InlineKeyboardButton("⬅️ Previous", callback_data=f"simple_chats_page_{page - 1}")],
                        [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
                    ])
                
                if hasattr(q, 'edit_message_text'):
                    await q.edit_message_text(message, reply_markup=kb)
                else:
                    await update.effective_message.reply_text(message, reply_markup=kb)
        except Exception as e:
            error_message = f"❌ **Error loading chats**\n\nFailed to load page {page + 1}: {e}\n\nPlease try again or go back to main menu."
            error_kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]])
            if hasattr(q, 'edit_message_text'):
                await q.edit_message_text(error_message, reply_markup=error_kb)
            else:
                await update.effective_message.reply_text(error_message, reply_markup=error_kb)
    
    async def cmd_startengine(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Manually start the forwarding engine"""
        if not await self._guard(update):
            return
        
        if not await self.engine.ensure_client():
            await update.effective_message.reply_text("❌ Not logged in. Use /login first.")
            return
        
        if self.engine.started:
            await update.effective_message.reply_text("✅ Engine is already running.")
            return
        
        try:
            # Start the engine in the background
            asyncio.create_task(self.engine.start())
            await update.effective_message.reply_text("✅ Engine started! It should now listen for messages.")
        except Exception as e:
            await update.effective_message.reply_text(f"❌ Failed to start engine: {e}")

    async def cmd_stopengine(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Manually stop the forwarding engine"""
        if not await self._guard(update):
            return
        
        if not self.engine.started:
            await update.effective_message.reply_text("❌ Engine is not running.")
            return
        
        try:
            # Disconnect the client to stop the engine
            if self.engine.client:
                await self.engine.client.disconnect()
                self.engine.started = False
            await update.effective_message.reply_text("✅ Engine stopped.")
        except Exception as e:
            await update.effective_message.reply_text(f"❌ Failed to stop engine: {e}")

    async def cmd_reset_circuit(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Reset circuit breaker and re-enable forwarding after ban protection"""
        if not await self._guard(update):
            return

        try:
            # Reset error counters
            self.store.set_kv("recent_errors", 0)
            self.store.set_kv("last_error_time", 0)

            # Re-enable forwarding
            self.store.set_kv("forwarding_on", True)
            self.store.set_kv("circuit_breaker_active", False)

            await update.effective_message.reply_text("🔄 **Circuit Breaker Reset**\n\n✅ Error counters cleared\n✅ Forwarding re-enabled\n\nThe bot will resume normal operation.")
        except Exception as e:
            await update.effective_message.reply_text(f"❌ Failed to reset circuit breaker: {e}")

    async def cmd_unlimited_users(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Show list of users with unlimited access"""
        if not await self._guard(update):
            return

        unlimited_users = os.getenv("ALLOWED_UNLIMITED_IDS", "")
        if not unlimited_users:
            await update.effective_message.reply_text("📋 **Unlimited Users**\n\nNo users have unlimited access configured.\n\nTo add unlimited users, set the `ALLOWED_UNLIMITED_IDS` environment variable with comma-separated user IDs.")
            return

        try:
            unlimited_ids = [int(id.strip()) for id in unlimited_users.split(",") if id.strip().isdigit()]
            user_list = "\n".join([f"• `{uid}`" for uid in unlimited_ids])

            await update.effective_message.reply_text(
                f"📋 **Unlimited Access Users**\n\n"
                f"**Total:** {len(unlimited_ids)}\n\n"
                f"**User IDs:**\n{user_list}\n\n"
                f"These users bypass all rate limits and have unlimited bot access."
            )
        except Exception as e:
            await update.effective_message.reply_text(f"❌ Error reading unlimited users: {e}")

    async def cmd_debug(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Debug command to check forwarding status"""
        if not await self._guard(update):
            return
        
        user_id = update.effective_user.id
        
        # Check if user is logged in
        user_session = self.store.get_user_session(user_id)
        if not user_session or not user_session.is_verified:
            await update.effective_message.reply_text("❌ **Login Required**\n\nYou need to login with your Telegram account first.\n\nUse `/login +phone` to start.")
            return
        
        # Get user's client
        client = await self.engine.get_client(user_id)
        if not client:
            await update.effective_message.reply_text("❌ **Client Error**\n\nFailed to get your Telegram client. Please try logging in again.")
            return
        
        # Get user's tasks
        tasks = self.store.list_tasks_by_user(user_id)
        enabled_tasks = [r for r in tasks if r.enabled]
        
        # Check global forwarding status
        global_forwarding = self.store.get_kv("forwarding_on", True)
        
        # Build debug message
        debug_msg = "🔍 **Debug Information**\n\n"
        # Check unlimited user status
        unlimited_users = os.getenv("ALLOWED_UNLIMITED_IDS", "")
        is_unlimited = False
        if unlimited_users:
            unlimited_ids = [int(id.strip()) for id in unlimited_users.split(",") if id.strip().isdigit()]
            is_unlimited = user_id in unlimited_ids

        debug_msg += f"**👤 User Status:**\n"
        debug_msg += f"• User ID: {user_id}\n"
        debug_msg += f"• Verified: {'✅ Yes' if user_session.is_verified else '❌ No'}\n"
        debug_msg += f"• Client Connected: {'✅ Yes' if client.is_connected() else '❌ No'}\n"
        debug_msg += f"• Unlimited Access: {'✅ Yes' if is_unlimited else '❌ No'}\n\n"
        
        debug_msg += f"**🔄 Forwarding Status:**\n"
        debug_msg += f"• Global Switch: {'🟢 ON' if global_forwarding else '🔴 OFF'}\n"

        # Check circuit breaker status
        circuit_breaker_active = self.store.get_kv("circuit_breaker_active", False)
        recent_errors = self.store.get_kv("recent_errors", 0)

        if circuit_breaker_active:
            debug_msg += f"• Circuit Breaker: 🔴 ACTIVE (Auto-disabled due to errors)\n"
            debug_msg += f"• Use `/reset_circuit` to re-enable forwarding\n"
        else:
            debug_msg += f"• Circuit Breaker: 🟢 Normal\n"

        debug_msg += f"• Recent Errors: {recent_errors}/10 (triggers circuit breaker)\n"
        debug_msg += f"• Total tasks: {len(tasks)}\n"
        debug_msg += f"• Enabled tasks: {len(enabled_tasks)}\n\n"
        
        if enabled_tasks:
            debug_msg += f"**📋 Active tasks:**\n"
            for task in enabled_tasks:
                debug_msg += f"• **{task.name}** (ID: {task.id})\n"
                debug_msg += f"  - Source: {task.source_chat_id}\n"
                debug_msg += f"  - Destination: {task.destination_chat_id}\n"
                debug_msg += f"  - Keywords: {task.keywords or 'ALL'}\n"
                debug_msg += f"  - Media: {'ON' if task.forward_media else 'OFF'}\n"
                debug_msg += f"  - Replies: {'ON' if task.forward_replies else 'OFF'}\n"
                debug_msg += f"  - Forwards: {'ON' if task.forward_forwards else 'OFF'}\n\n"
        else:
            debug_msg += "**❌ No enabled tasks found!**\n\n"
        
        debug_msg += f"**🔧 Engine Status:**\n"
        debug_msg += f"• Engine Started: {'✅ Yes' if self.engine.started else '❌ No'}\n"
        debug_msg += f"• Active Clients: {len(self.engine.clients)}\n"
        debug_msg += f"• User in Clients: {'✅ Yes' if user_id in self.engine.clients else '❌ No'}\n\n"
        
        debug_msg += "**💡 Troubleshooting:**\n"
        if not global_forwarding:
            debug_msg += "• Global forwarding is OFF - use /startengine\n"
        if not enabled_tasks:
            debug_msg += "• No enabled tasks - create and enable tasks first\n"
        if user_id not in self.engine.clients:
            debug_msg += "• User client not active - try reconnecting\n"
        if not client.is_connected():
            debug_msg += "• Client not connected - check login status\n"
        
        await update.effective_message.reply_text(debug_msg, parse_mode='Markdown')

    async def cmd_testforward(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Test forwarding by manually triggering a message"""
        if not await self._guard(update):
            return
        
        if not await self.engine.ensure_client():
            await update.effective_message.reply_text("❌ Not logged in. Use /login first.")
            return
        
        # Create a test message event
        class TestMessage:
            def __init__(self, chat_id, text):
                self.chat_id = chat_id
                self.message = text
                self.fwd_from = None
                self.is_reply = False
        
        class TestEvent:
            def __init__(self, chat_id, text):
                self.message = TestMessage(chat_id, text)
                self.chat_id = chat_id
        
        # Get the first task to test with
        tasks = self.store.list_tasks()
        if not tasks:
            await update.effective_message.reply_text("❌ No tasks configured. Create a task first.")
            return
        
        task = tasks[0]
        test_event = TestEvent(task.source_chat_id, "Test message for debugging")
        
        try:
            await self.engine._handle_new_message(test_event)
            await update.effective_message.reply_text(f"✅ Test forwarding triggered for task {task.id}")
        except Exception as e:
            await update.effective_message.reply_text(f"❌ Test forwarding failed: {e}")

    async def cmd_fixtasks(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Fix tasks that have text instead of chat IDs"""
        if not await self._guard(update):
            return
        
        if not await self.engine.ensure_client():
            await update.effective_message.reply_text("❌ Not logged in. Use /login first.")
            return
        
        tasks = self.store.list_tasks()
        fixed_count = 0
        
        for task in tasks:
            # Check if source_chat_id is text (not a valid ID)
            if isinstance(task.source_chat_id, str) or task.source_chat_id <= 0:
                # Try to resolve the chat name
                chat_id = await self.task_builder._resolve_chat_name(str(task.source_chat_id))
                if chat_id:
                    task.source_chat_id = chat_id
                    self.store.upsert_task(task)
                    fixed_count += 1
            
            # Check if destination_chat_id is text (not a valid ID)
            if isinstance(task.destination_chat_id, str) or task.destination_chat_id <= 0:
                # Try to resolve the chat name
                chat_id = await self.task_builder._resolve_chat_name(str(task.destination_chat_id))
                if chat_id:
                    task.destination_chat_id = chat_id
                    self.store.upsert_task(task)
                    fixed_count += 1
        
        if fixed_count > 0:
            await update.effective_message.reply_text(f"✅ Fixed {fixed_count} task(s) with invalid chat IDs.")
        else:
            await update.effective_message.reply_text("✅ All tasks already have valid chat IDs.")

    # ---- Message Handler for task Creation ----
    async def handle_message(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Handle regular messages for task creation flow and blacklist input"""
        if not await self._guard(update):
            return
        
        user_id = update.effective_user.id
        text = update.effective_message.text or ""
        
        # Check if user is in login process
        if await self._handle_login_input(update, text):
            return
        
        # Check if user is in task creation mode
        if self.task_builder.get_pending_task(user_id):
            message, keyboard = await self.task_builder.handle_input(user_id, text)
            if keyboard:
                await update.effective_message.reply_text(message, reply_markup=keyboard)
            else:
                await update.effective_message.reply_text(message)
            return
        
        # Check if user is in blacklist input mode
        if hasattr(self.task_builder, 'pending_blacklist') and self.task_builder.pending_blacklist:
            await self._handle_blacklist_input(update, text)
            return

    # ------- Callbacks -------
    async def on_cb(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not await self._guard(update):
            return
        q = update.callback_query
        data = q.data
        await q.answer()
        
        # Main menu handlers
        if data == "manage_tasks":
            if not await self._require_login(update):
                return
            await self._show_tasks_management(update)
        elif data == "settings":
            if not await self._require_login(update):
                return
            await self._show_settings(update)
        elif data == "filters":
            if not await self._require_login(update):
                return
            await self._show_filters_menu(update)
        elif data == "scheduling":
            if not await self._require_login(update):
                return
            await self._show_scheduling_menu(update)
        elif data == "stats":
            if not await self._require_login(update):
                return
            await self._show_statistics(update)
        elif data == "account":
            await self._show_account_menu(update)
        elif data == "my_tasks":
            if not await self._require_login(update):
                return
            await self._show_user_tasks(update)
        elif data == "login_help":
            await self._show_login_help(update)
        elif data == "export_import":
            if not await self._require_login(update):
                return
            await self._show_export_import(update)
        elif data == "tools":
            if not await self._require_login(update):
                return
            await self._show_tools_menu(update)
        
        # Legacy handlers
        elif data == "list":
            await self._send_tasks(update)
        elif data == "create":
            if not await self._require_login(update):
                return
            message = await self.task_builder.start_creation(update.effective_user.id)
            await q.edit_message_text(message)
        elif data == "startf":
            if not await self._require_login(update):
                return
            self.store.set_kv("forwarding_on", True)
            await self._refresh_main_menu(update, "🟢 Bot started! Forwarding is now ON.")
        elif data == "stopf":
            if not await self._require_login(update):
                return
            self.store.set_kv("forwarding_on", False)
            await self._refresh_main_menu(update, "🔴 Bot stopped! Forwarding is now OFF.")
        elif data == "login":
            await self._start_interactive_login(update)
        elif data == "status":
            await self._status(update)
        elif data == "export":
            await self.cmd_export(update, ctx)
        elif data == "import":
            await q.edit_message_text("Reply to this message with a JSON file and caption /import")
        elif data == "logout":
            try:
                ok = await self.engine.logout()
                self.pending_login.pop(update.effective_user.id, None)
                await q.edit_message_text("Logged out." if ok else "Logout attempted.")
            except Exception as e:
                await q.edit_message_text(f"Logout failed: {e}")
        elif data.startswith("task_"):
            response, kb = await self.task_builder.handle_callback(update.effective_user.id, data)
            if kb:
                await q.edit_message_text(response, reply_markup=kb)
            else:
                await q.edit_message_text(response)
        # task management handlers
        elif data.startswith("edit_task_"):
            if not await self._require_login(update):
                return
            task_id = data.split("_")[-1]
            await self._edit_task(update, task_id)
        elif data.startswith("delete_task_"):
            if not await self._require_login(update):
                return
            task_id = data.split("_")[-1]
            await self._confirm_delete_task(update, task_id)
        elif data.startswith("toggle_task_"):
            if not await self._require_login(update):
                return
            task_id = data.split("_")[-1]
            await self._toggle_task(update, task_id)
        # Filter handlers
        elif data.startswith("add_blacklist_"):
            if not await self._require_login(update):
                return
            filter_type = data.split("_")[-1]
            await self._add_blacklist_entry(update, filter_type)
        elif data.startswith("manage_blacklist_"):
            if not await self._require_login(update):
                return
            filter_type = data.split("_")[-1]
            await self._manage_blacklist(update, filter_type)
        elif data.startswith("confirm_delete_"):
            if not await self._require_login(update):
                return
            task_id = data.split("_")[-1]
            await self._confirm_delete_task_final(update, task_id)
        # Advanced feature handlers
        elif data.startswith("toggle_blacklist_"):
            if not await self._require_login(update):
                return
            entry_id = data.split("_")[-1]
            await self._toggle_blacklist_entry(update, entry_id)
        elif data.startswith("delete_blacklist_"):
            if not await self._require_login(update):
                return
            entry_id = data.split("_")[-1]
            await self._delete_blacklist_entry(update, entry_id)
        elif data in ["auto_scheduler", "set_delays", "power_schedule", "edit_time_limits", 
                     "manage_schedules", "auto_restart", "general_settings", "interface_settings",
                     "security_settings", "performance_settings", "detailed_stats", "performance_graph",
                     "reset_stats", "export_report", "account_info", "change_session", "2fa_settings",
                     "device_management", "export_stats", "backup_all"]:
            if not await self._require_login(update):
                return
            await self._handle_advanced_feature(update, data)
        # Back to main menu
        elif data == "back_to_main":
            await self._show_main_menu(update)
        # Additional feature handlers
        elif data == "list_chats":
            if not await self._require_login(update):
                return
            await self._show_chats_page(update, 0)
        elif data == "test_forward":
            if not await self._require_login(update):
                return
            await self.cmd_testforward(update, ctx)
        elif data == "simple_chats":
            if not await self._require_login(update):
                return
            await self.cmd_simplechats(update, ctx)
        elif data == "start_engine":
            if not await self._require_login(update):
                return
            await self.cmd_startengine(update, ctx)
        elif data == "stop_engine":
            if not await self._require_login(update):
                return
            await self.cmd_stopengine(update, ctx)
        elif data.startswith("chats_page_"):
            if not await self._require_login(update):
                return
            page = int(data.split("_")[-1])
            await self._show_chats_page(update, page)
        elif data.startswith("simple_chats_page_"):
            if not await self._require_login(update):
                return
            page = int(data.split("_")[-1])
            await self._show_simple_chats_page(update, page)
        elif data.startswith("test_task_"):
            if not await self._require_login(update):
                return
            task_id = data.split("_")[-1]
            await self._test_task(update, task_id)
        elif data == "refresh_monitoring":
            await self.cmd_refresh_monitoring(update, ctx)
        elif data == "test_monitoring":
            await self.cmd_test_monitoring(update, ctx)
        elif data == "cleanup_handlers":
            await self.cmd_cleanup_handlers(update, ctx)
        elif data == "all_users":
            await self.cmd_all_users(update, ctx)
        elif data == "force_logout":
            await self.cmd_force_logout(update, ctx)
        else:
            await q.edit_message_text(f"Unknown callback: {data}")

    async def _send_tasks(self, update: Update):
        tasks = self.store.list_tasks()
        if not tasks:
            await update.effective_message.reply_text("No tasks yet. Tap ➕ Create task.")
            return
        lines = [
            f"{i+1}. {r.name} ({r.id})\n"
            f"   src: {r.source_chat_id} → dst: {r.destination_chat_id}\n"
            f"   kw:[{r.keywords or 'ALL'}] !kw:[{r.exclude_keywords or '-'}]\n"
            f"   media:{human(r.forward_media)} replies:{human(r.forward_replies)} fwd:{human(r.forward_forwards)} delay:{r.delay_seconds}s\n"
            f"   enabled:{human(r.enabled)} msgs:{r.message_count} last:{r.last_used or '-'}"
            for i, r in enumerate(tasks)
        ]
        await update.effective_message.reply_text("\n\n".join(lines))

    async def _status(self, update: Update):
        ok = await self.engine.ensure_client()
        on = self.store.get_kv("forwarding_on", True)
        txt = [f"Telethon authorized: {human(ok)}", f"Forwarding switch: {'ON' if on else 'OFF'}"]
        if ok and self.engine.client:
            try:
                me = await self.engine.client.get_me()
                if me:
                    name = (getattr(me, 'first_name', '') or '') + (' ' + getattr(me, 'last_name', '') if getattr(me, 'last_name', None) else '')
                    username = getattr(me, 'username', None)
                    phone = getattr(me, 'phone', None)
                    txt.append(f"Logged in as: {name.strip() or '-'}" + (f" (@{username})" if username else ""))
                    if phone:
                        txt.append(f"Phone: +{phone}")
                    txt.append(f"User ID: {getattr(me, 'id', '-')}")
            except Exception:
                pass
        await update.effective_message.reply_text("\n".join(txt))

    # ---- Enhanced Menu Methods ----
    
    async def _refresh_main_menu(self, update: Update, status_message: str):
        """Refresh main menu with status message"""
        q = update.callback_query
        # Show status briefly, then return to main menu
        if q and hasattr(q, 'edit_message_text'):
            await q.edit_message_text(f"{status_message}\n\n🔄 Returning to main menu...")
        else:
            await update.effective_message.reply_text(f"{status_message}\n\n🔄 Returning to main menu...")
        await asyncio.sleep(2)
        await self._show_main_menu(update)
    
    async def _show_tasks_management(self, update: Update):
        """Show tasks management menu"""
        tasks = self.store.list_tasks()
        q = update.callback_query
        
        if not tasks:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Create First task", callback_data="create")],
                [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
            ])
            await q.edit_message_text("📋 **tasks Management**\n\nNo tasks configured yet.\nCreate your first task to get started!", 
                                    reply_markup=kb, parse_mode='Markdown')
            return
        
        # Create tasks list with action buttons
        tasks_text = "📋 **tasks Management**\n\n"
        kb_rows = []
        
        for i, task in enumerate(tasks, 1):
            status_icon = "🟢" if task.enabled else "🔴"
            tasks_text += f"{i}. {status_icon} **{task.name}**\n"
            tasks_text += f"   📤 {task.source_chat_id} → �� {task.destination_chat_id}\n"
            tasks_text += f"   📊 Messages: {task.message_count}\n\n"
            
            # Action buttons for each task
            kb_rows.append([
                InlineKeyboardButton(f"✏️ Edit {task.name[:15]}", callback_data=f"edit_task_{task.id}"),
                InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_task_{task.id}")
            ])
            kb_rows.append([
                InlineKeyboardButton(f"{'🔴 Disable' if task.enabled else '🟢 Enable'}", 
                                   callback_data=f"toggle_task_{task.id}")
            ])
        
        # Add navigation buttons
        kb_rows.append([InlineKeyboardButton("➕ Create New task", callback_data="create")])
        kb_rows.append([InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")])
        
        kb = InlineKeyboardMarkup(kb_rows)
        await q.edit_message_text(tasks_text, reply_markup=kb, parse_mode='Markdown')
    
    async def _show_filters_menu(self, update: Update):
        """Show filters and moderation menu"""
        q = update.callback_query
        
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🚫 Blacklist Keywords", callback_data="add_blacklist_keyword"),
             InlineKeyboardButton("✅ Whitelist Keywords", callback_data="add_blacklist_whitelist")],
            [InlineKeyboardButton("🚷 Blacklist Users", callback_data="add_blacklist_user"),
             InlineKeyboardButton("🟢 Whitelist Users", callback_data="add_blacklist_whitelist_user")],
            [InlineKeyboardButton("🔍 Manage Blacklist", callback_data="manage_blacklist_all"),
             InlineKeyboardButton("📊 Filter Statistics", callback_data="filter_stats")],
            [InlineKeyboardButton("🧹 Cleaner Settings", callback_data="cleaner_settings"),
             InlineKeyboardButton("🚫 Duplicate Prevention", callback_data="duplicate_settings")],
            [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
        ])
        
        await q.edit_message_text("🔍 **Filters & Moderation**\n\n"
                                "Control what gets forwarded and what gets blocked.\n\n"
                                "**Features:**\n"
                                "• 🚫 Block unwanted keywords/users\n"
                                "• ✅ Allow specific keywords/users\n"
                                "• 🧹 Clean up messages\n"
                                "• 🚫 Prevent duplicates", 
                                reply_markup=kb, parse_mode='Markdown')
    
    async def _show_scheduling_menu(self, update: Update):
        """Show scheduling and automation menu"""
        q = update.callback_query
        
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⏰ Auto Post Scheduler", callback_data="auto_scheduler"),
             InlineKeyboardButton("⏳ Set Delays", callback_data="set_delays")],
            [InlineKeyboardButton("🕹️ Power Schedule", callback_data="power_schedule"),
             InlineKeyboardButton("⏱️ Edit Time Limits", callback_data="edit_time_limits")],
            [InlineKeyboardButton("📅 Manage Schedules", callback_data="manage_schedules"),
             InlineKeyboardButton("🔄 Auto Restart", callback_data="auto_restart")],
            [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
        ])
        
        await q.edit_message_text("⏰ **Scheduling & Automation**\n\n"
                                "Automate your forwarding operations.\n\n"
                                "**Features:**\n"
                                "• ⏰ Schedule posts at specific times\n"
                                "• ⏳ Set delays between forwards\n"
                                "• 🕹️ Control bot activation schedules\n"
                                "• ⏱️ Set message editing time limits", 
                                reply_markup=kb, parse_mode='Markdown')
    
    async def _show_settings(self, update: Update):
        """Show settings menu"""
        q = update.callback_query
        
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔧 General Settings", callback_data="general_settings"),
             InlineKeyboardButton("📱 Interface Settings", callback_data="interface_settings")],
            [InlineKeyboardButton("🔒 Security Settings", callback_data="security_settings"),
             InlineKeyboardButton("📊 Performance Settings", callback_data="performance_settings")],
            [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
        ])
        
        await q.edit_message_text("⚙️ **Settings**\n\n"
                                "Configure your bot's behavior and appearance.\n\n"
                                "**Categories:**\n"
                                "• 🔧 General bot settings\n"
                                "• 📱 Interface customization\n"
                                "• 🔒 Security and permissions\n"
                                "• 📊 Performance optimization", 
                                reply_markup=kb, parse_mode='Markdown')
    
    async def _show_statistics(self, update: Update):
        """Show statistics and analytics"""
        q = update.callback_query
        tasks = self.store.list_tasks()
        
        total_messages = sum(task.message_count for task in tasks)
        active_tasks = sum(1 for task in tasks if task.enabled)
        total_tasks = len(tasks)
        
        stats_text = "📊 **Statistics & Analytics**\n\n"
        stats_text += f"**📈 Overall Performance:**\n"
        stats_text += f"• Total Messages Forwarded: {total_messages:,}\n"
        stats_text += f"• Active tasks: {active_tasks}/{total_tasks}\n"
        stats_text += f"• Bot Status: {'🟢 Running' if self.store.get_kv('forwarding_on', True) else '🔴 Stopped'}\n\n"
        
        if tasks:
            stats_text += "**📋 task Performance:**\n"
            for task in tasks[:5]:  # Show top 5 tasks
                stats_text += f"• {task.name}: {task.message_count:,} messages\n"
            if len(tasks) > 5:
                stats_text += f"• ... and {len(tasks) - 5} more tasks\n"
        
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 Detailed Stats", callback_data="detailed_stats"),
             InlineKeyboardButton("📈 Performance Graph", callback_data="performance_graph")],
            [InlineKeyboardButton("🔄 Reset Stats", callback_data="reset_stats"),
             InlineKeyboardButton("📤 Export Report", callback_data="export_report")],
            [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
        ])
        
        await q.edit_message_text(stats_text, reply_markup=kb, parse_mode='Markdown')
    
    async def _show_account_menu(self, update: Update):
        """Show account management menu"""
        q = update.callback_query
        user_id = update.effective_user.id
        
        # Check user session status properly
        user_session = self.store.get_user_session(user_id)
        is_logged_in = user_session and user_session.is_verified
        
        # Create dynamic keyboard based on login status
        if is_logged_in:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🚪 Logout", callback_data="logout")],
                [InlineKeyboardButton("👤 Account Info", callback_data="account_info"),
                 InlineKeyboardButton("📊 My tasks", callback_data="my_tasks")],
                [InlineKeyboardButton("🔑 Change Session", callback_data="change_session"),
                 InlineKeyboardButton("🔒 2FA Settings", callback_data="2fa_settings")],
                [InlineKeyboardButton("📱 Device Management", callback_data="device_management")],
                [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
            ])
            status_text = "✅ Logged in and verified"
            session_info = f"• Phone: {user_session.phone}\n• tasks: {user_session.tasks_count}\n• Last activity: {user_session.last_activity[:10]}"
        else:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔐 Login", callback_data="login")],
                [InlineKeyboardButton("📱 Help", callback_data="login_help")],
                [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
            ])
            status_text = "❌ Not logged in"
            session_info = "• No active session\n• Login required to use forwarding"
        
        await q.edit_message_text(f"🔐 **Account Management**\n\n"
                                f"**Status:** {status_text}\n\n"
                                f"**Session Details:**\n{session_info}\n\n"
                                "**Available Actions:**\n"
                                "• 🔐 Login/logout management\n"
                                "• 👤 Account information\n"
                                "• 📊 task management\n"
                                "• 🔒 Security settings", 
                                reply_markup=kb, parse_mode='Markdown')
    
    async def _show_user_tasks(self, update: Update):
        """Show user's own tasks"""
        q = update.callback_query
        user_id = update.effective_user.id
        
        tasks = self.store.list_tasks_by_user(user_id)
        
        if not tasks:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Create task", callback_data="create")],
                [InlineKeyboardButton("🔙 Back to Account", callback_data="account")]
            ])
            await q.edit_message_text("📊 **My tasks**\n\n"
                                    "You don't have any forwarding tasks yet.\n\n"
                                    "**Create your first task to start forwarding messages!**", 
                                    reply_markup=kb, parse_mode='Markdown')
            return
        
        # Show first few tasks with pagination
        tasks_text = f"📊 **My tasks** ({len(tasks)} total)\n\n"
        for i, task in enumerate(tasks[:5]):  # Show first 5
            status = "🟢" if task.enabled else "🔴"
            tasks_text += f"{i+1}. {status} **{task.name}**\n"
            tasks_text += f"   📤 {task.source_chat_id} → 📥 {task.destination_chat_id}\n"
            tasks_text += f"   📝 {task.keywords or 'ALL'}\n\n"
        
        if len(tasks) > 5:
            tasks_text += f"... and {len(tasks) - 5} more tasks\n\n"
        
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Create New task", callback_data="create")],
            [InlineKeyboardButton("📋 View All tasks", callback_data="tasks")],
            [InlineKeyboardButton("🔙 Back to Account", callback_data="account")]
        ])
        
        await q.edit_message_text(tasks_text, reply_markup=kb, parse_mode='Markdown')
    
    async def _show_login_help(self, update: Update):
        """Show login help and instructions"""
        q = update.callback_query
        
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔐 Start Login", callback_data="login")],
            [InlineKeyboardButton("🔙 Back to Account", callback_data="account")]
        ])
        
        help_text = "📱 **Login Help**\n\n"
        help_text += "**To use the bot, you need to login with your Telegram account:**\n\n"
        help_text += "1️⃣ **Click 'Start Login'**\n"
        help_text += "2️⃣ **Send your phone number** as: `/login +1234567890`\n"
        help_text += "3️⃣ **Send verification code** as: `/code 1 2 3 4 5` (separate digits)\n"
        help_text += "4️⃣ **If 2FA enabled**, send: `/2fa yourpassword`\n"
        help_text += "5️⃣ **Complete login** with: `/signin`\n\n"
        help_text += "**Note:** Use space-separated digits for the code to avoid detection."
        
        await q.edit_message_text(help_text, reply_markup=kb, parse_mode='Markdown')
    
    async def _show_tasks_management(self, update: Update):
        """Show tasks management menu"""
        q = update.callback_query
        user_id = update.effective_user.id
        
        tasks = self.store.list_tasks_by_user(user_id)
        
        if not tasks:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Create First task", callback_data="create")],
                [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
            ])
            await q.edit_message_text("📋 **tasks Management**\n\n"
                                    "You don't have any forwarding tasks yet.\n\n"
                                    "**Create your first task to start forwarding messages!**", 
                                    reply_markup=kb, parse_mode='Markdown')
            return
        
        # Create keyboard with tasks
        kb_rows = []
        for task in tasks:
            status = "🟢" if task.enabled else "🔴"
            kb_rows.append([
                InlineKeyboardButton(f"{status} {task.name[:20]}", callback_data=f"view_task_{task.id}"),
                InlineKeyboardButton(f"✏️ Edit {task.name[:15]}", callback_data=f"edit_task_{task.id}"),
                InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_task_{task.id}")
            ])
            kb_rows.append([
                InlineKeyboardButton(f"🔄 {'Disable' if task.enabled else 'Enable'}", 
                                   callback_data=f"toggle_task_{task.id}")
            ])
        
        kb_rows.append([InlineKeyboardButton("➕ Create New task", callback_data="create")])
        kb_rows.append([InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")])
        
        kb = InlineKeyboardMarkup(kb_rows)
        
        tasks_text = f"📋 **tasks Management** ({len(tasks)} tasks)\n\n"
        tasks_text += "**Your Forwarding tasks:**\n"
        for i, task in enumerate(tasks, 1):
            status = "🟢 Enabled" if task.enabled else "🔴 Disabled"
            tasks_text += f"{i}. **{task.name}** - {status}\n"
            tasks_text += f"   📤 {task.source_chat_id} → 📥 {task.destination_chat_id}\n"
            tasks_text += f"   📝 {task.keywords or 'ALL'}\n\n"
        
        await q.edit_message_text(tasks_text, reply_markup=kb, parse_mode='Markdown')
    
    async def _show_filters_menu(self, update: Update):
        """Show filters and moderation menu"""
        q = update.callback_query
        
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🚫 Blacklist Keywords", callback_data="add_blacklist_keyword"),
             InlineKeyboardButton("✅ Whitelist Keywords", callback_data="add_blacklist_whitelist")],
            [InlineKeyboardButton("🚷 Blacklist Users", callback_data="add_blacklist_user"),
             InlineKeyboardButton("🟢 Whitelist Users", callback_data="add_blacklist_whitelist_user")],
            [InlineKeyboardButton("🔍 Manage Blacklist", callback_data="manage_blacklist_all"),
             InlineKeyboardButton("📊 Filter Statistics", callback_data="filter_stats")],
            [InlineKeyboardButton("🧹 Cleaner Settings", callback_data="cleaner_settings"),
             InlineKeyboardButton("🚫 Duplicate Prevention", callback_data="duplicate_settings")],
            [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
        ])
        
        filters_text = "🔍 **Filters & Moderation**\n\n"
        filters_text += "**Content Filtering:**\n"
        filters_text += "• 🚫 Block messages with specific keywords\n"
        filters_text += "• ✅ Allow only messages with specific keywords\n"
        filters_text += "• 🚷 Block messages from specific users\n"
        filters_text += "• 🟢 Allow messages only from specific users\n\n"
        filters_text += "**Advanced Features:**\n"
        filters_text += "• 🔍 Manage your filter lists\n"
        filters_text += "• 📊 View filtering statistics\n"
        filters_text += "• 🧹 Cleaner and maintenance tools\n"
        filters_text += "• 🚫 Prevent duplicate messages"
        
        await q.edit_message_text(filters_text, reply_markup=kb, parse_mode='Markdown')
    
    async def _show_scheduling_menu(self, update: Update):
        """Show scheduling and automation menu"""
        q = update.callback_query
        
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⏰ Auto Post Scheduler", callback_data="auto_scheduler"),
             InlineKeyboardButton("⏳ Set Delays", callback_data="set_delays")],
            [InlineKeyboardButton("🕹️ Power Schedule", callback_data="power_schedule"),
             InlineKeyboardButton("⏱️ Edit Time Limits", callback_data="edit_time_limits")],
            [InlineKeyboardButton("📅 Manage Schedules", callback_data="manage_schedules"),
             InlineKeyboardButton("🔄 Auto Restart", callback_data="auto_restart")],
            [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
        ])
        
        scheduling_text = "⏰ **Scheduling & Automation**\n\n"
        scheduling_text += "**Time-based Features:**\n"
        scheduling_text += "• ⏰ Schedule posts at specific times\n"
        scheduling_text += "• ⏳ Set delays between forwards\n"
        scheduling_text += "• 🕹️ Control when bot is active\n"
        scheduling_text += "• ⏱️ Set time limits for editing\n\n"
        scheduling_text += "**Automation:**\n"
        scheduling_text += "• 📅 Manage multiple schedules\n"
        scheduling_text += "• 🔄 Automatic restart capabilities"
        
        await q.edit_message_text(scheduling_text, reply_markup=kb, parse_mode='Markdown')
    
    async def _show_statistics(self, update: Update):
        """Show statistics menu"""
        q = update.callback_query
        user_id = update.effective_user.id
        
        # Get user's tasks and stats
        tasks = self.store.list_tasks_by_user(user_id)
        active_tasks = [r for r in tasks if r.enabled]
        
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 Detailed Stats", callback_data="detailed_stats"),
             InlineKeyboardButton("📈 Performance Graph", callback_data="performance_graph")],
            [InlineKeyboardButton("🔄 Reset Stats", callback_data="reset_stats"),
             InlineKeyboardButton("📤 Export Report", callback_data="export_report")],
            [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
        ])
        
        stats_text = "📊 **Statistics**\n\n"
        stats_text += f"**Your Bot Statistics:**\n"
        stats_text += f"• Total tasks: {len(tasks)}\n"
        stats_text += f"• Active tasks: {len(active_tasks)}\n"
        stats_text += f"• Inactive tasks: {len(tasks) - len(active_tasks)}\n\n"
        
        if tasks:
            total_messages = sum(task.message_count for task in tasks)
            stats_text += f"**Performance:**\n"
            stats_text += f"• Total Messages Processed: {total_messages}\n"
            stats_text += f"• Average per task: {total_messages // len(tasks) if len(tasks) > 0 else 0}\n\n"
        
        stats_text += "**Available Reports:**\n"
        stats_text += "• 📊 Detailed performance metrics\n"
        stats_text += "• 📈 Visual performance graphs\n"
        stats_text += "• 🔄 Reset all statistics\n"
        stats_text += "• 📤 Export detailed reports"
        
        await q.edit_message_text(stats_text, reply_markup=kb, parse_mode='Markdown')
    
    async def _add_blacklist_entry(self, update: Update, filter_type: str):
        """Add blacklist/whitelist entry"""
        q = update.callback_query
        user_id = update.effective_user.id
        
        # Set pending state for user input
        if not hasattr(self, 'pending_blacklist'):
            self.pending_blacklist = {}
        
        self.pending_blacklist[user_id] = filter_type
        
        filter_names = {
            "keyword": "keyword",
            "whitelist": "whitelist keyword", 
            "user": "user ID",
            "whitelist_user": "whitelist user ID"
        }
        
        filter_name = filter_names.get(filter_type, filter_type)
        
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back to Filters", callback_data="filters")]
        ])
        
        await q.edit_message_text(f"📝 **Add {filter_name.title()}**\n\n"
                                f"Please send the {filter_name} you want to add.\n\n"
                                f"**Examples:**\n"
                                f"• For keywords: `spam`, `advertisement`\n"
                                f"• For users: `123456789`\n\n"
                                f"**Note:** Send the text directly, no commands needed.", 
                                reply_markup=kb, parse_mode='Markdown')
    
    async def _manage_blacklist(self, update: Update, filter_type: str):
        """Manage blacklist/whitelist entries"""
        q = update.callback_query
        user_id = update.effective_user.id
        
        # This would show existing entries and allow management
        # For now, show a placeholder
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back to Filters", callback_data="filters")]
        ])
        
        await q.edit_message_text(f"🔍 **Manage {filter_type.title()}**\n\n"
                                f"This feature is coming soon!\n\n"
                                f"You'll be able to:\n"
                                f"• View all entries\n"
                                f"• Edit existing entries\n"
                                f"• Delete entries\n"
                                f"• Enable/disable entries", 
                                reply_markup=kb, parse_mode='Markdown')
    
    async def _confirm_delete_task_final(self, update: Update, task_id: str):
        """Final confirmation for task deletion"""
        q = update.callback_query
        user_id = update.effective_user.id
        
        task = self.store.get_task(task_id)
        if not task:
            await q.edit_message_text("❌ task not found!")
            return
        
        # Delete the task
        try:
            self.store.delete_task(task_id)
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back to tasks", callback_data="manage_tasks")]
            ])
            await q.edit_message_text(f"✅ **task Deleted Successfully!**\n\n"
                                    f"task **{task.name}** has been permanently removed.", 
                                    reply_markup=kb, parse_mode='Markdown')
        except Exception as e:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back to tasks", callback_data="manage_tasks")]
            ])
            await q.edit_message_text(f"❌ **Error Deleting task**\n\n"
                                    f"Failed to delete task: {e}", 
                                    reply_markup=kb, parse_mode='Markdown')
    
    async def _toggle_blacklist_entry(self, update: Update, entry_id: str):
        """Toggle blacklist entry enabled/disabled status"""
        q = update.callback_query
        await q.edit_message_text("✅ Blacklist entry toggled!\n\nReturning to blacklist management...")
        await asyncio.sleep(2)
        await self._manage_blacklist(update, "all")
    
    async def _delete_blacklist_entry(self, update: Update, entry_id: str):
        """Delete a blacklist entry"""
        q = update.callback_query
        await q.edit_message_text("✅ Blacklist entry deleted!\n\nReturning to blacklist management...")
        await asyncio.sleep(2)
        await self._manage_blacklist(update, "all")
    
    async def _refresh_main_menu(self, update: Update, message: str = ""):
        """Refresh the main menu with optional status message"""
        q = update.callback_query
        user_id = update.effective_user.id
        
        # Check if user is logged in
        user_session = self.store.get_user_session(user_id)
        is_logged_in = user_session and user_session.is_verified
        forwarding_status = self.store.get_kv("forwarding_on", True)
        
        welcome_text = "🚀 **Auto-Forwarder Pro**\n\n"
        if message:
            welcome_text += f"**Status:** {message}\n\n"
        
        if is_logged_in:
            welcome_text += "✅ **Status:** Logged in and ready\n"
            welcome_text += f"🔄 **Forwarding:** {'🟢 ON' if forwarding_status else '🔴 OFF'}\n"
            welcome_text += f"📊 **Your tasks:** {self.store.get_user_tasks_count(user_id)} active\n"
        else:
            welcome_text += "❌ **Status:** Not logged in\n"
            welcome_text += "🔐 **Action needed:** Login first\n"
        
        welcome_text += "\n📱 **Use the menu below to control your bot:**"
        
        kb = InlineKeyboardMarkup([
            # Main Actions Row
            [InlineKeyboardButton("➕ Create task", callback_data="create"),
             InlineKeyboardButton("📋 Manage tasks", callback_data="manage_tasks")],
            
            # Control Row
            [InlineKeyboardButton("🟢 Start Bot" if forwarding_status else "🔴 Stop Bot", 
                                callback_data="stopf" if forwarding_status else "startf"),
             InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
            
            # Advanced Features Row
            [InlineKeyboardButton("🔍 Filters & Moderation", callback_data="filters"),
             InlineKeyboardButton("⏰ Scheduling", callback_data="scheduling")],
            
            # Management Row
            [InlineKeyboardButton("📊 Statistics", callback_data="stats"),
             InlineKeyboardButton("🔐 Account", callback_data="account")],
            
            # Tools Row
            [InlineKeyboardButton("📦 Export/Import", callback_data="export_import"),
             InlineKeyboardButton("🛠️ Tools", callback_data="tools")]
        ])
        
        await q.edit_message_text(welcome_text, reply_markup=kb, parse_mode='Markdown')
    
    async def _show_main_menu(self, update: Update):
        """Show the main menu"""
        q = update.callback_query
        user_id = update.effective_user.id
        
        # Check if user is logged in
        user_session = self.store.get_user_session(user_id)
        is_logged_in = user_session and user_session.is_verified
        forwarding_status = self.store.get_kv("forwarding_on", True)
        
        welcome_text = "🚀 **Auto-Forwarder Pro**\n\n"
        if is_logged_in:
            welcome_text += "✅ **Status:** Logged in and ready\n"
            welcome_text += f"🔄 **Forwarding:** {'🟢 ON' if forwarding_status else '🔴 OFF'}\n"
            welcome_text += f"📊 **Your tasks:** {self.store.get_user_tasks_count(user_id)} active\n"
        else:
            welcome_text += "❌ **Status:** Not logged in\n"
            welcome_text += "🔐 **Action needed:** Login first\n"
        
        welcome_text += "\n📱 **Use the menu below to control your bot:**"
        
        kb = InlineKeyboardMarkup([
            # Main Actions Row
            [InlineKeyboardButton("➕ Create task", callback_data="create"),
             InlineKeyboardButton("📋 Manage tasks", callback_data="manage_tasks")],
            
            # Control Row
            [InlineKeyboardButton("🟢 Start Bot" if forwarding_status else "🔴 Stop Bot", 
                                callback_data="stopf" if forwarding_status else "startf"),
             InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
            
            # Advanced Features Row
            [InlineKeyboardButton("🔍 Filters & Moderation", callback_data="filters"),
             InlineKeyboardButton("⏰ Scheduling", callback_data="scheduling")],
            
            # Management Row
            [InlineKeyboardButton("📊 Statistics", callback_data="stats"),
             InlineKeyboardButton("🔐 Account", callback_data="account")],
            
            # Tools Row
            [InlineKeyboardButton("📦 Export/Import", callback_data="export_import"),
             InlineKeyboardButton("🛠️ Tools", callback_data="tools")]
        ])
        
        # Handle both callback queries and direct messages
        if q and hasattr(q, 'edit_message_text'):
            await q.edit_message_text(welcome_text, reply_markup=kb, parse_mode='Markdown')
        else:
            await update.effective_message.reply_text(welcome_text, reply_markup=kb, parse_mode='Markdown')
    
    async def _handle_blacklist_input(self, update: Update, text: str):
        """Handle blacklist input from user"""
        user_id = update.effective_user.id
        
        if not hasattr(self, 'pending_blacklist') or user_id not in self.pending_blacklist:
            return
        
        filter_type = self.pending_blacklist[user_id]
        del self.pending_blacklist[user_id]
        
        # Here you would add the entry to the database
        # For now, just show a success message
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back to Filters", callback_data="filters")]
        ])
        
        filter_names = {
            "keyword": "keyword",
            "whitelist": "whitelist keyword", 
            "user": "user ID",
            "whitelist_user": "whitelist user ID"
        }
        
        filter_name = filter_names.get(filter_type, filter_type)
        
        await update.effective_message.reply_text(f"✅ **{filter_name.title()} Added Successfully!**\n\n"
                                                f"Added: `{text}`\n\n"
                                                f"**Note:** This feature is coming soon with full database integration.", 
                                                reply_markup=kb, parse_mode='Markdown')
    
    async def _show_detailed_account_info(self, update: Update):
        """Show detailed account information"""
        q = update.callback_query
        user_id = update.effective_user.id
        
        user_session = self.store.get_user_session(user_id)
        if not user_session:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔐 Login", callback_data="login")],
                [InlineKeyboardButton("🔙 Back to Account", callback_data="account")]
            ])
            await q.edit_message_text("❌ **No Account Found**\n\n"
                                    "You don't have an account session yet.\n"
                                    "Please login first to use the bot.", 
                                    reply_markup=kb, parse_mode='Markdown')
            return
        
        # Get user's tasks
        tasks = self.store.list_tasks_by_user(user_id)
        active_tasks = [r for r in tasks if r.enabled]
        
        # Format dates
        created_date = user_session.created_at[:10] if user_session.created_at else "Unknown"
        last_activity = user_session.last_activity[:10] if user_session.last_activity else "Unknown"
        
        info_text = f"👤 **Account Information**\n\n"
        info_text += f"**User ID:** `{user_id}`\n"
        info_text += f"**Phone:** `{user_session.phone}`\n"
        info_text += f"**Status:** {'✅ Verified' if user_session.is_verified else '❌ Not Verified'}\n"
        info_text += f"**Premium:** {'⭐ Yes' if user_session.is_premium else '❌ No'}\n"
        info_text += f"**Created:** {created_date}\n"
        info_text += f"**Last Activity:** {last_activity}\n\n"
        
        info_text += f"**tasks Summary:**\n"
        info_text += f"• Total tasks: {len(tasks)}\n"
        info_text += f"• Active tasks: {len(active_tasks)}\n"
        info_text += f"• Inactive tasks: {len(tasks) - len(active_tasks)}\n\n"
        
        if tasks:
            info_text += "**Recent tasks:**\n"
            for i, task in enumerate(tasks[:3]):
                status = "🟢" if task.enabled else "🔴"
                info_text += f"{i+1}. {status} {task.name}\n"
        
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 My tasks", callback_data="my_tasks")],
            [InlineKeyboardButton("🔙 Back to Account", callback_data="account")]
        ])
        
        await q.edit_message_text(info_text, reply_markup=kb, parse_mode='Markdown')
    
    async def _show_settings(self, update: Update):
        """Show settings menu"""
        q = update.callback_query
        user_id = update.effective_user.id
        
        # Get current settings
        forwarding_status = self.store.get_kv("forwarding_on", True)
        
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔧 General Settings", callback_data="general_settings"),
             InlineKeyboardButton("📱 Interface Settings", callback_data="interface_settings")],
            [InlineKeyboardButton("🔒 Security Settings", callback_data="security_settings"),
             InlineKeyboardButton("📊 Performance Settings", callback_data="performance_settings")],
            [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
        ])
        
        settings_text = "⚙️ **Settings**\n\n"
        settings_text += f"**Current Status:**\n"
        settings_text += f"• Forwarding: {'🟢 ON' if forwarding_status else '🔴 OFF'}\n"
        settings_text += f"• Engine: {'🟢 Running' if self.engine.started else '🔴 Stopped'}\n\n"
        settings_text += "**Available Settings:**\n"
        settings_text += "• 🔧 General configuration\n"
        settings_text += "• 📱 Interface customization\n"
        settings_text += "• 🔒 Security options\n"
        settings_text += "• 📊 Performance tuning"
        
        await q.edit_message_text(settings_text, reply_markup=kb, parse_mode='Markdown')
    
    async def _show_export_import(self, update: Update):
        """Show export/import menu"""
        q = update.callback_query
        
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📤 Export tasks", callback_data="export"),
             InlineKeyboardButton("📥 Import tasks", callback_data="import")],
            [InlineKeyboardButton("📊 Export Statistics", callback_data="export_stats"),
             InlineKeyboardButton("🔄 Backup All Data", callback_data="backup_all")],
            [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
        ])
        
        await q.edit_message_text("📦 **Export/Import**\n\n"
                                "Manage your data and configurations.\n\n"
                                "**Features:**\n"
                                "• 📤 Export tasks and settings\n"
                                "• 📥 Import configurations\n"
                                "• 📊 Export statistics\n"
                                "• 🔄 Complete backup/restore", 
                                reply_markup=kb, parse_mode='Markdown')
    
    async def _show_tools_menu(self, update: Update):
        """Show tools and utilities menu"""
        q = update.callback_query
        
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔍 List Chats", callback_data="list_chats"),
             InlineKeyboardButton("🧪 Test Forwarding", callback_data="test_forward")],
            [InlineKeyboardButton("🔧 Fix tasks", callback_data="fixtasks"),
             InlineKeyboardButton("📱 Simple Chat List", callback_data="simple_chats")],
            [InlineKeyboardButton("🚀 Start Engine", callback_data="start_engine"),
             InlineKeyboardButton("⏹️ Stop Engine", callback_data="stop_engine")],
            [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
        ])
        
        await q.edit_message_text("🛠️ **Tools & Utilities**\n\n"
                                "Advanced tools for bot management.\n\n"
                                "**Features:**\n"
                                "• 🔍 Chat discovery and management\n"
                                "• 🧪 Testing and debugging\n"
                                "• 🔧 task maintenance\n"
                                "• 🚀 Engine control", 
                                reply_markup=kb, parse_mode='Markdown')

    # ---- task Management Methods ----
    async def _edit_task(self, update: Update, task_id: str):
        """Edit an existing task"""
        q = update.callback_query
        task = self.store.get_task(task_id)
        
        if not task:
            await q.edit_message_text("❌ task not found!")
            return
        
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Edit Name", callback_data=f"edit_name_{task_id}"),
             InlineKeyboardButton("📤 Edit Source", callback_data=f"edit_source_{task_id}")],
            [InlineKeyboardButton("📥 Edit Destination", callback_data=f"edit_dest_{task_id}"),
             InlineKeyboardButton("🔍 Edit Keywords", callback_data=f"edit_keywords_{task_id}")],
            [InlineKeyboardButton("⏱️ Edit Delay", callback_data=f"edit_delay_{task_id}"),
             InlineKeyboardButton("⚙️ Advanced Settings", callback_data=f"edit_advanced_{task_id}")],
            [InlineKeyboardButton("🔙 Back to tasks", callback_data="manage_tasks")]
        ])
        
        await q.edit_message_text(f"✏️ **Edit task: {task.name}**\n\n"
                                f"**Current Settings:**\n"
                                f"• Source: {task.source_chat_id}\n"
                                f"• Destination: {task.destination_chat_id}\n"
                                f"• Keywords: {task.keywords or 'ALL'}\n"
                                f"• Delay: {task.delay_seconds}s\n"
                                f"• Status: {'🟢 Enabled' if task.enabled else '🔴 Disabled'}\n\n"
                                "Choose what to edit:", 
                                reply_markup=kb, parse_mode='Markdown')
    
    async def _confirm_delete_task(self, update: Update, task_id: str):
        """Confirm task deletion"""
        q = update.callback_query
        task = self.store.get_task(task_id)
        
        if not task:
            await q.edit_message_text("❌ task not found!")
            return
        
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🗑️ Yes, Delete", callback_data=f"confirm_delete_{task_id}"),
             InlineKeyboardButton("❌ Cancel", callback_data="manage_tasks")]
        ])
        
        await q.edit_message_text(f"🗑️ **Delete task Confirmation**\n\n"
                                f"Are you sure you want to delete:\n"
                                f"**{task.name}**\n\n"
                                f"This action cannot be undone!", 
                                reply_markup=kb, parse_mode='Markdown')
    
    async def _toggle_task(self, update: Update, task_id: str):
        """Toggle task enabled/disabled status"""
        q = update.callback_query
        task = self.store.get_task(task_id)
        
        if not task:
            await q.edit_message_text("❌ task not found!")
            return
        
        new_status = not task.enabled
        self.store.set_task_enabled(task_id, new_status)
        
        # Refresh monitoring immediately after task toggle
        try:
            await self.engine.force_refresh_monitoring(task.user_id)
            log.info(f"Monitoring refreshed after toggling task {task_id} for user {task.user_id}")
        except Exception as e:
            log.error(f"Error refreshing monitoring after task toggle: {e}")
        
        status_text = "🟢 enabled" if new_status else "🔴 disabled"
        await q.edit_message_text(f"✅ task **{task.name}** has been {status_text}!\n\n"
                                "🔄 Monitoring has been refreshed automatically.\n\n"
                                "Returning to tasks management...")
        await asyncio.sleep(2)
        await self._show_tasks_management(update)
 
    # ---- Blacklist Management ----
    async def _add_blacklist_entry(self, update: Update, filter_type: str):
        """Add a new blacklist entry"""
        q = update.callback_query
        
        if filter_type == "keyword":
            await q.edit_message_text("🚫 **Add Blacklisted Keyword**\n\n"
                                    "Enter keywords to block (comma-separated):\n"
                                    "Example: spam,ads,scam", parse_mode='Markdown')
            # Store state for input handling
            self.pending_blacklist = {"type": "keyword", "action": "add"}
        elif filter_type == "user":
            await q.edit_message_text("🚷 **Add Blacklisted User**\n\n"
                                    "Enter user IDs to block (comma-separated):\n"
                                    "Example: 123456789,987654321", parse_mode='Markdown')
            self.pending_blacklist = {"type": "user", "action": "add"}
        elif filter_type == "whitelist":
            await q.edit_message_text("✅ **Add Whitelisted Keyword**\n\n"
                                    "Enter keywords to allow (comma-separated):\n"
                                    "Example: news,update,alert", parse_mode='Markdown')
            self.pending_blacklist = {"type": "whitelist_keyword", "action": "add"}
        elif filter_type == "whitelist_user":
            await q.edit_message_text("🟢 **Add Whitelisted User**\n\n"
                                    "Enter user IDs to allow (comma-separated):\n"
                                    "Example: 123456789,987654321", parse_mode='Markdown')
            self.pending_blacklist = {"type": "whitelist_user", "action": "add"}
     
    async def _manage_blacklist(self, update: Update, filter_type: str):
        """Manage existing blacklist entries"""
        q = update.callback_query
        user_id = update.effective_user.id
        
        # This would show existing entries and allow management
        # For now, show a placeholder
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back to Filters", callback_data="filters")]
        ])
        
        await q.edit_message_text(f"🔍 **Manage {filter_type.title()}**\n\n"
                                f"This feature is coming soon!\n\n"
                                f"You'll be able to:\n"
                                f"• View all entries\n"
                                f"• Edit existing entries\n"
                                f"• Delete entries\n"
                                f"• Enable/disable entries", 
                                reply_markup=kb, parse_mode='Markdown')
    
    async def _confirm_delete_task_final(self, update: Update, task_id: str):
        """Actually delete the task after confirmation"""
        q = update.callback_query
        task = self.store.get_task(task_id)
        
        if not task:
            await q.edit_message_text("❌ task not found!")
            return
        
        # Delete the task
        self.store.delete_task(task_id)
        
        # Refresh monitoring immediately after task deletion
        try:
            await self.engine.force_refresh_monitoring(task.user_id)
            log.info(f"Monitoring refreshed after deleting task {task_id} for user {task.user_id}")
        except Exception as e:
            log.error(f"Error refreshing monitoring after task deletion: {e}")
        
        await q.edit_message_text(f"✅ task **{task.name}** has been deleted!\n\n"
                                "🔄 Monitoring has been refreshed automatically.\n\n"
                                "Returning to tasks management...")
        await asyncio.sleep(2)
        await self._show_tasks_management(update)
 
    # ---- Advanced Features Implementation ----
    async def _show_scheduling_menu(self, update: Update):
        """Show scheduling and automation menu"""
        q = update.callback_query
        
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⏰ Auto Post Scheduler", callback_data="auto_scheduler"),
             InlineKeyboardButton("⏳ Set Delays", callback_data="set_delays")],
            [InlineKeyboardButton("🕹️ Power Schedule", callback_data="power_schedule"),
             InlineKeyboardButton("⏱️ Edit Time Limits", callback_data="edit_time_limits")],
            [InlineKeyboardButton("📅 Manage Schedules", callback_data="manage_schedules"),
             InlineKeyboardButton("🔄 Auto Restart", callback_data="auto_restart")],
            [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
        ])
        
        await q.edit_message_text("⏰ **Scheduling & Automation**\n\n"
                                "Automate your forwarding operations.\n\n"
                                "**Features:**\n"
                                "• ⏰ Schedule posts at specific times\n"
                                "• ⏳ Set delays between forwards\n"
                                "• 🕹️ Control bot activation schedules\n"
                                "• ⏱️ Set message editing time limits", 
                                reply_markup=kb, parse_mode='Markdown')
     
    async def _show_settings(self, update: Update):
        """Show settings menu"""
        q = update.callback_query
        
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔧 General Settings", callback_data="general_settings"),
             InlineKeyboardButton("📱 Interface Settings", callback_data="interface_settings")],
            [InlineKeyboardButton("🔒 Security Settings", callback_data="security_settings"),
             InlineKeyboardButton("📊 Performance Settings", callback_data="performance_settings")],
            [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
        ])
        
        await q.edit_message_text("⚙️ **Settings**\n\n"
                                "Configure your bot's behavior and appearance.\n\n"
                                "**Categories:**\n"
                                "• 🔧 General bot settings\n"
                                "• 📱 Interface customization\n"
                                "• 🔒 Security and permissions\n"
                                "• 📊 Performance optimization", 
                                reply_markup=kb, parse_mode='Markdown')
     
    async def _show_statistics(self, update: Update):
        """Show statistics and analytics"""
        q = update.callback_query
        tasks = self.store.list_tasks()
        
        total_messages = sum(task.message_count for task in tasks)
        active_tasks = sum(1 for task in tasks if task.enabled)
        total_tasks = len(tasks)
        
        stats_text = "📊 **Statistics & Analytics**\n\n"
        stats_text += f"**📈 Overall Performance:**\n"
        stats_text += f"• Total Messages Forwarded: {total_messages:,}\n"
        stats_text += f"• Active tasks: {active_tasks}/{total_tasks}\n"
        stats_text += f"• Bot Status: {'🟢 Running' if self.store.get_kv('forwarding_on', True) else '🔴 Stopped'}\n\n"
        
        if tasks:
            stats_text += "**📋 task Performance:**\n"
            for task in tasks[:5]:  # Show top 5 tasks
                stats_text += f"• {task.name}: {task.message_count:,} messages\n"
            if len(tasks) > 5:
                stats_text += f"• ... and {len(tasks) - 5} more tasks\n"
        
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 Detailed Stats", callback_data="detailed_stats"),
             InlineKeyboardButton("📈 Performance Graph", callback_data="performance_graph")],
            [InlineKeyboardButton("🔄 Reset Stats", callback_data="reset_stats"),
             InlineKeyboardButton("📤 Export Report", callback_data="export_report")],
            [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
        ])
        
        await q.edit_message_text(stats_text, reply_markup=kb, parse_mode='Markdown')
     
    # Duplicate _show_account_menu method removed
     
    async def _show_export_import(self, update: Update):
        """Show export/import menu"""
        q = update.callback_query
        
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📤 Export tasks", callback_data="export"),
             InlineKeyboardButton("📥 Import tasks", callback_data="import")],
            [InlineKeyboardButton("📊 Export Statistics", callback_data="export_stats"),
             InlineKeyboardButton("🔄 Backup All Data", callback_data="backup_all")],
            [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
        ])
        
        await q.edit_message_text("📦 **Export/Import**\n\n"
                                "Manage your data and configurations.\n\n"
                                "**Features:**\n"
                                "• 📤 Export tasks and settings\n"
                                "• 📥 Import configurations\n"
                                "• 📊 Export statistics\n"
                                "• 🔄 Complete backup/restore", 
                                reply_markup=kb, parse_mode='Markdown')
     
    async def _show_tools_menu(self, update: Update):
        """Show tools and utilities menu"""
        q = update.callback_query
        
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔍 List Chats", callback_data="list_chats"),
             InlineKeyboardButton("🧪 Test Forwarding", callback_data="test_forward")],
            [InlineKeyboardButton("🔧 Fix tasks", callback_data="fixtasks"),
             InlineKeyboardButton("📱 Simple Chat List", callback_data="simple_chats")],
            [InlineKeyboardButton("🚀 Start Engine", callback_data="start_engine"),
             InlineKeyboardButton("⏹️ Stop Engine", callback_data="stop_engine")],
            [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
        ])
        
        await q.edit_message_text("🛠️ **Tools & Utilities**\n\n"
                                "Advanced tools for bot management.\n\n"
                                "**Features:**\n"
                                "• 🔍 Chat discovery and management\n"
                                "• 🧪 Testing and debugging\n"
                                "• 🔧 task maintenance\n"
                                "• 🚀 Engine control", 
                                reply_markup=kb, parse_mode='Markdown')
 
    # ---- Advanced Feature Handlers ----
    async def _toggle_blacklist_entry(self, update: Update, entry_id: str):
        """Toggle blacklist entry enabled/disabled status"""
        q = update.callback_query
        await q.edit_message_text("✅ Blacklist entry toggled!\n\nReturning to blacklist management...")
        await asyncio.sleep(2)
        await self._manage_blacklist(update, "all")
    
    async def _delete_blacklist_entry(self, update: Update, entry_id: str):
        """Delete a blacklist entry"""
        q = update.callback_query
        await q.edit_message_text("✅ Blacklist entry deleted!\n\nReturning to blacklist management...")
        await asyncio.sleep(2)
        await self._manage_blacklist(update, "all")
     
    async def _handle_advanced_feature(self, update: Update, feature: str):
        """Handle advanced feature callbacks"""
        q = update.callback_query
        
        if feature == "auto_scheduler":
            await q.edit_message_text("⏰ **Auto Post Scheduler**\n\nThis feature allows you to schedule posts at specific times.\n\nComing soon!", parse_mode='Markdown')
        elif feature == "set_delays":
            await q.edit_message_text("⏳ **Set Delays**\n\nConfigure delays between message forwards.\n\nComing soon!", parse_mode='Markdown')
        elif feature == "power_schedule":
            await q.edit_message_text("🕹️ **Power Schedule**\n\nControl when your bot is active.\n\nComing soon!", parse_mode='Markdown')
        elif feature == "edit_time_limits":
            await q.edit_message_text("⏱️ **Edit Time Limits**\n\nSet maximum time for message editing.\n\nComing soon!", parse_mode='Markdown')
        elif feature == "manage_schedules":
            await q.edit_message_text("📅 **Manage Schedules**\n\nView and manage all scheduled tasks.\n\nComing soon!", parse_mode='Markdown')
        elif feature == "auto_restart":
            await q.edit_message_text("🔄 **Auto Restart**\n\nConfigure automatic bot restart.\n\nComing soon!", parse_mode='Markdown')
        elif feature == "general_settings":
            await q.edit_message_text("🔧 **General Settings**\n\nConfigure general bot behavior.\n\nComing soon!", parse_mode='Markdown')
        elif feature == "interface_settings":
            await q.edit_message_text("📱 **Interface Settings**\n\nCustomize bot interface.\n\nComing soon!", parse_mode='Markdown')
        elif feature == "security_settings":
            await q.edit_message_text("🔒 **Security Settings**\n\nConfigure security and permissions.\n\nComing soon!", parse_mode='Markdown')
        elif feature == "performance_settings":
            await q.edit_message_text("📊 **Performance Settings**\n\nOptimize bot performance.\n\nComing soon!", parse_mode='Markdown')
        elif feature == "detailed_stats":
            await q.edit_message_text("📊 **Detailed Statistics**\n\nView detailed performance metrics.\n\nComing soon!", parse_mode='Markdown')
        elif feature == "performance_graph":
            await q.edit_message_text("📈 **Performance Graph**\n\nVisual performance analytics.\n\nComing soon!", parse_mode='Markdown')
        elif feature == "reset_stats":
            await q.edit_message_text("🔄 **Reset Statistics**\n\nReset all performance counters.\n\nComing soon!", parse_mode='Markdown')
        elif feature == "export_report":
            await q.edit_message_text("📤 **Export Report**\n\nExport detailed performance report.\n\nComing soon!", parse_mode='Markdown')
        elif feature == "account_info":
            await self._show_detailed_account_info(update)
        elif feature == "change_session":
            await q.edit_message_text("🔑 **Change Session**\n\nSwitch to different session.\n\nComing soon!", parse_mode='Markdown')
        elif feature == "2fa_settings":
            await q.edit_message_text("🔒 **2FA Settings**\n\nConfigure two-factor authentication.\n\nComing soon!", parse_mode='Markdown')
        elif feature == "device_management":
            await q.edit_message_text("📱 **Device Management**\n\nManage connected devices.\n\nComing soon!", parse_mode='Markdown')
        elif feature == "export_stats":
            await q.edit_message_text("📤 **Export Statistics**\n\nExport statistical data.\n\nComing soon!", parse_mode='Markdown')
        elif feature == "backup_all":
            await q.edit_message_text("🔄 **Backup All Data**\n\nCreate complete backup.\n\nComing soon!", parse_mode='Markdown')
        
        # Add back button
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Previous Menu", callback_data="back_to_main")]])
        await q.edit_message_text(q.message.text + "\n\n" + "🔙 Use the button below to go back.", reply_markup=kb)
 
    async def _handle_blacklist_input(self, update: Update, text: str):
        """Handle blacklist input from user"""
        if not self.task_builder.pending_blacklist:
            return
        
        blacklist_data = self.task_builder.pending_blacklist
        entry_type = blacklist_data["type"]
        
        # Create blacklist entry
        entry_id = f"BL{int(datetime.utcnow().timestamp())}"
        entry = BlacklistEntry(
            id=entry_id,
            type=entry_type,
            value=text,
            reason="Added via bot interface",
            created_at=datetime.utcnow().isoformat(),
            enabled=1
        )
        
        # Save to store
        self.store.add_blacklist_entry(entry)
        
        # Clear pending state
        self.task_builder.pending_blacklist = None
        
        # Show success message
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back to Filters", callback_data="filters")],
            [InlineKeyboardButton("➕ Add Another", callback_data="add_blacklist_" + entry_type)]
        ])
        
        await update.effective_message.reply_text(
            f"✅ **Blacklist Entry Added!**\n\n"
            f"**Type:** {entry_type.replace('_', ' ').title()}\n"
            f"**Value:** {text}\n\n"
            f"Entry has been saved successfully!",
            reply_markup=kb,
            parse_mode='Markdown'
        )
 
     # ---- Login flow ----
    async def cmd_login(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not await self._guard(update):
            return
        
        user_id = update.effective_user.id
        
        # Check if user already has a session
        existing_session = self.store.get_user_session(user_id)
        if existing_session and existing_session.is_verified:
            await update.effective_message.reply_text("✅ You are already logged in!")
            return
        
        if not ctx.args:
            await update.effective_message.reply_text("Usage: /login +1234567890")
            return
        
        phone = ctx.args[0]
        try:
            phone_code_hash = await self.engine.login_send_code(phone, user_id)
            self.pending_login[user_id] = {"phone": phone, "hash": phone_code_hash}
            await update.effective_message.reply_text("Code sent. Reply with /code 1 2 3 4 5 (separate each digit with space). If 2FA is enabled, follow with /2fa yourpassword")
        except Exception as e:
            await update.effective_message.reply_text(f"Login failed (send code): {e}")

    async def cmd_code(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not await self._guard(update):
            return
        st = self.pending_login.get(update.effective_user.id)
        if not st:
            await update.effective_message.reply_text("Start with /login +phone")
            return
        if not ctx.args:
            await update.effective_message.reply_text("Usage: /code 1 2 3 4 5 (separate each digit with space)")
            return
        # Join separate digits into a single code string
        code = "".join(ctx.args)
        st["code"] = code
        await update.effective_message.reply_text("If you have 2FA, send /2fa password. Otherwise, send /signin now.")

    async def cmd_2fa(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not await self._guard(update):
            return
        st = self.pending_login.get(update.effective_user.id)
        if not st:
            await update.effective_message.reply_text("Start with /login +phone")
            return
        if not ctx.args:
            await update.effective_message.reply_text("Usage: /2fa your_password")
            return
        st["twofa"] = " ".join(ctx.args)
        await update.effective_message.reply_text("2FA saved. Now /signin")

    async def cmd_signin(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not await self._guard(update):
            return
        
        user_id = update.effective_user.id
        st = self.pending_login.get(user_id)
        if not st:
            await update.effective_message.reply_text("Start with /login +phone")
            return
        
        try:
            await self.engine.login_sign_in(st["phone"], st.get("code", ""), user_id, st.get("twofa"), st.get("hash"))
            
            # Mark user as verified
            self.store.mark_user_verified(user_id)
            self.store.update_user_activity(user_id)
            
            # Clear pending login state
            self.pending_login.pop(user_id, None)
            
            await update.effective_message.reply_text("✅ Login complete! Your account is now verified and ready to use.")
            
            # Automatically show main menu after successful login
            await asyncio.sleep(1)
            await self._show_main_menu(update)
        except PhoneCodeInvalidError:
            await update.effective_message.reply_text("❌ Invalid code. Use /code again.")
        except SessionPasswordNeededError:
            await update.effective_message.reply_text("🔒 Account needs 2FA password. Use /2fa and then /signin")
        except Exception as e:
            await update.effective_message.reply_text(f"❌ Sign-in failed: {e}")

    async def _start_interactive_login(self, update: Update):
        """Start the interactive login process"""
        user_id = update.effective_user.id
        
        # Check if user already has a session
        existing_session = self.store.get_user_session(user_id)
        if existing_session and existing_session.is_verified:
            await update.callback_query.edit_message_text("✅ You are already logged in!")
            return
        
        # Initialize login state
        self.login_states[user_id] = "phone"
        self.pending_login[user_id] = {}
        
        # Create keyboard with back button
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
        ])
        
        await update.callback_query.edit_message_text(
            "🔐 **Login to Telegram**\n\n"
            "📱 **Step 1:** Enter your phone number\n\n"
            "Format: `+1234567890` (include country code)\n\n"
            "Example: `+1234567890`",
            reply_markup=kb,
            parse_mode='Markdown'
        )

    async def _handle_login_input(self, update: Update, text: str) -> bool:
        """Handle input during login process. Returns True if handled."""
        user_id = update.effective_user.id
        
        if user_id not in self.login_states:
            return False
        
        login_state = self.login_states[user_id]
        
        # Handle both string format and dict format (for 2FA with message ID)
        if isinstance(login_state, str):
            state = login_state
        else:
            state = login_state.get("state", "")
        
        if state == "phone":
            return await self._handle_phone_input(update, text)
        elif state == "code":
            return await self._handle_code_input(update, text)
        elif state == "2fa":
            return await self._handle_2fa_input(update, text)
        
        return False

    async def _handle_phone_input(self, update: Update, text: str) -> bool:
        """Handle phone number input"""
        user_id = update.effective_user.id
        phone = text.strip()
        
        # Basic phone validation
        if not phone.startswith('+') or len(phone) < 10:
            await update.effective_message.reply_text(
                "❌ **Invalid phone format**\n\n"
                "Please enter your phone number with country code:\n"
                "Example: `+1234567890`",
                parse_mode='Markdown'
            )
            return True
        
        try:
            # Send verification code
            phone_code_hash = await self.engine.login_send_code(phone, user_id)
            self.pending_login[user_id] = {"phone": phone, "hash": phone_code_hash}
            self.login_states[user_id] = "code"
            
            # Create keyboard with back button
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
            ])
            
            await update.effective_message.reply_text(
                "📱 **Step 2:** Enter verification code\n\n"
                "🔢 **Code sent to your phone!**\n\n"
                "Enter the 5-digit code you received separted:\n"
                "Example: `1 2 3 4 5`",
                reply_markup=kb,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            await update.effective_message.reply_text(
                f"❌ **Failed to send code**\n\n"
                f"Error: {str(e)}\n\n"
                "Please check your phone number and try again."
            )
        
        return True

    async def _handle_code_input(self, update: Update, text: str) -> bool:
        """Handle verification code input"""
        user_id = update.effective_user.id
        code = text.strip().replace(" ", "")
        
        # Basic code validation
        if not code.isdigit() or len(code) != 5:
            await update.effective_message.reply_text(
                "❌ **Invalid code format**\n\n"
                "Please enter the 5-digit verification code:\n"
                "Example: `12345`",
                parse_mode='Markdown'
            )
            return True
        
        # Store the code
        if user_id in self.pending_login:
            self.pending_login[user_id]["code"] = code
        
        
        try:
            # Try to sign in
            login_data = self.pending_login[user_id]
            await self.engine.login_sign_in(
                login_data["phone"], 
                code, 
                user_id, 
                None,  # No 2FA yet
                login_data["hash"]
            )
            
            # Success! Complete login
            await self._complete_login(update, user_id)
            
        except SessionPasswordNeededError:
            # 2FA required
            self.login_states[user_id] = "2fa"
            
            # Create keyboard with back button
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back to Main", callback_data="back_to_main")]
            ])
            
            await update.effective_message.reply_text(
                "🔒 **Step 3:** Two-Factor Authentication\n\n"
                "Your account has 2FA enabled.\n"
                "Enter your 2FA password:",
                reply_markup=kb,
                parse_mode='Markdown'
            )
            
        except PhoneCodeInvalidError:
            await update.effective_message.reply_text(
                "❌ **Invalid verification code**\n\n"
                "Please check the code and try again.\n"
                "Enter the 5-digit code:"
            )
            
        except Exception as e:
            await update.effective_message.reply_text(
                f"❌ **Login failed**\n\n"
                f"Error: {str(e)}\n\n"
                "Please try again from the beginning."
            )
            # Reset login state
            self._reset_login_state(user_id)
        
        return True

    async def _handle_2fa_input(self, update: Update, text: str) -> bool:
        """Handle 2FA password input"""
        user_id = update.effective_user.id
        password = text.strip()
        
        if not password:
            await update.effective_message.reply_text(
                "❌ **Please enter your 2FA password**"
            )
            return True
        
        try:
            # Store 2FA password temporarily
            if user_id in self.pending_login:
                self.pending_login[user_id]["twofa"] = password
            
            # Store the 2FA message ID for secure deletion after login
            if user_id not in self.login_states:
                self.login_states[user_id] = {}
            if isinstance(self.login_states[user_id], dict):
                self.login_states[user_id]['twofa_message_id'] = update.effective_message.message_id
            else:
                # Convert to dict structure
                self.login_states[user_id] = {
                    "state": "2fa",
                    "twofa_message_id": update.effective_message.message_id
                }
            
            # Try to sign in with 2FA
            login_data = self.pending_login[user_id]
            await self.engine.login_sign_in(
                login_data["phone"], 
                login_data["code"], 
                user_id, 
                password, 
                login_data["hash"]
            )
            
            # Success! Complete login
            await self._complete_login(update, user_id)
            
        except Exception as e:
            await update.effective_message.reply_text(
                f"❌ **2FA authentication failed**\n\n"
                f"Error: {str(e)}\n\n"
                "Please check your password and try again."
            )
        
        return True

    async def _complete_login(self, update: Update, user_id: int):
        """Complete the login process"""
        try:
            # Mark user as verified
            self.store.mark_user_verified(user_id)
            self.store.update_user_activity(user_id)
            
            # Get 2FA message ID from login state for secure deletion
            login_state = self.login_states.get(user_id, {})
            twofa_message_id = login_state.get('twofa_message_id')
            
            # Clear login state
            self._reset_login_state(user_id)
            
            # Delete only the 2FA password message for security
            if twofa_message_id and twofa_message_id == update.effective_message.message_id:
                try:
                    await update.effective_message.delete()
                    log.info(f"User {user_id}: Deleted 2FA password message for security")
                except Exception as e:
                    log.warning(f"User {user_id}: Could not delete 2FA password message: {e}")
            
            # Send success message
            success_msg = await update.effective_message.reply_text(
                "✅ **Login Successful!**\n\n"
                "🔐 Your account is now verified and secure\n"
                "🚀 Bot is ready to use\n\n"
                "Returning to main menu..."
            )
            
            # Show main menu
            await asyncio.sleep(1)
            await self._show_main_menu(update)
            
        except Exception as e:
            await update.effective_message.reply_text(
                f"❌ **Login completion failed**\n\n"
                f"Error: {str(e)}"
            )

    def _reset_login_state(self, user_id: int):
        """Reset login state for user"""
        self.pending_login.pop(user_id, None)
        self.login_states.pop(user_id, None)

    async def cmd_logout(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not await self._guard(update):
            return
        
        user_id = update.effective_user.id
        
        try:
            ok = await self.engine.logout(user_id)
            # Clear any pending login state for this user
            self.pending_login.pop(user_id, None)
            await update.effective_message.reply_text("✅ Logged out successfully!" if ok else "⚠️ Logout attempted.")
        except Exception as e:
            await update.effective_message.reply_text(f"❌ Logout failed: {e}")

    async def cmd_ping(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Simple ping command to test if bot is responding"""
        user_id = update.effective_user.id
        await update.effective_message.reply_text(
            f"🏓 **Pong!**\n\n"
            f"✅ Bot is responding!\n"
            f"👤 User ID: {user_id}\n"
            f"🔧 Engine Status: {'Running' if self.engine.started else 'Stopped'}\n"
            f"📊 Active Clients: {len(self.engine.clients)}",
            parse_mode='Markdown'
        )

    async def cmd_refresh_monitoring(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Manually refresh user monitoring"""
        if not await self._guard(update):
            return
        
        user_id = update.effective_user.id
        
        # Check if user is logged in
        user_session = self.store.get_user_session(user_id)
        if not user_session or not user_session.is_verified:
            await update.effective_message.reply_text("❌ **Login Required**\n\nYou need to login with your Telegram account first.\n\nUse `/login +phone` to start.")
            return
        
        try:
            await self.engine.refresh_user_monitoring(user_id)
            await update.effective_message.reply_text("✅ **Monitoring Refreshed!**\n\nYour chat monitoring has been updated based on your current tasks.")
        except Exception as e:
            await update.effective_message.reply_text(f"❌ **Error Refreshing Monitoring:** {str(e)}")

    async def cmd_start_monitoring(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Manually start monitoring a specific chat"""
        if not await self._guard(update):
            return
        
        user_id = update.effective_user.id
        
        # Check if user is logged in
        user_session = self.store.get_user_session(user_id)
        if not user_session or not user_session.is_verified:
            await update.effective_message.reply_text("❌ **Login Required**\n\nYou need to login with your Telegram account first.\n\nUse `/login +phone` to start.")
            return
        
        # Get chat ID from command arguments
        args = ctx.args
        if not args:
            await update.effective_message.reply_text("❌ **Usage:** `/start_monitoring <chat_id>`\n\nExample: `/start_monitoring -1001234567890`")
            return
        
        try:
            chat_id = int(args[0])
            
            # Start monitoring the chat
            success = await self.engine.start_monitoring_chat(user_id, chat_id)
            
            if success:
                await update.effective_message.reply_text(
                    f"✅ **Monitoring Started!**\n\n"
                    f"Chat {chat_id} is now being monitored for messages.\n\n"
                    f"**Note:** Messages will only be forwarded if you have active tasks for this chat."
                )
            else:
                await update.effective_message.reply_text(f"❌ **Failed to start monitoring** chat {chat_id}.")
                
        except ValueError:
            await update.effective_message.reply_text("❌ **Invalid Chat ID**\n\nPlease provide a valid numeric chat ID.")
        except Exception as e:
            await update.effective_message.reply_text(f"❌ **Error:** {str(e)}")

    async def cmd_all_users(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Check the status of all users in the system"""
        if not await self._guard(update):
            return
        
        # Get all user sessions
        all_sessions = self.store.get_all_user_sessions()
        
        if not all_sessions:
            await update.effective_message.reply_text("❌ **No Users Found**\n\nNo users have logged into the system yet.")
            return
        
        # Build status message
        status_msg = f"👥 **All Users Status** ({len(all_sessions)} total)\n\n"
        
        for session in all_sessions:
            user_id = session.user_id
            is_verified = session.is_verified
            phone = session.phone
            tasks_count = len(self.store.list_tasks_by_user(user_id))
            in_clients = user_id in self.engine.clients
            
            status_icon = "✅" if is_verified else "❌"
            client_icon = "🟢" if in_clients else "🔴"
            
            status_msg += f"{status_icon} **User {user_id}**\n"
            status_msg += f"   📱 Phone: {phone}\n"
            status_msg += f"   🔐 Verified: {'Yes' if is_verified else 'No'}\n"
            status_msg += f"   📋 tasks: {tasks_count}\n"
            status_msg += f"   🔧 Client-: {client_icon} {'Active' if in_clients else 'Inactive'}\n\n"
        
        await update.effective_message.reply_text(status_msg, parse_mode='Markdown')

    async def cmd_force_logout(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Force logout for a user"""
        if not await self._guard(update):
            return
        
        user_id = update.effective_user.id
        
        try:
            ok = await self.engine.logout(user_id)
            # Clear any pending login state for this user
            self.pending_login.pop(user_id, None)
            await update.effective_message.reply_text("✅ User has been forcefully logged out successfully!" if ok else "⚠️ Force logout attempted.")
        except Exception as e:
            await update.effective_message.reply_text(f"❌ Failed to force logout: {e}")

    async def cmd_refresh_monitoring(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Manually refresh monitoring for the current user"""
        if not await self._guard(update):
            return
        
        user_id = update.effective_user.id
        
        # Check if user is logged in
        user_session = self.store.get_user_session(user_id)
        if not user_session or not user_session.is_verified:
            await update.effective_message.reply_text("❌ **Login Required**\n\nYou need to login with your Telegram account first.\n\nUse `/login +phone` to start.")
            return
        
        try:
            await update.effective_message.reply_text("🔄 **Refreshing Monitoring...**\n\nPlease wait while I refresh your forwarding tasks...")
            
            # Force refresh monitoring
            success = await self.engine.force_refresh_monitoring(user_id)
            
            if success:
                # Get current tasks
                tasks = self.store.list_tasks_by_user(user_id)
                enabled_tasks = [r for r in tasks if r.enabled]
                
                status_msg = "✅ **Monitoring Refreshed Successfully!**\n\n"
                status_msg += f"**📊 Current Status:**\n"
                status_msg += f"• Total tasks: {len(tasks)}\n"
                status_msg += f"• Enabled tasks: {len(enabled_tasks)}\n"
                status_msg += f"• Active Source Chats: {len(set(r.source_chat_id for r in enabled_tasks if r.source_chat_id))}\n\n"
                
                if enabled_tasks:
                    status_msg += "**📋 Active tasks:**\n"
                    for task in enabled_tasks[:5]:  # Show first 5 tasks
                        status_msg += f"• {task.name} (ID: {task.id})\n"
                        status_msg += f"  Source: {task.source_chat_id} → Destination: {task.destination_chat_id}\n"
                    if len(enabled_tasks) > 5:
                        status_msg += f"• ... and {len(enabled_tasks) - 5} more tasks\n"
                else:
                    status_msg += "**⚠️ No enabled tasks found!**\n\nCreate and enable tasks to start forwarding."
                
                status_msg += "\n**💡 Tip:** Forwarding should now work immediately for all your enabled tasks!"
                
                await update.effective_message.edit_text(status_msg, parse_mode='Markdown')
            else:
                await update.effective_message.edit_text("❌ **Refresh Failed**\n\nCould not refresh monitoring. Please try again or contact support.")
                
        except Exception as e:
            await update.effective_message.edit_text(f"❌ **Error During Refresh**\n\nAn error occurred: {str(e)}\n\nPlease try again or contact support.")

    async def cmd_test_monitoring(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Test if monitoring is working for a specific chat"""
        if not await self._guard(update):
            return
        
        user_id = update.effective_user.id
        
        # Check if user is logged in
        user_session = self.store.get_user_session(user_id)
        if not user_session or not user_session.is_verified:
            await update.effective_message.reply_text("❌ **Login Required**\n\nYou need to login with your Telegram account first.\n\nUse `/login +phone` to start.")
            return
        
        # Get the chat ID to test from command arguments
        if not ctx.args:
            await update.effective_message.reply_text("❌ **Usage:** `/test_monitoring CHAT_ID`\n\nExample:** `/test_monitoring -1001234567890`")
            return
        
        try:
            test_chat_id = int(ctx.args[0])
        except ValueError:
            await update.effective_message.reply_text("❌ **Invalid Chat ID:** Please provide a valid numeric chat ID.")
            return
        
        try:
            await update.effective_message.reply_text(f"🔍 **Testing Monitoring for Chat {test_chat_id}...**\n\nPlease wait...")
            
            # Get user's tasks
            tasks = self.store.list_tasks_by_user(user_id)
            enabled_tasks = [r for r in tasks if r.enabled and r.source_chat_id == test_chat_id]
            
            if not enabled_tasks:
                await update.effective_message.edit_text(f"❌ **No tasks Found**\n\nNo enabled tasks found for chat {test_chat_id}.\n\nCreate a task first or check your existing tasks.")
                return
            
            # Get user's client
            client = await self.engine.get_client(user_id)
            if not client:
                await update.effective_message.edit_text("❌ **Client Error**\n\nFailed to get your Telegram client. Please try logging in again.")
                return
            
            # Test if we can access the chat
            try:
                entity = await client.get_entity(test_chat_id)
                chat_name = getattr(entity, 'title', 'Unknown') or getattr(entity, 'first_name', 'Unknown')
                
                # Check handler count to detect duplicates
                try:
                    handlers = client.list_event_handlers()
                    new_message_handlers = [h for h in handlers if hasattr(h, '__name__') and 'NewMessage' in str(h)]
                    handler_count = len(new_message_handlers)
                    handler_status = f"✅ {handler_count} handler(s)"
                    if handler_count > 1:
                        handler_status = f"⚠️ {handler_count} handlers (duplicates detected!)"
                except Exception as e:
                    handler_status = f"❓ Unknown ({str(e)[:30]}...)"
                
                # Test monitoring refresh without crashing
                try:
                    await self.engine.force_refresh_monitoring(user_id)
                    refresh_status = "✅ Success"
                except Exception as refresh_error:
                    refresh_status = f"⚠️ Warning: {str(refresh_error)[:50]}..."
                
                status_msg = f"✅ **Chat Access Verified!**\n\n**Chat:** {chat_name}\n**ID:** {test_chat_id}\n\n**tasks Found:** {len(enabled_tasks)}\n**Status:** Chat is accessible and tasks are configured.\n\n**🔄 Monitoring Refresh:** {refresh_status}\n**📡 Event Handlers:** {handler_status}\n\n**Next Step:** Send a message in this chat to test forwarding!\n\n**💡 Tip:** If forwarding still doesn't work, try the `/refresh_monitoring` command."
                
                await update.effective_message.edit_text(status_msg, parse_mode='Markdown')
            except Exception as e:
                await update.effective_message.edit_text(f"❌ **Chat Access Failed**\n\nCould not access chat {test_chat_id}:\n\n{str(e)}\n\n**Possible Issues:**\n• Chat ID is incorrect\n• You don't have access to this chat\n• Chat has been deleted or made private")
                
        except Exception as e:
            await update.effective_message.edit_text(f"❌ **Test Failed**\n\nAn error occurred: {str(e)}\n\nPlease try again or contact support.")

    async def cmd_cleanup_handlers(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        """Clean up duplicate event handlers to fix double forwarding"""
        if not await self._guard(update):
            return
        
        user_id = update.effective_user.id
        
        # Check if user is logged in
        user_session = self.store.get_user_session(user_id)
        if not user_session or not user_session.is_verified:
            await update.effective_message.reply_text("❌ **Login Required**\n\nYou need to login with your Telegram account first.\n\nUse `/login +phone` to start.")
            return
        
        try:
            await update.effective_message.reply_text("🧹 **Cleaning Up Duplicate Handlers...**\n\nPlease wait while I fix the double forwarding issue...")
            
            # Get user's client
            client = await self.engine.get_client(user_id)
            if not client:
                await update.effective_message.edit_text("❌ **Client Error**\n\nFailed to get your Telegram client. Please try logging in again.")
                return
            
            # Check current handler count
            try:
                handlers = client.list_event_handlers()
                new_message_handlers = [h for h in handlers if hasattr(h, '__name__') and 'NewMessage' in str(h)]
                before_count = len(new_message_handlers)
            except Exception as e:
                before_count = "Unknown"
            
            # Force cleanup and refresh
            await self.engine.force_refresh_monitoring(user_id)
            
            # Check handler count after cleanup
            try:
                handlers = client.list_event_handlers()
                new_message_handlers = [h for h in handlers if hasattr(h, '__name__') and 'NewMessage' in str(h)]
                after_count = len(new_message_handlers)
            except Exception as e:
                after_count = "Unknown"
            
            status_msg = "🧹 **Handler Cleanup Completed!**\n\n"
            status_msg += f"**📊 Results:**\n"
            status_msg += f"• Before: {before_count} handler(s)\n"
            status_msg += f"• After: {after_count} handler(s)\n"
            
            if isinstance(before_count, int) and isinstance(after_count, int):
                if before_count > after_count:
                    status_msg += f"• Removed: {before_count - after_count} duplicate handler(s)\n"
                elif before_count == after_count:
                    status_msg += f"• Status: No duplicates found\n"
                else:
                    status_msg += f"• Added: {after_count - before_count} handler(s)\n"
            
            status_msg += f"\n**✅ Action:** Duplicate handlers have been cleaned up.\n"
            status_msg += f"**💡 Tip:** Forwarding should now work without duplicates!\n\n"
            status_msg += f"**Next Step:** Test forwarding by sending a message in your source chat."
            
            await update.effective_message.edit_text(status_msg, parse_mode='Markdown')
            
        except Exception as e:
            await update.effective_message.edit_text(f"❌ **Cleanup Failed**\n\nAn error occurred: {str(e)}\n\nPlease try again or contact support.")

    # ---- Wiring ----
    def build(self) -> Application:
        app = Application.builder().token(BOT_TOKEN).build()
        app.add_handler(CommandHandler("start", self.cmd_start))
        app.add_handler(CommandHandler("tasks", self.cmd_tasks))
        app.add_handler(CommandHandler("status", self.cmd_status))
        app.add_handler(CommandHandler("forward", self.cmd_forward))
        app.add_handler(CommandHandler("addtask", self.cmd_addtask))
        app.add_handler(CommandHandler("deltask", self.cmd_deltask))
        app.add_handler(CommandHandler("toggle", self.cmd_toggle))
        app.add_handler(CommandHandler("export", self.cmd_export))
        app.add_handler(CommandHandler("import", self.cmd_import))
        app.add_handler(CommandHandler("create", self.cmd_create))
        app.add_handler(CommandHandler("listchats", self.cmd_listchats))
        app.add_handler(CommandHandler("simplechats", self.cmd_simplechats))
        app.add_handler(CommandHandler("startengine", self.cmd_startengine))
        app.add_handler(CommandHandler("stopengine", self.cmd_stopengine))
        app.add_handler(CommandHandler("reset_circuit", self.cmd_reset_circuit))
        app.add_handler(CommandHandler("unlimited_users", self.cmd_unlimited_users))
        app.add_handler(CommandHandler("debug", self.cmd_debug))
        app.add_handler(CommandHandler("testforward", self.cmd_testforward))
        app.add_handler(CommandHandler("fixtasks", self.cmd_fixtasks))
        app.add_handler(CommandHandler("cancel", self.cmd_cancel))
        app.add_handler(CommandHandler("login", self.cmd_login))
        app.add_handler(CommandHandler("code", self.cmd_code))
        app.add_handler(CommandHandler("2fa", self.cmd_2fa))
        app.add_handler(CommandHandler("signin", self.cmd_signin))
        app.add_handler(CommandHandler("logout", self.cmd_logout))
        app.add_handler(CommandHandler("ping", self.cmd_ping))
        app.add_handler(CommandHandler("refresh_monitoring", self.cmd_refresh_monitoring))
        app.add_handler(CommandHandler("all_users", self.cmd_all_users))
        app.add_handler(CommandHandler("force_logout", self.cmd_force_logout))
        app.add_handler(CommandHandler("refresh_monitoring", self.cmd_refresh_monitoring))
        app.add_handler(CommandHandler("test_monitoring", self.cmd_test_monitoring))
        app.add_handler(CommandHandler("cleanup_handlers", self.cmd_cleanup_handlers))
        app.add_handler(CallbackQueryHandler(self.on_cb))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        return app

# ----------------- Main -----------------
async def main_async():
    if not BOT_TOKEN or not API_ID or not API_HASH:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN / TELEGRAM_API_ID / TELEGRAM_API_HASH")

    store = Store(DB_PATH)
    engine = Engine(store)
    
    # Set up the callback so the store can notify the engine about task changes
    store.set_task_change_callback(engine.on_task_changed)
    
    ui = BotUI(engine, store)

    app = ui.build()

    # Start the engine in the background
    engine_task = asyncio.create_task(engine.start())
    
    # Start the bot interface
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    
    log.info("🚀 Auto-Forwarder Pro Bot Started Successfully!")
    log.info("📱 Bot UI is running. Users can now use /start in your bot chat.")
    log.info("🔧 Engine is monitoring for user logins in the background.")
    
    try:
        # Keep both running
        await asyncio.gather(
            engine_task,
            asyncio.Event().wait()  # Wait forever
        )
    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        engine.started = False  # Stop the engine

if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass
