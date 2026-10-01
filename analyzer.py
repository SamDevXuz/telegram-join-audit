"""Хранилище SQLite + движок анализа заявок на вступление.

Модуль не зависит от Telegram — его можно тестировать отдельно.
Задача: найти в списке ожидающих заявок следы массово созданных
аккаунтов.
"""

import re
import secrets
import sqlite3
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone

import config


# ================================================================
# ХРАНЕНИЕ
# ================================================================

db = sqlite3.connect(config.DB_FILE, check_same_thread=False)
db.row_factory = sqlite3.Row

SCHEMA = """
CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS channels (
    channel_id INTEGER PRIMARY KEY,
    title      TEXT NOT NULL,
    username   TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS scans (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id     INTEGER NOT NULL,
    started_at     TEXT NOT NULL,
    finished_at    TEXT,
    telegram_count INTEGER,
    fetched_count  INTEGER,
    status         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS requests (
    scan_id      INTEGER NOT NULL,
    user_id      INTEGER NOT NULL,
    username     TEXT,
    first_name   TEXT,
    last_name    TEXT,
    request_date TEXT NOT NULL,
    PRIMARY KEY (scan_id, user_id)
);

CREATE TABLE IF NOT EXISTS declines (
    channel_id  INTEGER NOT NULL,
    user_id     INTEGER NOT NULL,
    username    TEXT,
    risk        INTEGER,
    declined_at TEXT NOT NULL,
    PRIMARY KEY (channel_id, user_id)
);

-- Результат глубокой проверки. Привязан к аккаунту, а не к скану —
-- переживает повторное сканирование.
CREATE TABLE IF NOT EXISTS profiles (
    user_id     INTEGER PRIMARY KEY,
    photo_count INTEGER,
    photo_date  TEXT,
    checked_at  TEXT NOT NULL
);

-- Доступ к панели доказательств. Код одноразовый: закрепляется за тем,
-- кто первым его ввёл, после чего никто другой по нему не войдёт.
CREATE TABLE IF NOT EXISTS guest_codes (
    code       TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    note       TEXT,
    used_by    INTEGER,
    used_name  TEXT,
    used_at    TEXT
);

-- Наблюдение в реальном времени. Сканер читает уже существующую очередь,
-- а это пишет каждую заявку в момент прихода. Два важных отличия:
--   1. Достаточно прав администратора у бота — сессия user-аккаунта не нужна.
--   2. Известно, ПО КАКОЙ ссылке пришла заявка, то есть трафик можно
--      разделить по продавцам.
CREATE TABLE IF NOT EXISTS live_requests (
    channel_id   INTEGER NOT NULL,
    user_id      INTEGER NOT NULL,
    request_date TEXT NOT NULL,
    username     TEXT,
    first_name   TEXT,
    last_name    TEXT,
    has_photo    INTEGER DEFAULT 0,
    is_premium   INTEGER DEFAULT 0,
    is_scam      INTEGER DEFAULT 0,
    is_fake      INTEGER DEFAULT 0,
    is_bot       INTEGER DEFAULT 0,
    last_seen    TEXT,
    access_hash  INTEGER,
    invite_link  TEXT,
    invite_title TEXT,
    PRIMARY KEY (channel_id, user_id)
);

-- Атрибуция по продавцу. В апдейте Telegram ОБРЕЗАЕТ ссылку
-- ("https://t.me/+kHGCZCH1..."), полную форму отдаёт только тому боту,
-- который сам её создал. Поэтому полная ссылка хранится здесь, а
-- пришедшая заявка сопоставляется по префиксу.
CREATE TABLE IF NOT EXISTS sellers (
    channel_id INTEGER NOT NULL,
    prefix     TEXT NOT NULL,
    name       TEXT NOT NULL,
    link       TEXT NOT NULL,
    created_at TEXT NOT NULL,
    revoked_at TEXT,
    PRIMARY KEY (channel_id, prefix)
);

-- Сюда попадает посторонний, нажавший /start и проявивший интерес.
-- Смысл — один раз уведомить владельца и не потерять контакт.
CREATE TABLE IF NOT EXISTS leads (
    user_id    INTEGER PRIMARY KEY,
    username   TEXT,
    name       TEXT,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_requests_scan ON requests(scan_id);
CREATE INDEX IF NOT EXISTS idx_scans_channel ON scans(channel_id, status);
CREATE INDEX IF NOT EXISTS idx_guest_used ON guest_codes(used_by);
CREATE INDEX IF NOT EXISTS idx_live_date ON live_requests(channel_id, request_date);
"""

# В базе v1 этих колонок не было — добавляются при запуске
EXTRA_COLUMNS = {
    "has_photo": "INTEGER DEFAULT 0",
    "is_premium": "INTEGER DEFAULT 0",
    "is_scam": "INTEGER DEFAULT 0",
    "is_fake": "INTEGER DEFAULT 0",
    "is_bot": "INTEGER DEFAULT 0",
    "last_seen": "TEXT",
    "access_hash": "INTEGER",
}


def init():
    db.executescript(SCHEMA)
    have = {r["name"] for r in db.execute("PRAGMA table_info(requests)")}
    for name, ddl in EXTRA_COLUMNS.items():
        if name not in have:
            db.execute(f"ALTER TABLE requests ADD COLUMN {name} {ddl}")

    # Данные о фото раньше лежали в requests. Теперь они в profiles —
    # если старые колонки останутся, имена столкнутся с JOIN в scan_rows.
    for name in ("photo_count", "photo_date"):
        if name in have:
            db.execute(f"ALTER TABLE requests DROP COLUMN {name}")
    db.commit()


def set_setting(key, value):
    db.execute(
        "INSERT INTO settings(key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, str(value)),
    )
    db.commit()


def get_setting(key):
    row = db.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else None


def clear_setting(key):
    db.execute("DELETE FROM settings WHERE key = ?", (key,))
    db.commit()


def guest_scan_id():
    """Скан, закреплённый в гостевой панели. None — показывается последний.

    Чистка уменьшает доказательную базу (отклонённые аккаунты уходят из
    списка), поэтому скан, сделанный до чистки, имеет смысл закрепить.
    """
    value = get_setting("guest_scan_id")
    return int(value) if value else None


def save_channel(channel_id, title, username):
    db.execute(
        "INSERT INTO channels(channel_id, title, username, updated_at) "
        "VALUES (?, ?, ?, ?) ON CONFLICT(channel_id) DO UPDATE SET "
        "title = excluded.title, "
        "username = excluded.username, "
        "updated_at = excluded.updated_at",
        (channel_id, title, username, now_utc().isoformat()),
    )
    db.commit()


def channel_title(channel_id):
    row = db.execute(
        "SELECT title FROM channels WHERE channel_id = ?", (channel_id,)
    ).fetchone()
    return row["title"] if row else str(channel_id)


def selected_channel_id():
    value = get_setting("selected_channel_id")
    return int(value) if value else None


def start_scan(channel_id):
    cur = db.execute(
        "INSERT INTO scans(channel_id, started_at, status) VALUES (?, ?, 'running')",
        (channel_id, now_utc().isoformat()),
    )
    db.commit()
    return cur.lastrowid


def add_requests(scan_id, batch):
    db.executemany(
        "INSERT OR REPLACE INTO requests("
        "  scan_id, user_id, username, first_name, last_name, request_date,"
        "  has_photo, is_premium, is_scam, is_fake, is_bot, last_seen, access_hash"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [(scan_id, *item) for item in batch],
    )
    db.commit()


def finish_scan(scan_id, status, telegram_count, fetched_count):
    db.execute(
        "UPDATE scans SET finished_at = ?, telegram_count = ?, "
        "fetched_count = ?, status = ? WHERE id = ?",
        (now_utc().isoformat(), telegram_count, fetched_count, status, scan_id),
    )
    db.commit()


def recent_scans(channel_id, limit=2):
    return db.execute(
        "SELECT * FROM scans WHERE channel_id = ? AND status = 'completed' "
        "ORDER BY id DESC LIMIT ?",
        (channel_id, limit),
    ).fetchall()


def scan_by_id(scan_id):
    return db.execute("SELECT * FROM scans WHERE id = ?", (scan_id,)).fetchone()


def later_scans(channel_id, scan_id):
    """Завершённые сканы после указанного — от старых к новым."""
    return db.execute(
        "SELECT * FROM scans WHERE channel_id = ? AND id > ? "
        "AND status = 'completed' ORDER BY id",
        (channel_id, scan_id),
    ).fetchall()


def latest_scan(channel_id=None):
    channel_id = channel_id or selected_channel_id()
    if not channel_id:
        return None
    scans = recent_scans(channel_id, 1)
    return scans[0] if scans else None


def scan_rows(scan_id):
    return db.execute(
        "SELECT r.*, p.photo_count, p.photo_date FROM requests r"
        " LEFT JOIN profiles p ON p.user_id = r.user_id"
        " WHERE r.scan_id = ? ORDER BY r.request_date",
        (scan_id,),
    ).fetchall()


def log_declines(channel_id, entries):
    """Пишет отклонённых — чтобы потом было известно, кого именно отклонили."""
    db.executemany(
        "INSERT OR REPLACE INTO declines"
        "(channel_id, user_id, username, risk, declined_at) VALUES (?, ?, ?, ?, ?)",
        [(channel_id, uid, username, risk, now_utc().isoformat())
         for uid, username, risk in entries],
    )
    db.commit()


def save_photo_info(entries):
    """Результат глубокой проверки: (user_id, число фото, дата самой старой)."""
    db.executemany(
        "INSERT OR REPLACE INTO profiles"
        "(user_id, photo_count, photo_date, checked_at) VALUES (?, ?, ?, ?)",
        [(uid, count, date, now_utc().isoformat()) for uid, count, date in entries],
    )
    db.commit()


def decline_count(channel_id):
    return db.execute(
        "SELECT COUNT(*) FROM declines WHERE channel_id = ?", (channel_id,)
    ).fetchone()[0]


def decline_log(channel_id):
    """Отклонённые по времени. Гостевая панель строит из этого историю."""
    return db.execute(
        "SELECT user_id, username, risk, declined_at FROM declines "
        "WHERE channel_id = ? ORDER BY declined_at",
        (channel_id,),
    ).fetchall()


# ================================================================
# ПОТОК В РЕАЛЬНОМ ВРЕМЕНИ
# ================================================================

LIVE_FIELDS = (
    "channel_id", "user_id", "request_date", "username", "first_name",
    "last_name", "has_photo", "is_premium", "is_scam", "is_fake", "is_bot",
    "last_seen", "access_hash", "invite_link", "invite_title",
)


def record_live(channel_id, item):
    """Пишет одну пришедшую заявку. Возвращает True, если заявка новая.

    `item` — значения LIVE_FIELDS, идущие после channel_id.

    У одного аккаунта в одном канале может быть только ОДНА ожидающая
    заявка, поэтому строка на аккаунт одна. Но отклонённый аккаунт может
    подать заявку повторно — возможно, уже по ссылке другого продавца.
    Поэтому заявка с более свежей датой заменяет старую; иначе повторный
    залив записался бы на прежнего продавца.

    Если дата та же самая — это повторный апдейт Telegram, игнорируется.
    """
    user_id, request_date = item[0], item[1]
    seen = db.execute(
        "SELECT request_date FROM live_requests "
        "WHERE channel_id = ? AND user_id = ?",
        (channel_id, user_id),
    ).fetchone()
    if seen and seen["request_date"] >= request_date:
        return False

    marks = ", ".join("?" * len(LIVE_FIELDS))
    db.execute(
        f"INSERT OR REPLACE INTO live_requests({', '.join(LIVE_FIELDS)}) "
        f"VALUES ({marks})",
        (channel_id, *item),
    )
    db.commit()
    return True


def live_rows(channel_id, since=None):
    """Живые записи — в форме, которую понимает analyze().

    Джойнятся с profiles, то есть для аккаунтов, прошедших глубокую
    проверку, данные о фото видны и здесь.
    """
    sql = (
        "SELECT l.*, p.photo_count, p.photo_date FROM live_requests l"
        " LEFT JOIN profiles p ON p.user_id = l.user_id"
        " WHERE l.channel_id = ?"
    )
    args = [channel_id]
    if since is not None:
        sql += " AND l.request_date >= ?"
        args.append(parse_dt(since).isoformat())
    return db.execute(sql + " ORDER BY l.request_date", args).fetchall()


def live_count(channel_id):
    return db.execute(
        "SELECT COUNT(*) FROM live_requests WHERE channel_id = ?", (channel_id,)
    ).fetchone()[0]


def live_links(channel_id):
    """Подсчёт по ссылкам — кто из продавцов сколько привёл.

    Возвращает список (ссылка, название, количество), от большего к меньшему.
    """
    return db.execute(
        "SELECT invite_link, invite_title, COUNT(*) AS n FROM live_requests "
        "WHERE channel_id = ? GROUP BY invite_link ORDER BY n DESC",
        (channel_id,),
    ).fetchall()


# ================================================================
# ПРОДАВЦЫ (АТРИБУЦИЯ)
# ================================================================
#
# В апдейте Telegram отдаёт ссылку обрезанной: "https://t.me/+kHGCZCH1..."
# — то есть "https://t.me/+" и 8 символов хеша. Полную ссылку видит только
# тот бот, который её создал. Поэтому ссылку создаёт сам бот, полную форму
# пишет сюда, а пришедшую заявку находит по префиксу. 8 символов ~48 бит —
# в пределах одного канала коллизий не будет.

LINK_PREFIX_LEN = 22


def link_prefix(link):
    """Приводит обрезанную и полную ссылку к общему ключу сравнения."""
    return link[:LINK_PREFIX_LEN] if link else None


def add_seller(channel_id, name, link):
    db.execute(
        "INSERT OR REPLACE INTO sellers"
        "(channel_id, prefix, name, link, created_at) VALUES (?, ?, ?, ?, ?)",
        (channel_id, link_prefix(link), name, link, now_utc().isoformat()),
    )
    db.commit()


def sellers(channel_id):
    return db.execute(
        "SELECT * FROM sellers WHERE channel_id = ? ORDER BY created_at",
        (channel_id,),
    ).fetchall()


def revoke_seller(channel_id, prefix):
    """Помечает ссылку как отозванную.

    Строка не удаляется — иначе потеряется атрибуция старых заявок,
    пришедших по ней.
    """
    db.execute(
        "UPDATE sellers SET revoked_at = ? WHERE channel_id = ? AND prefix = ?",
        (now_utc().isoformat(), channel_id, prefix),
    )
    db.commit()


# ================================================================
# ГОСТЕВЫЕ КОДЫ
# ================================================================

def new_guest_code(note=None):
    """Создаёт и возвращает одноразовый код."""
    code = secrets.token_hex(4)
    db.execute(
        "INSERT INTO guest_codes(code, created_at, note) VALUES (?, ?, ?)",
        (code, now_utc().isoformat(), note),
    )
    db.commit()
    return code


def redeem_guest_code(code, user_id, name):
    """Закрепляет код за аккаунтом.

    Возвращает: "ok" | "used" (занят другим аккаунтом) | "expired" | "bad".
    Если тот же аккаунт введёт код повторно — снова "ok", код не теряется.
    """
    row = db.execute(
        "SELECT * FROM guest_codes WHERE code = ?", (code.strip().lower(),)
    ).fetchone()
    if not row:
        return "bad"
    if row["used_by"] is not None:
        return "ok" if row["used_by"] == user_id else "used"

    age = (now_utc() - parse_dt(row["created_at"])).total_seconds()
    if age > config.GUEST_CODE_HOURS * 3600:
        return "expired"

    db.execute(
        "UPDATE guest_codes SET used_by = ?, used_name = ?, used_at = ? "
        "WHERE code = ?",
        (user_id, name, now_utc().isoformat(), row["code"]),
    )
    db.commit()
    return "ok"


def guest_name(user_id):
    """Имя, если это гость, иначе None."""
    row = db.execute(
        "SELECT used_name FROM guest_codes WHERE used_by = ? "
        "ORDER BY used_at DESC LIMIT 1",
        (user_id,),
    ).fetchone()
    return row["used_name"] if row else None


def is_guest(user_id):
    return (
        db.execute(
            "SELECT 1 FROM guest_codes WHERE used_by = ? LIMIT 1", (user_id,)
        ).fetchone()
        is not None
    )


def guest_codes():
    return db.execute(
        "SELECT * FROM guest_codes ORDER BY created_at DESC LIMIT 20"
    ).fetchall()


def revoke_guests():
    """Удаляет все гостевые коды — доступ закрывается немедленно."""
    cur = db.execute("DELETE FROM guest_codes")
    db.commit()
    return cur.rowcount


def add_lead(user_id, username, name):
    """Пишет заинтересовавшегося. True только в первый раз — лишь тогда
    уходит уведомление владельцу, иначе один человек мог бы завалить его
    сообщениями, просто нажимая кнопку."""
    if db.execute(
        "SELECT 1 FROM leads WHERE user_id = ?", (user_id,)
    ).fetchone():
        return False
    db.execute(
        "INSERT INTO leads(user_id, username, name, created_at) "
        "VALUES (?, ?, ?, ?)",
        (user_id, username, name, now_utc().isoformat()),
    )
    db.commit()
    return True


def scan_user_ids(scan_id):
    return {
        r["user_id"]
        for r in db.execute(
            "SELECT user_id FROM requests WHERE scan_id = ?", (scan_id,)
        )
    }


# ================================================================
# ВРЕМЯ
# ================================================================

def now_utc():
    return datetime.now(timezone.utc)


def parse_dt(value):
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(value)
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def local(value):
    return parse_dt(value).astimezone(config.TIMEZONE)


# ================================================================
# СЕМЕЙСТВА USERNAME
# ================================================================

# "userpn12003" -> ("userpn", 12003)
_SPLIT = re.compile(r"^(.*?)(\d+)$")


def split_username(username):
    """Отделяет цифры на конце username. Если основы нет — (None, None)."""
    if not username:
        return None, None
    m = _SPLIT.match(username.lower())
    if not m:
        return None, None
    prefix, digits = m.group(1), m.group(2)
    if not prefix or not any(c.isalpha() for c in prefix):
        return None, None
    return prefix, int(digits)


# "behzod6385", "ruslanpn66344" — только строчные, без разделителей, 3-5 цифр.
# Живые люди обычно используют заглавную букву или подчёркивание:
# "Abrorbek_Sobirov1701", "Sardor221109", "STARxUZB374".
_GENERATED = re.compile(r"^[a-z]+\d{3,5}$")

# Число из этого диапазона может быть годом рождения — признак живого человека.
BIRTH_YEARS = range(1980, 2013)


def is_generated(username, is_premium=False):
    """Есть ли в username след генератора?

    Premium-аккаунты не трогаем: за Premium заплачены деньги. Это не
    доказательство, что человек живой, но достаточная причина не трогать
    аккаунт автоматическим правилом.
    """
    if not username or is_premium:
        return False
    if not _GENERATED.match(username):
        return False
    _, number = split_username(username)
    return number not in BIRTH_YEARS


@dataclass
class Family:
    """Набор username с общей основой, заканчивающихся цифрами."""

    prefix: str = ""
    numbers: list = field(default_factory=list)
    user_ids: set = field(default_factory=set)
    samples: list = field(default_factory=list)

    @property
    def count(self):
        return len(self.user_ids)

    @property
    def lo(self):
        return min(self.numbers)

    @property
    def hi(self):
        return max(self.numbers)

    @property
    def density(self):
        """Какая доля числового диапазона занята. 1.0 = строго подряд."""
        span = self.hi - self.lo + 1
        return self.count / span if span > 0 else 1.0


@dataclass
class Window:
    """Одна «сессия залива» — склеенные соседние горячие минуты."""

    minutes: list = field(default_factory=list)
    rows: list = field(default_factory=list)
    start: datetime = None
    end: datetime = None

    photo_rate: float = 0.0
    username_rate: float = 0.0
    premium_rate: float = 0.0
    family_share: float = 0.0
    uniform: bool = False

    @property
    def count(self):
        return len(self.rows)

    @property
    def minutes_long(self):
        return max((self.end - self.start).total_seconds() / 60, 1)

    @property
    def per_minute(self):
        return self.count / self.minutes_long


# ================================================================
# РЕЗУЛЬТАТ АНАЛИЗА
# ================================================================

@dataclass
class Report:
    total: int = 0
    rows: list = field(default_factory=list)

    with_username: int = 0
    no_username: int = 0
    no_photo: int = 0
    premium: int = 0
    flagged: int = 0

    families: list = field(default_factory=list)
    family_users: set = field(default_factory=set)
    generated_users: set = field(default_factory=set)

    minutes: Counter = field(default_factory=Counter)
    burst_threshold: int = 0
    burst_minutes: list = field(default_factory=list)
    burst_users: set = field(default_factory=set)
    peak60: tuple = (0, None)

    windows: list = field(default_factory=list)
    uniform_users: set = field(default_factory=set)
    base_photo_rate: float = 0.0
    base_username_rate: float = 0.0

    clusters: list = field(default_factory=list)
    cluster_users: set = field(default_factory=set)
    flagged_users: set = field(default_factory=set)

    deep_total: int = 0
    photo_days: list = field(default_factory=list)
    batch_days: set = field(default_factory=set)
    batch_users: set = field(default_factory=set)
    human_users: set = field(default_factory=set)

    scores: dict = field(default_factory=dict)
    reasons: dict = field(default_factory=dict)
    suspects: list = field(default_factory=list)

    risk: int = 0
    first: datetime = None
    last: datetime = None
    span_minutes: float = 0.0
    avg_per_minute: float = 0.0


def analyze(rows):
    rep = Report(total=len(rows), rows=list(rows))
    if not rows:
        return rep

    # ---------------------------------------------------- сигналы профиля
    for r in rows:
        if r["username"]:
            rep.with_username += 1
        else:
            rep.no_username += 1
        if not r["has_photo"]:
            rep.no_photo += 1
        if r["is_premium"]:
            rep.premium += 1
        if r["is_scam"] or r["is_fake"]:
            rep.flagged += 1

    # ---------------------------------------------------- семейства username
    buckets = defaultdict(Family)
    for r in rows:
        prefix, number = split_username(r["username"])
        if prefix is None:
            continue
        fam = buckets[prefix]
        fam.prefix = prefix
        fam.numbers.append(number)
        fam.user_ids.add(r["user_id"])
        if len(fam.samples) < 5:
            fam.samples.append(r["username"])

    rep.families = sorted(
        (f for f in buckets.values() if f.count >= config.FAMILY_MIN),
        key=lambda f: f.count,
        reverse=True,
    )
    for fam in rep.families:
        rep.family_users |= fam.user_ids

    # След генератора виден и без семейства:
    # "ruslanpn66344", "behzod6385" — каждый сам по себе, но шаблон один.
    rep.generated_users = {
        r["user_id"] for r in rows
        if is_generated(r["username"], r["is_premium"])
    }

    # ---------------------------------------------------- всплески во времени
    dates = sorted(parse_dt(r["request_date"]) for r in rows)
    rep.first, rep.last = dates[0], dates[-1]
    rep.span_minutes = (rep.last - rep.first).total_seconds() / 60

    by_minute = defaultdict(list)
    for r in rows:
        by_minute[local(r["request_date"]).strftime("%d.%m %H:%M")].append(r)
    rep.minutes = Counter({k: len(v) for k, v in by_minute.items()})

    counts = list(rep.minutes.values())
    median = statistics.median(counts) if counts else 0
    rep.avg_per_minute = rep.total / max(len(rep.minutes), 1)
    rep.burst_threshold = max(config.BURST_MIN_ABS, int(median * config.BURST_FACTOR))

    hot = {k for k, c in rep.minutes.items() if c >= rep.burst_threshold}
    rep.burst_minutes = sorted(
        ((k, rep.minutes[k]) for k in hot), key=lambda kv: kv[1], reverse=True
    )
    for key in hot:
        for r in by_minute[key]:
            rep.burst_users.add(r["user_id"])

    rep.peak60 = _peak_window(dates, 60)

    # ---------------------------------------------------- сессии залива
    rep.windows = _sessions(by_minute, hot, rows)
    organic = [r for r in rows if r["user_id"] not in rep.burst_users] or rows
    rep.base_photo_rate = sum(1 for r in organic if r["has_photo"]) / len(organic)
    rep.base_username_rate = sum(1 for r in organic if r["username"]) / len(organic)

    for win in rep.windows:
        n = win.count
        win.photo_rate = sum(1 for r in win.rows if r["has_photo"]) / n
        win.username_rate = sum(1 for r in win.rows if r["username"]) / n
        win.premium_rate = sum(1 for r in win.rows if r["is_premium"]) / n
        win.family_share = sum(
            1 for r in win.rows if r["user_id"] in rep.family_users
        ) / n
        # Живая аудитория «неполная». Если доля аватарок/username в сессии
        # заметно выше общего фона — аккаунты готовили.
        win.uniform = (
            win.photo_rate - rep.base_photo_rate >= config.UNIFORMITY_GAP
            or win.username_rate - rep.base_username_rate >= config.UNIFORMITY_GAP
        )
        if win.uniform:
            rep.uniform_users |= {r["user_id"] for r in win.rows}

    # ---------------------------------------------------- партии аватарок
    # Живой человек ставит фото тогда, когда ему захотелось. Если несколько
    # сотен аккаунтов загрузили фото в один день — их готовили вместе.
    deep = [r for r in rows if r["photo_count"] is not None]
    rep.deep_total = len(deep)
    if deep:
        # Аккаунт с 5+ фото — живой человек. В партию его не записываем.
        rep.human_users = {
            r["user_id"] for r in deep
            if r["photo_count"] >= config.HUMAN_PHOTOS
        }
        days = Counter(r["photo_date"][:10] for r in deep if r["photo_date"])
        rep.photo_days = days.most_common()
        limit = max(config.BATCH_MIN_ABS, int(len(deep) * config.BATCH_SHARE))
        rep.batch_days = {d for d, c in days.items() if c >= limit}
        rep.batch_users = {
            r["user_id"] for r in deep
            if r["photo_date"] and r["photo_date"][:10] in rep.batch_days
            and r["user_id"] not in rep.human_users
        }

    rep.clusters = _id_clusters(rows)
    for members in rep.clusters:
        if len(members) >= config.CLUSTER_MIN:
            rep.cluster_users |= set(members)

    # ---------------------------------------------------- оценки риска
    family_size = {}
    for fam in rep.families:
        for uid in fam.user_ids:
            family_size[uid] = fam.count

    for r in rows:
        score, why = _score(r, rep, family_size)
        rep.scores[r["user_id"]] = score
        rep.reasons[r["user_id"]] = why

    rep.suspects = sorted(
        (r for r in rows if rep.scores[r["user_id"]] >= config.SUSPECT_THRESHOLD),
        key=lambda r: rep.scores[r["user_id"]],
        reverse=True,
    )

    # ---------------------------------------------------- риск канала
    #
    # Риск = доля аккаунтов с групповой уликой. «Групповая улика» — это не
    # сам по себе профиль, а связь с другими: общее семейство username,
    # сессия залива с однородными профилями, кластер ID.
    # Это гораздо надёжнее одиночных сигналов вроде «нет аватарки».
    rep.flagged_users = (
        rep.uniform_users | rep.family_users | rep.cluster_users
        | rep.generated_users | rep.batch_users
    )
    rep.risk = round(100 * len(rep.flagged_users) / rep.total)
    return rep


def _peak_window(sorted_dates, seconds):
    """Самое плотное скользящее окно в `seconds`: (сколько, начало)."""
    best_count, best_start = 0, None
    left = 0
    for right, dt in enumerate(sorted_dates):
        while (dt - sorted_dates[left]).total_seconds() > seconds:
            left += 1
        if right - left + 1 > best_count:
            best_count = right - left + 1
            best_start = sorted_dates[left]
    return best_count, best_start


def _sessions(by_minute, hot, rows):
    """Склеивает соседние горячие минуты в одну сессию."""
    if not hot:
        return []

    keyed = sorted(
        (min(parse_dt(r["request_date"]) for r in by_minute[k]), k) for k in hot
    )
    gap = config.BURST_GAP_MINUTES * 60

    sessions, current = [], Window(minutes=[keyed[0][1]], rows=list(by_minute[keyed[0][1]]))
    previous = keyed[0][0]

    for moment, key in keyed[1:]:
        if (moment - previous).total_seconds() <= gap + 60:
            current.minutes.append(key)
            current.rows.extend(by_minute[key])
        else:
            sessions.append(current)
            current = Window(minutes=[key], rows=list(by_minute[key]))
        previous = moment

    sessions.append(current)

    for win in sessions:
        moments = [parse_dt(r["request_date"]) for r in win.rows]
        win.start, win.end = min(moments), max(moments)
        # «Холодные» минуты внутри интервала тоже принадлежат сессии:
        # если поток на секунды просел, это не повод рвать её надвое.
        win.rows = [r for r in rows if win.start <= parse_dt(r["request_date"]) <= win.end]

    return sorted(sessions, key=lambda w: w.count, reverse=True)


def _id_clusters(rows):
    """Группы с близкими user_id. След массовой регистрации.

    На выдержанной ферме не работает: аккаунты куплены вразнобой и старые.
    Ловит только свежесозданные партии.
    """
    ids = sorted({r["user_id"] for r in rows})
    if not ids:
        return []
    groups, current = [], [ids[0]]
    for uid in ids[1:]:
        if uid - current[-1] <= config.ID_GAP:
            current.append(uid)
        else:
            groups.append(current)
            current = [uid]
    groups.append(current)
    return sorted(
        (g for g in groups if len(g) >= config.CLUSTER_MIN),
        key=len,
        reverse=True,
    )


def _score(row, rep, family_size):
    """Оценка риска 0-100 для одного аккаунта и список причин.

    Показывается пользователю, но на решение не влияет: помечаются только
    групповые признаки (см. flagged_users).
    """
    uid = row["user_id"]
    score = 0
    why = []

    size = family_size.get(uid, 0)
    if size >= 20:
        score += 38
        why.append(f"большое семейство username ({size})")
    elif size >= 8:
        score += 32
        why.append(f"семейство username ({size})")
    elif size >= config.FAMILY_MIN:
        score += 24
        why.append(f"малое семейство username ({size})")
    elif split_username(row["username"])[0]:
        score += 8
        why.append("username заканчивается цифрой")

    if uid in rep.generated_users:
        score += 28
        why.append("шаблон генератора username (имя + случайные цифры)")

    if not row["username"]:
        score += 6
        why.append("нет username")

    if not row["has_photo"]:
        score += 16
        why.append("нет аватарки")

    if uid in rep.burst_users:
        score += 26
        why.append("пришёл внутри волны")

    if uid in rep.uniform_users:
        score += 20
        why.append("профиль неестественно одинаков с другими")

    if uid in rep.cluster_users:
        score += 18
        why.append("кластер подряд идущих ID")

    if uid in rep.batch_users:
        score += 30
        why.append(f"аватарка загружена в день партии ({row['photo_date'][:10]})")

    if uid in rep.human_users:
        score -= 25
        why.append(f"фотографий профиля: {row['photo_count']} — признак живого человека")

    if row["is_scam"] or row["is_fake"]:
        score += 40
        why.append("метка Telegram scam/fake")

    if not row["last_name"]:
        score += 4

    if row["is_premium"]:
        score -= 20
        why.append("Premium (риск снижен)")

    return max(0, min(100, score)), why


# ================================================================
# ГРУППИРОВКА ДЛЯ ГРАФИКА
# ================================================================

def timeline(rep, buckets=20):
    """Разбивает время примерно на `buckets` интервалов и считает заявки.

    Возвращает список (подпись, количество) и длину интервала в секундах.
    """
    if not rep.rows:
        return [], 0

    span = max((rep.last - rep.first).total_seconds(), 60)
    step = max(60, int(span / buckets))
    for nice in (60, 300, 900, 1800, 3600, 10800, 21600, 86400):
        if step <= nice:
            step = nice
            break

    fmt = "%H:%M" if step < 86400 else "%d.%m"
    counter = Counter()
    start = rep.first
    for r in rep.rows:
        offset = int((parse_dt(r["request_date"]) - start).total_seconds() // step)
        counter[offset] += 1

    result = []
    for i in range(max(counter) + 1):
        moment = local(start).timestamp() + i * step
        label = datetime.fromtimestamp(moment, config.TIMEZONE).strftime(fmt)
        result.append((label, counter.get(i, 0)))
    return result, step
