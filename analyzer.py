"""SQLite saqlash + join-request tahlil dvigateli.

Bu modul Telegram'ga bog'liq emas — uni alohida test qilish mumkin.
Asosiy vazifa: pending join request'lar ro'yxatidan ommaviy yaratilgan
akkauntlar izini topish.
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
# SAQLASH
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

-- Chuqur tekshiruv natijasi. Skanga emas, akkauntga bog'langan —
-- qayta skanerlagandan keyin ham saqlanib qoladi.
CREATE TABLE IF NOT EXISTS profiles (
    user_id     INTEGER PRIMARY KEY,
    photo_count INTEGER,
    photo_date  TEXT,
    checked_at  TEXT NOT NULL
);

-- Dalil panelini ko'rishga ruxsat. Kod bir martalik: birinchi ishlatgan
-- akkauntga biriktiriladi, keyin boshqa hech kim u bilan kira olmaydi.
CREATE TABLE IF NOT EXISTS guest_codes (
    code       TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    note       TEXT,
    used_by    INTEGER,
    used_name  TEXT,
    used_at    TEXT
);

-- Jonli kuzatuv. Skaner mavjud navbatni o'qiydi, bu esa kelayotgan har bir
-- so'rovni o'sha zahoti yozadi. Ikki muhim farqi bor:
--   1. Bot admin bo'lsa yetarli — userbot sessiyasi kerak emas.
--   2. So'rov QAYSI havola orqali kelganini ham biladi, ya'ni traffikni
--      sotuvchi bo'yicha ajratish mumkin.
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

-- Sotuvchi bo'yicha atributsiya. Telegram yangilanish ichida havolani
-- KESIB yuboradi ("https://t.me/+kHGCZCH1..."), to'liq shaklni faqat
-- havolani o'zi yaratgan botga beradi. Shuning uchun to'liq havola shu
-- yerda saqlanadi, kelgan so'rov esa prefiks bo'yicha solishtiriladi.
CREATE TABLE IF NOT EXISTS sellers (
    channel_id INTEGER NOT NULL,
    prefix     TEXT NOT NULL,
    name       TEXT NOT NULL,
    link       TEXT NOT NULL,
    created_at TEXT NOT NULL,
    revoked_at TEXT,
    PRIMARY KEY (channel_id, prefix)
);

-- Begona odam /start bosib qiziqish bildirsa shu yerga tushadi.
-- Maqsadi — egaga bir martagina xabar berish va aloqani yo'qotmaslik.
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

# v1 bazasida bu ustunlar yo'q edi — ishga tushganda qo'shiladi
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

    # Rasm ma'lumoti avval requests ichida edi. Endi u profiles jadvalida —
    # eski ustunlar qolsa scan_rows dagi JOIN bilan nom to'qnashadi.
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
    """Mehmon panelida qotirilgan skan. None bo'lsa — oxirgisi ko'rsatiladi.

    Tozalash dalilni kamaytiradi (rad etilgan akkauntlar ro'yxatdan chiqadi),
    shuning uchun tozalashdan oldingi skanni qotirib qo'yish kerak bo'ladi.
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
    """Berilgan skandan keyingi tugallangan skanlar — eskidan yangiga."""
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
    """Rad etilganlarni yozib qo'yadi — keyin kimni rad etganingiz ma'lum bo'lsin."""
    db.executemany(
        "INSERT OR REPLACE INTO declines"
        "(channel_id, user_id, username, risk, declined_at) VALUES (?, ?, ?, ?, ?)",
        [(channel_id, uid, username, risk, now_utc().isoformat())
         for uid, username, risk in entries],
    )
    db.commit()


def save_photo_info(entries):
    """Chuqur tekshiruv natijasi: (user_id, rasm soni, eng eski rasm sanasi)."""
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
    """Rad etilganlar vaqt bo'yicha. Mehmon paneli shundan tarix yasaydi."""
    return db.execute(
        "SELECT user_id, username, risk, declined_at FROM declines "
        "WHERE channel_id = ? ORDER BY declined_at",
        (channel_id,),
    ).fetchall()


# ================================================================
# JONLI OQIM
# ================================================================

LIVE_FIELDS = (
    "channel_id", "user_id", "request_date", "username", "first_name",
    "last_name", "has_photo", "is_premium", "is_scam", "is_fake", "is_bot",
    "last_seen", "access_hash", "invite_link", "invite_title",
)


def record_live(channel_id, item):
    """Kelgan bitta so'rovni yozadi. Yangi so'rov bo'lsa True qaytaradi.

    `item` — LIVE_FIELDS dagi channel_id dan keyingi qiymatlar ketma-ketligi.

    Telegram'da bitta akkauntning bitta kanalda faqat BITTA kutayotgan
    so'rovi bo'ladi, shuning uchun qator har akkauntga bittadan. Lekin
    rad etilgan akkaunt qayta so'rov yuborishi mumkin — ehtimol boshqa
    sotuvchining havolasi orqali. Shuning uchun sanasi yangiroq so'rov
    eskisini almashtiradi; aks holda qayta quyish eski sotuvchiga
    yozilib qolardi.

    Aynan o'sha sana kelsa — bu Telegram'ning takroriy yangilanishi,
    e'tiborsiz qoldiriladi.
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
    """Jonli yozuvlar — analyze() tushunadigan shaklda.

    profiles bilan birlashtiriladi, ya'ni chuqur tekshiruv o'tgan akkauntlar
    uchun rasm ma'lumoti bu yerda ham ko'rinadi.
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
    """Havola bo'yicha hisob — qaysi sotuvchi qancha olib kelgani.

    Qaytaradi: (havola, nomi, soni) ro'yxati, ko'pdan ozga.
    """
    return db.execute(
        "SELECT invite_link, invite_title, COUNT(*) AS n FROM live_requests "
        "WHERE channel_id = ? GROUP BY invite_link ORDER BY n DESC",
        (channel_id,),
    ).fetchall()


# ================================================================
# SOTUVCHILAR (ATRIBUTSIYA)
# ================================================================
#
# Telegram yangilanishda havolani kesib beradi: "https://t.me/+kHGCZCH1..."
# — ya'ni "https://t.me/+" va hash'ning 8 belgisi. To'liq havolani faqat
# uni yaratgan bot ko'ra oladi. Shuning uchun havolani botning o'zi
# yaratadi, to'lig'ini bu yerga yozadi, kelgan so'rovni esa prefiks
# bo'yicha topadi. 8 belgi ~48 bit — bir kanal ichida to'qnashuv bo'lmaydi.

LINK_PREFIX_LEN = 22


def link_prefix(link):
    """Kesik va to'liq havolani bitta solishtirish kalitiga keltiradi."""
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
    """Havolani bekor qilingan deb belgilaydi.

    Qator o'chirilmaydi — u orqali kelgan eski so'rovlar tarixda qoladi,
    aks holda atributsiya yo'qoladi.
    """
    db.execute(
        "UPDATE sellers SET revoked_at = ? WHERE channel_id = ? AND prefix = ?",
        (now_utc().isoformat(), channel_id, prefix),
    )
    db.commit()


# ================================================================
# MEHMON KODLARI
# ================================================================

def new_guest_code(note=None):
    """Bir martalik kod yaratadi va qaytaradi."""
    code = secrets.token_hex(4)
    db.execute(
        "INSERT INTO guest_codes(code, created_at, note) VALUES (?, ?, ?)",
        (code, now_utc().isoformat(), note),
    )
    db.commit()
    return code


def redeem_guest_code(code, user_id, name):
    """Kodni akkauntga biriktiradi.

    Qaytaradi: "ok" | "used" (boshqa akkaunt olgan) | "expired" | "bad".
    O'sha akkaunt qayta kiritsa yana "ok" — kod yo'qolib qolmaydi.
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
    """Mehmon bo'lsa — ismi, bo'lmasa None."""
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
    """Hamma mehmon kodini o'chiradi — kirish darhol yopiladi."""
    cur = db.execute("DELETE FROM guest_codes")
    db.commit()
    return cur.rowcount


def add_lead(user_id, username, name):
    """Qiziquvchini yozadi. Birinchi marta bo'lsa True — faqat shunda
    egaga xabar boradi, aks holda bitta odam tugmani bosaverib
    egani ko'mib tashlashi mumkin edi."""
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
# VAQT
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
# USERNAME OILALARI
# ================================================================

# "userpn12003" -> ("userpn", 12003)
_SPLIT = re.compile(r"^(.*?)(\d+)$")


def split_username(username):
    """Username oxiridagi raqamni ajratadi. Prefiks topilmasa (None, None)."""
    if not username:
        return None, None
    m = _SPLIT.match(username.lower())
    if not m:
        return None, None
    prefix, digits = m.group(1), m.group(2)
    if not prefix or not any(c.isalpha() for c in prefix):
        return None, None
    return prefix, int(digits)


# "behzod6385", "ruslanpn66344" — faqat kichik harf, ajratuvchisiz, 3-5 raqam.
# Tirik odamlar odatda bosh harf yoki pastki chiziq ishlatadi:
# "Abrorbek_Sobirov1701", "Sardor221109", "STARxUZB374".
_GENERATED = re.compile(r"^[a-z]+\d{3,5}$")

# Shu oraliqdagi raqam tug'ilgan yil bo'lishi mumkin — haqiqiy odam belgisi.
BIRTH_YEARS = range(1980, 2013)


def is_generated(username, is_premium=False):
    """Username generator izini ko'rsatadimi?

    Premium akkaunt hisobga olinmaydi — u pul to'langan, ya'ni tirik odam.
    """
    if not username or is_premium:
        return False
    if not _GENERATED.match(username):
        return False
    _, number = split_username(username)
    return number not in BIRTH_YEARS


@dataclass
class Family:
    """Bir xil prefiks + raqam bilan tugaydigan usernamelar to'plami."""

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
        """Raqam oralig'ining necha foizi band. 1.0 = to'liq ketma-ket."""
        span = self.hi - self.lo + 1
        return self.count / span if span > 0 else 1.0


@dataclass
class Window:
    """Yonma-yon to'lqin daqiqalaridan iborat bitta "quyish seansi"."""

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
# TAHLIL NATIJASI
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

    # ---------------------------------------------------- profil signallari
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

    # ---------------------------------------------------- username oilalari
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

    # Oila kichik bo'lsa ham generator izi ko'rinib turadi:
    # "ruslanpn66344", "behzod6385" — har biri yolg'iz, lekin naqsh bitta.
    rep.generated_users = {
        r["user_id"] for r in rows
        if is_generated(r["username"], r["is_premium"])
    }

    # ---------------------------------------------------- vaqt to'lqinlari
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

    # ---------------------------------------------------- quyish seanslari
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
        # Tirik auditoriya "chala" bo'ladi. Seansda rasm/username ulushi
        # umumiy fondan sezilarli yuqori bo'lsa — akkauntlar tayyorlangan.
        win.uniform = (
            win.photo_rate - rep.base_photo_rate >= config.UNIFORMITY_GAP
            or win.username_rate - rep.base_username_rate >= config.UNIFORMITY_GAP
        )
        if win.uniform:
            rep.uniform_users |= {r["user_id"] for r in win.rows}

    # ---------------------------------------------------- user_id klasterlari
    # ---------------------------------------------------- rasm partiyalari
    # Tirik odam rasmini o'zi xohlagan kuni qo'yadi. Bir necha yuz akkaunt
    # aynan bir kunda rasm yuklagan bo'lsa — ular birga tayyorlangan.
    deep = [r for r in rows if r["photo_count"] is not None]
    rep.deep_total = len(deep)
    if deep:
        # 5+ rasmi bor akkaunt — tirik odam. Uni partiyaga qo'shmaymiz.
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

    # ---------------------------------------------------- risk ballari
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

    # ---------------------------------------------------- kanal risk balli
    #
    # Risk = guruh dalili bor akkauntlar ulushi. "Guruh dalili" deganda
    # yakka profil emas, balki boshqalar bilan bog'liqlik tushuniladi:
    # bir xil username oilasi, bir xil profilli quyish seansi, ID klasteri.
    # Bu yakka signallarga (rasm yo'q) qaraganda ancha ishonchli.
    rep.flagged_users = (
        rep.uniform_users | rep.family_users | rep.cluster_users
        | rep.generated_users | rep.batch_users
    )
    rep.risk = round(100 * len(rep.flagged_users) / rep.total)
    return rep


def _peak_window(sorted_dates, seconds):
    """Eng zich `seconds` soniyalik siljuvchi oyna: (nechta, boshlanish vaqti)."""
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
    """Yonma-yon to'lqin daqiqalarini bitta seansga birlashtiradi."""
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
        # Seans ichidagi "sovuq" daqiqalar ham seansga tegishli — quyish
        # oqimi bir zumga sekinlashgani uni alohida qilmaydi.
        win.rows = [r for r in rows if win.start <= parse_dt(r["request_date"]) <= win.end]

    return sorted(sessions, key=lambda w: w.count, reverse=True)


def _id_clusters(rows):
    """user_id lari bir-biriga yaqin guruhlar. Ommaviy ro'yxatdan o'tish izi."""
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
    """Bitta akkaunt uchun 0-100 risk balli va sabablar ro'yxati."""
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
# GRAFIK UCHUN GURUHLASH
# ================================================================

def timeline(rep, buckets=20):
    """Vaqtni ~`buckets` ta bo'lakka bo'lib sanaydi.

    Qaytaradi: (label, count) ro'yxati va bo'lak uzunligi (soniya).
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
