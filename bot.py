import os
import json
import logging
import re
from datetime import datetime, timezone, time
from threading import Thread

import pytz
from flask import Flask
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from telegram.ext import (
    Updater,
    MessageHandler,
    Filters,
    CallbackContext,
    CommandHandler,
)

# =================== إعدادات أساسية ===================

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATA_FILE = "suqya_users.json"

# معرف الأدمن (أنت)
ADMIN_ID = 931350292

# معرف المشرفة (الأخوات)
SUPERVISOR_ID = 8395818573

# ملف اللوج
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# =================== خادم ويب بسيط لـ Render ===================

app = Flask(__name__)

@app.route("/")
def index():
    return "Suqya Al-Kawther bot is running ✅"

def run_flask():
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)

# =================== تخزين البيانات ===================

def load_data():
    if not os.path.exists(DATA_FILE):
        return {}
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return {}

def save_data():
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Error saving data: {e}")

data = load_data()

def get_user_record(user):
    """
    ينشئ أو يرجع سجل المستخدم، ويحدّث آخر نشاط،
    ويضمن وجود الحقول الجديدة في السجلات القديمة.
    """
    user_id = str(user.id)
    now_iso = datetime.now(timezone.utc).isoformat()

    if user_id not in data:
        data[user_id] = {
            "user_id": user.id,
            "first_name": user.first_name,
            "username": user.username,
            "created_at": now_iso,
            "last_active": now_iso,
            # إعدادات الماء
            "gender": None,
            "age": None,
            "weight": None,
            "water_liters": None,
            "cups_goal": None,
            "reminders_on": False,
            # تقدم الماء اليومي
            "today_date": None,
            "today_cups": 0,
            # ورد القرآن
            "quran_pages_goal": None,
            "quran_pages_today": 0,
            "quran_today_date": None,
            # أرقام إحصائية
            "tasbih_total": 0,
            "adhkar_count": 0,
            # مذكّرات قلبي
            "heart_memos": [],
            # نظام النقاط والمستويات والميداليات
            "points": 0,
            "level": 0,
            "medals": [],
            "best_rank": None,
            # 🆕 نظام الاستمرارية
            "daily_completion_streak": 0,
            "last_completion_date": None,
            "has_daily_activity_medal": False,
            "has_consistency_medal": False,
        }
    else:
        record = data[user_id]
        record["first_name"] = user.first_name
        record["username"] = user.username
        record["last_active"] = now_iso

        # ضمان الحقول
        record.setdefault("gender", None)
        record.setdefault("age", None)
        record.setdefault("weight", None)
        record.setdefault("water_liters", None)
        record.setdefault("cups_goal", None)
        record.setdefault("reminders_on", False)
        record.setdefault("today_date", None)
        record.setdefault("today_cups", 0)
        record.setdefault("quran_pages_goal", None)
        record.setdefault("quran_pages_today", 0)
        record.setdefault("quran_today_date", None)
        record.setdefault("tasbih_total", 0)
        record.setdefault("adhkar_count", 0)
        record.setdefault("heart_memos", [])
        record.setdefault("points", 0)
        record.setdefault("level", 0)
        record.setdefault("medals", [])
        record.setdefault("best_rank", None)
        # 🆕 ضمان الحقول الجديدة
        record.setdefault("daily_completion_streak", 0)
        record.setdefault("last_completion_date", None)
        record.setdefault("has_daily_activity_medal", False)
        record.setdefault("has_consistency_medal", False)

    save_data()
    return data[user_id]

def update_user_record(user_id: int, **kwargs):
    uid = str(user_id)
    if uid not in data:
        return
    data[uid].update(kwargs)
    data[uid]["last_active"] = datetime.now(timezone.utc).isoformat()
    save_data()

def get_all_user_ids():
    return [int(uid) for uid in data.keys()]

def is_admin(user_id: int) -> bool:
    return ADMIN_ID is not None and user_id == ADMIN_ID

def is_supervisor(user_id: int) -> bool:
    return SUPERVISOR_ID is not None and user_id == SUPERVISOR_ID

# =================== حالات الإدخال ===================

WAITING_GENDER = set()
WAITING_AGE = set()
WAITING_WEIGHT = set()

WAITING_QURAN_GOAL = set()
WAITING_QURAN_ADD_PAGES = set()

WAITING_TASBIH = set()
ACTIVE_TASBIH = {}

# مذكّرات قلبي
WAITING_MEMO_MENU = set()
WAITING_MEMO_ADD = set()
WAITING_MEMO_EDIT_SELECT = set()
WAITING_MEMO_EDIT_TEXT = set()
WAITING_MEMO_DELETE_SELECT = set()
MEMO_EDIT_INDEX = {}

# دعم / إدارة
WAITING_SUPPORT_GENDER = set()
WAITING_SUPPORT = set()
WAITING_BROADCAST = set()

# =================== الأزرار ===================

# رئيسية
BTN_ADHKAR_MAIN = "أذكاري 🤲"
BTN_QURAN_MAIN = "وردي القرآني 📖"
BTN_TASBIH_MAIN = "السبحة 📿"
BTN_MEMOS_MAIN = "مذكّرات قلبي 🩵"
BTN_WATER_MAIN = "منبّه الماء 💧"
BTN_STATS = "احصائياتي 📊"

BTN_SUPPORT = "تواصل مع الدعم ✉️"

BTN_CANCEL = "إلغاء ❌"
BTN_BACK_MAIN = "رجوع للقائمة الرئيسية ⬅️"

# المنافسات و المجتمع
BTN_COMP_MAIN = "المنافسات و المجتمع 🏅"
BTN_MY_PROFILE = "ملفي التنافسي 🎯"
BTN_TOP10 = "أفضل 10 🏅"
BTN_TOP100 = "أفضل 100 🏆"

# لوحة المدير
BTN_ADMIN_PANEL = "لوحة التحكم 🛠"
BTN_ADMIN_USERS_COUNT = "عدد المستخدمين 👥"
BTN_ADMIN_USERS_LIST = "قائمة المستخدمين 📄"
BTN_ADMIN_BROADCAST = "رسالة جماعية 📢"
BTN_ADMIN_RANKINGS = "ترتيب المنافسة (تفصيلي) 📊"

MAIN_KEYBOARD_USER = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_ADHKAR_MAIN), KeyboardButton(BTN_QURAN_MAIN)],
        [KeyboardButton(BTN_TASBIH_MAIN), KeyboardButton(BTN_MEMOS_MAIN)],
        [KeyboardButton(BTN_WATER_MAIN), KeyboardButton(BTN_STATS)],
        [KeyboardButton(BTN_SUPPORT), KeyboardButton(BTN_COMP_MAIN)],
    ],
    resize_keyboard=True,
)

MAIN_KEYBOARD_ADMIN = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_ADHKAR_MAIN), KeyboardButton(BTN_QURAN_MAIN)],
        [KeyboardButton(BTN_TASBIH_MAIN), KeyboardButton(BTN_MEMOS_MAIN)],
        [KeyboardButton(BTN_WATER_MAIN), KeyboardButton(BTN_STATS)],
        [KeyboardButton(BTN_SUPPORT), KeyboardButton(BTN_COMP_MAIN)],
        [KeyboardButton(BTN_ADMIN_PANEL)],
    ],
    resize_keyboard=True,
)

CANCEL_KB = ReplyKeyboardMarkup(
    [[KeyboardButton(BTN_CANCEL)]],
    resize_keyboard=True,
)

# ---- منبّه الماء ----
BTN_WATER_LOG = "سجلت كوب ماء 🥤"
BTN_WATER_ADD_CUPS = "إضافة عدد أكواب 🧮🥤"
BTN_WATER_STATUS = "مستواي اليوم 📊"
BTN_WATER_SETTINGS = "إعدادات الماء ⚙️"

BTN_WATER_NEED = "حساب احتياج الماء 🧮"
BTN_WATER_REM_ON = "تشغيل التذكير ⏰"
BTN_WATER_REM_OFF = "إيقاف التذكير 📴"

BTN_WATER_BACK_MENU = "رجوع إلى منبّه الماء ⬅️"

BTN_GENDER_MALE = "🧔‍♂️ ذكر"
BTN_GENDER_FEMALE = "👩 أنثى"

WATER_MENU_KB_USER = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_WATER_LOG), KeyboardButton(BTN_WATER_ADD_CUPS)],
        [KeyboardButton(BTN_WATER_STATUS)],
        [KeyboardButton(BTN_WATER_SETTINGS)],
        [KeyboardButton(BTN_BACK_MAIN)],
    ],
    resize_keyboard=True,
)

WATER_MENU_KB_ADMIN = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_WATER_LOG), KeyboardButton(BTN_WATER_ADD_CUPS)],
        [KeyboardButton(BTN_WATER_STATUS)],
        [KeyboardButton(BTN_WATER_SETTINGS)],
        [KeyboardButton(BTN_BACK_MAIN), KeyboardButton(BTN_ADMIN_PANEL)],
    ],
    resize_keyboard=True,
)

WATER_SETTINGS_KB_USER = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_WATER_NEED)],
        [KeyboardButton(BTN_WATER_REM_ON), KeyboardButton(BTN_WATER_REM_OFF)],
        [KeyboardButton(BTN_WATER_BACK_MENU)],
        [KeyboardButton(BTN_BACK_MAIN)],
    ],
    resize_keyboard=True,
)

WATER_SETTINGS_KB_ADMIN = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_WATER_NEED)],
        [KeyboardButton(BTN_WATER_REM_ON), KeyboardButton(BTN_WATER_REM_OFF)],
        [KeyboardButton(BTN_WATER_BACK_MENU)],
        [KeyboardButton(BTN_BACK_MAIN), KeyboardButton(BTN_ADMIN_PANEL)],
    ],
    resize_keyboard=True,
)

GENDER_KB = ReplyKeyboardMarkup(
    [[KeyboardButton(BTN_GENDER_MALE), KeyboardButton(BTN_GENDER_FEMALE)]],
    resize_keyboard=True,
)

# ---- ورد القرآن ----
BTN_QURAN_SET_GOAL = "تعيين ورد اليوم 📌"
BTN_QURAN_ADD_PAGES = "سجلت صفحات اليوم ✅"
BTN_QURAN_STATUS = "مستوى وردي اليوم 📊"
BTN_QURAN_RESET_DAY = "إعادة تعيين ورد اليوم 🔁"

QURAN_MENU_KB_USER = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_QURAN_SET_GOAL)],
        [KeyboardButton(BTN_QURAN_ADD_PAGES), KeyboardButton(BTN_QURAN_STATUS)],
        [KeyboardButton(BTN_QURAN_RESET_DAY)],
        [KeyboardButton(BTN_BACK_MAIN)],
    ],
    resize_keyboard=True,
)

QURAN_MENU_KB_ADMIN = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_QURAN_SET_GOAL)],
        [KeyboardButton(BTN_QURAN_ADD_PAGES), KeyboardButton(BTN_QURAN_STATUS)],
        [KeyboardButton(BTN_QURAN_RESET_DAY)],
        [KeyboardButton(BTN_BACK_MAIN), KeyboardButton(BTN_ADMIN_PANEL)],
    ],
    resize_keyboard=True,
)

# ---- أذكاري ----
BTN_ADHKAR_MORNING = "أذكار الصباح 🌅"
BTN_ADHKAR_EVENING = "أذكار المساء 🌙"
BTN_ADHKAR_GENERAL = "أذكار عامة 💭"

ADHKAR_MENU_KB_USER = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_ADHKAR_MORNING), KeyboardButton(BTN_ADHKAR_EVENING)],
        [KeyboardButton(BTN_ADHKAR_GENERAL)],
        [KeyboardButton(BTN_BACK_MAIN)],
    ],
    resize_keyboard=True,
)

ADHKAR_MENU_KB_ADMIN = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_ADHKAR_MORNING), KeyboardButton(BTN_ADHKAR_EVENING)],
        [KeyboardButton(BTN_ADHKAR_GENERAL)],
        [KeyboardButton(BTN_BACK_MAIN), KeyboardButton(BTN_ADMIN_PANEL)],
    ],
    resize_keyboard=True,
)

# ---- السبحة ----
BTN_TASBIH_TICK = "تسبيحة ✅"
BTN_TASBIH_END = "إنهاء الذكر ⬅️"

TASBIH_RUN_KB_USER = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_TASBIH_TICK)],
        [KeyboardButton(BTN_TASBIH_END)],
        [KeyboardButton(BTN_CANCEL)],
    ],
    resize_keyboard=True,
)

TASBIH_RUN_KB_ADMIN = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_TASBIH_TICK)],
        [KeyboardButton(BTN_TASBIH_END)],
        [KeyboardButton(BTN_CANCEL), KeyboardButton(BTN_ADMIN_PANEL)],
    ],
    resize_keyboard=True,
)

TASBIH_ITEMS = [
    ("سبحان الله", 33),
    ("الحمد لله", 33),
    ("الله أكبر", 34),
    ("سبحان الله وبحمده", 100),
    ("لا إله إلا الله", 100),
    ("اللهم صل وسلم على سيدنا محمد", 50),
]

def build_tasbih_menu(is_admin_flag: bool):
    rows = [[KeyboardButton(f"{text} ({count})")] for text, count in TASBIH_ITEMS]
    last_row = [KeyboardButton(BTN_BACK_MAIN)]
    if is_admin_flag:
        last_row.append(KeyboardButton(BTN_ADMIN_PANEL))
    rows.append(last_row)
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

# ---- مذكّرات قلبي ----
BTN_MEMO_ADD = "➕ إضافة مذكرة"
BTN_MEMO_EDIT = "✏️ تعديل مذكرة"
BTN_MEMO_DELETE = "🗑 حذف مذكرة"
BTN_MEMO_BACK = "رجوع ⬅️"

def build_memos_menu_kb(is_admin_flag: bool):
    rows = [
        [KeyboardButton(BTN_MEMO_ADD)],
        [KeyboardButton(BTN_MEMO_EDIT), KeyboardButton(BTN_MEMO_DELETE)],
        [KeyboardButton(BTN_MEMO_BACK)],
    ]
    if is_admin_flag:
        rows.append([KeyboardButton(BTN_ADMIN_PANEL)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

# ---- لوحة التحكم ----
ADMIN_PANEL_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_ADMIN_USERS_COUNT), KeyboardButton(BTN_ADMIN_USERS_LIST)],
        [KeyboardButton(BTN_ADMIN_BROADCAST), KeyboardButton(BTN_ADMIN_RANKINGS)],
        [KeyboardButton(BTN_BACK_MAIN)],
    ],
    resize_keyboard=True,
)

# ---- المنافسات و المجتمع ----
COMP_MENU_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_MY_PROFILE)],
        [KeyboardButton(BTN_TOP10)],
        [KeyboardButton(BTN_TOP100)],
        [KeyboardButton(BTN_BACK_MAIN)],
    ],
    resize_keyboard=True,
)

# =================== نظام النقاط (ثوابت) ===================

POINTS_PER_WATER_CUP = 1
POINTS_WATER_DAILY_BONUS = 20

POINTS_PER_QURAN_PAGE = 3
POINTS_QURAN_DAILY_BONUS = 30

def tasbih_points_for_session(target_count: int) -> int:
    return max(target_count // 10, 1)

# =================== نظام الميداليات المتقدم ===================

MEDAL_RULES = [
    (1, "ميدالية بداية الطريق 🟢"),
    (3, "ميدالية الاستمرار 🎓"),
    (5, "ميدالية الهمة العالية 🔥"),
    (10, "بطل سُقيا الكوثر 🏆"),
]

MEDAL_DAILY_ACTIVITY = "ميدالية النشاط اليومي ⚡"
MEDAL_CONSISTENCY = "ميدالية الاستمرارية 📅"

WAITING_CONSISTENCY_CHECK = {}

# =================== دوال مساعدة عامة ===================

def user_main_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return MAIN_KEYBOARD_ADMIN if is_admin(user_id) else MAIN_KEYBOARD_USER

def water_menu_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return WATER_MENU_KB_ADMIN if is_admin(user_id) else WATER_MENU_KB_USER

def water_settings_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return WATER_SETTINGS_KB_ADMIN if is_admin(user_id) else WATER_SETTINGS_KB_USER

def adhkar_menu_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return ADHKAR_MENU_KB_ADMIN if is_admin(user_id) else ADHKAR_MENU_KB_USER

def quran_menu_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return QURAN_MENU_KB_ADMIN if is_admin(user_id) else QURAN_MENU_KB_USER

def tasbih_run_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    return TASBIH_RUN_KB_ADMIN if is_admin(user_id) else TASBIH_RUN_KB_USER

def ensure_today_water(record):
    """تصفير عدّاد الماء إذا تغيّر اليوم."""
    today_str = datetime.now(timezone.utc).date().isoformat()
    if record.get("today_date") != today_str:
        record["today_date"] = today_str
        record["today_cups"] = 0
        save_data()

def ensure_today_quran(record):
    """تصفير ورد اليوم لو تغيّر التاريخ."""
    today_str = datetime.now(timezone.utc).date().isoformat()
    if record.get("quran_today_date") != today_str:
        record["quran_today_date"] = today_str
        record["quran_pages_today"] = 0
        save_data()

def format_water_status_text(record):
    """نص حالة الماء اليوم."""
    ensure_today_water(record)
    cups_goal = record.get("cups_goal")
    today_cups = record.get("today_cups", 0)

    if not cups_goal:
        return (
            "لم تقم بعد بحساب احتياجك من الماء.\n"
            "اذهب إلى «منبّه الماء 💧» ثم «إعدادات الماء ⚙️» ثم «حساب احتياج الماء 🧮»."
        )

    remaining = max(cups_goal - today_cups, 0)
    percent = min(int(today_cups / cups_goal * 100), 100)

    text = (
        "📊 مستوى شرب الماء اليوم:\n\n"
        f"- الأكواس التي شربتها: {today_cups} من {cups_goal} كوب.\n"
        f"- نسبة الإنجاز التقريبية: {percent}%.\n\n"
    )

    if remaining > 0:
        text += (
            f"تبقّى لك تقريبًا {remaining} كوب لتصل لهدفك اليومي.\n"
            "استمر بهدوء، كوب بعد كوب 💧."
        )
    else:
        text += (
            "ما شاء الله، وصلت لهدفك اليومي من الماء 🎉\n"
            "حافظ على هذا المستوى قدر استطاعتك."
        )

    return text

def format_quran_status_text(record):
    """نص حالة ورد القرآن اليوم."""
    ensure_today_quran(record)
    goal = record.get("quran_pages_goal")
    today = record.get("quran_pages_today", 0)

    if not goal:
        return (
            "لم تضبط بعد وردك من القرآن.\n"
            "اذهب إلى «وردي القرآني 📖» ثم «تعيين ورد اليوم 📌»."
        )

    remaining = max(goal - today, 0)
    percent = min(int(today / goal * 100), 100)

    text = (
        "📖 حالة وردك القرآني اليوم:\n\n"
        f"- الصفحات التي قرأتها اليوم: {today} من {goal} صفحة.\n"
        f"- نسبة الإنجاز التقريبية: {percent}%.\n\n"
    )

    if remaining > 0:
        text += (
            f"تبقّى لك تقريبًا {remaining} صفحة لتكمل ورد اليوم.\n"
            "اقرأ على مهل مع تدبّر 🤍."
        )
    else:
        text += (
            "الحمد لله، أتممت وردك لهذا اليوم 🎉\n"
            "ثبتك الله على ملازمة كتابه."
        )

    return text

def increment_adhkar_count(user_id: int, amount: int = 1):
    uid = str(user_id)
    if uid not in data:
        return
    record = data[uid]
    record["adhkar_count"] = record.get("adhkar_count", 0) + amount
    save_data()

def increment_tasbih_total(user_id: int, amount: int = 1):
    uid = str(user_id)
    if uid not in data:
        return
    record = data[uid]
    record["tasbih_total"] = record.get("tasbih_total", 0) + amount
    save_data()

# =================== نظام النقاط / المستويات / الميداليات / الترتيب ===================

def get_users_sorted_by_points():
    return sorted(
        data.values(),
        key=lambda r: r.get("points", 0),
        reverse=True,
    )

def check_rank_improvement(user_id: int, record: dict, context: CallbackContext = None):
    """يتأكد إذا ترتيب المستخدم تحسّن ويرسل له رسالة لو دخل توب 10 أو توب 100."""
    sorted_users = get_users_sorted_by_points()
    rank = None
    for idx, rec in enumerate(sorted_users, start=1):
        if rec.get("user_id") == user_id:
            rank = idx
            break

    if rank is None:
        return

    best_rank = record.get("best_rank")
    if best_rank is not None and rank >= best_rank:
        return

    record["best_rank"] = rank
    save_data()

    if context is None:
        return

    try:
        if rank <= 10:
            context.bot.send_message(
                chat_id=user_id,
                text=(
                    f"🏅 مبروك! دخلت ضمن أفضل 10 مستخدمين في لوحة الشرف.\n"
                    f"ترتيبك الحالي: #{rank}"
                ),
            )
        elif rank <= 100:
            context.bot.send_message(
                chat_id=user_id,
                text=(
                    f"🏆 تهانينا! أصبحت ضمن أفضل 100 مستخدم في المنافسة.\n"
                    f"ترتيبك الحالي: #{rank}"
                ),
            )
    except Exception as e:
        logger.error(f"Error sending rank improvement message to {user_id}: {e}")

def check_daily_completion(user_id: int, context: CallbackContext = None):
    """
    يتحقق إذا أكمل المستخدم الماء + القرآن في نفس اليوم.
    """
    uid = str(user_id)
    if uid not in data:
        return False

    record = data[uid]
    
    ensure_today_water(record)
    ensure_today_quran(record)
    
    today_str = datetime.now(timezone.utc).date().isoformat()
    
    water_completed = (
        record.get("cups_goal") and 
        record.get("today_cups", 0) >= record.get("cups_goal", 0)
    )
    quran_completed = (
        record.get("quran_pages_goal") and 
        record.get("quran_pages_today", 0) >= record.get("quran_pages_goal", 0)
    )
    
    if not (water_completed and quran_completed):
        return False
    
    last_date = record.get("last_completion_date")
    
    today_obj = datetime.fromisoformat(today_str).date()
    
    if last_date:
        last_obj = datetime.fromisoformat(last_date).date()
        days_diff = (today_obj - last_obj).days
        
        if days_diff == 1:
            record["daily_completion_streak"] += 1
        elif days_diff > 1:
            record["daily_completion_streak"] = 1
    else:
        record["daily_completion_streak"] = 1
    
    record["last_completion_date"] = today_str
    
    if not record.get("has_daily_activity_medal"):
        record["has_daily_activity_medal"] = True
        if MEDAL_DAILY_ACTIVITY not in record.get("medals", []):
            record["medals"].append(MEDAL_DAILY_ACTIVITY)
        
        if context:
            try:
                context.bot.send_message(
                    chat_id=user_id,
                    text=(
                        "🎉 مبروك! حصلت على ميدالية النشاط اليومي ⚡\n"
                        "لأنك أكملت هدف الماء + القرآن في نفس اليوم.\n"
                        "استمر على هذا الخط الممتاز!"
                    ),
                )
            except Exception as e:
                logger.error(f"Error sending daily activity medal message: {e}")
    
    if (record.get("daily_completion_streak", 0) >= 7 and 
        not record.get("has_consistency_medal")):
        record["has_consistency_medal"] = True
        if MEDAL_CONSISTENCY not in record.get("medals", []):
            record["medals"].append(MEDAL_CONSISTENCY)
        
        if context:
            try:
                context.bot.send_message(
                    chat_id=user_id,
                    text=(
                        "👑 عظيم جداً! حصلت على ميدالية الاستمرارية 📅\n\n"
                        "استمررت 7 أيام متتالية في إكمال أهدافك اليومية!\n"
                        "هذا دليل على عزيمة قوية وإصرار جميل.\n\n"
                        "استمر في هذا المسار المبارك 🤍"
                    ),
                )
            except Exception as e:
                logger.error(f"Error sending consistency medal message: {e}")
    
    save_data()
    return True

def format_medals_list(medals: list) -> str:
    """تنسيق قائمة الميداليات بشكل جميل."""
    if not medals:
        return "(لا توجد ميداليات بعد)"
    return " — ".join(medals)

def format_ranking_entry(rank: int, user_record: dict) -> str:
    """تنسيق إدخال واحد من لوحة الترتيب."""
    name = user_record.get("first_name", "مستخدم")
    points = user_record.get("points", 0)
    medals = user_record.get("medals", [])
    
    first_line = f"{rank}) {name} — 🎯 {points} نقطة"
    medals_line = format_medals_list(medals)
    
    return f"{first_line}\n{medals_line}"

def format_user_profile(user_record: dict) -> str:
    """تنسيق ملف المستخدم التنافسي."""
    name = user_record.get("first_name", "مستخدم")
    points = user_record.get("points", 0)
    level = user_record.get("level", 0)
    medals = user_record.get("medals", [])
    streak = user_record.get("daily_completion_streak", 0)
    
    sorted_users = get_users_sorted_by_points()
    rank = None
    for idx, rec in enumerate(sorted_users, start=1):
        if rec.get("user_id") == user_record.get("user_id"):
            rank = idx
            break
    
    rank_text = f"#{rank}" if rank else "غير مصنف"
    
    profile_text = (
        f"🎯 ملفك التنافسي:\n\n"
        f"الاسم: {name}\n"
        f"الترتيب: {rank_text}\n"
        f"النقاط: 🎯 {points} نقطة\n"
        f"المستوى: {level}\n"
        f"سلسلة النشاط: {streak} يوم متتالي 🔥\n\n"
    )
    
    profile_text += f"الميداليات:\n{format_medals_list(medals)}"
    
    return profile_text

def update_level_and_medals(user_id: int, record: dict, context: CallbackContext = None):
    """تحديث المستوى والميداليات وإرسال رسائل التهنئة."""
    old_level = record.get("level", 0)
    points = record.get("points", 0)

    new_level = points // 20

    if new_level == old_level:
        check_rank_improvement(user_id, record, context)
        return

    record["level"] = new_level
    medals = record.get("medals", [])
    new_medals = []

    medal_rules = [
        (1, "ميدالية بداية الطريق 🟢"),
        (3, "ميدالية الاستمرار 🎓"),
        (5, "ميدالية الهمة العالية 🔥"),
        (10, "بطل سُقيا الكوثر 🏆"),
    ]

    for lvl, name in medal_rules:
        if new_level >= lvl and name not in medals:
            medals.append(name)
            new_medals.append(name)

    record["medals"] = medals
    save_data()

    check_rank_improvement(user_id, record, context)

    if context is not None:
        try:
            msg = f"🎉 مبروك! وصلت إلى المستوى {new_level}.\n"
            if new_medals:
                msg += "وحصلت على الميداليات التالية:\n" + "\n".join(f"- {m}" for m in new_medals)
            context.bot.send_message(chat_id=user_id, text=msg)
        except Exception as e:
            logger.error(f"Error sending level up message to {user_id}: {e}")

def add_points(user_id: int, amount: int, context: CallbackContext = None, reason: str = ""):
    """إضافة نقاط للمستخدم ثم تحديث مستواه وترتيبه."""
    if amount <= 0:
        return

    uid = str(user_id)
    if uid not in data:
        return

    record = data[uid]
    record["points"] = record.get("points", 0) + amount
    update_level_and_medals(user_id, record, context)
    
    check_daily_completion(user_id, context)

# =================== دوال عرض الترتيب المحدثة ===================

def get_top_rankings(count: int = 10) -> str:
    """الحصول على أفضل X مستخدمين بالتنسيق الجديد."""
    sorted_users = get_users_sorted_by_points()
    top_users = sorted_users[:count]
    
    ranking_text = f"🏆 أفضل {count}:\n\n"
    
    for idx, user_rec in enumerate(top_users, start=1):
        ranking_text += format_ranking_entry(idx, user_rec)
        ranking_text += "\n\n"
    
    return ranking_text.strip()

def handle_top_10(update: Update, context: CallbackContext):
    """عرض أفضل 10 مستخدمين."""
    user = update.effective_user
    get_user_record(user)
    
    ranking_text = get_top_rankings(10)
    
    update.message.reply_text(
        ranking_text,
        reply_markup=COMP_MENU_KB,
    )

def handle_top_100(update: Update, context: CallbackContext):
    """عرض أفضل 100 مستخدم."""
    user = update.effective_user
    get_user_record(user)
    
    ranking_text = get_top_rankings(100)
    
    if len(ranking_text) > 4096:
        parts = [ranking_text[i:i+4000] for i in range(0, len(ranking_text), 4000)]
        for part in parts:
            update.message.reply_text(part)
        update.message.reply_text(
            "انتهت قائمة أفضل 100 مستخدم.",
            reply_markup=COMP_MENU_KB,
        )
    else:
        update.message.reply_text(
            ranking_text,
            reply_markup=COMP_MENU_KB,
        )

def handle_my_profile(update: Update, context: CallbackContext):
    """عرض الملف التنافسي للمستخدم."""
    user = update.effective_user
    record = get_user_record(user)
    
    profile_text = format_user_profile(record)
    
    update.message.reply_text(
        profile_text,
        reply_markup=COMP_MENU_KB,
    )

# =================== أذكار ثابتة ===================

ADHKAR_MORNING_TEXT = (
    "أذكار الصباح 🌅:\n\n"
    "1⃣ آية الكرسي مرة واحدة.\n"
    "2⃣ قل هو الله أحد، قل أعوذ برب الفلق، قل أعوذ برب الناس: ثلاث مرات.\n"
    "3⃣ «أصبحنا وأصبح الملك لله...».\n"
    "4⃣ «اللهم ما أصبح بي من نعمة...».\n"
    "5⃣ «اللهم إني أصبحت أشهدك...» أربع مرات.\n"
    "6⃣ «حسبي الله لا إله إلا هو...» سبع مرات.\n"
    "7⃣ «اللهم صل وسلم على سيدنا محمد» عددًا كثيرًا.\n\n"
    "للتسبيح بعدد معيّن استخدم زر «السبحة 📿»."
)

ADHKAR_EVENING_TEXT = (
    "أذكار المساء 🌙:\n\n"
    "1⃣ آية الكرسي مرة واحدة.\n"
    "2⃣ قل هو الله أحد، قل أعوذ برب الفلق، قل أعوذ برب الناس: ثلاث مرات.\n"
    "3⃣ «أمسينا وأمسى الملك لله...».\n"
    "4⃣ «اللهم ما أمسى بي من نعمة...».\n"
    "5⃣ «اللهم إني أمسيت أشهدك...» أربع مرات.\n"
    "6⃣ «باسم الله الذي لا يضر...» ثلاث مرات.\n"
    "7⃣ الإكثار من الصلاة على النبي ﷺ.\n\n"
    "للتسبيح بعدد معيّن استخدم زر «السبحة 📿»."
)

ADHKAR_GENERAL_TEXT = (
    "أذكار عامة 💚:\n\n"
    "• «أستغفر الله العظيم وأتوب إليه».\n"
    "• «لا إله إلا الله وحده لا شريك له».\n"
    "• «سبحان الله، والحمد لله، ولا إله إلا الله».\n"
    "• «لا حول ولا قوة إلا بالله».\n"
    "• «اللهم صل وسلم على سيدنا محمد».\n\n"
    "استعمل «السبحة 📿» لاختيار ذكر وعدد معيّن."
)

# =================== أوامر البوت ===================

def start_command(update: Update, context: CallbackContext):
    user = update.effective_user
    is_new = str(user.id) not in data
    get_user_record(user)

    kb = user_main_keyboard(user.id)

    update.message.reply_text(
        f"مرحبًا {user.first_name} 👋\n\n"
        "أهلًا بك في بوت *سُقيا الكوثر*.\n"
        "يساعدك على الأذكار والقرآن والماء والنقاط والميداليات 🎖\n\n"
        "اختر من القائمة:",
        reply_markup=kb,
        parse_mode="Markdown",
    )

    if is_new and ADMIN_ID is not None:
        try:
            context.bot.send_message(
                chat_id=ADMIN_ID,
                text=(
                    "👤 مستخدم جديد:\n"
                    f"الاسم: {user.full_name}\n"
                    f"ID: `{user.id}`"
                ),
                parse_mode="Markdown",
            )
        except Exception as e:
            logger.error(f"Error notifying admin: {e}")

def help_command(update: Update, context: CallbackContext):
    kb = user_main_keyboard(update.effective_user.id)
    update.message.reply_text(
        "الأوامر المتاحة:\n\n"
        "• أذكاري 🤲 → الأذكار\n"
        "• وردي القرآني 📖 → القرآن\n"
        "• السبحة 📿 → التسبيح\n"
        "• مذكّرات قلبي 🩵 → المذكرات\n"
        "• منبّه الماء 💧 → تتبع الماء\n"
        "• احصائياتي 📊 → إحصائيات\n"
        "• تواصل مع الدعم ✉️ → الدعم\n"
        "• المنافسات و المجتمع 🏅 → لوحات الشرف",
        reply_markup=kb,
    )

# =================== قسم منبّه الماء ===================

def open_water_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    get_user_record(user)
    kb = water_menu_keyboard(user.id)
    update.message.reply_text(
        "منبّه الماء 💧:\n"
        "• سجّل ما تشربه من أكواب.\n"
        "• شاهد مستواك اليوم.\n"
        "• عدّل إعداداتك.\n"
        "كل كوب يزيد نقاطك 🎯",
        reply_markup=kb,
    )

def open_water_settings(update: Update, context: CallbackContext):
    kb = water_settings_keyboard(update.effective_user.id)
    update.message.reply_text(
        "إعدادات الماء ⚙️:\n"
        "1) حساب احتياجك اليومي من الماء.\n"
        "2) تشغيل أو إيقاف التذكير.\n"
        "3) الرجوع إلى منبّه الماء.",
        reply_markup=kb,
    )

def handle_water_need_start(update: Update, context: CallbackContext):
    user_id = update.effective_user.id

    WAITING_GENDER.add(user_id)
    WAITING_AGE.discard(user_id)
    WAITING_WEIGHT.discard(user_id)

    update.message.reply_text(
        "أولًا: اختر الجنس:",
        reply_markup=GENDER_KB,
    )

def handle_gender_input(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    text = update.message.text.strip()

    if text == BTN_CANCEL:
        WAITING_GENDER.discard(user_id)
        update.message.reply_text(
            "تم الإلغاء.",
            reply_markup=user_main_keyboard(user_id),
        )
        return

    if text not in [BTN_GENDER_MALE, BTN_GENDER_FEMALE]:
        update.message.reply_text(
            "رجاءً اختر من الخيارات:",
            reply_markup=GENDER_KB,
        )
        return

    record = get_user_record(user)
    gender = "male" if text == BTN_GENDER_MALE else "female"
    record["gender"] = gender
    save_data()

    WAITING_GENDER.discard(user_id)
    WAITING_AGE.add(user_id)

    update.message.reply_text(
        "أرسل عمرك (بالسنوات):",
        reply_markup=CANCEL_KB,
    )

def handle_age_input(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    text = update.message.text.strip()

    if text == BTN_CANCEL:
        WAITING_AGE.discard(user_id)
        update.message.reply_text(
            "تم الإلغاء.",
            reply_markup=user_main_keyboard(user_id),
        )
        return

    try:
        age = int(text)
        if age <= 0 or age > 120:
            raise ValueError()
    except ValueError:
        update.message.reply_text(
            "رجاءً أرسل عمرًا صحيحًا:",
            reply_markup=CANCEL_KB,
        )
        return

    record = get_user_record(user)
    record["age"] = age
    save_data()

    WAITING_AGE.discard(user_id)
    WAITING_WEIGHT.add(user_id)

    update.message.reply_text(
        "الآن أرسل وزنك بالكيلوغرام:",
        reply_markup=CANCEL_KB,
    )

def handle_weight_input(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    text = update.message.text.strip()

    if text == BTN_CANCEL:
        WAITING_WEIGHT.discard(user_id)
        update.message.reply_text(
            "تم الإلغاء.",
            reply_markup=user_main_keyboard(user_id),
        )
        return

    try:
        weight = float(text.replace(",", "."))
        if weight <= 20 or weight > 300:
            raise ValueError()
    except ValueError:
        update.message.reply_text(
            "رجاءً أرسل وزنًا صحيحًا:",
            reply_markup=CANCEL_KB,
        )
        return

    record = get_user_record(user)
    record["weight"] = weight

    if record.get("gender") == "male":
        rate = 0.035
    else:
        rate = 0.033

    water_liters = weight * rate
    cups_goal = max(int(round(water_liters * 1000 / 250)), 1)

    record["water_liters"] = round(water_liters, 2)
    record["cups_goal"] = cups_goal
    save_data()

    WAITING_WEIGHT.discard(user_id)

    update.message.reply_text(
        "تم حساب احتياجك اليومي من الماء 💧\n\n"
        f"- تقريبًا: {record['water_liters']} لتر في اليوم.\n"
        f"- ما يعادل: {cups_goal} كوب (250 مل للكوب).\n\n"
        "كل كوب تسجّله يعطيك نقاطًا 🎯",
        reply_markup=water_menu_keyboard(user_id),
    )

def handle_log_cup(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)

    if not record.get("cups_goal"):
        update.message.reply_text(
            "احسب احتياجك من الماء أولًا.",
            reply_markup=water_menu_keyboard(user.id),
        )
        return

    ensure_today_water(record)
    before = record.get("today_cups", 0)
    record["today_cups"] = before + 1

    add_points(user.id, POINTS_PER_WATER_CUP, context)

    cups_goal = record.get("cups_goal")
    after = record["today_cups"]
    if cups_goal and before < cups_goal <= after:
        add_points(user.id, POINTS_WATER_DAILY_BONUS, context)

    save_data()

    status_text = format_water_status_text(record)
    update.message.reply_text(
        f"🥤 تم تسجيل كوب ماء.\n\n{status_text}",
        reply_markup=water_menu_keyboard(user.id),
    )

def handle_add_cups(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    text = (update.message.text or "").strip()

    if not record.get("cups_goal"):
        update.message.reply_text(
            "احسب احتياجك من الماء أولًا.",
            reply_markup=water_menu_keyboard(user.id),
        )
        return

    if text == BTN_WATER_ADD_CUPS:
        update.message.reply_text(
            "أرسل عدد الأكواب:",
            reply_markup=CANCEL_KB,
        )
        return

    try:
        cups = int(text)
        if cups <= 0 or cups > 50:
            raise ValueError()
    except ValueError:
        update.message.reply_text(
            "أرسل عددًا صحيحًا:",
            reply_markup=water_menu_keyboard(user.id),
        )
        return

    ensure_today_water(record)
    before = record.get("today_cups", 0)
    record["today_cups"] = before + cups

    add_points(user.id, cups * POINTS_PER_WATER_CUP, context)

    cups_goal = record.get("cups_goal")
    after = record["today_cups"]
    if cups_goal and before < cups_goal <= after:
        add_points(user.id, POINTS_WATER_DAILY_BONUS, context)

    save_data()

    status_text = format_water_status_text(record)
    update.message.reply_text(
        f"🥤 تم إضافة {cups} كوب.\n\n{status_text}",
        reply_markup=water_menu_keyboard(user.id),
    )

def handle_status(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    text = format_water_status_text(record)
    update.message.reply_text(
        text,
        reply_markup=water_menu_keyboard(user.id),
    )

def handle_reminders_on(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)

    if not record.get("cups_goal"):
        update.message.reply_text(
            "احسب احتياجك أولًا.",
            reply_markup=water_settings_keyboard(user.id),
        )
        return

    record["reminders_on"] = True
    save_data()

    update.message.reply_text(
        "تم تشغيل التذكيرات ⏰",
        reply_markup=water_settings_keyboard(user.id),
    )

def handle_reminders_off(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    record["reminders_on"] = False
    save_data()

    update.message.reply_text(
        "تم إيقاف التذكيرات 📴",
        reply_markup=water_settings_keyboard(user.id),
    )

# =================== قسم ورد القرآن ===================

def open_quran_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    get_user_record(user)
    kb = quran_menu_keyboard(user.id)
    update.message.reply_text(
        "وردي القرآني 📖:\n"
        "• عيّن عدد صفحات اليوم.\n"
        "• سجّل ما قرأته.\n"
        "• شاهد مستوى إنجازك.\n"
        "كل صفحة تزيد نقاطك 🎯",
        reply_markup=kb,
    )

def handle_quran_set_goal(update: Update, context: CallbackContext):
    user_id = update.effective_user.id

    WAITING_QURAN_GOAL.add(user_id)
    WAITING_QURAN_ADD_PAGES.discard(user_id)

    update.message.reply_text(
        "أرسل عدد الصفحات لهذا اليوم:",
        reply_markup=CANCEL_KB,
    )

def handle_quran_goal_input(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    text = (update.message.text or "").strip()

    if text == BTN_CANCEL:
        WAITING_QURAN_GOAL.discard(user_id)
        update.message.reply_text(
            "تم الإلغاء.",
            reply_markup=user_main_keyboard(user_id),
        )
        return

    try:
        pages = int(text)
        if pages <= 0 or pages > 200:
            raise ValueError()
    except ValueError:
        update.message.reply_text(
            "أرسل عددًا صحيحًا:",
            reply_markup=CANCEL_KB,
        )
        return

    record = get_user_record(user)
    ensure_today_quran(record)
    record["quran_pages_goal"] = pages
    save_data()

    WAITING_QURAN_GOAL.discard(user_id)

    update.message.reply_text(
        f"تم تعيين ورد اليوم: {pages} صفحة.",
        reply_markup=quran_menu_keyboard(user_id),
    )

def handle_quran_add_pages_start(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)

    if not record.get("quran_pages_goal"):
        update.message.reply_text(
            "عيّن الورد أولًا.",
            reply_markup=quran_menu_keyboard(user.id),
        )
        return

    WAITING_QURAN_ADD_PAGES.add(user.id)
    update.message.reply_text(
        "أرسل عدد الصفحات التي قرأتها:",
        reply_markup=CANCEL_KB,
    )

def handle_quran_add_pages_input(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    text = (update.message.text or "").strip()

    if text == BTN_CANCEL:
        WAITING_QURAN_ADD_PAGES.discard(user_id)
        update.message.reply_text(
            "تم الإلغاء.",
            reply_markup=user_main_keyboard(user_id),
        )
        return

    try:
        pages = int(text)
        if pages <= 0 or pages > 100:
            raise ValueError()
    except ValueError:
        update.message.reply_text(
            "أرسل عددًا صحيحًا:",
            reply_markup=CANCEL_KB,
        )
        return

    record = get_user_record(user)
    ensure_today_quran(record)

    before = record.get("quran_pages_today", 0)
    record["quran_pages_today"] = before + pages

    add_points(user_id, pages * POINTS_PER_QURAN_PAGE, context)

    goal = record.get("quran_pages_goal")
    after = record["quran_pages_today"]
    if goal and before < goal <= after:
        add_points(user_id, POINTS_QURAN_DAILY_BONUS, context)

    save_data()

    WAITING_QURAN_ADD_PAGES.discard(user_id)

    status_text = format_quran_status_text(record)
    update.message.reply_text(
        f"تم إضافة {pages} صفحة.\n\n{status_text}",
        reply_markup=quran_menu_keyboard(user_id),
    )

def handle_quran_status(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    text = format_quran_status_text(record)
    update.message.reply_text(
        text,
        reply_markup=quran_menu_keyboard(user.id),
    )

def handle_quran_reset_day(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)

    ensure_today_quran(record)
    record["quran_pages_today"] = 0
    save_data()

    update.message.reply_text(
        "تم إعادة تعيين الورد.",
        reply_markup=quran_menu_keyboard(user.id),
    )

# =================== قسم الأذكار ===================

def open_adhkar_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    get_user_record(user)
    kb = adhkar_menu_keyboard(user.id)
    update.message.reply_text(
        "أذكاري 🤲:\n"
        "• أذكار الصباح.\n"
        "• أذكار المساء.\n"
        "• أذكار عامة.",
        reply_markup=kb,
    )

def send_morning_adhkar(update: Update, context: CallbackContext):
    user = update.effective_user
    increment_adhkar_count(user.id, 1)
    kb = adhkar_menu_keyboard(user.id)
    update.message.reply_text(
        ADHKAR_MORNING_TEXT,
        reply_markup=kb,
    )

def send_evening_adhkar(update: Update, context: CallbackContext):
    user = update.effective_user
    increment_adhkar_count(user.id, 1)
    kb = adhkar_menu_keyboard(user.id)
    update.message.reply_text(
        ADHKAR_EVENING_TEXT,
        reply_markup=kb,
    )

def send_general_adhkar(update: Update, context: CallbackContext):
    user = update.effective_user
    increment_adhkar_count(user.id, 1)
    kb = adhkar_menu_keyboard(user.id)
    update.message.reply_text(
        ADHKAR_GENERAL_TEXT,
        reply_markup=kb,
    )

# =================== قسم السبحة ===================

def open_tasbih_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    ACTIVE_TASBIH.pop(user.id, None)
    WAITING_TASBIH.discard(user.id)

    kb = build_tasbih_menu(is_admin(user.id))
    update.message.reply_text(
        "اختر الذكر الذي تريد التسبيح به:",
        reply_markup=kb,
    )

def start_tasbih_for_choice(update: Update, context: CallbackContext, choice_text: str):
    user = update.effective_user
    user_id = user.id

    for dhikr, count in TASBIH_ITEMS:
        label = f"{dhikr} ({count})"
        if choice_text == label:
            ACTIVE_TASBIH[user_id] = {
                "text": dhikr,
                "target": count,
                "current": 0,
            }
            WAITING_TASBIH.add(user_id)
            update.message.reply_text(
                f"بدأنا التسبيح:\n"
                f"الذكر: {dhikr}\n"
                f"العدد: {count} مرة.\n\n"
                "اضغط «تسبيحة ✅».",
                reply_markup=tasbih_run_keyboard(user_id),
            )
            return

    update.message.reply_text(
        "اختر من القائمة."
    )

def handle_tasbih_tick(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id

    if user_id not in ACTIVE_TASBIH:
        update.message.reply_text(
            "لم تبدأ التسبيح بعد."
        )
        return

    session = ACTIVE_TASBIH[user_id]
    session["current"] += 1

    current = session["current"]
    target = session["target"]

    if current < target:
        update.message.reply_text(
            f"✅ التسبيحة رقم {current} من {target}",
            reply_markup=tasbih_run_keyboard(user_id),
        )
    else:
        points_earned = tasbih_points_for_session(target)
        increment_tasbih_total(user_id, target)
        add_points(user_id, points_earned, context)

        del ACTIVE_TASBIH[user_id]
        WAITING_TASBIH.discard(user_id)

        update.message.reply_text(
            f"🎉 أتممت التسبيح!\n\n"
            f"الذكر: {session['text']}\n"
            f"عدد التسبيحات: {target}\n"
            f"النقاط المكتسبة: {points_earned} 🎯",
            reply_markup=user_main_keyboard(user_id),
        )

def handle_tasbih_end(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id

    if user_id in ACTIVE_TASBIH:
        session = ACTIVE_TASBIH[user_id]
        completed = session["current"]
        total = session["target"]

        if completed > 0:
            increment_tasbih_total(user_id, completed)
            points_earned = tasbih_points_for_session(completed)
            add_points(user_id, points_earned, context)

        del ACTIVE_TASBIH[user_id]

    WAITING_TASBIH.discard(user_id)

    update.message.reply_text(
        "انتهى التسبيح.",
        reply_markup=user_main_keyboard(user_id),
    )

# =================== معالج رسائل عام ===================

def handle_message(update: Update, context: CallbackContext):
    """معالج رسائل النص العام."""
    user = update.effective_user
    user_id = user.id
    text = (update.message.text or "").strip()

    # المنافسات
    if text == BTN_MY_PROFILE:
        handle_my_profile(update, context)
        return
    elif text == BTN_TOP10:
        handle_top_10(update, context)
        return
    elif text == BTN_TOP100:
        handle_top_100(update, context)
        return

    # منبّه الماء
    elif text == BTN_WATER_MAIN:
        open_water_menu(update, context)
        return
    elif text == BTN_WATER_LOG:
        handle_log_cup(update, context)
        return
    elif text == BTN_WATER_ADD_CUPS:
        handle_add_cups(update, context)
        return
    elif text == BTN_WATER_STATUS:
        handle_status(update, context)
        return
    elif text == BTN_WATER_SETTINGS:
        open_water_settings(update, context)
        return
    elif text == BTN_WATER_NEED:
        handle_water_need_start(update, context)
        return
    elif text == BTN_WATER_REM_ON:
        handle_reminders_on(update, context)
        return
    elif text == BTN_WATER_REM_OFF:
        handle_reminders_off(update, context)
        return
    elif text == BTN_WATER_BACK_MENU:
        open_water_menu(update, context)
        return

    # ورد القرآن
    elif text == BTN_QURAN_MAIN:
        open_quran_menu(update, context)
        return
    elif text == BTN_QURAN_SET_GOAL:
        handle_quran_set_goal(update, context)
        return
    elif text == BTN_QURAN_ADD_PAGES:
        handle_quran_add_pages_start(update, context)
        return
    elif text == BTN_QURAN_STATUS:
        handle_quran_status(update, context)
        return
    elif text == BTN_QURAN_RESET_DAY:
        handle_quran_reset_day(update, context)
        return

    # الأذكار
    elif text == BTN_ADHKAR_MAIN:
        open_adhkar_menu(update, context)
        return
    elif text == BTN_ADHKAR_MORNING:
        send_morning_adhkar(update, context)
        return
    elif text == BTN_ADHKAR_EVENING:
        send_evening_adhkar(update, context)
        return
    elif text == BTN_ADHKAR_GENERAL:
        send_general_adhkar(update, context)
        return

    # السبحة
    elif text == BTN_TASBIH_MAIN:
        open_tasbih_menu(update, context)
        return
    elif text == BTN_TASBIH_TICK:
        handle_tasbih_tick(update, context)
        return
    elif text == BTN_TASBIH_END:
        handle_tasbih_end(update, context)
        return
    elif user_id in WAITING_TASBIH:
        for dhikr, count in TASBIH_ITEMS:
            if text == f"{dhikr} ({count})":
                start_tasbih_for_choice(update, context, text)
                return

    # حالات الانتظار - الماء
    elif user_id in WAITING_GENDER:
        handle_gender_input(update, context)
        return
    elif user_id in WAITING_AGE:
        handle_age_input(update, context)
        return
    elif user_id in WAITING_WEIGHT:
        handle_weight_input(update, context)
        return

    # حالات الانتظار - القرآن
    elif user_id in WAITING_QURAN_GOAL:
        handle_quran_goal_input(update, context)
        return
    elif user_id in WAITING_QURAN_ADD_PAGES:
        handle_quran_add_pages_input(update, context)
        return

    # أزرار أخرى
    elif text == BTN_COMP_MAIN:
        update.message.reply_text(
            "المنافسات و المجتمع 🏅",
            reply_markup=COMP_MENU_KB,
        )
        return
    elif text == BTN_BACK_MAIN:
        update.message.reply_text(
            "رجعنا للقائمة الرئيسية ⬅️",
            reply_markup=user_main_keyboard(user_id),
        )
        return
    elif text == BTN_CANCEL:
        WAITING_GENDER.discard(user_id)
        WAITING_AGE.discard(user_id)
        WAITING_WEIGHT.discard(user_id)
        WAITING_QURAN_GOAL.discard(user_id)
        WAITING_QURAN_ADD_PAGES.discard(user_id)
        WAITING_TASBIH.discard(user_id)
        ACTIVE_TASBIH.pop(user_id, None)
        update.message.reply_text(
            "تم الإلغاء.",
            reply_markup=user_main_keyboard(user_id),
        )
        return

    # إذا لم يطابق أي شيء
    update.message.reply_text(
        "لم أفهم الأمر. اختر من القائمة.",
        reply_markup=user_main_keyboard(user_id),
    )

# =================== إعداد البوت الرئيسي ===================

def main():
    updater = Updater(BOT_TOKEN)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start_command))
    dp.add_handler(CommandHandler("help", help_command))
    
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))

    # تشغيل Flask في خيط منفصل
    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()

    # تشغيل البوت
    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
