import os
import json
import logging
import re
import random
from datetime import datetime, timezone, time, timedelta
from threading import Thread
from typing import List, Dict

import pytz
from flask import Flask
from telegram import (
    Update,
    User, # تم إضافة User هنا
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Updater,
    MessageHandler,
    Filters,
    CallbackContext,
    CommandHandler,
    CallbackQueryHandler,
)

# =================== إعدادات أساسية ===================

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATA_FILE = "suqya_users.json"

# معرف الأدمن (أنت)
ADMIN_ID = 931350292  # غيّره لو احتجت مستقبلاً

# معرف المشرفة (الأخوات)
SUPERVISOR_ID = 1745150161  # المشرفة

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

# =================== إعدادات افتراضية للجرعة التحفيزية (على مستوى البوت) ===================

DEFAULT_MOTIVATION_HOURS_UTC = [6, 9, 12, 15, 18, 21]

DEFAULT_MOTIVATION_MESSAGES = [
    "🍃 تذكّر: قليلٌ دائم خيرٌ من كثير منقطع، خطوة اليوم تقرّبك من نسختك الأفضل 🤍",
    "💧 جرعة ماء + آية من القرآن + ذكر بسيط = راحة قلب يوم كامل بإذن الله.",
    "🤍 مهما كان يومك مزدحمًا، قلبك يستحق لحظات هدوء مع ذكر الله.",
    "📖 لو شعرت بثقل، افتح المصحف صفحة واحدة فقط… ستشعر أن همّك خفّ ولو قليلًا.",
    "💫 لا تستصغر كوب ماء تشربه بنية حفظ الصحة، ولا صفحة قرآن تقرؤها بنية القرب من الله.",
    "🕊 قل: الحمد لله الآن… أحيانًا شكرٌ صادق يغيّر مزاج يومك كله.",
    "🌿 استعن بالله ولا تعجز، كل محاولة للالتزام خير، حتى لو تعثّرت بعدها.",
]

GLOBAL_KEY = "_global_config"

MOTIVATION_HOURS_UTC = []
MOTIVATION_MESSAGES = []

CURRENT_MOTIVATION_JOBS = []


def get_global_config():
    """
    يرجع (أو ينشئ) الإعدادات العامة للبوت (مثل أوقات الجرعة التحفيزية ورسائلها).
    تُخزَّن تحت مفتاح خاص في نفس ملف JSON.
    """
    cfg = data.get(GLOBAL_KEY)
    changed = False

    if not cfg or not isinstance(cfg, dict):
        cfg = {}
        changed = True

    if "motivation_hours" not in cfg or not cfg.get("motivation_hours"):
        cfg["motivation_hours"] = DEFAULT_MOTIVATION_HOURS_UTC.copy()
        changed = True

    if "motivation_messages" not in cfg or not cfg.get("motivation_messages"):
        cfg["motivation_messages"] = DEFAULT_MOTIVATION_MESSAGES.copy()
        changed = True

    if "benefits" not in cfg or not isinstance(cfg.get("benefits"), list):
        cfg["benefits"] = []
        changed = True

    data[GLOBAL_KEY] = cfg
    if changed:
        save_data()
    return cfg


_global_cfg = get_global_config()
MOTIVATION_HOURS_UTC = _global_cfg["motivation_hours"]
MOTIVATION_MESSAGES = _global_cfg["motivation_messages"]

# =================== سجلات المستخدمين ===================


def get_next_benefit_id():
    """يرجع المعرف الفريد التالي للفائدة."""
    cfg = get_global_config()
    benefits = cfg.get("benefits", [])
    if not benefits:
        return 1
    # البحث عن أكبر ID موجود
    max_id = max(b.get("id", 0) for b in benefits)
    return max_id + 1


def get_benefits():
    """يرجع قائمة الفوائد من الإعدادات العامة."""
    cfg = get_global_config()
    return cfg.get("benefits", [])


def save_benefits(benefits_list):
    """يحفظ قائمة الفوائد المحدثة في الإعدادات العامة."""
    cfg = get_global_config()
    cfg["benefits"] = benefits_list
    data[GLOBAL_KEY] = cfg
    save_data()


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
            # إشعار المستخدم الجديد
            "is_new_user": True, # علامة مؤقتة للإشعار

            # حالة الحظر
            "is_banned": False,
            "banned_by": None,
            "banned_at": None,
            "ban_reason": None,
            # إعدادات الماء
            "gender": None,  # نستخدمها أيضًا في الدعم
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
            # رسائل إلى نفسي
            "letters_to_self": [],
            # نظام النقاط والمستويات والميداليات
            "points": 0,
            "level": 0,
            "medals": [],
            "best_rank": None,
            # الاستمرارية اليومية (ماء + قرآن)
            "daily_full_streak": 0,
            "last_full_day": None,
            # الجرعة التحفيزية (الإشعارات)
            "motivation_on": True,
        }
    else:
        record = data[user_id]
        record["first_name"] = user.first_name
        record["username"] = user.username
        record["last_active"] = now_iso

        # ضمان الحقول
        record.setdefault("is_banned", False)
        record.setdefault("banned_by", None)
        record.setdefault("banned_at", None)
        record.setdefault("ban_reason", None)
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
        record.setdefault("letters_to_self", [])
        record.setdefault("points", 0)
        record.setdefault("level", 0)
        record.setdefault("medals", [])
        record.setdefault("best_rank", None)
        record.setdefault("daily_full_streak", 0)
        record.setdefault("last_full_day", None)
        record.setdefault("motivation_on", True)
        record.setdefault("is_new_user", False) # ضمان وجود الحقل

        record.setdefault("is_new_user", False) # ضمان وجود الحقل


        medals = record.get("medals", [])
        if medals:
            new_medals = []
            for m in medals:
                if m == "ميدالية الاستمرار 💫":
                    new_medals.append("ميدالية الاستمرار 🎓")
                elif m == "ميدالية بطل سُقيا الكوثر 👑":
                    new_medals.append("ميدالية بطل سُقيا الكوثر 🏆")
                else:
                    new_medals.append(m)
            record["medals"] = new_medals

    save_data()
    return data[user_id]


def update_user_record(user_id: int, **kwargs):
    uid = str(user_id)
    if uid not in data:
        return
    data[uid].update(kwargs)
    data[uid]["last_active"] = datetime.now(timezone.utc).isoformat()
    save_data()


def add_points(user_id: int, points: int):
    """يضيف نقاطًا للمستخدم ويحفظ البيانات."""
    uid = str(user_id)
    if uid not in data or uid == GLOBAL_KEY:
        return
    
    record = data[uid]
    current_points = record.get("points", 0)
    record["points"] = current_points + points
    save_data()


def get_all_user_ids():
    return [int(uid) for uid in data.keys() if uid != GLOBAL_KEY]


def get_active_user_ids():
    """يرجع قائمة المستخدمين النشطين (غير المحظورين)"""
    return [int(uid) for uid, rec in data.items() 
            if uid != GLOBAL_KEY and not rec.get("is_banned", False)]


def get_banned_user_ids():
    """يرجع قائمة المستخدمين المحظورين"""
    return [int(uid) for uid, rec in data.items() 
            if uid != GLOBAL_KEY and rec.get("is_banned", False)]


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
ACTIVE_TASBIH = {}      # user_id -> { "text": str, "target": int, "current": int }

# مذكّرات قلبي
WAITING_MEMO_MENU = set()
WAITING_MEMO_ADD = set()
WAITING_MEMO_EDIT_SELECT = set()
WAITING_MEMO_EDIT_TEXT = set()
WAITING_MEMO_DELETE_SELECT = set()
MEMO_EDIT_INDEX = {}

# رسائل إلى نفسي
WAITING_LETTER_MENU = set()
WAITING_LETTER_ADD = set()
WAITING_LETTER_ADD_CONTENT = set()
WAITING_LETTER_REMINDER_OPTION = set()
WAITING_LETTER_CUSTOM_DATE = set()
WAITING_LETTER_DELETE_SELECT = set()
LETTER_CURRENT_DATA = {}  # user_id -> { "content": str, "reminder_date": str }

# دعم / إدارة
WAITING_SUPPORT_GENDER = set()
WAITING_SUPPORT = set()
WAITING_BROADCAST = set()

# فوائد ونصائح
WAITING_BENEFIT_TEXT = set()
WAITING_BENEFIT_EDIT_TEXT = set()
WAITING_BENEFIT_DELETE_CONFIRM = set()
BENEFIT_EDIT_ID = {} # user_id -> benefit_id

# إدارة الجرعة التحفيزية (من لوحة التحكم)
WAITING_MOTIVATION_ADD = set()
WAITING_MOTIVATION_DELETE = set()
WAITING_MOTIVATION_TIMES = set()

# نظام الحظر
WAITING_BAN_USER = set()
WAITING_UNBAN_USER = set()
WAITING_BAN_REASON = set()
BAN_TARGET_ID = {}  # user_id -> target_user_id

# =================== الأزرار ===================

# رئيسية
BTN_ADHKAR_MAIN = "أذكاري 🤲"
BTN_QURAN_MAIN = "وردي القرآني 📖"
BTN_TASBIH_MAIN = "السبحة 📿"
BTN_MEMOS_MAIN = "مذكّرات قلبي 🩵"
BTN_WATER_MAIN = "منبّه الماء 💧"
BTN_STATS = "احصائياتي 📊"
BTN_LETTER_MAIN = "رسالة إلى نفسي 💌"

BTN_SUPPORT = "تواصل مع الدعم ✉️"
BTN_NOTIFICATIONS_MAIN = "الاشعارات 🔔"

BTN_CANCEL = "إلغاء ❌"
BTN_BACK_MAIN = "رجوع للقائمة الرئيسية ⬅️"

# المنافسات و المجتمع
BTN_COMP_MAIN = "المنافسات و المجتمع 🏅"
BTN_MY_PROFILE = "ملفي التنافسي 🎯"
BTN_TOP10 = "أفضل 10 🏅"
BTN_TOP100 = "أفضل 100 🏆"

# فوائد و نصائح
BTN_BENEFITS_MAIN = "مجتمع الفوائد و النصائح 💡"
BTN_BENEFIT_ADD = "✍️ أضف فائدة / نصيحة"
BTN_BENEFIT_VIEW = "📖 استعراض الفوائد"
BTN_BENEFIT_TOP10 = "🏆 أفضل 10 فوائد"
BTN_MY_BENEFITS = "فوائدي (تعديل/حذف) 📝"
BTN_BENEFIT_EDIT = "تعديل الفائدة ✏️"
BTN_BENEFIT_DELETE = "حذف الفائدة 🗑️"

# لوحة المدير
BTN_ADMIN_PANEL = "لوحة التحكم 🛠"
BTN_ADMIN_USERS_COUNT = "عدد المستخدمين 👥"
BTN_ADMIN_USERS_LIST = "قائمة المستخدمين 📄"
BTN_ADMIN_BROADCAST = "رسالة جماعية 📢"
BTN_ADMIN_RANKINGS = "ترتيب المنافسة (تفصيلي) 📊"
BTN_ADMIN_BAN_USER = "حظر مستخدم ⚠️"
BTN_ADMIN_UNBAN_USER = "فك حظر مستخدم ✅"
BTN_ADMIN_BANNED_LIST = "قائمة المحظورين 🚫"

# إعدادات الجرعة التحفيزية (داخل لوحة التحكم)
BTN_ADMIN_MOTIVATION_MENU = "إعدادات الجرعة التحفيزية 💡"
BTN_ADMIN_MOTIVATION_LIST = "عرض رسائل الجرعة 📜"
BTN_ADMIN_MOTIVATION_ADD = "إضافة رسالة تحفيزية ➕"
BTN_ADMIN_MOTIVATION_DELETE = "حذف رسالة تحفيزية 🗑"
BTN_ADMIN_MOTIVATION_TIMES = "تعديل أوقات الجرعة ⏰"

# جرعة تحفيزية للمستخدم
BTN_MOTIVATION_ON = "تشغيل الجرعة التحفيزية ✨"
BTN_MOTIVATION_OFF = "إيقاف الجرعة التحفيزية 😴"

# رسالة إلى نفسي
BTN_LETTER_ADD = "✍️ كتابة رسالة جديدة"
BTN_LETTER_VIEW = "📋 عرض الرسائل"
BTN_LETTER_DELETE = "🗑 حذف رسالة"
BTN_LETTER_BACK = "رجوع ⬅️"

# خيارات التذكير لرسالة إلى نفسي
BTN_REMINDER_WEEK = "بعد أسبوع 📅"
BTN_REMINDER_MONTH = "بعد شهر 🌙"
BTN_REMINDER_2MONTHS = "بعد شهرين 📆"
BTN_REMINDER_CUSTOM = "تاريخ مخصص 🗓️"
BTN_REMINDER_NONE = "بدون تذكير ❌"

# ===== تعديل القوائم الرئيسية حسب طلبك =====

MAIN_KEYBOARD_USER = ReplyKeyboardMarkup(
    [
        # السطر الأول: أذكاري بجانب وردي القرآني
        [KeyboardButton(BTN_ADHKAR_MAIN), KeyboardButton(BTN_QURAN_MAIN)],
        # السطر الثاني: السبحة بجانب منبه الماء
        [KeyboardButton(BTN_TASBIH_MAIN), KeyboardButton(BTN_WATER_MAIN)],
        # السطر الثالث: مذكرات قلبي بجانب رسالة إلى نفسي
        [KeyboardButton(BTN_MEMOS_MAIN), KeyboardButton(BTN_LETTER_MAIN)],
        # السطر الرابع: احصائياتي بجانب المنافسات و المجتمع
        [KeyboardButton(BTN_STATS), KeyboardButton(BTN_COMP_MAIN)],
        # السطر الخامس: فوائد ونصائح
        [KeyboardButton(BTN_BENEFITS_MAIN)],
        # السطر السادس: الاشعارات على اليسار، التواصل مع الدعم على اليمين
        [KeyboardButton(BTN_NOTIFICATIONS_MAIN), KeyboardButton(BTN_SUPPORT)],
    ],
    resize_keyboard=True,
)

MAIN_KEYBOARD_ADMIN = ReplyKeyboardMarkup(
    [
        # السطر الأول: أذكاري بجانب وردي القرآني
        [KeyboardButton(BTN_ADHKAR_MAIN), KeyboardButton(BTN_QURAN_MAIN)],
        # السطر الثاني: السبحة بجانب منبه الماء
        [KeyboardButton(BTN_TASBIH_MAIN), KeyboardButton(BTN_WATER_MAIN)],
        # السطر الثالث: مذكرات قلبي بجانب رسالة إلى نفسي
        [KeyboardButton(BTN_MEMOS_MAIN), KeyboardButton(BTN_LETTER_MAIN)],
        # السطر الرابع: احصائياتي بجانب المنافسات و المجتمع
        [KeyboardButton(BTN_STATS), KeyboardButton(BTN_COMP_MAIN)],
        # السطر الخامس: فوائد ونصائح
        [KeyboardButton(BTN_BENEFITS_MAIN)],
        # السطر السادس: الاشعارات على اليسار، التواصل مع الدعم على اليمين
        [KeyboardButton(BTN_NOTIFICATIONS_MAIN), KeyboardButton(BTN_SUPPORT)],
        # السطر السابع: لوحة التحكم (فقط للمدير)
        [KeyboardButton(BTN_ADMIN_PANEL)],
    ],
    resize_keyboard=True,
)

MAIN_KEYBOARD_SUPERVISOR = ReplyKeyboardMarkup(
    [
        # السطر الأول: أذكاري بجانب وردي القرآني
        [KeyboardButton(BTN_ADHKAR_MAIN), KeyboardButton(BTN_QURAN_MAIN)],
        # السطر الثاني: السبحة بجانب منبه الماء
        [KeyboardButton(BTN_TASBIH_MAIN), KeyboardButton(BTN_WATER_MAIN)],
        # السطر الثالث: مذكرات قلبي بجانب رسالة إلى نفسي
        [KeyboardButton(BTN_MEMOS_MAIN), KeyboardButton(BTN_LETTER_MAIN)],
        # السطر الرابع: احصائياتي بجانب المنافسات و المجتمع
        [KeyboardButton(BTN_STATS), KeyboardButton(BTN_COMP_MAIN)],
        # السطر الخامس: فوائد ونصائح
        [KeyboardButton(BTN_BENEFITS_MAIN)],
        # السطر السادس: الاشعارات على اليسار، التواصل مع الدعم على اليمين
        [KeyboardButton(BTN_NOTIFICATIONS_MAIN), KeyboardButton(BTN_SUPPORT)],
        # السطر السابع: لوحة التحكم (للمشرفة)
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

WATER_SETTINGS_KB_ADMIN = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_WATER_NEED)],
        [KeyboardButton(BTN_WATER_REM_ON), KeyboardButton(BTN_WATER_REM_OFF)],
        [KeyboardButton(BTN_WATER_BACK_MENU)],
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

# ---- فوائد و نصائح ----
BENEFITS_MENU_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_BENEFIT_ADD)],
        [KeyboardButton(BTN_BENEFIT_VIEW)],
        [KeyboardButton(BTN_BENEFIT_TOP10)],
        [KeyboardButton(BTN_MY_BENEFITS)],
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

# ---- رسالة إلى نفسي ----
def build_letters_menu_kb(is_admin_flag: bool):
    rows = [
        [KeyboardButton(BTN_LETTER_ADD)],
        [KeyboardButton(BTN_LETTER_VIEW), KeyboardButton(BTN_LETTER_DELETE)],
        [KeyboardButton(BTN_LETTER_BACK)],
    ]
    if is_admin_flag:
        rows.append([KeyboardButton(BTN_ADMIN_PANEL)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


REMINDER_OPTIONS_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_REMINDER_WEEK), KeyboardButton(BTN_REMINDER_MONTH)],
        [KeyboardButton(BTN_REMINDER_2MONTHS), KeyboardButton(BTN_REMINDER_CUSTOM)],
        [KeyboardButton(BTN_REMINDER_NONE)],
        [KeyboardButton(BTN_CANCEL)],
    ],
    resize_keyboard=True,
)

# ---- لوحة التحكم ----
ADMIN_PANEL_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_ADMIN_USERS_COUNT), KeyboardButton(BTN_ADMIN_USERS_LIST)],
        [KeyboardButton(BTN_ADMIN_BROADCAST), KeyboardButton(BTN_ADMIN_RANKINGS)],
        [KeyboardButton(BTN_ADMIN_BAN_USER), KeyboardButton(BTN_ADMIN_UNBAN_USER)],
        [KeyboardButton(BTN_ADMIN_BANNED_LIST)],
        [KeyboardButton(BTN_ADMIN_MOTIVATION_MENU)],
        [KeyboardButton(BTN_BACK_MAIN)],
    ],
    resize_keyboard=True,
)

SUPERVISOR_PANEL_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_ADMIN_USERS_COUNT)],
        [KeyboardButton(BTN_ADMIN_BROADCAST)],
        [KeyboardButton(BTN_ADMIN_BAN_USER), KeyboardButton(BTN_ADMIN_UNBAN_USER)],
        [KeyboardButton(BTN_ADMIN_BANNED_LIST)],
        [KeyboardButton(BTN_ADMIN_MOTIVATION_MENU)],
        [KeyboardButton(BTN_BACK_MAIN)],
    ],
    resize_keyboard=True,
)

ADMIN_MOTIVATION_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_ADMIN_MOTIVATION_LIST)],
        [KeyboardButton(BTN_ADMIN_MOTIVATION_ADD)],
        [KeyboardButton(BTN_ADMIN_MOTIVATION_DELETE)],
        [KeyboardButton(BTN_ADMIN_MOTIVATION_TIMES)],
        [KeyboardButton(BTN_BACK_MAIN), KeyboardButton(BTN_ADMIN_PANEL)],
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

# ---- الاشعارات / الجرعة التحفيزية (للمستخدم) ----
def notifications_menu_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    if is_admin(user_id):
        return ReplyKeyboardMarkup(
            [
                [KeyboardButton(BTN_MOTIVATION_ON)],
                [KeyboardButton(BTN_MOTIVATION_OFF)],
                [KeyboardButton(BTN_BACK_MAIN), KeyboardButton(BTN_ADMIN_PANEL)],
            ],
            resize_keyboard=True,
        )
    else:
        return ReplyKeyboardMarkup(
            [
                [KeyboardButton(BTN_MOTIVATION_ON)],
                [KeyboardButton(BTN_MOTIVATION_OFF)],
                [KeyboardButton(BTN_BACK_MAIN)],
            ],
            resize_keyboard=True,
        )

# =================== نظام النقاط ===================

POINTS_PER_WATER_CUP = 1
POINTS_WATER_DAILY_BONUS = 20

POINTS_PER_QURAN_PAGE = 3
POINTS_QURAN_DAILY_BONUS = 30
POINTS_PER_LETTER = 5


def tasbih_points_for_session(target_count: int) -> int:
    return max(target_count // 10, 1)

# =================== دوال مساعدة عامة ===================


def user_main_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    if is_admin(user_id):
        return MAIN_KEYBOARD_ADMIN
    if is_supervisor(user_id):
        return MAIN_KEYBOARD_SUPERVISOR
    return MAIN_KEYBOARD_USER


def admin_panel_keyboard_for(user_id: int) -> ReplyKeyboardMarkup:
    if is_admin(user_id):
        return ADMIN_PANEL_KB
    if is_supervisor(user_id):
        return SUPERVISOR_PANEL_KB
    return user_main_keyboard(user_id)


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
    today_str = datetime.now(timezone.utc).date().isoformat()
    if record.get("today_date") != today_str:
        record["today_date"] = today_str
        record["today_cups"] = 0
        save_data()


def ensure_today_quran(record):
    today_str = datetime.now(timezone.utc).date().isoformat()
    if record.get("quran_today_date") != today_str:
        record["quran_today_date"] = today_str
        record["quran_pages_today"] = 0
        save_data()


def format_water_status_text(record):
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
        f"- الأكواب التي شربتها: {today_cups} من {cups_goal} كوب.\n"
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
            "اقرأ على مهل مع تدبّر، فالمقصود صلاح القلب قبل كثرة الصفحات 🤍."
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

# =================== نظام النقاط / المستويات / الميداليات ===================


def get_users_sorted_by_points():
    return sorted(
        [r for k, r in data.items() if k != GLOBAL_KEY],
        key=lambda r: r.get("points", 0),
        reverse=True,
    )


def check_rank_improvement(user_id: int, record: dict, context: CallbackContext = None):
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


def update_level_and_medals(user_id: int, record: dict, context: CallbackContext = None):
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
        (10, "ميدالية بطل سُقيا الكوثر 🏆"),
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


def check_daily_full_activity(user_id: int, record: dict, context: CallbackContext = None):
    ensure_today_water(record)
    ensure_today_quran(record)

    cups_goal = record.get("cups_goal")
    q_goal = record.get("quran_pages_goal")
    if not cups_goal or not q_goal:
        return

    today_cups = record.get("today_cups", 0)
    q_today = record.get("quran_pages_today", 0)

    if today_cups < cups_goal or q_today < q_goal:
        return

    today_date = datetime.now(timezone.utc).date()
    today_str = today_date.isoformat()

    medals = record.get("medals", []) or []
    streak = record.get("daily_full_streak", 0) or 0
    last_full_day = record.get("last_full_day")

    got_new_daily_medal = False
    got_new_streak_medal = False

    if "ميدالية النشاط اليومي ⚡" not in medals:
        medals.append("ميدالية النشاط اليومي ⚡")
        got_new_daily_medal = True

    if last_full_day == today_str:
        pass
    elif last_full_day:
        try:
            y, m, d = map(int, last_full_day.split("-"))
            last_date = datetime(y, m, d, tzinfo=timezone.utc).date()
            if (today_date - last_date).days == 1:
                streak += 1
            else:
                streak = 1
        except Exception:
            streak = 1
    else:
        streak = 1

    record["daily_full_streak"] = streak
    record["last_full_day"] = today_str

    if streak >= 7 and "ميدالية الاستمرارية 📅" not in medals:
        medals.append("ميدالية الاستمرارية 📅")
        got_new_streak_medal = True

    record["medals"] = medals
    save_data()

    if context is not None:
        try:
            if got_new_daily_medal:
                context.bot.send_message(
                    chat_id=user_id,
                    text=(
                        "⚡ مبروك! أنجزت هدف الماء وهدف القرآن في نفس اليوم لأول مرة.\n"
                        "هذه *ميدالية النشاط اليومي*، بداية جميلة لاستمرار أجمل 🤍"
                    ),
                    parse_mode="Markdown",
                )
            if got_new_streak_medal:
                context.bot.send_message(
                    chat_id=user_id,
                    text=(
                        "📅 ما شاء الله! حافظت على نشاطك اليومي (ماء + قرآن) لمدة ٧ أيام متتالية.\n"
                        "حصلت على *ميدالية الاستمرارية* 🏆\n"
                        "استمر، فالقليل الدائم أحبّ إلى الله من الكثير المنقطع 🤍"
                    ),
                    parse_mode="Markdown",
                )
        except Exception as e:
            logger.error(f"Error sending daily activity medals messages to {user_id}: {e}")


def add_points(user_id: int, amount: int, context: CallbackContext = None, reason: str = ""):
    if amount <= 0:
        return

    uid = str(user_id)
    if uid not in data:
        return

    record = data[uid]
    record["points"] = record.get("points", 0) + amount
    update_level_and_medals(user_id, record, context)

# =================== أذكار ثابتة ===================

ADHKAR_MORNING_TEXT = (
    "أذكار الصباح (من بعد الفجر حتى ارتفاع الشمس) 🌅:\n\n"
    "1⃣ آية الكرسي: «اللّه لا إله إلا هو الحيّ القيّوم...» مرة واحدة.\n"
    "2⃣ قل هو الله أحد، قل أعوذ برب الفلق، قل أعوذ برب الناس: ثلاث مرات.\n"
    "3⃣ «أصبحنا وأصبح الملك لله، والحمد لله، لا إله إلا الله وحده لا شريك له، "
    "له الملك وله الحمد وهو على كل شيء قدير».\n"
    "4⃣ «اللهم ما أصبح بي من نعمة أو بأحد من خلقك فمنك وحدك لا شريك لك، لك الحمد ولك الشكر».\n"
    "5⃣ «اللهم إني أصبحت أشهدك وأشهد حملة عرشك وملائكتك وجميع خلقك، "
    "أنك أنت الله لا إله إلا أنت وحدك لا شريك لك، وأن محمدًا عبدك ورسولك» أربع مرات.\n"
    "6⃣ «حسبي الله لا إله إلا هو عليه توكلت وهو رب العرش العظيم» سبع مرات.\n"
    "7⃣ «اللهم صل وسلم على سيدنا محمد» عددًا كثيرًا.\n\n"
    "للتسبيح بعدد معيّن (مثل 33 أو 100) يمكنك استخدام زر «السبحة 📿»."
)

ADHKAR_EVENING_TEXT = (
    "أذكار المساء (من بعد العصر حتى الليل) 🌙:\n\n"
    "1⃣ آية الكرسي مرة واحدة.\n"
    "2⃣ قل هو الله أحد، قل أعوذ برب الفلق، قل أعوذ برب الناس: ثلاث مرات.\n"
    "3⃣ «أمسينا وأمسى الملك لله، والحمد لله، لا إله إلا الله وحده لا شريك له، "
    "له الملك وله الحمد وهو على كل شيء قدير».\n"
    "4⃣ «اللهم ما أمسى بي من نعمة أو بأحد من خلقك فمنك وحدك لا شريك لك، لك الحمد ولك الشكر».\n"
    "5⃣ «اللهم إني أمسيت أشهدك وأشهد حملة عرشك وملائكتك وجميع خلقك، "
    "أنك أنت الله لا إله إلا أنت وحدك لا شريك لك، وأن محمدًا عبدك ورسولك» أربع مرات.\n"
    "6⃣ «باسم الله الذي لا يضر مع اسمه شيء في الأرض ولا في السماء وهو السميع العليم» ثلاث مرات.\n"
    "7⃣ الإكثار من الصلاة على النبي ﷺ: «اللهم صل وسلم على سيدنا محمد».\n\n"
    "للتسبيح بعدد معيّن يمكنك استخدام زر «السبحة 📿»."
)

ADHKAR_GENERAL_TEXT = (
    "أذكار عامة تثبّت القلب وتريح الصدر 💚:\n\n"
    "• «أستغفر الله العظيم وأتوب إليه».\n"
    "• «لا إله إلا الله وحده لا شريك له، له الملك وله الحمد وهو على كل شيء قدير».\n"
    "• «سبحان الله، والحمد لله، ولا إله إلا الله، والله أكبر».\n"
    "• «لا حول ولا قوة إلا بالله».\n"
    "• «اللهم صل وسلم على سيدنا محمد».\n\n"
    "يمكنك استعمال «السبحة 📿» لاختيار ذكر وعدد تسبيحات معيّن والعدّ عليه."
)

# =================== أوامر البوت ===================


def start_command(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        ban_reason = record.get("ban_reason", "لم يتم تحديد السبب")
        banned_at = record.get("banned_at")
        banned_by = record.get("banned_by")
        
        try:
            banned_by_name = data.get(str(banned_by), {}).get("first_name", "إدارة البوت") if banned_by else "إدارة البوت"
        except:
            banned_by_name = "إدارة البوت"
            
        message_text = (
            "⛔️ *لقد تم حظرك من استخدام البوت*\n\n"
            f"🔒 *السبب:* {ban_reason}\n"
            f"🕒 *تاريخ الحظر:* {banned_at if banned_at else 'غير محدد'}\n"
            f"👤 *بواسطة:* {banned_by_name}\n\n"
            "للاستفسار يمكنك التواصل مع الدعم."
        )
        
        update.message.reply_text(
            message_text,
            parse_mode="Markdown"
        )
        return
    
    is_new =     # إرسال إشعار للمدير ورسالة ترحيب للمستخدم الجديد
    if record.get("is_new_user", False):
        send_new_user_notification(update, context)
        # إزالة علامة المستخدم الجديد بعد الإشعار
        update_user_record(user.id, is_new_user=False)
        return # الخروج بعد إرسال رسالة الترحيب من الدالة send_new_user_notification
    
    # رسالة ترحيب للمستخدم العائد
    kb = user_main_keyboard(user.id)

    update.message.reply_text(
        f"مرحبًا {user.first_name} 👋\n\n"
        "أهلًا بك في بوت *سُقيا الكوثر*.\n"
        "يساعدك على تنظيم شرب الماء، وضبط وردك القرآني، والمحافظة على الأذكار والتسبيح، "
        "وتسجيل مذكّرات قلبك، مع نظام نقاط ومنافسات وميداليات تحفيزية 🎖\n\n"
        "اختر من القائمة أسفل الشاشة ما يناسبك:",
        reply_markup=kb,
        parse_mode="Markdown",
    ) notifying admin about new user: {e}")


def help_command(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    kb = user_main_keyboard(update.effective_user.id)
    update.message.reply_text(
        "طريقة الاستخدام:\n\n"
        "• أذكاري 🤲 → أذكار الصباح والمساء وأذكار عامة.\n"
        "• وردي القرآني 📖 → تعيين عدد الصفحات التي تقرؤها يوميًا ومتابعة تقدمك.\n"
        "• السبحة 📿 → اختيار ذكر معيّن والعدّ عليه بعدد محدد من التسبيحات.\n"
        "• مذكّرات قلبي 🩵 → كتابة مشاعرك وخواطرك مع إمكانية التعديل والحذف.\n"
        "• رسالة إلى نفسي 💌 → كتابة رسائل مستقبلية مع تذكير بعد وقت معين.\n"
        "• منبّه الماء 💧 → حساب احتياجك من الماء، تسجيل الأكواب، وتفعيل التذكير.\n"
        "• احصائياتي 📊 → ملخّص بسيط لإنجازاتك اليوم.\n"
        "• تواصل مع الدعم ✉️ → لإرسال رسالة للدعم والرد عليك لاحقًا.\n"
        "• المنافسات و المجتمع 🏅 → لرؤية مستواك ونقاطك ولوحات الشرف.\n"
        "• الاشعارات 🔔 → تشغيل أو إيقاف الجرعة التحفيزية خلال اليوم.",
        reply_markup=kb,
    )

# =================== قسم رسالة إلى نفسي ===================


def open_letters_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id
    record = get_user_record(user)
    letters = record.get("letters_to_self", [])

    WAITING_LETTER_MENU.add(user_id)
    WAITING_LETTER_ADD.discard(user_id)
    WAITING_LETTER_ADD_CONTENT.discard(user_id)
    WAITING_LETTER_REMINDER_OPTION.discard(user_id)
    WAITING_LETTER_CUSTOM_DATE.discard(user_id)
    WAITING_LETTER_DELETE_SELECT.discard(user_id)
    LETTER_CURRENT_DATA.pop(user_id, None)

    letters_text = format_letters_list(letters)
    kb = build_letters_menu_kb(is_admin(user_id))

    update.message.reply_text(
        f"💌 رسالة إلى نفسي:\n\n{letters_text}\n\n"
        "يمكنك كتابة رسالة إلى نفسك المستقبلية مع تذكير بعد أسبوع، شهر، أو تاريخ مخصص.\n"
        "سأرسل لك الرسالة عندما يحين الموعد المحدد 🤍",
        reply_markup=kb,
    )


def format_letters_list(letters: List[Dict]) -> str:
    if not letters:
        return "لا توجد رسائل بعد."
    
    lines = []
    for idx, letter in enumerate(letters, start=1):
        content_preview = letter.get("content", "")[:30]
        reminder_date = letter.get("reminder_date")
        
        if reminder_date:
            try:
                reminder_dt = datetime.fromisoformat(reminder_date).astimezone(timezone.utc)
                now = datetime.now(timezone.utc)
                if reminder_dt <= now:
                    status = "✅ تم إرسالها"
                else:
                    time_left = reminder_dt - now
                    days = time_left.days
                    hours = time_left.seconds // 3600
                    status = f"⏳ بعد {days} يوم و {hours} ساعة"
            except:
                status = "📅 بتاريخ معين"
        else:
            status = "❌ بدون تذكير"
        
        lines.append(f"{idx}. {content_preview}... ({status})")
    
    return "\n".join(lines)


def handle_letter_add_start(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id

    WAITING_LETTER_MENU.discard(user_id)
    WAITING_LETTER_ADD.add(user_id)

    update.message.reply_text(
        "اكتب الآن نص الرسالة التي تريد إرسالها إلى نفسك في المستقبل 💌\n\n"
        "يمكن أن تكون:\n"
        "• تذكيرًا لهدف ما\n"
        "• كلمات تشجيعية لنفسك المستقبلية\n"
        "• دعاء تتمنى أن تتذكره\n"
        "• أي شيء تريد أن تقرأه لاحقًا",
        reply_markup=CANCEL_KB,
    )


def handle_letter_add_content(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id
    text = (update.message.text or "").strip()

    if text == BTN_CANCEL:
        WAITING_LETTER_ADD.discard(user_id)
        open_letters_menu(update, context)
        return

    if len(text) < 3:
        update.message.reply_text(
            "الرجاء كتابة رسالة أطول قليلًا (3 أحرف على الأقل).",
            reply_markup=CANCEL_KB,
        )
        return

    LETTER_CURRENT_DATA[user_id] = {"content": text}
    WAITING_LETTER_ADD.discard(user_id)
    WAITING_LETTER_REMINDER_OPTION.add(user_id)

    update.message.reply_text(
        f"📝 تم حفظ محتوى الرسالة.\n\n"
        f"الآن اختر متى تريد أن أذكّرك بها:\n\n"
        f"• {BTN_REMINDER_WEEK}: سأرسلها لك بعد أسبوع من الآن\n"
        f"• {BTN_REMINDER_MONTH}: سأرسلها لك بعد شهر\n"
        f"• {BTN_REMINDER_2MONTHS}: سأرسلها لك بعد شهرين\n"
        f"• {BTN_REMINDER_CUSTOM}: حدد تاريخًا مخصصًا\n"
        f"• {BTN_REMINDER_NONE}: بدون تذكير (ستبقى مخزنة فقط)",
        reply_markup=REMINDER_OPTIONS_KB,
    )


def handle_reminder_option(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id
    text = (update.message.text or "").strip()

    if text == BTN_CANCEL:
        WAITING_LETTER_REMINDER_OPTION.discard(user_id)
        LETTER_CURRENT_DATA.pop(user_id, None)
        open_letters_menu(update, context)
        return

    if user_id not in LETTER_CURRENT_DATA:
        WAITING_LETTER_REMINDER_OPTION.discard(user_id)
        update.message.reply_text(
            "حدث خطأ، يرجى المحاولة مرة أخرى.",
            reply_markup=build_letters_menu_kb(is_admin(user_id)),
        )
        return

    now = datetime.now(timezone.utc)
    reminder_date = None

    if text == BTN_REMINDER_WEEK:
        reminder_date = now + timedelta(days=7)
    elif text == BTN_REMINDER_MONTH:
        reminder_date = now + timedelta(days=30)
    elif text == BTN_REMINDER_2MONTHS:
        reminder_date = now + timedelta(days=60)
    elif text == BTN_REMINDER_CUSTOM:
        WAITING_LETTER_REMINDER_OPTION.discard(user_id)
        WAITING_LETTER_CUSTOM_DATE.add(user_id)
        update.message.reply_text(
            "أرسل التاريخ الذي تريد التذكير فيه بالصيغة:\n"
            "`YYYY-MM-DD HH:MM`\n\n"
            "مثال: `2024-12-25 15:30`\n\n"
            "ملاحظة: التوقيت المستخدم هو UTC (التوقيت العالمي).",
            reply_markup=CANCEL_KB,
            parse_mode="Markdown",
        )
        return
    elif text == BTN_REMINDER_NONE:
        reminder_date = None
    else:
        update.message.reply_text(
            "رجاءً اختر من الخيارات المتاحة.",
            reply_markup=REMINDER_OPTIONS_KB,
        )
        return

    # حفظ الرسالة
    record = get_user_record(user)
    letters = record.get("letters_to_self", [])
    
    new_letter = {
        "content": LETTER_CURRENT_DATA[user_id]["content"],
        "created_at": now.isoformat(),
        "reminder_date": reminder_date.isoformat() if reminder_date else None,
        "sent": False
    }
    
    letters.append(new_letter)
    record["letters_to_self"] = letters
    
    # إضافة نقاط
    add_points(user_id, POINTS_PER_LETTER, context, "كتابة رسالة إلى النفس")
    save_data()

    # جدولة التذكير إذا كان هناك تاريخ
    if reminder_date and context.job_queue:
        try:
            context.job_queue.run_once(
                send_letter_reminder,
                when=reminder_date,
                context={
                    "user_id": user_id,
                    "letter_content": new_letter["content"],
                    "letter_index": len(letters) - 1
                },
                name=f"letter_reminder_{user_id}_{len(letters)-1}"
            )
        except Exception as e:
            logger.error(f"Error scheduling letter reminder: {e}")

    WAITING_LETTER_REMINDER_OPTION.discard(user_id)
    LETTER_CURRENT_DATA.pop(user_id, None)

    if reminder_date:
        reminder_str = reminder_date.strftime("%Y-%m-%d %H:%M")
        message = (
            f"✅ تم حفظ رسالتك بنجاح!\n\n"
            f"📅 سأرسلها لك في:\n{reminder_str} (UTC)\n\n"
            f"🎯 لقد حصلت على {POINTS_PER_LETTER} نقاط إضافية!"
        )
    else:
        message = (
            f"✅ تم حفظ رسالتك بنجاح!\n\n"
            f"📝 ستكون متاحة دائمًا في قسم «رسالة إلى نفسي 💌»\n\n"
            f"🎯 لقد حصلت على {POINTS_PER_LETTER} نقاط إضافية!"
        )

    update.message.reply_text(
        message,
        reply_markup=build_letters_menu_kb(is_admin(user_id)),
    )


def handle_custom_date_input(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id
    text = (update.message.text or "").strip()

    if text == BTN_CANCEL:
        WAITING_LETTER_CUSTOM_DATE.discard(user_id)
        LETTER_CURRENT_DATA.pop(user_id, None)
        open_letters_menu(update, context)
        return

    if user_id not in LETTER_CURRENT_DATA:
        WAITING_LETTER_CUSTOM_DATE.discard(user_id)
        update.message.reply_text(
            "حدث خطأ، يرجى المحاولة مرة أخرى.",
            reply_markup=build_letters_menu_kb(is_admin(user_id)),
        )
        return

    try:
        # تحليل التاريخ
        if "T" in text:
            reminder_date = datetime.fromisoformat(text).astimezone(timezone.utc)
        else:
            reminder_date = datetime.strptime(text, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        
        now = datetime.now(timezone.utc)
        if reminder_date <= now:
            update.message.reply_text(
                "الرجاء إدخال تاريخ في المستقبل، وليس في الماضي أو الحاضر.",
                reply_markup=CANCEL_KB,
            )
            return

        # حفظ الرسالة
        record = get_user_record(user)
        letters = record.get("letters_to_self", [])
        
        new_letter = {
            "content": LETTER_CURRENT_DATA[user_id]["content"],
            "created_at": now.isoformat(),
            "reminder_date": reminder_date.isoformat(),
            "sent": False
        }
        
        letters.append(new_letter)
        record["letters_to_self"] = letters
        
        # إضافة نقاط
        add_points(user_id, POINTS_PER_LETTER, context, "كتابة رسالة إلى النفس")
        save_data()

        # جدولة التذكير
        if context.job_queue:
            try:
                context.job_queue.run_once(
                    send_letter_reminder,
                    when=reminder_date,
                    context={
                        "user_id": user_id,
                        "letter_content": new_letter["content"],
                        "letter_index": len(letters) - 1
                    },
                    name=f"letter_reminder_{user_id}_{len(letters)-1}"
                )
            except Exception as e:
                logger.error(f"Error scheduling letter reminder: {e}")

        WAITING_LETTER_CUSTOM_DATE.discard(user_id)
        LETTER_CURRENT_DATA.pop(user_id, None)

        reminder_str = reminder_date.strftime("%Y-%m-%d %H:%M")
        update.message.reply_text(
            f"✅ تم حفظ رسالتك بنجاح!\n\n"
            f"📅 سأرسلها لك في:\n{reminder_str} (UTC)\n\n"
            f"🎯 لقد حصلت على {POINTS_PER_LETTER} نقاط إضافية!",
            reply_markup=build_letters_menu_kb(is_admin(user_id)),
        )

    except ValueError:
        update.message.reply_text(
            "صيغة التاريخ غير صحيحة. الرجاء استخدام الصيغة:\n"
            "`YYYY-MM-DD HH:MM`\n"
            "مثال: `2024-12-25 15:30`",
            reply_markup=CANCEL_KB,
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error(f"Error processing custom date: {e}")
        update.message.reply_text(
            "حدث خطأ في معالجة التاريخ. الرجاء المحاولة مرة أخرى.",
            reply_markup=CANCEL_KB,
        )


def send_letter_reminder(context: CallbackContext):
    job = context.job
    user_id = job.context["user_id"]
    letter_content = job.context["letter_content"]
    letter_index = job.context["letter_index"]

    try:
        # تحديث حالة الرسالة في البيانات
        uid = str(user_id)
        if uid in data:
            record = data[uid]
            letters = record.get("letters_to_self", [])
            if letter_index < len(letters):
                letters[letter_index]["sent"] = True
                save_data()

        # إرسال الرسالة للمستخدم
        context.bot.send_message(
            chat_id=user_id,
            text=f"💌 رسالة من نفسك السابقة:\n\n{letter_content}\n\n"
                 f"⏰ هذا هو الموعد الذي طلبت التذكير فيه 🤍",
        )
    except Exception as e:
        logger.error(f"Error sending letter reminder to {user_id}: {e}")


def handle_letter_view(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    record = get_user_record(user)
    letters = record.get("letters_to_self", [])

    if not letters:
        update.message.reply_text(
            "لا توجد رسائل بعد.\n"
            "يمكنك كتابة رسالة جديدة من زر «✍️ كتابة رسالة جديدة».",
            reply_markup=build_letters_menu_kb(is_admin(user.id)),
        )
        return

    letters_with_details = []
    for idx, letter in enumerate(letters, start=1):
        content = letter.get("content", "")
        created_at = letter.get("created_at", "")
        reminder_date = letter.get("reminder_date")
        sent = letter.get("sent", False)

        try:
            created_dt = datetime.fromisoformat(created_at).astimezone(timezone.utc)
            created_str = created_dt.strftime("%Y-%m-%d")
        except:
            created_str = "تاريخ غير معروف"

        if reminder_date:
            try:
                reminder_dt = datetime.fromisoformat(reminder_date).astimezone(timezone.utc)
                now = datetime.now(timezone.utc)
                if reminder_dt <= now or sent:
                    status = "✅ تم إرسالها"
                else:
                    time_left = reminder_dt - now
                    days = time_left.days
                    hours = time_left.seconds // 3600
                    status = f"⏳ بعد {days} يوم و {hours} ساعة"
            except:
                status = "📅 بتاريخ معين"
        else:
            status = "📝 مخزنة"

        letters_with_details.append(
            f"{idx}. {content[:50]}...\n"
            f"   📅 كتبت في: {created_str}\n"
            f"   📌 الحالة: {status}"
        )

    text = "📋 رسائلك إلى نفسك:\n\n" + "\n\n".join(letters_with_details)
    update.message.reply_text(
        text,
        reply_markup=build_letters_menu_kb(is_admin(user.id)),
    )


def handle_letter_delete_select(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id
    record = get_user_record(user)
    letters = record.get("letters_to_self", [])

    if not letters:
        update.message.reply_text(
            "لا توجد رسائل لحذفها حاليًا.",
            reply_markup=build_letters_menu_kb(is_admin(user_id)),
        )
        return

    WAITING_LETTER_MENU.discard(user_id)
    WAITING_LETTER_DELETE_SELECT.add(user_id)

    letters_text = format_letters_list(letters)
    update.message.reply_text(
        f"🗑 اختر رقم الرسالة التي تريد حذفها:\n\n{letters_text}\n\n"
        "أرسل الرقم الآن، أو اضغط «إلغاء ❌».",
        reply_markup=CANCEL_KB,
    )


def handle_letter_delete_input(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id
    record = get_user_record(user)
    letters = record.get("letters_to_self", [])
    text = (update.message.text or "").strip()

    if text == BTN_CANCEL:
        WAITING_LETTER_DELETE_SELECT.discard(user_id)
        open_letters_menu(update, context)
        return

    try:
        idx = int(text) - 1
        if idx < 0 or idx >= len(letters):
            raise ValueError()
    except ValueError:
        update.message.reply_text(
            "رجاءً أرسل رقم صحيح من القائمة، أو اضغط «إلغاء ❌».",
            reply_markup=CANCEL_KB,
        )
        return

    deleted = letters.pop(idx)
    record["letters_to_self"] = letters
    save_data()

    WAITING_LETTER_DELETE_SELECT.discard(user_id)

    content_preview = deleted.get("content", "")[:50]
    update.message.reply_text(
        f"🗑 تم حذف الرسالة:\n\n{content_preview}...",
        reply_markup=build_letters_menu_kb(is_admin(user_id)),
    )
    open_letters_menu(update, context)

# =================== قسم منبّه الماء ===================


def open_water_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    get_user_record(user)
    kb = water_menu_keyboard(user.id)
    update.message.reply_text(
        "منبّه الماء 💧:\n"
        "• سجّل ما تشربه من أكواب.\n"
        "• شاهد مستواك اليوم.\n"
        "• عدّل إعداداتك وتشغيل التذكير.\n"
        "كل كوب يزيد نقاطك ويرفع مستواك 🎯",
        reply_markup=kb,
    )


def open_water_settings(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    kb = water_settings_keyboard(update.effective_user.id)
    update.message.reply_text(
        "إعدادات الماء ⚙️:\n"
        "1) حساب احتياجك اليومي من الماء بناءً على الجنس والعمر والوزن.\n"
        "2) تشغيل أو إيقاف التذكير الدوري بالماء.\n"
        "3) الرجوع إلى منبّه الماء مباشرة.",
        reply_markup=kb,
    )


def handle_water_need_start(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
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
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id
    text = update.message.text.strip()

    if text == BTN_CANCEL:
        WAITING_GENDER.discard(user_id)
        update.message.reply_text(
            "تم الإلغاء. عدنا للقائمة الرئيسية.",
            reply_markup=user_main_keyboard(user_id),
        )
        return

    if text not in [BTN_GENDER_MALE, BTN_GENDER_FEMALE]:
        update.message.reply_text(
            "رجاءً اختر من الخيارات الظاهرة:",
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
        "جميل.\nالآن أرسل عمرك (بالسنوات)، مثال: 25",
        reply_markup=CANCEL_KB,
    )


def handle_age_input(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id
    text = update.message.text.strip()

    if text == BTN_CANCEL:
        WAITING_AGE.discard(user_id)
        update.message.reply_text(
            "تم الإلغاء. عدنا للقائمة الرئيسية.",
            reply_markup=user_main_keyboard(user_id),
        )
        return

    try:
        age = int(text)
        if age <= 0 or age > 120:
            raise ValueError()
    except ValueError:
        update.message.reply_text(
            "رجاءً أرسل عمرًا صحيحًا بالأرقام فقط، مثال: 20",
            reply_markup=CANCEL_KB,
        )
        return

    record = get_user_record(user)
    record["age"] = age
    save_data()

    WAITING_AGE.discard(user_id)
    WAITING_WEIGHT.add(user_id)

    update.message.reply_text(
        "شكرًا.\nالآن أرسل وزنك بالكيلوغرام، مثال: 70",
        reply_markup=CANCEL_KB,
    )


def handle_weight_input(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id
    text = update.message.text.strip()

    if text == BTN_CANCEL:
        WAITING_WEIGHT.discard(user_id)
        update.message.reply_text(
            "تم الإلغاء. عدنا للقائمة الرئيسية.",
            reply_markup=user_main_keyboard(user_id),
        )
        return

    try:
        weight = float(text.replace(",", "."))
        if weight <= 20 or weight > 300:
            raise ValueError()
    except ValueError:
        update.message.reply_text(
            "رجاءً أرسل وزنًا صحيحًا بالكيلوغرام، مثال: 65",
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
        f"- ما يعادل تقريبًا: {cups_goal} كوب (بمتوسط 250 مل للكوب).\n\n"
        "وزّع أكوابك على اليوم، وسأذكّرك وأساعدك على المتابعة.\n"
        "كل كوب تسجّله يعطيك نقاطًا إضافية 🎯",
        reply_markup=water_menu_keyboard(user_id),
    )


def handle_log_cup(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    record = get_user_record(user)

    if not record.get("cups_goal"):
        update.message.reply_text(
            "لم تقم بعد بحساب احتياجك من الماء.\n"
            "اذهب إلى «إعدادات الماء ⚙️» ثم «حساب احتياج الماء 🧮».",
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

    check_daily_full_activity(user.id, record, context)

    status_text = format_water_status_text(record)
    update.message.reply_text(
        f"🥤 تم تسجيل كوب ماء.\n\n{status_text}",
        reply_markup=water_menu_keyboard(user.id),
    )


def handle_add_cups(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    record = get_user_record(user)
    text = (update.message.text or "").strip()

    if not record.get("cups_goal"):
        update.message.reply_text(
            "قبل استخدام هذه الميزة، احسب احتياجك من الماء أولًا من خلال:\n"
            "«إعدادات الماء ⚙️» → «حساب احتياج الماء 🧮».",
            reply_markup=water_menu_keyboard(user.id),
        )
        return

    if text == BTN_WATER_ADD_CUPS:
        update.message.reply_text(
            "أرسل الآن عدد الأكواب التي شربتها (بالأرقام فقط)، مثال: 2 أو 3.\n"
            "وسيتم إضافتها مباشرة إلى عدّاد اليوم.",
            reply_markup=CANCEL_KB,
        )
        return

    try:
        cups = int(text)
        if cups <= 0 or cups > 50:
            raise ValueError()
    except ValueError:
        update.message.reply_text(
            "لو كنت تريد إضافة عدد من الأكواب، أرسل رقمًا منطقيًا مثل: 2 أو 3.\n"
            "أو استخدم بقية الأزرار للقائمة.",
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

    check_daily_full_activity(user.id, record, context)

    status_text = format_water_status_text(record)
    update.message.reply_text(
        f"🥤 تم إضافة {cups} كوب إلى عدّادك اليوم.\n\n{status_text}",
        reply_markup=water_menu_keyboard(user.id),
    )


def handle_status(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    record = get_user_record(user)
    text = format_water_status_text(record)
    update.message.reply_text(
        text,
        reply_markup=water_menu_keyboard(user.id),
    )


def handle_reminders_on(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    record = get_user_record(user)

    if not record.get("cups_goal"):
        update.message.reply_text(
            "قبل تشغيل التذكير، احسب احتياجك من الماء من خلال:\n"
            "«حساب احتياج الماء 🧮».",
            reply_markup=water_settings_keyboard(user.id),
        )
        return

    record["reminders_on"] = True
    save_data()

    update.message.reply_text(
        "تم تشغيل تذكيرات الماء ⏰\n"
        "ستصلك رسائل خلال اليوم لتذكيرك بالشرب.",
        reply_markup=water_settings_keyboard(user.id),
    )


def handle_reminders_off(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    record = get_user_record(user)
    record["reminders_on"] = False
    save_data()

    update.message.reply_text(
        "تم إيقاف تذكيرات الماء 📴\n"
        "يمكنك تشغيلها مرة أخرى وقتما شئت.",
        reply_markup=water_settings_keyboard(user.id),
    )

# =================== قسم ورد القرآن ===================


def open_quran_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    get_user_record(user)
    kb = quran_menu_keyboard(user.id)
    update.message.reply_text(
        "وردي القرآني 📖:\n"
        "• عيّن عدد صفحات اليوم.\n"
        "• سجّل ما قرأته.\n"
        "• شاهد مستوى إنجازك.\n"
        "• يمكنك إعادة تعيين ورد اليوم.\n"
        "كل صفحة تضيفها تزيد نقاطك وترفع مستواك 🎯",
        reply_markup=kb,
    )


def handle_quran_set_goal(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = update.effective_user.id

    WAITING_QURAN_GOAL.add(user_id)
    WAITING_QURAN_ADD_PAGES.discard(user_id)

    update.message.reply_text(
        "أرسل عدد الصفحات التي تريد قراءتها اليوم من القرآن، مثال: 5 أو 10.",
        reply_markup=CANCEL_KB,
    )


def handle_quran_goal_input(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id
    text = (update.message.text or "").strip()

    if text == BTN_CANCEL:
        WAITING_QURAN_GOAL.discard(user_id)
        update.message.reply_text(
            "تم الإلغاء. عدنا للقائمة الرئيسية.",
            reply_markup=user_main_keyboard(user_id),
        )
        return

    try:
        pages = int(text)
        if pages <= 0 or pages > 200:
            raise ValueError()
    except ValueError:
        update.message.reply_text(
            "رجاءً أرسل عدد صفحات منطقيًا، مثل: 5 أو 10 أو 20.",
            reply_markup=CANCEL_KB,
        )
        return

    record = get_user_record(user)
    ensure_today_quran(record)
    record["quran_pages_goal"] = pages
    save_data()

    WAITING_QURAN_GOAL.discard(user_id)

    update.message.reply_text(
        f"تم تعيين ورد اليوم: {pages} صفحة.\n"
        "يمكنك تسجيل ما قرأته من خلال «سجلت صفحات اليوم ✅».",
        reply_markup=quran_menu_keyboard(user_id),
    )


def handle_quran_add_pages_start(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    record = get_user_record(user)

    if not record.get("quran_pages_goal"):
        update.message.reply_text(
            "لم تضبط بعد ورد اليوم.\n"
            "استخدم «تعيين ورد اليوم 📌» أولًا.",
            reply_markup=quran_menu_keyboard(user.id),
        )
        return

    WAITING_QURAN_ADD_PAGES.add(user.id)
    update.message.reply_text(
        "أرسل الآن عدد الصفحات التي قرأتها من ورد اليوم، مثال: 2 أو 3.",
        reply_markup=CANCEL_KB,
    )


def handle_quran_add_pages_input(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id
    text = (update.message.text or "").strip()

    if text == BTN_CANCEL:
        WAITING_QURAN_ADD_PAGES.discard(user_id)
        update.message.reply_text(
            "تم الإلغاء. عدنا للقائمة الرئيسية.",
            reply_markup=user_main_keyboard(user_id),
        )
        return

    try:
        pages = int(text)
        if pages <= 0 or pages > 100:
            raise ValueError()
    except ValueError:
        update.message.reply_text(
            "رجاءً أرسل عدد صفحات صحيحًا، مثل: 1 أو 2 أو 5.",
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

    check_daily_full_activity(user_id, record, context)

    WAITING_QURAN_ADD_PAGES.discard(user_id)

    status_text = format_quran_status_text(record)
    update.message.reply_text(
        f"تم إضافة {pages} صفحة إلى وردك اليوم.\n\n{status_text}",
        reply_markup=quran_menu_keyboard(user_id),
    )


def handle_quran_status(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    record = get_user_record(user)
    text = format_quran_status_text(record)
    update.message.reply_text(
        text,
        reply_markup=quran_menu_keyboard(user.id),
    )


def handle_quran_reset_day(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    record = get_user_record(user)

    ensure_today_quran(record)
    record["quran_pages_today"] = 0
    save_data()

    update.message.reply_text(
        "تم إعادة تعيين ورد اليوم.\n"
        "يمكنك البدء من جديد في حساب الصفحات لهذا اليوم.",
        reply_markup=quran_menu_keyboard(user.id),
    )

# =================== قسم الأذكار ===================


def open_adhkar_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    get_user_record(user)
    kb = adhkar_menu_keyboard(user.id)
    update.message.reply_text(
        "أذكاري 🤲:\n"
        "• أذكار الصباح.\n"
        "• أذكار المساء.\n"
        "• أذكار عامة تريح القلب.",
        reply_markup=kb,
    )


def send_morning_adhkar(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    increment_adhkar_count(user.id, 1)
    kb = adhkar_menu_keyboard(user.id)
    update.message.reply_text(
        ADHKAR_MORNING_TEXT,
        reply_markup=kb,
    )


def send_evening_adhkar(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    increment_adhkar_count(user.id, 1)
    kb = adhkar_menu_keyboard(user.id)
    update.message.reply_text(
        ADHKAR_EVENING_TEXT,
        reply_markup=kb,
    )


def send_general_adhkar(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    increment_adhkar_count(user.id, 1)
    kb = adhkar_menu_keyboard(user.id)
    update.message.reply_text(
        ADHKAR_GENERAL_TEXT,
        reply_markup=kb,
    )

# =================== قسم السبحة ===================


def open_tasbih_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    ACTIVE_TASBIH.pop(user.id, None)
    WAITING_TASBIH.discard(user.id)

    kb = build_tasbih_menu(is_admin(user.id))
    text = "اختر الذكر الذي تريد التسبيح به، وسيقوم البوت بالعدّ لك:"
    update.message.reply_text(
        text,
        reply_markup=kb,
    )


def start_tasbih_for_choice(update: Update, context: CallbackContext, choice_text: str):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
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
                f"العدد المطلوب: {count} مرة.\n\n"
                "اضغط «تسبيحة ✅» في كل مرة تذكر فيها، أو «إنهاء الذكر ⬅️» عندما تنتهي.",
                reply_markup=tasbih_run_keyboard(user_id),
            )
            return

    update.message.reply_text(
        "رجاءً اختر من الأذكار الظاهرة في القائمة.",
        reply_markup=build_tasbih_menu(is_admin(user_id)),
    )


def handle_tasbih_tick(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id

    state = ACTIVE_TASBIH.get(user_id)
    if not state:
        update.message.reply_text(
            "ابدأ أولًا باختيار ذكر من قائمة «السبحة 📿».",
            reply_markup=build_tasbih_menu(is_admin(user_id)),
        )
        return

    state["current"] += 1
    increment_tasbih_total(user_id, 1)

    current = state["current"]
    target = state["target"]
    dhikr = state["text"]

    if current < target:
        update.message.reply_text(
            f"{dhikr}\n"
            f"العدد الحالي: {current} / {target}.",
            reply_markup=tasbih_run_keyboard(user_id),
        )
    else:
        reward_points = tasbih_points_for_session(target)
        add_points(user_id, reward_points, context)

        update.message.reply_text(
            f"اكتمل التسبيح على: {dhikr}\n"
            f"وصلت إلى {target} تسبيحة. تقبّل الله منك 🤍.\n\n"
            "اختر تسبيحة أخرى من القائمة:",
            reply_markup=build_tasbih_menu(is_admin(user_id)),
        )
        ACTIVE_TASBIH.pop(user_id, None)
        WAITING_TASBIH.discard(user_id)


def handle_tasbih_end(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = update.effective_user.id
    ACTIVE_TASBIH.pop(user_id, None)
    WAITING_TASBIH.discard(user_id)

    update.message.reply_text(
        "تم إنهاء جلسة التسبيح الحالية.\n"
        "يمكنك اختيار ذكر جديد من «السبحة 📿».",
        reply_markup=build_tasbih_menu(is_admin(user_id)),
    )

# =================== مذكّرات قلبي ===================


def format_memos_list(memos):
    if not memos:
        return "لا توجد مذكّرات بعد."
    return "\n\n".join(f"{idx+1}. {m}" for idx, m in enumerate(memos))


def open_memos_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id
    record = get_user_record(user)
    memos = record.get("heart_memos", [])

    WAITING_MEMO_MENU.add(user_id)
    WAITING_MEMO_ADD.discard(user_id)
    WAITING_MEMO_EDIT_SELECT.discard(user_id)
    WAITING_MEMO_EDIT_TEXT.discard(user_id)
    WAITING_MEMO_DELETE_SELECT.discard(user_id)
    MEMO_EDIT_INDEX.pop(user_id, None)

    memos_text = format_memos_list(memos)
    kb = build_memos_menu_kb(is_admin(user_id))

    update.message.reply_text(
        f"🩵 مذكّرات قلبي:\n\n{memos_text}\n\n"
        "يمكنك إضافة، تعديل، أو حذف أي مذكرة من الأزرار بالأسفل.",
        reply_markup=kb,
    )


def handle_memo_add_start(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = update.effective_user.id

    WAITING_MEMO_MENU.discard(user_id)
    WAITING_MEMO_ADD.add(user_id)

    update.message.reply_text(
        "اكتب الآن المذكرة التي تريد حفظها في قلبك.\n"
        "يمكن أن تكون شعورًا، دعاءً، موقفًا، أو أي شيء يهمّك 🤍",
        reply_markup=CANCEL_KB,
    )


def handle_memo_add_input(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id
    text = (update.message.text or "").strip()

    if text == BTN_CANCEL:
        WAITING_MEMO_ADD.discard(user_id)
        open_memos_menu(update, context)
        return

    record = get_user_record(user)
    memos = record.get("heart_memos", [])
    memos.append(text)
    record["heart_memos"] = memos
    save_data()

    WAITING_MEMO_ADD.discard(user_id)

    update.message.reply_text(
        "تم حفظ مذكّرتك في قلب البوت 🤍.",
        reply_markup=build_memos_menu_kb(is_admin(user_id)),
    )
    open_memos_menu(update, context)


def handle_memo_edit_select(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id
    record = get_user_record(user)
    memos = record.get("heart_memos", [])

    if not memos:
        update.message.reply_text(
            "لا توجد مذكّرات لتعديلها حاليًا.",
            reply_markup=build_memos_menu_kb(is_admin(user_id)),
        )
        return

    WAITING_MEMO_MENU.discard(user_id)
    WAITING_MEMO_EDIT_SELECT.add(user_id)

    memos_text = format_memos_list(memos)
    update.message.reply_text(
        f"✏️ اختر رقم المذكرة التي تريد تعديلها:\n\n{memos_text}\n\n"
        "أرسل الرقم الآن، أو اضغط «إلغاء ❌».",
        reply_markup=CANCEL_KB,
    )


def handle_memo_edit_index_input(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id
    record = get_user_record(user)
    memos = record.get("heart_memos", [])
    text = (update.message.text or "").strip()

    if text == BTN_CANCEL:
        WAITING_MEMO_EDIT_SELECT.discard(user_id)
        open_memos_menu(update, context)
        return

    try:
        idx = int(text) - 1
        if idx < 0 or idx >= len(memos):
            raise ValueError()
    except ValueError:
        update.message.reply_text(
            "رجاءً أرسل رقم صحيح من القائمة الموجودة أمامك، أو اضغط «إلغاء ❌».",
            reply_markup=CANCEL_KB,
        )
        return

    MEMO_EDIT_INDEX[user_id] = idx
    WAITING_MEMO_EDIT_SELECT.discard(user_id)
    WAITING_MEMO_EDIT_TEXT.add(user_id)

    update.message.reply_text(
        f"✏️ أرسل النص الجديد للمذكرة رقم {idx+1}:",
        reply_markup=CANCEL_KB,
    )


def handle_memo_edit_text_input(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id
    record = get_user_record(user)
    memos = record.get("heart_memos", [])
    text = (update.message.text or "").strip()

    if text == BTN_CANCEL:
        WAITING_MEMO_EDIT_TEXT.discard(user_id)
        MEMO_EDIT_INDEX.pop(user_id, None)
        open_memos_menu(update, context)
        return

    idx = MEMO_EDIT_INDEX.get(user_id)
    if idx is None or idx < 0 or idx >= len(memos):
        WAITING_MEMO_EDIT_TEXT.discard(user_id)
        MEMO_EDIT_INDEX.pop(user_id, None)
        update.message.reply_text(
            "حدث خطأ بسيط في اختيار المذكرة، جرّب من جديد من «مذكّرات قلبي 🩵».",
            reply_markup=user_main_keyboard(user_id),
        )
        return

    mems[idx] = text
    record["heart_memos"] = memos
    save_data()

    WAITING_MEMO_EDIT_TEXT.discard(user_id)
    MEMO_EDIT_INDEX.pop(user_id, None)

    update.message.reply_text(
        "تم تعديل المذكرة بنجاح ✅.",
        reply_markup=build_memos_menu_kb(is_admin(user_id)),
    )
    open_memos_menu(update, context)


def handle_memo_delete_select(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id
    record = get_user_record(user)
    memos = record.get("heart_memos", [])

    if not memos:
        update.message.reply_text(
            "لا توجد مذكّرات لحذفها حاليًا.",
            reply_markup=build_memos_menu_kb(is_admin(user_id)),
        )
        return

    WAITING_MEMO_MENU.discard(user_id)
    WAITING_MEMO_DELETE_SELECT.add(user_id)

    memos_text = format_memos_list(memos)
    update.message.reply_text(
        f"🗑 اختر رقم المذكرة التي تريد حذفها:\n\n{memos_text}\n\n"
        "أرسل الرقم الآن، أو اضغط «إلغاء ❌».",
        reply_markup=CANCEL_KB,
    )


def handle_memo_delete_index_input(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id
    record = get_user_record(user)
    memos = record.get("heart_memos", [])
    text = (update.message.text or "").strip()

    if text == BTN_CANCEL:
        WAITING_MEMO_DELETE_SELECT.discard(user_id)
        open_memos_menu(update, context)
        return

    try:
        idx = int(text) - 1
        if idx < 0 or idx >= len(memos):
            raise ValueError()
    except ValueError:
        update.message.reply_text(
            "رجاءً أرسل رقم صحيح من القائمة الموجودة أمامك، أو اضغط «إلغاء ❌».",
            reply_markup=CANCEL_KB,
        )
        return

    deleted = memos.pop(idx)
    record["heart_memos"] = memos
    save_data()

    WAITING_MEMO_DELETE_SELECT.discard(user_id)

    update.message.reply_text(
        f"🗑 تم حذف المذكرة:\n\n{deleted}",
        reply_markup=build_memos_menu_kb(is_admin(user_id)),
    )
    open_memos_menu(update, context)

# =================== احصائياتي ===================


def handle_stats(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id
    record = get_user_record(user)

    ensure_today_water(record)
    ensure_today_quran(record)

    cups_goal = record.get("cups_goal")
    today_cups = record.get("today_cups", 0)

    q_goal = record.get("quran_pages_goal")
    q_today = record.get("quran_pages_today", 0)

    tasbih_total = record.get("tasbih_total", 0)
    adhkar_count = record.get("adhkar_count", 0)

    memos_count = len(record.get("heart_memos", []))
    letters_count = len(record.get("letters_to_self", []))

    points = record.get("points", 0)
    level = record.get("level", 0)
    medals = record.get("medals", [])

    text_lines = ["احصائياتك لليوم 📊:\n"]

    if cups_goal:
        text_lines.append(f"- الماء: {today_cups} / {cups_goal} كوب.")
    else:
        text_lines.append("- الماء: لم يتم حساب احتياجك بعد.")

    if q_goal:
        text_lines.append(f"- ورد القرآن: {q_today} / {q_goal} صفحة.")
    else:
        text_lines.append("- ورد القرآن: لم تضبط وردًا لليوم بعد.")

    text_lines.append(f"- عدد المرات التي استخدمت فيها قسم الأذكار: {adhkar_count} مرة.")
    text_lines.append(f"- مجموع التسبيحات المسجّلة عبر السبحة: {tasbih_total} تسبيحة.")
    text_lines.append(f"- عدد مذكّرات قلبك المسجّلة: {memos_count} مذكرة.")
    text_lines.append(f"- عدد رسائلك إلى نفسك: {letters_count} رسالة.")

    text_lines.append(f"- مجموع نقاطك: {points} نقطة.")
    if level <= 0:
        text_lines.append("- مستواك الحالي: 0 (أول مستوى فعلي يبدأ من 20 نقطة).")
    else:
        text_lines.append(f"- المستوى الحالي: {level}.")
    if medals:
        text_lines.append("- ميدالياتك: " + "، ".join(medals))

    update.message.reply_text(
        "\n".join(text_lines),
        reply_markup=user_main_keyboard(user_id),
    )

# =================== قسم الفوائد والنصائح ===================

def open_benefits_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    if record.get("is_banned", False):
        return

    update.message.reply_text(
        "💡 مجتمع الفوائد و النصائح:\n"
        "شارك فائدة، استعرض فوائد الآخرين، وشارك في التقييم لتحفيز المشاركة.",
        reply_markup=BENEFITS_MENU_KB,
    )


def handle_add_benefit_start(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    if record.get("is_banned", False):
        return

    WAITING_BENEFIT_TEXT.add(user.id)
    update.message.reply_text(
        "✍️ أرسل الفائدة أو النصيحة القصيرة التي تود مشاركتها الآن.\n"
        "ملاحظة: يجب أن تكون 5 أحرف على الأقل.",
        reply_markup=CANCEL_KB,
    )


def handle_add_benefit_text(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    
    if user_id not in WAITING_BENEFIT_TEXT:
        return

    text = update.message.text.strip()
    
    if text == BTN_CANCEL:
        WAITING_BENEFIT_TEXT.discard(user_id)
        update.message.reply_text(
            "تم إلغاء إضافة الفائدة.",
            reply_markup=BENEFITS_MENU_KB,
        )
        return
    
    if len(text) < 5:
        update.message.reply_text(
            "⚠️ يجب أن تكون الفائدة 5 أحرف على الأقل. حاول مرة أخرى:",
            reply_markup=CANCEL_KB,
        )
        return

    # إزالة المستخدم من حالة الانتظار قبل إكمال العملية
    WAITING_BENEFIT_TEXT.discard(user_id)

    # 1. تخزين الفائدة
    benefit_id = get_next_benefit_id()
    now_iso = datetime.now(timezone.utc).isoformat()
    
    # التأكد من وجود اسم للمستخدم، وإلا استخدام "مستخدم مجهول"
    first_name = user.first_name if user.first_name else "مستخدم مجهول"
    
    new_benefit = {
        "id": benefit_id,
        "text": text,
        "user_id": user_id,
        "first_name": first_name,
        "date": now_iso,
        "likes_count": 0,
        "liked_by": [],
    }

    benefits = get_benefits()
    benefits.append(new_benefit)
    save_benefits(benefits)

    # 2. منح النقاط
    add_points(user_id, 2)

    # 3. إرسال رسالة تأكيد
    update.message.reply_text(
        "✅ تم إضافة فائدتك بنجاح! شكرًا لمشاركتك.\n"
        f"لقد حصلت على 2 نقطة مكافأة.",
        reply_markup=BENEFITS_MENU_KB,
    )


def handle_view_benefits(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    if record.get("is_banned", False):
        return

    benefits = get_benefits()
    
    if not benefits:
        update.message.reply_text(
            "لا توجد فوائد أو نصائح مضافة حتى الآن. كن أول من يشارك! 💡",
            reply_markup=BENEFITS_MENU_KB,
        )
        return

    # عرض آخر 5 فوائد
    latest_benefits = sorted(benefits, key=lambda b: b.get("date", ""), reverse=True)[:5]
    
    # التحقق من صلاحيات المدير/المشرف
    is_privileged = is_admin(user.id) or is_supervisor(user.id)
    
    update.message.reply_text(
        "📖 آخر 5 فوائد ونصائح مضافة:",
        reply_markup=BENEFITS_MENU_KB,
    )
    
    for benefit in latest_benefits:
        # تنسيق التاريخ
        try:
            dt = datetime.fromisoformat(benefit["date"].replace('Z', '+00:00'))
            date_str = dt.strftime("%Y-%m-%d")
        except:
            date_str = "تاريخ غير معروف"
            
        text_benefit = (
            f"• *{benefit['text']}*\n"
            f"  - من: {benefit['first_name']} | الإعجابات: {benefit['likes_count']} 👍\n"
            f"  - تاريخ الإضافة: {date_str}\n"
        )
        
        # إضافة زر الإعجاب
        like_button_text = f"👍 أعجبني ({benefit['likes_count']})"
        
        # التحقق مما إذا كان المستخدم قد أعجب بالفعل
        if user.id in benefit.get("liked_by", []):
            like_button_text = f"✅ أعجبتني ({benefit['likes_count']})"
        
        # استخدام InlineKeyboardCallbackData للإعجاب
        keyboard_row = [
            InlineKeyboardButton(
                like_button_text, 
                callback_data=f"like_benefit_{benefit['id']}"
            )
        ]
        
        # إضافة زر الحذف للمدير/المشرف فقط
        if is_privileged:
            keyboard_row.append(
                InlineKeyboardButton(
                    "🗑 حذف الفائدة (إشراف)", 
                    callback_data=f"admin_delete_benefit_{benefit['id']}"
                )
            )
            
        keyboard = [keyboard_row]
        
        update.message.reply_text(
            text=text_benefit,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
        )
        
    # إرسال رسالة ختامية ولوحة المفاتيح الرئيسية للقسم
    update.message.reply_text(
        "انتهى عرض آخر الفوائد.",
        reply_markup=BENEFITS_MENU_KB,
    )


def handle_my_benefits(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    record = get_user_record(user)
    
    if record.get("is_banned", False):
        return

    benefits = get_benefits()
    user_benefits = [b for b in benefits if b.get("user_id") == user_id]
    
    if not user_benefits:
        update.message.reply_text(
            "📝 لم تقم بإضافة أي فوائد بعد.",
            reply_markup=BENEFITS_MENU_KB,
        )
        return

    update.message.reply_text(
        f"📝 فوائدك ({len(user_benefits)} فائدة):",
        reply_markup=BENEFITS_MENU_KB,
    )
    
    for benefit in user_benefits:
        # تنسيق التاريخ
        try:
            dt = datetime.fromisoformat(benefit["date"].replace('Z', '+00:00'))
            date_str = dt.strftime("%Y-%m-%d")
        except:
            date_str = "تاريخ غير معروف"
            
        text_benefit = (
            f"• *{benefit['text']}*\n"
            f"  - الإعجابات: {benefit['likes_count']} 👍\n"
            f"  - تاريخ الإضافة: {date_str}\n"
        )
        
        # أزرار التعديل والحذف
        keyboard = [[
            InlineKeyboardButton(
                BTN_BENEFIT_EDIT, 
                callback_data=f"edit_benefit_{benefit['id']}"
            ),
            InlineKeyboardButton(
                BTN_BENEFIT_DELETE, 
                callback_data=f"delete_benefit_{benefit['id']}"
            )
        ]]
        
        update.message.reply_text(
            text=text_benefit,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
        )


def handle_edit_benefit_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    
    try:
        benefit_id = int(query.data.split("_")[-1])
    except ValueError:
        query.answer("خطأ في تحديد الفائدة.")
        return

    benefits = get_benefits()
    benefit = next((b for b in benefits if b.get("id") == benefit_id), None)
    
    if benefit is None:
        query.answer("هذه الفائدة غير موجودة.")
        return
        
    # يجب أن يكون المستخدم هو صاحب الفائدة لتعديلها
    if benefit.get("user_id") != user_id:
        query.answer("لا تملك صلاحية تعديل هذه الفائدة.")
        return

    # حفظ ID الفائدة وحالة الانتظار
    BENEFIT_EDIT_ID[user_id] = benefit_id
    WAITING_BENEFIT_EDIT_TEXT.add(user_id)
    
    query.answer("أرسل النص الجديد الآن.")
    
    context.bot.send_message(
        chat_id=user_id,
        text=f"✏️ أرسل النص الجديد للفائدة رقم {benefit_id} الآن.\n"
             f"النص الحالي: *{benefit['text']}*",
        reply_markup=CANCEL_KB,
        parse_mode="Markdown",
    )
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    
    try:
        benefit_id = int(query.data.split("_")[-1])
    except ValueError:
        query.answer("خطأ في تحديد الفائدة.")
        return

    benefits = get_benefits()
    
    # التحقق من الصلاحية: إما صاحب الفائدة أو مدير/مشرف
    is_owner = lambda b: b.get("id") == benefit_id and b.get("user_id") == user_id
    is_privileged = is_admin(user_id) or is_supervisor(user_id)
    
    benefit = next((b for b in benefits if b.get("id") == benefit_id), None)
    
    if benefit is None:
        query.answer("هذه الفائدة غير موجودة.")
        return
        
    # يجب أن يكون المستخدم هو صاحب الفائدة لتعديلها
    if benefit.get("user_id") != user_id:
        query.answer("لا تملك صلاحية تعديل هذه الفائدة.")
        return

    # حفظ ID الفائدة وحالة الانتظار
    BENEFIT_EDIT_ID[user_id] = benefit_id
    WAITING_BENEFIT_EDIT_TEXT.add(user_id)
    
    query.answer("أرسل النص الجديد الآن.")
    

    



def handle_edit_benefit_text(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    
    if user_id not in WAITING_BENEFIT_EDIT_TEXT:
        return

    text = update.message.text.strip()
    
    # الإلغاء
    if text == BTN_CANCEL:
        WAITING_BENEFIT_EDIT_TEXT.discard(user_id)
        BENEFIT_EDIT_ID.pop(user_id, None)
        update.message.reply_text(
            "❌ تم إلغاء التعديل.\nعدنا لقسم مجتمع الفوائد و النصائح.",
            reply_markup=BENEFITS_MENU_KB,
        )
        return
    
    if len(text) < 5:
        update.message.reply_text(
            "⚠️ يجب أن تكون الفائدة 5 أحرف على الأقل. حاول مرة أخرى:",
            reply_markup=CANCEL_KB,
        )
        return

    benefit_id = BENEFIT_EDIT_ID.get(user_id)
    
    benefits = get_benefits()
    
    for i, b in enumerate(benefits):
        if b.get("id") == benefit_id and b.get("user_id") == user_id:
            benefits[i]["text"] = text
            save_benefits(benefits)
            
            WAITING_BENEFIT_EDIT_TEXT.discard(user_id)
            BENEFIT_EDIT_ID.pop(user_id, None)
            
            update.message.reply_text(
                "✅ تم تعديل الفائدة بنجاح.",
                reply_markup=BENEFITS_MENU_KB,
            )
            return

    WAITING_BENEFIT_EDIT_TEXT.discard(user_id)
    BENEFIT_EDIT_ID.pop(user_id, None)
    update.message.reply_text(
        "⚠️ حدث خطأ: لم يتم العثور على الفائدة أو لا تملك صلاحية تعديلها.",
        reply_markup=BENEFITS_MENU_KB,
    )


def handle_delete_benefit_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    
    try:
        benefit_id = int(query.data.split("_")[-1])
    except ValueError:
        query.answer("خطأ في تحديد الفائدة.")
        return

    benefits = get_benefits()
    benefit = next((b for b in benefits if b.get("id") == benefit_id and b.get("user_id") == user_id), None)
    
    if benefit is None:
        query.answer("لا تملك صلاحية حذف هذه الفائدة أو أنها غير موجودة.")
        return

    # حفظ ID الفائدة وحالة الانتظار للتأكيد
    BENEFIT_EDIT_ID[user_id] = benefit_id
    WAITING_BENEFIT_DELETE_CONFIRM.add(user_id)
    
    query.answer("تأكيد الحذف.")
    
    keyboard = [[
        InlineKeyboardButton("✅ نعم، متأكد من الحذف", callback_data=f"confirm_delete_benefit_{benefit_id}"),
        InlineKeyboardButton("❌ لا، إلغاء", callback_data="cancel_delete_benefit")
    ]]
    
    context.bot.send_message(
        chat_id=user_id,
        text=f"⚠️ هل أنت متأكد من حذف الفائدة رقم {benefit_id}؟\n"
             f"النص: *{benefit['text']}*",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown",
    )


def handle_delete_benefit_confirm_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    
    # تحديد ما إذا كان الحذف هو حذف مستخدم عادي أو حذف إشرافي
    is_admin_delete = query.data.startswith("confirm_admin_delete_benefit_")
    
    if query.data == "cancel_delete_benefit" or query.data == "cancel_admin_delete_benefit":
        WAITING_BENEFIT_DELETE_CONFIRM.discard(user_id)
        BENEFIT_EDIT_ID.pop(user_id, None)
        query.answer("تم إلغاء الحذف.")
        query.edit_message_text(
            text="تم إلغاء عملية الحذف.",
            reply_markup=None,
        )
        return

    try:
        benefit_id = int(query.data.split("_")[-1])
    except ValueError:
        query.answer("خطأ في تحديد الفائدة.")
        return

    benefits = get_benefits()
    
    # التحقق من الصلاحية: إما صاحب الفائدة أو مدير/مشرف
    is_privileged = is_admin(user_id) or is_supervisor(user_id)
    
    # البحث عن الفائدة
    benefit_to_delete = next((b for b in benefits if b.get("id") == benefit_id), None)
    
    if benefit_to_delete is None:
        query.answer("هذه الفائدة غير موجودة.")
        query.edit_message_text(
            text="⚠️ حدث خطأ: هذه الفائدة غير موجودة.",
            reply_markup=None,
        )
        WAITING_BENEFIT_DELETE_CONFIRM.discard(user_id)
        BENEFIT_EDIT_ID.pop(user_id, None)
        return

    is_owner = benefit_to_delete.get("user_id") == user_id
    
    # إذا كان حذف مستخدم عادي، يجب أن يكون هو المالك
    if not is_admin_delete and not is_owner:
        query.answer("لا تملك صلاحية حذف هذه الفائدة.")
        query.edit_message_text(
            text="⚠️ حدث خطأ: لا تملك صلاحية حذف هذه الفائدة.",
            reply_markup=None,
        )
        WAITING_BENEFIT_DELETE_CONFIRM.discard(user_id)
        BENEFIT_EDIT_ID.pop(user_id, None)
        return
        
    # إذا كان حذف إشرافي، يجب أن يكون لديه صلاحية
    if is_admin_delete and not is_privileged:
        query.answer("لا تملك صلاحية حذف فوائد الآخرين.")
        query.edit_message_text(
            text="⚠️ حدث خطأ: لا تملك صلاحية حذف فوائد الآخرين.",
            reply_markup=None,
        )
        WAITING_BENEFIT_DELETE_CONFIRM.discard(user_id)
        BENEFIT_EDIT_ID.pop(user_id, None)
        return

    # حذف الفائدة
    initial_count = len(benefits)
    benefits[:] = [b for b in benefits if b.get("id") != benefit_id]
    
    if len(benefits) < initial_count:
        save_benefits(benefits)
        query.answer("✅ تم حذف الفائدة بنجاح.")
        query.edit_message_text(
            text=f"✅ تم حذف الفائدة رقم {benefit_id} بنجاح.",
            reply_markup=None,
        )
        
        # إرسال رسالة لصاحب الفائدة إذا كان الحذف إشرافيًا
        if is_admin_delete and benefit_to_delete.get("user_id") != user_id:
            try:
                context.bot.send_message(
                    chat_id=benefit_to_delete.get("user_id"),
                    text=f"⚠️ تنبيه: تم حذف فائدتك رقم {benefit_id} بواسطة المشرف/المدير.\n"
                         f"النص المحذوف: *{benefit_to_delete['text']}*\n"
                         f"يرجى مراجعة سياسات المجتمع.",
                    parse_mode="Markdown",
                )
            except Exception as e:
                logger.error(f"Error sending deletion message to benefit owner: {e}")
                
        WAITING_BENEFIT_DELETE_CONFIRM.discard(user_id)
        BENEFIT_EDIT_ID.pop(user_id, None)
        return # المشكلة 2: الخروج بعد الحذف الناجح
                
    else:
        query.answer("⚠️ حدث خطأ: لم يتم العثور على الفائدة.")
        query.edit_message_text(
            text="⚠️ حدث خطأ: لم يتم العثور على الفائدة.",
            reply_markup=None,
        )

    WAITING_BENEFIT_DELETE_CONFIRM.discard(user_id)
    BENEFIT_EDIT_ID.pop(user_id, None)

    try:
        benefit_id = int(query.data.split("_")[-1])
    except ValueError:
        query.answer("خطأ في تحديد الفائدة.")
        return

    benefits = get_benefits()
    
    # التحقق من الصلاحية: إما صاحب الفائدة أو مدير/مشرف
    is_privileged = is_admin(user_id) or is_supervisor(user_id)
    
    # البحث عن الفائدة
    benefit_to_delete = next((b for b in benefits if b.get("id") == benefit_id), None)
    
    if benefit_to_delete is None:
        query.answer("هذه الفائدة غير موجودة.")
        query.edit_message_text(
            text="⚠️ حدث خطأ: هذه الفائدة غير موجودة.",
            reply_markup=None,
        )
        WAITING_BENEFIT_DELETE_CONFIRM.discard(user_id)
        BENEFIT_EDIT_ID.pop(user_id, None)
        return

    is_owner = benefit_to_delete.get("user_id") == user_id
    
    if not is_owner and not is_privileged:
        query.answer("لا تملك صلاحية حذف هذه الفائدة.")
        query.edit_message_text(
            text="⚠️ حدث خطأ: لا تملك صلاحية حذف هذه الفائدة.",
            reply_markup=None,
        )
        WAITING_BENEFIT_DELETE_CONFIRM.discard(user_id)
        BENEFIT_EDIT_ID.pop(user_id, None)
        return

    # حذف الفائدة
    initial_count = len(benefits)
    benefits[:] = [b for b in benefits if b.get("id") != benefit_id]
    
    if len(benefits) < initial_count:
        save_benefits(benefits)
        query.answer("✅ تم حذف الفائدة بنجاح.")
        query.edit_message_text(
            text=f"✅ تم حذف الفائدة رقم {benefit_id} بنجاح.",
            reply_markup=None,
        )
    else:
        query.answer("⚠️ حدث خطأ: لم يتم العثور على الفائدة.")
        query.edit_message_text(
            text="⚠️ حدث خطأ: لم يتم العثور على الفائدة.",
            reply_markup=None,
        )

    WAITING_BENEFIT_DELETE_CONFIRM.discard(user_id)
    BENEFIT_EDIT_ID.pop(user_id, None)


def handle_top10_benefits(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    if record.get("is_banned", False):
        return

    benefits = get_benefits()
    
    if not benefits:
        update.message.reply_text(
            "لا توجد فوائد مضافة بعد لتصنيفها. 💡",
            reply_markup=BENEFITS_MENU_KB,
        )
        return

    # ترتيب الفوائد حسب عدد الإعجابات تنازليًا
    sorted_benefits = sorted(benefits, key=lambda b: b.get("likes_count", 0), reverse=True)
    
    text = "🏆 أفضل 10 فوائد ونصائح (حسب الإعجابات):\n\n"
    
    for i, benefit in enumerate(sorted_benefits[:10], start=1):
        text += f"{i}. *{benefit['text']}*\n"
        text += f"   - من: {benefit['first_name']} | الإعجابات: {benefit['likes_count']} 👍\n\n"
        
    update.message.reply_text(
        text=text,
        reply_markup=BENEFITS_MENU_KB,
        parse_mode="Markdown",
    )


def check_and_award_medal(context: CallbackContext):
    """
    دالة تفحص أفضل 10 فوائد وتمنح الوسام لصاحبها إذا لم يكن لديه.
    """
    benefits = get_benefits()
    if not benefits:
        return

    # ترتيب الفوائد حسب عدد الإعجابات تنازليًا
    sorted_benefits = sorted(benefits, key=lambda b: b.get("likes_count", 0), reverse=True)
    
    top_10_user_ids = set()
    for benefit in sorted_benefits[:10]:
        top_10_user_ids.add(benefit["user_id"])
        
    MEDAL_TEXT = "وسام صاحب فائدة من العشرة الأوائل 💡🏅"
    
    for user_id in top_10_user_ids:
        uid_str = str(user_id)
        if uid_str in data:
            record = data[uid_str]
            medals = record.get("medals", [])
            
            if MEDAL_TEXT not in medals:
                medals.append(MEDAL_TEXT)
                record["medals"] = medals
                save_data()
                
                # إرسال رسالة تهنئة
                try:
                    context.bot.send_message(
                        chat_id=user_id,
                        text=f"تهانينا! 🎉\n"
                             f"لقد حصلت على وسام جديد: *{MEDAL_TEXT}*\n"
                             f"أحد فوائدك وصل إلى قائمة أفضل 10 فوائد. استمر في المشاركة! 🤍",
                        parse_mode="Markdown",
                    )
                except Exception as e:
                    logger.error(f"Error sending medal message to {user_id}: {e}")


def handle_admin_delete_benefit_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    
    # التحقق من الصلاحية
    if not (is_admin(user_id) or is_supervisor(user_id)):
        query.answer("لا تملك صلاحية حذف فوائد الآخرين.")
        return

    try:
        benefit_id = int(query.data.split("_")[-1])
    except ValueError:
        query.answer("خطأ في تحديد الفائدة.")
        return

    benefits = get_benefits()
    benefit = next((b for b in benefits if b.get("id") == benefit_id), None)
    
    if benefit is None:
        query.answer("هذه الفائدة غير موجودة.")
        return

    # حفظ ID الفائدة وحالة الانتظار للتأكيد
    # نستخدم BENEFIT_EDIT_ID لتخزين ID الفائدة المراد حذفها مؤقتًا
    BENEFIT_EDIT_ID[user_id] = benefit_id
    WAITING_BENEFIT_DELETE_CONFIRM.add(user_id)
    
    query.answer("تأكيد الحذف.")
    
    keyboard = [[
        InlineKeyboardButton("✅ نعم، متأكد من الحذف", callback_data=f"confirm_admin_delete_benefit_{benefit_id}"),
        InlineKeyboardButton("❌ لا، إلغاء", callback_data="cancel_admin_delete_benefit")
    ]]
    
    context.bot.send_message(
        chat_id=user_id,
        text=f"⚠️ هل أنت متأكد من حذف الفائدة رقم {benefit_id} للمستخدم {benefit['first_name']}؟\n"
             f"النص: *{benefit['text']}*",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown",
    )


def handle_like_benefit_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    
    if query.data.startswith("like_benefit_"):
        try:
            benefit_id = int(query.data.split("_")[-1])
        except ValueError:
            query.answer("خطأ في تحديد الفائدة.")
            return

        benefits = get_benefits()
        benefit_index = -1
        benefit = None
        
        for i, b in enumerate(benefits):
            if b.get("id") == benefit_id:
                benefit_index = i
                benefit = b
                break
        
        if benefit is None:
            query.answer("هذه الفائدة لم تعد موجودة.")
            return

        liked_by = benefit.get("liked_by", [])
        
        if user_id in liked_by:
            query.answer("لقد أعجبت بهذه الفائدة مسبقًا.")
            return
            
        # لا يمكن الإعجاب بفائدة كتبها المستخدم نفسه
        if user_id == benefit["user_id"]:
            query.answer("لا يمكنك الإعجاب بفائدتك الخاصة.")
            return
        
        # 1. إضافة الإعجاب
        liked_by.append(user_id)
        benefit["likes_count"] = benefit.get("likes_count", 0) + 1
        benefit["liked_by"] = liked_by
        
        # 2. منح نقطة لصاحب الفائدة
        owner_id = benefit["user_id"]
        add_points(owner_id, 1)
        
        # 3. حفظ التغييرات
        benefits[benefit_index] = benefit
        save_benefits(benefits)
        
        # 4. تحديث زر الإعجاب
        new_likes_count = benefit["likes_count"]
        new_button_text = f"✅ أعجبتني ({new_likes_count})"
        
        keyboard = [[
            InlineKeyboardButton(
                new_button_text, 
                callback_data=f"like_benefit_{benefit_id}"
            )
        ]]
        
        try:
            query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"Error editing message reply markup: {e}")
            
        query.answer(f"تم الإعجاب! الفائدة لديها الآن {new_likes_count} إعجاب.")
        
        # 5. فحص ومنح الوسام
        check_and_award_medal(context)


# =================== الاشعارات / الجرعة التحفيزية للمستخدم ===================


def open_notifications_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    record = get_user_record(user)
    kb = notifications_menu_keyboard(user.id)

    status = "مفعّلة ✅" if record.get("motivation_on", True) else "موقفة ⛔️"

    update.message.reply_text(
        "الاشعارات 🔔:\n"
        f"• حالة الجرعة التحفيزية الحالية: {status}\n\n"
        "الجرعة التحفيزية هي رسائل قصيرة ولطيفة خلال اليوم تشرح القلب "
        "وتعينك على الاستمرار في الماء والقرآن والذكر 🤍\n\n"
        "يمكنك تشغيلها أو إيقافها من الأزرار بالأسفل.",
        reply_markup=kb,
    )


def handle_motivation_on(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    record = get_user_record(user)
    record["motivation_on"] = True
    save_data()

    update.message.reply_text(
        "تم تشغيل الجرعة التحفيزية ✨\n"
        "ستصلك رسائل تحفيزية في أوقات مختلفة من اليوم 🤍",
        reply_markup=notifications_menu_keyboard(user.id),
    )


def handle_motivation_off(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    record = get_user_record(user)
    record["motivation_on"] = False
    save_data()

    update.message.reply_text(
        "تم إيقاف الجرعة التحفيزية 😴\n"
        "يمكنك تشغيلها مرة أخرى من نفس المكان متى أحببت.",
        reply_markup=notifications_menu_keyboard(user.id),
    )

# =================== تذكيرات الماء ===================

REMINDER_HOURS_UTC = [7, 10, 13, 16, 19]


def water_reminder_job(context: CallbackContext):
    logger.info("Running water reminder job...")
    bot = context.bot

    for uid in get_active_user_ids():
        rec = data.get(str(uid)) or {}
        if not rec.get("reminders_on"):
            continue

        ensure_today_water(rec)
        cups_goal = rec.get("cups_goal")
        today_cups = rec.get("today_cups", 0)
        if not cups_goal:
            continue

        remaining = max(cups_goal - today_cups, 0)

        try:
            bot.send_message(
                chat_id=uid,
                text=(
                    "تذكير لطيف بشرب الماء 💧:\n\n"
                    f"شربت حتى الآن: {today_cups} من {cups_goal} كوب.\n"
                    f"المتبقي لهذا اليوم تقريبًا: {remaining} كوب.\n\n"
                    "لو استطعت الآن، خذ كوب ماء وسجّله في البوت."
                ),
            )
        except Exception as e:
            logger.error(f"Error sending water reminder to {uid}: {e}")

# =================== الجرعة التحفيزية (JobQueue + إدارة) ===================


def motivation_job(context: CallbackContext):
    logger.info("Running motivation job...")
    bot = context.bot

    for uid in get_active_user_ids():
        rec = data.get(str(uid)) or {}

        if rec.get("motivation_on") is False:
            continue

        if not MOTIVATION_MESSAGES:
            continue

        msg = random.choice(MOTIVATION_MESSAGES)

        try:
            bot.send_message(
                chat_id=uid,
                text=msg,
            )
        except Exception as e:
            logger.error(f"Error sending motivation message to {uid}: {e}")

# ======== لوحة التحكم لإدارة الجرعة التحفيزية (أدمن + مشرفة) ========


def open_admin_motivation_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    if not (is_admin(user.id) or is_supervisor(user.id)):
        update.message.reply_text(
            "هذا القسم خاص بالإدارة فقط.",
            reply_markup=user_main_keyboard(user.id),
        )
        return

    hours_text = ", ".join(str(h) for h in MOTIVATION_HOURS_UTC) if MOTIVATION_HOURS_UTC else "لا توجد أوقات مضبوطة"
    count = len(MOTIVATION_MESSAGES)

    update.message.reply_text(
        "إعدادات الجرعة التحفيزية 💡:\n\n"
        f"- عدد الرسائل الحالية: {count}\n"
        f"- الأوقات الحالية (بتوقيت UTC): {hours_text}\n\n"
        "يمكنك من هنا:\n"
        "• عرض كل الرسائل.\n"
        "• إضافة رسالة جديدة.\n"
        "• حذف رسالة.\n"
        "• تعديل أوقات الإرسال.",
        reply_markup=ADMIN_MOTIVATION_KB,
    )


def handle_admin_motivation_list(update: Update, context: CallbackContext):
    user = update.effective_user
    if not (is_admin(user.id) or is_supervisor(user.id)):
        return

    if not MOTIVATION_MESSAGES:
        text = "لا توجد رسائل جرعة تحفيزية حاليًا."
    else:
        lines = ["قائمة رسائل الجرعة التحفيزية الحالية 📜:\n"]
        for idx, m in enumerate(MOTIVATION_MESSAGES, start=1):
            lines.append(f"{idx}) {m}")
        text = "\n".join(lines)

    update.message.reply_text(
        text,
        reply_markup=ADMIN_MOTIVATION_KB,
    )


def handle_admin_motivation_add_start(update: Update, context: CallbackContext):
    user = update.effective_user
    if not (is_admin(user.id) or is_supervisor(user.id)):
        return

    WAITING_MOTIVATION_ADD.add(user.id)
    WAITING_MOTIVATION_DELETE.discard(user.id)
    WAITING_MOTIVATION_TIMES.discard(user.id)

    update.message.reply_text(
        "اكتب الآن نص الرسالة التحفيزية الجديدة التي تريد إضافتها 🌟\n\n"
        "يمكنك كتابة جملة قصيرة، دعاء، أو عبارة تشجيعية.",
        reply_markup=CANCEL_KB,
    )


def handle_admin_motivation_add_input(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    if not (is_admin(user_id) or is_supervisor(user_id)):
        WAITING_MOTIVATION_ADD.discard(user_id)
        return

    text = (update.message.text or "").strip()

    if text == BTN_CANCEL:
        WAITING_MOTIVATION_ADD.discard(user_id)
        open_admin_motivation_menu(update, context)
        return

    if not text:
        update.message.reply_text(
            "الرجاء إرسال نص غير فارغ 😊",
            reply_markup=CANCEL_KB,
        )
        return

    MOTIVATION_MESSAGES.append(text)

    cfg = get_global_config()
    cfg["motivation_messages"] = MOTIVATION_MESSAGES
    save_data()

    WAITING_MOTIVATION_ADD.discard(user_id)

    update.message.reply_text(
        "تمت إضافة الرسالة التحفيزية بنجاح ✅",
        reply_markup=ADMIN_MOTIVATION_KB,
    )
    handle_admin_motivation_list(update, context)


def handle_admin_motivation_delete_start(update: Update, context: CallbackContext):
    user = update.effective_user
    if not (is_admin(user.id) or is_supervisor(user.id)):
        return

    if not MOTIVATION_MESSAGES:
        update.message.reply_text(
            "لا توجد رسائل لحذفها حاليًا.",
            reply_markup=ADMIN_MOTIVATION_KB,
        )
        return

    WAITING_MOTIVATION_DELETE.add(user.id)
    WAITING_MOTIVATION_ADD.discard(user.id)
    WAITING_MOTIVATION_TIMES.discard(user.id)

    lines = ["🗑 اختر رقم الرسالة التي تريد حذفها:\n"]
    for idx, m in enumerate(MOTIVATION_MESSAGES, start=1):
        lines.append(f"{idx}) {m}")
    lines.append("\nأرسل رقم الرسالة، أو اضغط «إلغاء ❌».")
    update.message.reply_text(
        "\n".join(lines),
        reply_markup=CANCEL_KB,
    )


def handle_admin_motivation_delete_input(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    if not (is_admin(user_id) or is_supervisor(user_id)):
        WAITING_MOTIVATION_DELETE.discard(user_id)
        return

    text = (update.message.text or "").strip()

    if text == BTN_CANCEL:
        WAITING_MOTIVATION_DELETE.discard(user_id)
        open_admin_motivation_menu(update, context)
        return

    try:
        idx = int(text) - 1
        if idx < 0 or idx >= len(MOTIVATION_MESSAGES):
            raise ValueError()
    except ValueError:
        update.message.reply_text(
            "رجاءً أرسل رقم صحيح من القائمة، أو اضغط «إلغاء ❌».",
            reply_markup=CANCEL_KB,
        )
        return

    deleted = MOTIVATION_MESSAGES.pop(idx)

    cfg = get_global_config()
    cfg["motivation_messages"] = MOTIVATION_MESSAGES
    save_data()

    WAITING_MOTIVATION_DELETE.discard(user_id)

    update.message.reply_text(
        f"🗑 تم حذف الرسالة التالية:\n\n{deleted}",
        reply_markup=ADMIN_MOTIVATION_KB,
    )
    handle_admin_motivation_list(update, context)


def handle_admin_motivation_times_start(update: Update, context: CallbackContext):
    user = update.effective_user
    if not (is_admin(user.id) or is_supervisor(user.id)):
        return

    WAITING_MOTIVATION_TIMES.add(user.id)
    WAITING_MOTIVATION_ADD.discard(user.id)
    WAITING_MOTIVATION_DELETE.discard(user.id)

    current = ", ".join(str(h) for h in MOTIVATION_HOURS_UTC) if MOTIVATION_HOURS_UTC else "لا توجد"
    update.message.reply_text(
        "تعديل أوقات الجرعة التحفيزية ⏰\n\n"
        f"الأوقات الحالية (بتوقيت UTC): {current}\n\n"
        "أرسل الأوقات الجديدة بالأرقام (0–23) مفصولة بفواصل، مثال:\n"
        "`6,9,12,15,18,21`\n\n"
        "أو اضغط «إلغاء ❌».",
        reply_markup=CANCEL_KB,
        parse_mode="Markdown",
    )


def handle_admin_motivation_times_input(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    if not (is_admin(user_id) or is_supervisor(user_id)):
        WAITING_MOTIVATION_TIMES.discard(user_id)
        return

    msg = update.message
    text = (msg.text or "").strip()

    if text == BTN_CANCEL:
        WAITING_MOTIVATION_TIMES.discard(user_id)
        open_admin_motivation_menu(update, context)
        return

    parts = re.findall(r"\d+", text)
    hours = sorted({int(p) for p in parts if 0 <= int(p) <= 23})

    if not hours:
        msg.reply_text(
            "رجاءً أرسل ساعات صحيحة بين 0 و 23 مثل: 6,9,12,15,18,21",
            reply_markup=CANCEL_KB,
        )
        return

    global MOTIVATION_HOURS_UTC, CURRENT_MOTIVATION_JOBS
    MOTIVATION_HOURS_UTC = hours

    cfg = get_global_config()
    cfg["motivation_hours"] = MOTIVATION_HOURS_UTC
    save_data()

    for job in list(CURRENT_MOTIVATION_JOBS):
        try:
            job.schedule_removal()
        except Exception:
            pass
    CURRENT_MOTIVATION_JOBS = []

    for h in MOTIVATION_HOURS_UTC:
        try:
            job = context.job_queue.run_daily(
                motivation_job,
                time=time(hour=h, minute=0, tzinfo=pytz.UTC),
                name=f"motivation_job_{h}",
            )
            CURRENT_MOTIVATION_JOBS.append(job)
        except Exception as e:
            logger.error(f"Error scheduling motivation job at hour {h}: {e}")

    WAITING_MOTIVATION_TIMES.discard(user_id)

    hours_text = ", ".join(str(h) for h in MOTIVATION_HOURS_UTC)
    msg.reply_text(
        f"تم تحديث أوقات الجرعة التحفيزية بنجاح ✅\n"
        f"الأوقات الجديدة (بتوقيت UTC): {hours_text}",
        reply_markup=ADMIN_MOTIVATION_KB,
    )

# =================== المنافسات و المجتمع ===================


def open_comp_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    update.message.reply_text(
        "المنافسات و المجتمع 🏅:\n"
        "• شاهد ملفك التنافسي (مستواك، نقاطك، ميدالياتك، ترتيبك).\n"
        "• اطّلع على أفضل 10 و أفضل 100 مستخدم.\n"
        "كل عمل صالح تسجّله هنا يرفعك في لوحة الشرف 🤍",
        reply_markup=COMP_MENU_KB,
    )


def handle_my_profile(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id
    record = get_user_record(user)

    points = record.get("points", 0)
    level = record.get("level", 0)
    medals = record.get("medals", []) or []
    best_rank = record.get("best_rank")

    sorted_users = get_users_sorted_by_points()
    rank = None
    for idx, rec in enumerate(sorted_users, start=1):
        if rec.get("user_id") == user_id:
            rank = idx
            break

    lines = [
        "ملفي التنافسي 🎯:\n",
        f"- النقاط الكلية: 🎯 {points} نقطة",
    ]

    if level <= 0:
        lines.append("- المستوى الحالي: 0 (أول مستوى يبدأ من 20 نقطة).")
    else:
        lines.append(f"- المستوى الحالي: {level}")

    if rank is not None:
        lines.append(f"- ترتيبي الحالي: #{rank}")
    if best_rank is not None:
        lines.append(f"- أفضل ترتيب وصلت له: #{best_rank}")

    if medals:
        lines.append("\n- ميدالياتي:")
        lines.append("  " + " — ".join(medals))
    else:
        lines.append("\n- ميدالياتي: (لا توجد ميداليات بعد)")

    update.message.reply_text(
        "\n".join(lines),
        reply_markup=COMP_MENU_KB,
    )


def handle_top10(update: Update, context: CallbackContext):
    sorted_users = get_users_sorted_by_points()
    # استبعاد المستخدمين المحظورين
    top = [user for user in sorted_users if not user.get("is_banned", False)][:10]

    if not top:
        update.message.reply_text(
            "لا توجد بيانات منافسة كافية حتى الآن.",
            reply_markup=COMP_MENU_KB,
        )
        return

    lines = ["🏅 أفضل 10 مستخدمين:\n"]
    for idx, rec in enumerate(top, start=1):
        name = rec.get("first_name") or "مستخدم"
        points = rec.get("points", 0)
        medals = rec.get("medals", []) or []

        lines.append(f"{idx}) {name} — 🎯 {points} نقطة")

        if medals:
            medals_line = " — ".join(medals)
        else:
            medals_line = "(لا توجد ميداليات بعد)"
        lines.append(medals_line)
        lines.append("")

    update.message.reply_text(
        "\n".join(lines),
        reply_markup=COMP_MENU_KB,
    )


def handle_top100(update: Update, context: CallbackContext):
    sorted_users = get_users_sorted_by_points()
    # استبعاد المستخدمين المحظورين
    top = [user for user in sorted_users if not user.get("is_banned", False)][:100]

    if not top:
        update.message.reply_text(
            "لا توجد بيانات منافسة كافية حتى الآن.",
            reply_markup=COMP_MENU_KB,
        )
        return

    lines = ["🏆 أفضل 100 مستخدم:\n"]
    for idx, rec in enumerate(top, start=1):
        name = rec.get("first_name") or "مستخدم"
        points = rec.get("points", 0)
        medals = rec.get("medals", []) or []

        lines.append(f"{idx}) {name} — 🎯 {points} نقطة")

        if medals:
            medals_line = " — ".join(medals)
        else:
            medals_line = "(لا توجد ميداليات بعد)"
        lines.append(medals_line)
        lines.append("")

    update.message.reply_text(
        "\n".join(lines),
        reply_markup=COMP_MENU_KB,
    )

# =================== نظام الحظر ===================


def handle_admin_ban_user(update: Update, context: CallbackContext):
    user = update.effective_user
    if not (is_admin(user.id) or is_supervisor(user.id)):
        update.message.reply_text(
            "هذا القسم خاص بالإدارة فقط.",
            reply_markup=user_main_keyboard(user.id),
        )
        return

    WAITING_BAN_USER.add(user.id)
    WAITING_UNBAN_USER.discard(user.id)
    WAITING_BAN_REASON.discard(user.id)
    BAN_TARGET_ID.pop(user.id, None)

    update.message.reply_text(
        "⚡ حظر مستخدم:\n\n"
        "أرسل الآن معرف المستخدم (ID) الذي تريد حظره.\n"
        "يمكنك الحصول على ID من «قائمة المستخدمين 📄» أو من الرد على رسالة المستخدم.\n\n"
        "أو اضغط «إلغاء ❌».",
        reply_markup=CANCEL_KB,
    )


def handle_admin_unban_user(update: Update, context: CallbackContext):
    user = update.effective_user
    if not (is_admin(user.id) or is_supervisor(user.id)):
        update.message.reply_text(
            "هذا القسم خاص بالإدارة فقط.",
            reply_markup=user_main_keyboard(user.id),
        )
        return

    WAITING_UNBAN_USER.add(user.id)
    WAITING_BAN_USER.discard(user.id)
    WAITING_BAN_REASON.discard(user.id)
    BAN_TARGET_ID.pop(user.id, None)

    banned_users = get_banned_user_ids()
    if not banned_users:
        update.message.reply_text(
            "لا يوجد مستخدمون محظورون حاليًا.",
            reply_markup=admin_panel_keyboard_for(user.id),
        )
        WAITING_UNBAN_USER.discard(user.id)
        return

    banned_list = []
    for uid in banned_users[:50]:  # عرض أول 50 فقط
        rec = data.get(str(uid), {})
        name = rec.get("first_name", "مستخدم") or "مستخدم"
        ban_reason = rec.get("ban_reason", "بدون سبب") or "بدون سبب"
        banned_at = rec.get("banned_at", "غير محدد") or "غير محدد"
        banned_list.append(f"• {name} (ID: {uid})\n  السبب: {ban_reason}\n  التاريخ: {banned_at}")

    update.message.reply_text(
        "✅ فك حظر مستخدم:\n\n"
        "قائمة المستخدمين المحظورين:\n\n" + "\n\n".join(banned_list) + "\n\n"
        "أرسل الآن معرف المستخدم (ID) الذي تريد فك حظره.\n"
        "أو اضغط «إلغاء ❌».",
        reply_markup=CANCEL_KB,
    )


def handle_admin_banned_list(update: Update, context: CallbackContext):
    user = update.effective_user
    if not (is_admin(user.id) or is_supervisor(user.id)):
        update.message.reply_text(
            "هذا القسم خاص بالإدارة فقط.",
            reply_markup=user_main_keyboard(user.id),
        )
        return

    banned_users = get_banned_user_ids()
    if not banned_users:
        update.message.reply_text(
            "لا يوجد مستخدمون محظورون حاليًا 🎉",
            reply_markup=admin_panel_keyboard_for(user.id),
        )
        return

    banned_list = []
    total = len(banned_users)
    
    for idx, uid in enumerate(banned_users[:100], start=1):  # عرض أول 100 فقط
        rec = data.get(str(uid), {})
        name = rec.get("first_name", "مستخدم") or "مستخدم"
        username = rec.get("username", "لا يوجد")
        ban_reason = rec.get("ban_reason", "بدون سبب") or "بدون سبب"
        banned_at = rec.get("banned_at", "غير محدد") or "غير محدد"
        banned_by = rec.get("banned_by", "غير معروف")
        
        banned_by_name = "إدارة البوت"
        if banned_by:
            banned_by_rec = data.get(str(banned_by), {})
            banned_by_name = banned_by_rec.get("first_name", "إدارة البوت") or "إدارة البوت"
        
        user_info = f"{idx}. {name}"
        if username and username != "لا يوجد":
            user_info += f" (@{username})"
        user_info += f" (ID: {uid})"
        
        banned_list.append(
            f"{user_info}\n"
            f"   السبب: {ban_reason}\n"
            f"   التاريخ: {banned_at}\n"
            f"   المحظور بواسطة: {banned_by_name}"
        )

    text = f"🚫 قائمة المستخدمين المحظورين (الإجمالي: {total}):\n\n" + "\n\n".join(banned_list)
    
    if total > 100:
        text += f"\n\n... وهناك {total - 100} مستخدم محظور إضافي."

    update.message.reply_text(
        text,
        reply_markup=admin_panel_keyboard_for(user.id),
    )


def handle_ban_user_id_input(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    if not (is_admin(user_id) or is_supervisor(user_id)):
        WAITING_BAN_USER.discard(user_id)
        return

    text = (update.message.text or "").strip()

    if text == BTN_CANCEL:
        WAITING_BAN_USER.discard(user_id)
        handle_admin_panel(update, context)
        return

    try:
        target_id = int(text)
        
        # منع حظر الأدمن أو المشرفة
        if target_id == ADMIN_ID or target_id == SUPERVISOR_ID:
            update.message.reply_text(
                "❌ لا يمكن حظر الأدمن أو المشرفة!",
                reply_markup=CANCEL_KB,
            )
            return
            
        # منع حظر النفس
        if target_id == user_id:
            update.message.reply_text(
                "❌ لا يمكنك حظر نفسك!",
                reply_markup=CANCEL_KB,
            )
            return

        target_record = data.get(str(target_id))
        if not target_record:
            update.message.reply_text(
                "❌ المستخدم غير موجود في قاعدة البيانات.",
                reply_markup=CANCEL_KB,
            )
            return

        if target_record.get("is_banned", False):
            update.message.reply_text(
                "⚠️ هذا المستخدم محظور بالفعل.",
                reply_markup=CANCEL_KB,
            )
            return

        BAN_TARGET_ID[user_id] = target_id
        WAITING_BAN_USER.discard(user_id)
        WAITING_BAN_REASON.add(user_id)

        target_name = target_record.get("first_name", "مستخدم") or "مستخدم"
        update.message.reply_text(
            f"📝 المستخدم المحدد: {target_name} (ID: {target_id})\n\n"
            "الآن أرسل سبب الحظر:\n"
            "(مثال: مخالفة الشروط، إساءة استخدام، إلخ)",
            reply_markup=CANCEL_KB,
        )

    except ValueError:
        update.message.reply_text(
            "❌ رجاءً أرسل معرف مستخدم صحيح (أرقام فقط).\n"
            "مثال: 123456789",
            reply_markup=CANCEL_KB,
        )


def handle_unban_user_id_input(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    if not (is_admin(user_id) or is_supervisor(user_id)):
        WAITING_UNBAN_USER.discard(user_id)
        return

    text = (update.message.text or "").strip()

    if text == BTN_CANCEL:
        WAITING_UNBAN_USER.discard(user_id)
        handle_admin_panel(update, context)
        return

    try:
        target_id = int(text)
        
        target_record = data.get(str(target_id))
        if not target_record:
            update.message.reply_text(
                "❌ المستخدم غير موجود في قاعدة البيانات.",
                reply_markup=CANCEL_KB,
            )
            return

        if not target_record.get("is_banned", False):
            update.message.reply_text(
                "✅ هذا المستخدم غير محظور أصلاً.",
                reply_markup=CANCEL_KB,
            )
            return

        # فك الحظر
        target_record["is_banned"] = False
        target_record["banned_by"] = None
        target_record["banned_at"] = None
        target_record["ban_reason"] = None
        save_data()

        WAITING_UNBAN_USER.discard(user_id)

        target_name = target_record.get("first_name", "مستخدم") or "مستخدم"
        
        # إرسال رسالة للمستخدم المحظور سابقاً
        try:
            context.bot.send_message(
                chat_id=target_id,
                text=f"🎉 تم فك حظرك من بوت سُقيا الكوثر!\n\n"
                     f"يمكنك الآن استخدام البوت مرة أخرى 🤍\n\n"
                     f"نرحب بك مجدداً ونتمنى لك تجربة مفيدة."
            )
        except Exception as e:
            logger.error(f"Error notifying unbanned user {target_id}: {e}")

        update.message.reply_text(
            f"✅ تم فك حظر المستخدم: {target_name} (ID: {target_id}) بنجاح.",
            reply_markup=admin_panel_keyboard_for(user_id),
        )

    except ValueError:
        update.message.reply_text(
            "❌ رجاءً أرسل معرف مستخدم صحيح (أرقام فقط).\n"
            "مثال: 123456789",
            reply_markup=CANCEL_KB,
        )


def handle_ban_reason_input(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    if not (is_admin(user_id) or is_supervisor(user_id)):
        WAITING_BAN_REASON.discard(user_id)
        return

    text = (update.message.text or "").strip()

    if text == BTN_CANCEL:
        WAITING_BAN_REASON.discard(user_id)
        BAN_TARGET_ID.pop(user_id, None)
        handle_admin_panel(update, context)
        return

    if user_id not in BAN_TARGET_ID:
        WAITING_BAN_REASON.discard(user_id)
        update.message.reply_text(
            "حدث خطأ، يرجى المحاولة مرة أخرى.",
            reply_markup=admin_panel_keyboard_for(user_id),
        )
        return

    target_id = BAN_TARGET_ID[user_id]
    target_record = data.get(str(target_id))
    
    if not target_record:
        WAITING_BAN_REASON.discard(user_id)
        BAN_TARGET_ID.pop(user_id, None)
        update.message.reply_text(
            "❌ المستخدم غير موجود!",
            reply_markup=admin_panel_keyboard_for(user_id),
        )
        return

    # تطبيق الحظر
    target_record["is_banned"] = True
    target_record["banned_by"] = user_id
    target_record["banned_at"] = datetime.now(timezone.utc).isoformat()
    target_record["ban_reason"] = text
    save_data()

    WAITING_BAN_REASON.discard(user_id)
    BAN_TARGET_ID.pop(user_id, None)

    target_name = target_record.get("first_name", "مستخدم") or "مستخدم"
    
    # إرسال رسالة للمستخدم المحظور
    try:
        context.bot.send_message(
            chat_id=target_id,
            text=f"⛔️ لقد تم حظرك من استخدام بوت سُقيا الكوثر!\n\n"
                 f"السبب: {text}\n\n"
                 f"للاستفسار يمكنك التواصل مع الدعم."
        )
    except Exception as e:
        logger.error(f"Error notifying banned user {target_id}: {e}")

    # إعلام الأدمن الآخر (إذا كان الحظر من المشرفة)
    if is_supervisor(user_id) and ADMIN_ID is not None:
        try:
            admin_name = data.get(str(user_id), {}).get("first_name", "المشرفة") or "المشرفة"
            context.bot.send_message(
                chat_id=ADMIN_ID,
                text=f"⚠️ تم حظر مستخدم بواسطة المشرفة:\n\n"
                     f"المستخدم: {target_name} (ID: {target_id})\n"
                     f"السبب: {text}\n"
                     f"بواسطة: {admin_name}"
            )
        except Exception as e:
            logger.error(f"Error notifying admin about ban: {e}")

    update.message.reply_text(
        f"✅ تم حظر المستخدم: {target_name} (ID: {target_id}) بنجاح.\n"
        f"السبب: {text}",
        reply_markup=admin_panel_keyboard_for(user_id),
    )

# =================== نظام الدعم ولوحة التحكم ===================


def handle_contact_support(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    record = get_user_record(user)
    user_id = user.id

    gender = record.get("gender")

    if gender in ["male", "female"]:
        WAITING_SUPPORT.add(user_id)
        update.message.reply_text(
            "✉️ اكتب الآن رسالتك التي تريد إرسالها للدعم.\n"
            "اشرح ما تحتاجه بهدوء، وسيتم الاطلاع عليها بإذن الله.\n\n"
            "للإلغاء اضغط «إلغاء ❌».",
            reply_markup=CANCEL_KB,
        )
        return

    WAITING_SUPPORT_GENDER.add(user_id)
    update.message.reply_text(
        "قبل إرسال رسالتك للدعم، اختر الجنس:\n\n"
        "🧔‍♂️ لو كنت رجلًا → تصل رسالتك للمشرف.\n"
        "👩 لو كنت امرأة → تصل رسالتك للمشرفة.\n\n"
        "اختر من الأزرار بالأسفل 👇",
        reply_markup=GENDER_KB,
    )


def handle_admin_panel(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id

    if not (is_admin(user_id) or is_supervisor(user_id)):
        update.message.reply_text(
            "هذا القسم خاص بالإدارة فقط.",
            reply_markup=user_main_keyboard(user_id),
        )
        return

    if is_admin(user_id):
        text = (
            "لوحة التحكم 🛠:\n"
            "• عرض عدد المستخدمين.\n"
            "• عرض قائمة المستخدمين.\n"
            "• إرسال رسالة جماعية.\n"
            "• عرض ترتيب المنافسة تفصيليًا.\n"
            "• حظر وفك حظر المستخدمين.\n"
            "• عرض قائمة المحظورين.\n"
            "• إدارة رسائل وأوقات الجرعة التحفيزية 💡."
        )
    else:
        text = (
            "لوحة التحكم 🛠 (المشرفة):\n"
            "• إرسال رسالة جماعية لكل المستخدمين.\n"
            "• عرض عدد المستخدمين.\n"
            "• حظر وفك حظر المستخدمين.\n"
            "• عرض قائمة المحظورين.\n"
            "• إدارة رسائل وأوقات الجرعة التحفيزية 💡."
        )

    update.message.reply_text(
        text,
        reply_markup=admin_panel_keyboard_for(user_id),
    )


def handle_admin_users_count(update: Update, context: CallbackContext):
    user = update.effective_user
    if not (is_admin(user.id) or is_supervisor(user.id)):
        return

    total_users = len(get_all_user_ids())
    active_users = len(get_active_user_ids())
    banned_users = len(get_banned_user_ids())

    update.message.reply_text(
        f"📊 إحصائيات المستخدمين:\n\n"
        f"👥 إجمالي المستخدمين: {total_users}\n"
        f"✅ المستخدمين النشطين: {active_users}\n"
        f"🚫 المستخدمين المحظورين: {banned_users}",
        reply_markup=admin_panel_keyboard_for(user.id),
    )


def handle_admin_users_list(update: Update, context: CallbackContext):
    user = update.effective_user
    if not is_admin(user.id):
        return

    lines = []
    for uid_str, rec in data.items():
        if uid_str == GLOBAL_KEY:
            continue
        
        name = rec.get("first_name") or "بدون اسم"
        username = rec.get("username")
        is_banned = rec.get("is_banned", False)
        status = "🚫" if is_banned else "✅"
        
        line = f"{status} {name} | ID: {uid_str}"
        if username:
            line += f" | @{username}"
        
        if is_banned:
            line += " (محظور)"
        
        lines.append(line)

    if not lines:
        text = "لا يوجد مستخدمون مسجّلون بعد."
    else:
        text = "قائمة المستخدمين:\n\n" + "\n".join(lines[:200])

    update.message.reply_text(
        text,
        reply_markup=ADMIN_PANEL_KB,
    )


def handle_admin_broadcast_start(update: Update, context: CallbackContext):
    user = update.effective_user
    if not (is_admin(user.id) or is_supervisor(user.id)):
        return

    WAITING_BROADCAST.add(user.id)
    update.message.reply_text(
        "اكتب الآن الرسالة التي تريد إرسالها لكل مستخدمي البوت.\n"
        "مثال: تذكير، نصيحة، أو إعلان مهم.\n\n"
        "للإلغاء اضغط «إلغاء ❌».",
        reply_markup=CANCEL_KB,
    )


def handle_admin_broadcast_input(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    text = (update.message.text or "").strip()

    if text == BTN_CANCEL:
        WAITING_BROADCAST.discard(user_id)
        handle_admin_panel(update, context)
        return

    if not (is_admin(user_id) or is_supervisor(user_id)):
        WAITING_BROADCAST.discard(user_id)
        update.message.reply_text(
            "هذه الميزة خاصة بالإدارة فقط.",
            reply_markup=user_main_keyboard(user_id),
        )
        return

    user_ids = get_active_user_ids()  # إرسال فقط للمستخدمين النشطين (غير المحظورين)
    sent = 0
    failed = 0
    
    for uid in user_ids:
        try:
            update.effective_message.bot.send_message(
                chat_id=uid,
                text=f"📢 رسالة من الدعم:\n\n{text}",
            )
            sent += 1
        except Exception as e:
            logger.error(f"Error sending broadcast to {uid}: {e}")
            failed += 1

    WAITING_BROADCAST.discard(user_id)

    update.message.reply_text(
        f"✅ تم إرسال الرسالة إلى {sent} مستخدم.\n"
        f"❌ فشل إرسال الرسالة إلى {failed} مستخدم.",
        reply_markup=admin_panel_keyboard_for(user_id),
    )


def handle_admin_rankings(update: Update, context: CallbackContext):
    user = update.effective_user
    if not is_admin(user.id):
        return

    sorted_users = get_users_sorted_by_points()
    # استبعاد المستخدمين المحظورين
    top = [user for user in sorted_users if not user.get("is_banned", False)][:200]

    if not top:
        update.message.reply_text(
            "لا توجد بيانات منافسة كافية حتى الآن.",
            reply_markup=ADMIN_PANEL_KB,
        )
        return

    lines = ["📊 ترتيب المستخدمين بالنقاط (تفصيلي):\n"]
    for idx, rec in enumerate(top, start=1):
        name = rec.get("first_name") or "مستخدم"
        username = rec.get("username")
        uid = rec.get("user_id")
        level = rec.get("level", 0)
        points = rec.get("points", 0)
        medals = rec.get("medals", [])
        medals_text = "، ".join(medals) if medals else "لا توجد"

        line = f"{idx}) {name} (ID: {uid}"
        if username:
            line += f" | @{username}"
        line += f") — مستوى {level} — {points} نقطة — ميداليات: {medals_text}"
        lines.append(line)

    chunk = "\n".join(lines[:80])
    update.message.reply_text(
        chunk,
        reply_markup=ADMIN_PANEL_KB,
    )


def send_new_user_notification_to_admin(user: User, context: CallbackContext):
    """
    يرسل إشعارًا للأدمن عند انضمام مستخدم جديد.
    """
    if not ADMIN_ID:
        return

    username = f"@{user.username}" if user.username else "لا يوجد"
    join_time = datetime.now(pytz.timezone('Asia/Riyadh')).strftime("%Y-%m-%d | %I:%M %p")

    text = (
        f"🔔 مستخدم جديد دخل البوت 🎉\n\n"
        f"👤 الاسم: {user.first_name}\n"
        f"🆔 User ID: `{user.id}`\n"
        f"🧑‍💻 Username: {username}\n"
        f"🕒 الانضمام: {join_time}"
    )

    try:
        context.bot.send_message(
            chat_id=ADMIN_ID,
            text=text,
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error(f"Error sending new user notification to admin: {e}")


def send_new_user_notification_to_admin(user: User, context: CallbackContext):
    """
    يرسل إشعارًا للأدمن عند انضمام مستخدم جديد.
    """
    if not ADMIN_ID:
        return

    username = f"@{user.username}" if user.username else "لا يوجد"
    join_time = datetime.now(pytz.timezone('Asia/Riyadh')).strftime("%Y-%m-%d | %I:%M %p")

    text = (
        f"🔔 مستخدم جديد دخل البوت 🎉\n\n"
        f"👤 الاسم: {user.first_name}\n"
        f"🆔 User ID: `{user.id}`\n"
        f"🧑‍💻 Username: {username}\n"
        f"🕒 الانضمام: {join_time}"
    )

    try:
        context.bot.send_message(
            chat_id=ADMIN_ID,
            text=text,
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error(f"Error sending new user notification to admin: {e}")


def forward_support_to_admin(user, text: str, context: CallbackContext):
    uid = str(user.id)
    record = data.get(uid, {})
    gender = record.get("gender")

    admin_msg = (
        "📩 رسالة جديدة للدعم:\n\n"
        f"الاسم: {user.full_name}\n"
        f"اسم المستخدم: @{user.username if user.username else 'لا يوجد'}\n"
        f"ID: `{user.id}`\n"
        f"الجنس: {'ذكر' if gender == 'male' else 'أنثى' if gender == 'female' else 'غير محدد'}\n\n"
        f"محتوى الرسالة:\n{text}"
    )

    if ADMIN_ID is not None:
        try:
            context.bot.send_message(
                chat_id=ADMIN_ID,
                text=admin_msg,
                parse_mode="Markdown",
            )
        except Exception as e:
            logger.error(f"Error sending support message to admin: {e}")

    if gender == "female" and SUPERVISOR_ID is not None:
        supervisor_msg = (
            "📩 رسالة جديدة من أخت (دعم نسائي):\n\n"
            f"الاسم: {user.full_name}\n"
            f"اسم المستخدم: @{user.username if user.username else 'لا يوجد'}\n"
            f"ID: {user.id}\n"
            "الجنس: أنثى\n\n"
            f"محتوى الرسالة:\n{text}"
        )
        try:
            context.bot.send_message(
                chat_id=SUPERVISOR_ID,
                text=supervisor_msg,
            )
        except Exception as e:
            logger.error(f"Error sending support message to supervisor: {e}")


def try_handle_admin_reply(update: Update, context: CallbackContext) -> bool:
    user = update.effective_user
    msg = update.message
    text = (msg.text or "").strip()

    if not is_admin(user.id):
        return False

    if not msg.reply_to_message:
        return False

    original = msg.reply_to_message.text or ""
    m = re.search(r"ID:\s*`?(\d+)`?", original)
    if not m:
        return False

    target_id = int(m.group(1))
    try:
        context.bot.send_message(
            chat_id=target_id,
            text=f"💌 رد من الدعم:\n\n{text}",
        )
        msg.reply_text(
            "تم إرسال ردّك للمستخدم.",
            reply_markup=admin_panel_keyboard_for(user.id),
        )
    except Exception as e:
        logger.error(f"Error sending admin reply to {target_id}: {e}")
        msg.reply_text(
            "حدث خطأ أثناء إرسال الرد للمستخدم.",
            reply_markup=admin_panel_keyboard_for(user.id),
        )
    return True

# =================== هاندلر الرسائل ===================


def handle_text(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    msg = update.message
    text = (msg.text or "").strip()

    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا في بداية كل رسالة
    if record.get("is_banned", False):
        # السماح فقط بالرد على رسائل الدعم إذا كان محظوراً
        if msg.reply_to_message and msg.reply_to_message.from_user.id == context.bot.id:
            original = msg.reply_to_message.text or ""
            if "لقد تم حظرك" in original or "رد من الدعم" in original or "رد من المشرفة" in original:
                forward_support_to_admin(user, text, context)
                msg.reply_text(
                    "📨 رسالتك وصلت للدعم. سيتم الرد عليك قريبًا.",
                )
                return
        
        # منع أي استخدام آخر للبوت
        return
    
    main_kb = user_main_keyboard(user_id)

    # تحديد الجنس للدعم
    if user_id in WAITING_SUPPORT_GENDER:
        if text == BTN_GENDER_MALE:
            record["gender"] = "male"
            save_data()
            WAITING_SUPPORT_GENDER.discard(user_id)
            WAITING_SUPPORT.add(user_id)
            msg.reply_text(
                "جميل 🤍\n"
                "الآن اكتب رسالتك التي تريد إرسالها للدعم:",
                reply_markup=CANCEL_KB,
            )
            return
        elif text == BTN_GENDER_FEMALE:
            record["gender"] = "female"
            save_data()
            WAITING_SUPPORT_GENDER.discard(user_id)
            WAITING_SUPPORT.add(user_id)
            msg.reply_text(
                "جميل 🤍\n"
                "الآن اكتب رسالتك التي تريد إرسالها للدعم النسائي:",
                reply_markup=CANCEL_KB,
            )
            return
        elif text == BTN_CANCEL:
            WAITING_SUPPORT_GENDER.discard(user_id)
            msg.reply_text(
                "تم الإلغاء. عدنا للقائمة الرئيسية.",
                reply_markup=main_kb,
            )
            return
        else:
            msg.reply_text(
                "رجاءً اختر من الأزرار الموجودة 👇",
                reply_markup=GENDER_KB,
            )
            return

    # رد المشرفة
    if is_supervisor(user_id) and msg.reply_to_message:
        original = msg.reply_to_message.text or ""
        m = re.search(r"ID:\s*`?(\d+)`?", original)
        if m:
            target_id = int(m.group(1))
            try:
                context.bot.send_message(
                    chat_id=target_id,
                    text=f"💌 رد من المشرفة:\n\n{text}",
                )
                if ADMIN_ID is not None:
                    try:
                        context.bot.send_message(
                            chat_id=ADMIN_ID,
                            text=(
                                "📨 نسخة من رد المشرفة:\n\n"
                                f"إلى ID: {target_id}\n"
                                f"نص الرد:\n{text}"
                            ),
                        )
                    except Exception as e:
                        logger.error(f"Error sending supervisor reply copy to admin: {e}")

                msg.reply_text(
                    "✅ تم إرسال ردّك للأخت.",
                    reply_markup=main_kb,
                )
            except Exception as e:
                logger.error(f"Error sending supervisor reply to user {target_id}: {e}")
                msg.reply_text(
                    "⚠️ حدث خطأ أثناء إرسال الرد.",
                    reply_markup=main_kb,
                )
            return

    # رد الأدمن
    if try_handle_admin_reply(update, context):
        return

    # رد المستخدم على ردود الدعم
    if (
        not is_admin(user_id)
        and not is_supervisor(user_id)
        and msg.reply_to_message
        and msg.reply_to_message.from_user.id == context.bot.id
    ):
        original = msg.reply_to_message.text or ""
        if (
            original.startswith("💌 رد من الدعم")
            or original.startswith("📢 رسالة من الدعم")
            or original.startswith("💌 رد من المشرفة")
            or "رسالتك وصلت للدعم" in original
        ):
            forward_support_to_admin(user, text, context)
            msg.reply_text(
                "📨 ردّك وصل للدعم 🤍",
                reply_markup=main_kb,
            )
            return

    # زر إلغاء عام
    if text == BTN_CANCEL:
        # إزالة المستخدم من جميع حالات الانتظار
        WAITING_GENDER.discard(user_id)
        WAITING_AGE.discard(user_id)
        WAITING_WEIGHT.discard(user_id)
        WAITING_QURAN_GOAL.discard(user_id)
        WAITING_QURAN_ADD_PAGES.discard(user_id)
        WAITING_TASBIH.discard(user_id)
        ACTIVE_TASBIH.pop(user_id, None)
        WAITING_MEMO_MENU.discard(user_id)
        WAITING_MEMO_ADD.discard(user_id)
        WAITING_MEMO_EDIT_SELECT.discard(user_id)
        WAITING_MEMO_EDIT_TEXT.discard(user_id)
        WAITING_MEMO_DELETE_SELECT.discard(user_id)
        MEMO_EDIT_INDEX.pop(user_id, None)
        WAITING_LETTER_MENU.discard(user_id)
        WAITING_LETTER_ADD.discard(user_id)
        WAITING_LETTER_ADD_CONTENT.discard(user_id)
        WAITING_LETTER_REMINDER_OPTION.discard(user_id)
        WAITING_LETTER_CUSTOM_DATE.discard(user_id)
        WAITING_LETTER_DELETE_SELECT.discard(user_id)
        LETTER_CURRENT_DATA.pop(user_id, None)
        WAITING_SUPPORT_GENDER.discard(user_id)
        WAITING_SUPPORT.discard(user_id)
        WAITING_BROADCAST.discard(user_id)
        WAITING_MOTIVATION_ADD.discard(user_id)
        WAITING_MOTIVATION_DELETE.discard(user_id)
        WAITING_MOTIVATION_TIMES.discard(user_id)
        WAITING_BAN_USER.discard(user_id)
        WAITING_UNBAN_USER.discard(user_id)
        WAITING_BAN_REASON.discard(user_id)
        BAN_TARGET_ID.pop(user_id, None)
        
        # حالة خاصة: إلغاء تعديل الفائدة (المشكلة 1)
        if user_id in WAITING_BENEFIT_EDIT_TEXT:
            WAITING_BENEFIT_EDIT_TEXT.discard(user_id)
            BENEFIT_EDIT_ID.pop(user_id, None)
            update.message.reply_text(
                "❌ تم إلغاء التعديل.\nعدنا لقسم مجتمع الفوائد و النصائح.",
                reply_markup=BENEFITS_MENU_KB,
            )
            return
        
        # حالة خاصة: إلغاء إضافة فائدة
        if user_id in WAITING_BENEFIT_TEXT:
            WAITING_BENEFIT_TEXT.discard(user_id)
            update.message.reply_text(
                "تم إلغاء إضافة الفائدة.",
                reply_markup=BENEFITS_MENU_KB,
            )
            return
            
        # حالة خاصة: إلغاء تأكيد حذف الفائدة
        if user_id in WAITING_BENEFIT_DELETE_CONFIRM:
            WAITING_BENEFIT_DELETE_CONFIRM.discard(user_id)
            BENEFIT_EDIT_ID.pop(user_id, None)
            update.message.reply_text(
                "تم إلغاء عملية الحذف.",
                reply_markup=BENEFITS_MENU_KB,
            )
            return
        
        # إذا كان الإلغاء من أي مكان آخر، نعود للقائمة الرئيسية
        main_kb = user_main_keyboard(user_id)
        update.message.reply_text(
            "تم الإلغاء. عدنا للقائمة الرئيسية.",
            reply_markup=main_kb,
        )
        return

    # حالات إدخال الماء
    if user_id in WAITING_GENDER:
        handle_gender_input(update, context)
        return

    if user_id in WAITING_AGE:
        handle_age_input(update, context)
        return

    if user_id in WAITING_WEIGHT:
        handle_weight_input(update, context)
        return

    # حالات ورد القرآن
    if user_id in WAITING_QURAN_GOAL:
        handle_quran_goal_input(update, context)
        return

    if user_id in WAITING_QURAN_ADD_PAGES:
        handle_quran_add_pages_input(update, context)
        return

    # حالة السبحة
    if user_id in WAITING_TASBIH:
        if text == BTN_TASBIH_TICK:
            handle_tasbih_tick(update, context)
            return
        elif text == BTN_TASBIH_END:
            handle_tasbih_end(update, context)
            return
        else:
            handle_tasbih_tick(update, context)
            return

    # مذكّرات قلبي
    if user_id in WAITING_MEMO_ADD:
        handle_memo_add_input(update, context)
        return

    if user_id in WAITING_MEMO_EDIT_SELECT:
        handle_memo_edit_index_input(update, context)
        return

    if user_id in WAITING_MEMO_EDIT_TEXT:
        handle_memo_edit_text_input(update, context)
        return

    if user_id in WAITING_MEMO_DELETE_SELECT:
        handle_memo_delete_index_input(update, context)
        return

    # رسالة إلى نفسي
    if user_id in WAITING_LETTER_ADD:
        handle_letter_add_content(update, context)
        return

    if user_id in WAITING_LETTER_REMINDER_OPTION:
        handle_reminder_option(update, context)
        return

    if user_id in WAITING_LETTER_CUSTOM_DATE:
        handle_custom_date_input(update, context)
        return

    if user_id in WAITING_LETTER_DELETE_SELECT:
        handle_letter_delete_input(update, context)
        return

    # إدارة الجرعة التحفيزية
    if user_id in WAITING_MOTIVATION_ADD:
        handle_admin_motivation_add_input(update, context)
        return

    if user_id in WAITING_MOTIVATION_DELETE:
        handle_admin_motivation_delete_input(update, context)
        return

    if user_id in WAITING_MOTIVATION_TIMES:
        handle_admin_motivation_times_input(update, context)
        return

    # نظام الحظر
    if user_id in WAITING_BAN_USER:
        handle_ban_user_id_input(update, context)
        return

    if user_id in WAITING_UNBAN_USER:
        handle_unban_user_id_input(update, context)
        return

    if user_id in WAITING_BAN_REASON:
        handle_ban_reason_input(update, context)
        return

    # الدعم
    if user_id in WAITING_SUPPORT:
        WAITING_SUPPORT.discard(user_id)
        forward_support_to_admin(user, text, context)

        gender = record.get("gender")
        if gender == "female":
            reply_txt = (
                "📨 تم إرسال رسالتك إلى الدعم النسائي (المشرفة) 🤍\n"
                "سيتم الاطلاع عليها والرد عليك في أقرب وقت بإذن الله."
            )
        else:
            reply_txt = (
                "📨 تم إرسال رسالتك إلى الدعم 🤍\n"
                "سيتم الاطلاع عليها والرد عليك في أقرب وقت بإذن الله."
            )

        msg.reply_text(
            reply_txt,
            reply_markup=main_kb,
        )
        return

    # رسالة جماعية
    if user_id in WAITING_BROADCAST:
        handle_admin_broadcast_input(update, context)
        return

    # فوائد ونصائح
    if user_id in WAITING_BENEFIT_TEXT:
        handle_add_benefit_text(update, context)
        return

    if user_id in WAITING_BENEFIT_EDIT_TEXT:
        handle_edit_benefit_text(update, context)
        return

    # الأزرار الرئيسية
    if text == BTN_ADHKAR_MAIN:
        open_adhkar_menu(update, context)
        return

    if text == BTN_QURAN_MAIN:
        open_quran_menu(update, context)
        return

    if text == BTN_TASBIH_MAIN:
        open_tasbih_menu(update, context)
        return

    if text == BTN_MEMOS_MAIN:
        open_memos_menu(update, context)
        return

    if text == BTN_WATER_MAIN:
        open_water_menu(update, context)
        return

    if text == BTN_STATS:
        handle_stats(update, context)
        return

    if text == BTN_LETTER_MAIN:
        open_letters_menu(update, context)
        return

    if text == BTN_SUPPORT:
        handle_contact_support(update, context)
        return

    if text == BTN_COMP_MAIN:
        open_comp_menu(update, context)
        return

    if text == BTN_BENEFITS_MAIN:
        open_benefits_menu(update, context)
        return

    if text == BTN_NOTIFICATIONS_MAIN:
        open_notifications_menu(update, context)
        return

    if text == BTN_BACK_MAIN:
        msg.reply_text(
            "عدنا إلى القائمة الرئيسية.",
            reply_markup=main_kb,
        )
        return

    # قوائم الأذكار
    if text == BTN_ADHKAR_MORNING:
        send_morning_adhkar(update, context)
        return

    if text == BTN_ADHKAR_EVENING:
        send_evening_adhkar(update, context)
        return

    if text == BTN_ADHKAR_GENERAL:
        send_general_adhkar(update, context)
        return

    # منبّه الماء
    if text == BTN_WATER_LOG:
        handle_log_cup(update, context)
        return

    if text == BTN_WATER_STATUS:
        handle_status(update, context)
        return

    if text == BTN_WATER_SETTINGS:
        open_water_settings(update, context)
        return

    if text == BTN_WATER_NEED:
        handle_water_need_start(update, context)
        return

    if text == BTN_WATER_REM_ON:
        handle_reminders_on(update, context)
        return

    if text == BTN_WATER_REM_OFF:
        handle_reminders_off(update, context)
        return

    if text == BTN_WATER_ADD_CUPS:
        handle_add_cups(update, context)
        return

    if text == BTN_WATER_BACK_MENU:
        open_water_menu(update, context)
        return

    if text.isdigit():
        handle_add_cups(update, context)
        return

    # ورد القرآن
    if text == BTN_QURAN_SET_GOAL:
        handle_quran_set_goal(update, context)
        return

    if text == BTN_QURAN_ADD_PAGES:
        handle_quran_add_pages_start(update, context)
        return

    if text == BTN_QURAN_STATUS:
        handle_quran_status(update, context)
        return

    if text == BTN_QURAN_RESET_DAY:
        handle_quran_reset_day(update, context)
        return

    # السبحة: اختيار الذكر
    for dhikr, count in TASBIH_ITEMS:
        label = f"{dhikr} ({count})"
        if text == label:
            start_tasbih_for_choice(update, context, text)
            return

    # مذكّرات قلبي
    if text == BTN_MEMO_ADD:
        handle_memo_add_start(update, context)
        return

    if text == BTN_MEMO_EDIT:
        handle_memo_edit_select(update, context)
        return

    if text == BTN_MEMO_DELETE:
        handle_memo_delete_select(update, context)
        return

    if text == BTN_MEMO_BACK:
        msg.reply_text(
            "تم الرجوع للقائمة الرئيسية.",
            reply_markup=main_kb,
        )
        return

    # فوائد ونصائح
    if text == BTN_BENEFIT_ADD:
        handle_add_benefit_start(update, context)
        return

    if text == BTN_BENEFIT_VIEW:
        handle_view_benefits(update, context)
        return

    if text == BTN_BENEFIT_TOP10:
        handle_top10_benefits(update, context)
        return

    if text == BTN_MY_BENEFITS:
        handle_my_benefits(update, context)
        return

    # رسالة إلى نفسي
    if text == BTN_LETTER_ADD:
        handle_letter_add_start(update, context)
        return

    if text == BTN_LETTER_VIEW:
        handle_letter_view(update, context)
        return

    if text == BTN_LETTER_DELETE:
        handle_letter_delete_select(update, context)
        return

    if text == BTN_LETTER_BACK:
        msg.reply_text(
            "تم الرجوع للقائمة الرئيسية.",
            reply_markup=main_kb,
        )
        return

    # خيارات التذكير (لرسالة إلى نفسي)
    if text in [BTN_REMINDER_WEEK, BTN_REMINDER_MONTH, BTN_REMINDER_2MONTHS, BTN_REMINDER_CUSTOM, BTN_REMINDER_NONE]:
        handle_reminder_option(update, context)
        return

    # المنافسات
    if text == BTN_MY_PROFILE:
        handle_my_profile(update, context)
        return

    if text == BTN_TOP10:
        handle_top10(update, context)
        return

    if text == BTN_TOP100:
        handle_top100(update, context)
        return

    # الجرعة التحفيزية للمستخدم
    if text == BTN_MOTIVATION_ON:
        handle_motivation_on(update, context)
        return

    if text == BTN_MOTIVATION_OFF:
        handle_motivation_off(update, context)
        return

    # لوحة التحكم (أدمن / مشرفة)
    if text == BTN_ADMIN_PANEL:
        handle_admin_panel(update, context)
        return

    if text == BTN_ADMIN_USERS_COUNT:
        handle_admin_users_count(update, context)
        return

    if text == BTN_ADMIN_USERS_LIST:
        handle_admin_users_list(update, context)
        return

    if text == BTN_ADMIN_BROADCAST:
        handle_admin_broadcast_start(update, context)
        return

    if text == BTN_ADMIN_RANKINGS:
        handle_admin_rankings(update, context)
        return

    if text == BTN_ADMIN_BAN_USER:
        handle_admin_ban_user(update, context)
        return

    if text == BTN_ADMIN_UNBAN_USER:
        handle_admin_unban_user(update, context)
        return

    if text == BTN_ADMIN_BANNED_LIST:
        handle_admin_banned_list(update, context)
        return

    if text == BTN_ADMIN_MOTIVATION_MENU:
        open_admin_motivation_menu(update, context)
        return

    if text == BTN_ADMIN_MOTIVATION_LIST:
        handle_admin_motivation_list(update, context)
        return

    if text == BTN_ADMIN_MOTIVATION_ADD:
        handle_admin_motivation_add_start(update, context)
        return

    if text == BTN_ADMIN_MOTIVATION_DELETE:
        handle_admin_motivation_delete_start(update, context)
        return

    if text == BTN_ADMIN_MOTIVATION_TIMES:
        handle_admin_motivation_times_start(update, context)
        return

    # أي نص آخر
    msg.reply_text(
        "تنبيه: رسالتك الآن لا تصل للدعم بشكل مباشر.\n"
        "لو حاب ترسل رسالة للدعم:\n"
        "1️⃣ اضغط على زر «تواصل مع الدعم ✉️»\n"
        "2️⃣ أو اضغط على الرسالة التي وصلتك من البوت، ثم اختر Reply / الرد، واكتب رسالتك.",
        reply_markup=main_kb,
    )

# =================== تشغيل البوت ===================


def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN غير موجود في متغيرات البيئة!")

    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher
    job_queue = updater.job_queue

    dp.add_handler(CommandHandler("start", start_command))
    dp.add_handler(CommandHandler("help", help_command))
    
    # Callbacks
    dp.add_handler(CallbackQueryHandler(handle_like_benefit_callback, pattern=r"^like_benefit_\d+$"))
    dp.add_handler(CallbackQueryHandler(handle_edit_benefit_callback, pattern=r"^edit_benefit_\d+$"))
    dp.add_handler(CallbackQueryHandler(handle_delete_benefit_callback, pattern=r"^delete_benefit_\d+$"))
    dp.add_handler(CallbackQueryHandler(handle_admin_delete_benefit_callback, pattern=r"^admin_delete_benefit_\d+$"))
    dp.add_handler(CallbackQueryHandler(handle_delete_benefit_confirm_callback, pattern=r"^confirm_delete_benefit_\d+$|^cancel_delete_benefit$|^confirm_admin_delete_benefit_\d+$|^cancel_admin_delete_benefit$"))

    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_text))

    # تشغيل مهمة التحقق من الميداليات يوميًا في منتصف الليل بتوقيت UTC
    job_queue.run_daily(
        check_and_award_medal,
        time=time(hour=0, minute=0, tzinfo=pytz.UTC),
        name="check_and_award_medal",
    )

    for h in REMINDER_HOURS_UTC:
        job_queue.run_daily(
            water_reminder_job,
            time=time(hour=h, minute=0, tzinfo=pytz.UTC),
            name=f"water_reminder_{h}",
        )

    global CURRENT_MOTIVATION_JOBS
    CURRENT_MOTIVATION_JOBS = []
    for h in MOTIVATION_HOURS_UTC:
        try:
            job = job_queue.run_daily(
                motivation_job,
                time=time(hour=h, minute=0, tzinfo=pytz.UTC),
                name=f"motivation_job_{h}",
            )
            CURRENT_MOTIVATION_JOBS.append(job)
        except Exception as e:
            logger.error(f"Error scheduling motivation job at hour {h}: {e}")

    Thread(target=run_flask, daemon=True).start()

    logger.info("Suqya Al-Kawther bot is starting...")
    updater.start_polling()
    updater.idle()


if __name__ == "__main__":
    main()
# =================== وظائف إضافية ===================

def send_new_user_notification(update: Update, context: CallbackContext):
    """
    يرسل إشعارًا للمدير عند انضمام مستخدم جديد، ورسالة ترحيب للمستخدم.
    """
    user = update.effective_user
    user_id = user.id
    
    # 1. إرسال رسالة الترحيب للمستخدم الجديد
    welcome_message = (
        "🤍 أهلاً بك في سقيا الكوثر\n"
        "هنا تُسقى أرواحنا بالذكر والطمأنينة…\n"
        "ونتشارك نُصحًا ينفع القلب ويُرضي الله 🌿"
    )
    
    try:
        context.bot.send_message(
            chat_id=user_id,
            text=welcome_message,
            reply_markup=user_main_keyboard(user_id),
        )
    except Exception as e:
        logger.error(f"Error sending welcome message to new user {user_id}: {e}")
        
    # 2. إرسال الإشعار للمدير
    if ADMIN_ID is not None:
        # محاولة الحصول على معلومات إضافية (غير مضمونة)
        username_text = f"@{user.username}" if user.username else "غير متوفر"
        
        # تنسيق وقت الانضمام
        # نفترض أن datetime و timezone و pytz مستوردة
        now_utc = datetime.now(timezone.utc)
        # استخدام توقيت افتراضي (مثل توقيت مكة)
        try:
            local_tz = pytz.timezone("Africa/Cairo") 
        except:
            local_tz = timezone.utc 
            
        now_local = now_utc.astimezone(local_tz)
        join_time_str = now_local.strftime("%d-%m-%Y | %I:%M %p")
        
        notification_message = (
            "🔔 مستخدم جديد دخل البوت 🎉\n"
            f"👤 الاسم: {user.first_name}\n"
            f"🆔 User ID: {user.id}\n"
            f"🧑‍💻 Username: {username_text}\n"
            "\n"
            "🌍 المنطقة: غير متوفر (Telegram لا يوفرها)\n"
            "📱 الجهاز: غير متوفر (Telegram لا يوفرها)\n"
            "🧭 المصدر: غير متوفر (Telegram لا يوفرها)\n"
            f"🕒 الانضمام: {join_time_str} (توقيت محلي افتراضي)\n"
        )
        
        try:
            context.bot.send_message(
                chat_id=ADMIN_ID,
                text=notification_message,
            )
        except Exception as e:
            logger.error(f"Error sending new user notification to admin {ADMIN_ID}: {e}")

# =================== وظائف إضافية ===================

def send_new_user_notification(update: Update, context: CallbackContext):
    """
    يرسل إشعارًا للمدير عند انضمام مستخدم جديد، ورسالة ترحيب للمستخدم.
    """
    user = update.effective_user
    user_id = user.id
    
    # 1. إرسال رسالة الترحيب للمستخدم الجديد
    welcome_message = (
        "🤍 أهلاً بك في سقيا الكوثر\n"
        "هنا تُسقى أرواحنا بالذكر والطمأنينة…\n"
        "ونتشارك نُصحًا ينفع القلب ويُرضي الله 🌿"
    )
    
    try:
        # نفترض أن user_main_keyboard معرفة في مكان آخر
        context.bot.send_message(
            chat_id=user_id,
            text=welcome_message,
            reply_markup=user_main_keyboard(us
