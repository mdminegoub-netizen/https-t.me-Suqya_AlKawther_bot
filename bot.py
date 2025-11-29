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

# 👇 أضف هذا الجزء هنا 👇

# مشرف الرجال (أنت)
ADMIN_ID = 931350292   # ضع معرفك الصحيح هنا

# مشرفة النساء
FEMALE_ADMIN_ID = 931350292  # ضع معرف المشرفة هنا

# حالات نظام الدعم
WAITING_SUPPORT = set()
WAITING_BROADCAST = set()
SUPPORT_LAST_USER = {}

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

# =================== حالات الإدخال ===================

WAITING_GENDER = set()
WAITING_AGE = set()
WAITING_WEIGHT = set()

WAITING_QURAN_GOAL = set()
WAITING_QURAN_ADD_PAGES = set()

WAITING_TASBIH = set()  # أثناء العدّ
ACTIVE_TASBIH = {}      # user_id -> { "text": str, "target": int, "current": int }

# مذكّرات قلبي
WAITING_MEMO_MENU = set()
WAITING_MEMO_ADD = set()
WAITING_MEMO_EDIT_SELECT = set()
WAITING_MEMO_EDIT_TEXT = set()
WAITING_MEMO_DELETE_SELECT = set()
MEMO_EDIT_INDEX = {}

# دعم / إدارة
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
# لوحة الدعم للمستخدم
SUPPORT_MENU_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton("✍️ كتابة رسالة للدعم")],
        [KeyboardButton(BTN_BACK_MAIN)],
    ],
    resize_keyboard=True,
)

# لوحة إلغاء الدعم
SUPPORT_CANCEL_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_CANCEL)],
        [KeyboardButton(BTN_BACK_MAIN)],
    ],
    resize_keyboard=True,
)
BTN_CANCEL = "إلغاء ❌"
BTN_BACK_MAIN = "رجوع للقائمة الرئيسية ⬅️"

# لوحة المدير (تظهر فقط للأدمن)
BTN_ADMIN_PANEL = "لوحة التحكم 🛠"
BTN_ADMIN_USERS_COUNT = "عدد المستخدمين 👥"
BTN_ADMIN_USERS_LIST = "قائمة المستخدمين 📄"
BTN_ADMIN_BROADCAST = "رسالة جماعية 📢"

MAIN_KEYBOARD_USER = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_ADHKAR_MAIN), KeyboardButton(BTN_QURAN_MAIN)],
        [KeyboardButton(BTN_TASBIH_MAIN), KeyboardButton(BTN_MEMOS_MAIN)],
        [KeyboardButton(BTN_WATER_MAIN), KeyboardButton(BTN_STATS)],
        [KeyboardButton(BTN_SUPPORT)],
    ],
    resize_keyboard=True,
)

MAIN_KEYBOARD_ADMIN = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_ADHKAR_MAIN), KeyboardButton(BTN_QURAN_MAIN)],
        [KeyboardButton(BTN_TASBIH_MAIN), KeyboardButton(BTN_MEMOS_MAIN)],
        [KeyboardButton(BTN_WATER_MAIN), KeyboardButton(BTN_STATS)],
        [KeyboardButton(BTN_SUPPORT)],
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
        [KeyboardButton(BTN_BACK_MAIN)],
    ],
    resize_keyboard=True,
)

WATER_SETTINGS_KB_ADMIN = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_WATER_NEED)],
        [KeyboardButton(BTN_WATER_REM_ON), KeyboardButton(BTN_WATER_REM_OFF)],
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

# قائمة الأذكار المتاحة في السبحة (ذكر، عدد)
TASBIH_ITEMS = [
    ("سبحان الله", 33),
    ("الحمد لله", 33),
    ("الله أكبر", 34),
    ("سبحان الله وبحمده", 100),
    ("لا إله إلا الله", 100),
    ("اللهم صل وسلم على سيدنا محمد", 50),
]

def build_tasbih_menu(is_admin: bool):
    rows = [[KeyboardButton(f"{text} ({count})")] for text, count in TASBIH_ITEMS]
    last_row = [KeyboardButton(BTN_BACK_MAIN)]
    if is_admin:
        last_row.append(KeyboardButton(BTN_ADMIN_PANEL))
    rows.append(last_row)
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

# ---- مذكّرات قلبي ----
BTN_MEMO_ADD = "➕ إضافة مذكرة"
BTN_MEMO_EDIT = "✏️ تعديل مذكرة"
BTN_MEMO_DELETE = "🗑 حذف مذكرة"
BTN_MEMO_BACK = "رجوع ⬅️"

def build_memos_menu_kb(is_admin: bool):
    rows = [
        [KeyboardButton(BTN_MEMO_ADD)],
        [KeyboardButton(BTN_MEMO_EDIT), KeyboardButton(BTN_MEMO_DELETE)],
        [KeyboardButton(BTN_MEMO_BACK)],
    ]
    if is_admin:
        rows.append([KeyboardButton(BTN_ADMIN_PANEL)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

# ---- لوحة التحكم ----
ADMIN_PANEL_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_ADMIN_USERS_COUNT), KeyboardButton(BTN_ADMIN_USERS_LIST)],
        [KeyboardButton(BTN_ADMIN_BROADCAST)],
        [KeyboardButton(BTN_BACK_MAIN)],
    ],
    resize_keyboard=True,
)

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
    """تصفير ورد اليوم لو تغيّر التاريخ (تبقى الأهداف كما هي)."""
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

# =================== أذكار ثابتة (مختصرة) ===================

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
    is_new = str(user.id) not in data
    get_user_record(user)

    kb = user_main_keyboard(user.id)

    update.message.reply_text(
        f"مرحبًا {user.first_name} 👋\n\n"
        "أهلًا بك في بوت *سُقيا الكوثر*.\n"
        "يساعدك على تنظيم شرب الماء، وضبط وردك القرآني، والمحافظة على الأذكار والتسبيح، "
        "وتسجيل مذكّرات قلبك.\n\n"
        "اختر من القائمة أسفل الشاشة ما يناسبك:",
        reply_markup=kb,
        parse_mode="Markdown",
    )

    # إشعار الأدمن بدخول مستخدم جديد
    if is_new and ADMIN_ID is not None:
        try:
            context.bot.send_message(
                chat_id=ADMIN_ID,
                text=(
                    "👤 مستخدم جديد دخل البوت:\n\n"
                    f"الاسم: {user.full_name}\n"
                    f"اليوزر: @{user.username if user.username else 'لا يوجد'}\n"
                    f"ID: `{user.id}`"
                ),
                parse_mode="Markdown",
            )
        except Exception as e:
            logger.error(f"Error notifying admin about new user: {e}")


def help_command(update: Update, context: CallbackContext):
    kb = user_main_keyboard(update.effective_user.id)
    update.message.reply_text(
        "طريقة الاستخدام:\n\n"
        "• أذكاري 🤲 → أذكار الصباح والمساء وأذكار عامة.\n"
        "• وردي القرآني 📖 → تعيين عدد الصفحات التي تقرؤها يوميًا ومتابعة تقدمك.\n"
        "• السبحة 📿 → اختيار ذكر معيّن والعدّ عليه بعدد محدد من التسبيحات.\n"
        "• مذكّرات قلبي 🩵 → كتابة مشاعرك وخواطرك مع إمكانية التعديل والحذف.\n"
        "• منبّه الماء 💧 → حساب احتياجك من الماء، تسجيل الأكواب، وتفعيل التذكير.\n"
        "• احصائياتي 📊 → ملخّص بسيط لإنجازاتك اليوم.\n"
        "• تواصل مع الدعم ✉️ → لإرسال رسالة للدعم والرد عليك لاحقًا.",
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
        "• عدّل إعداداتك وتشغيل التذكير.",
        reply_markup=kb,
    )


def open_water_settings(update: Update, context: CallbackContext):
    kb = water_settings_keyboard(update.effective_user.id)
    update.message.reply_text(
        "إعدادات الماء ⚙️:\n"
        "1) حساب احتياجك اليومي من الماء بناءً على الجنس والعمر والوزن.\n"
        "2) تشغيل أو إيقاف التذكير الدوري بالماء.",
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

    # حساب احتياج الماء حسب الجنس
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
        "وزّع أكوابك على اليوم، وسأذكّرك وأساعدك على المتابعة.",
        reply_markup=water_menu_keyboard(user_id),
    )


def handle_log_cup(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)

    if not record.get("cups_goal"):
        update.message.reply_text(
            "لم تقم بعد بحساب احتياجك من الماء.\n"
            "اذهب إلى «إعدادات الماء ⚙️» ثم «حساب احتياج الماء 🧮».",
            reply_markup=water_menu_keyboard(user.id),
        )
        return

    ensure_today_water(record)
    record["today_cups"] = record.get("today_cups", 0) + 1
    save_data()

    status_text = format_water_status_text(record)
    update.message.reply_text(
        f"🥤 تم تسجيل كوب ماء.\n\n{status_text}",
        reply_markup=water_menu_keyboard(user.id),
    )


def handle_add_cups(update: Update, context: CallbackContext):
    """وصف طريقة إضافة عدد أكواب، أو التعامل مع الرقم نفسه."""
    user = update.effective_user
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
    record["today_cups"] = record.get("today_cups", 0) + cups
    save_data()

    status_text = format_water_status_text(record)
    update.message.reply_text(
        f"🥤 تم إضافة {cups} كوب إلى عدّادك اليوم.\n\n{status_text}",
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
    get_user_record(user)
    kb = quran_menu_keyboard(user.id)
    update.message.reply_text(
        "وردي القرآني 📖:\n"
        "• عيّن عدد صفحات اليوم.\n"
        "• سجّل ما قرأته.\n"
        "• شاهد مستوى إنجازك.\n"
        "• يمكنك إعادة تعيين ورد اليوم.",
        reply_markup=kb,
    )


def handle_quran_set_goal(update: Update, context: CallbackContext):
    user_id = update.effective_user.id

    WAITING_QURAN_GOAL.add(user_id)
    WAITING_QURAN_ADD_PAGES.discard(user_id)

    update.message.reply_text(
        "أرسل عدد الصفحات التي تريد قراءتها اليوم من القرآن، مثال: 5 أو 10.",
        reply_markup=CANCEL_KB,
    )


def handle_quran_goal_input(update: Update, context: CallbackContext):
    user = update.effective_user
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

    record["quran_pages_today"] = record.get("quran_pages_today", 0) + pages
    save_data()

    WAITING_QURAN_ADD_PAGES.discard(user_id)

    status_text = format_quran_status_text(record)
    update.message.reply_text(
        f"تم إضافة {pages} صفحة إلى وردك اليوم.\n\n{status_text}",
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
        "تم إعادة تعيين ورد اليوم.\n"
        "يمكنك البدء من جديد في حساب الصفحات لهذا اليوم.",
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
        "• أذكار عامة تريح القلب.",
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
    text = "اختر الذكر الذي تريد التسبيح به، وسيقوم البوت بالعدّ لك:"
    update.message.reply_text(
        text,
        reply_markup=kb,
    )


def start_tasbih_for_choice(update: Update, context: CallbackContext, choice_text: str):
    user = update.effective_user
    user_id = user.id

    # choice_text شكلها: "سبحان الله (33)"
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
        update.message.reply_text(
            f"اكتمل التسبيح على: {dhikr}\n"
            f"وصلت إلى {target} تسبيحة. تقبّل الله منك 🤍.",
            reply_markup=user_main_keyboard(user_id),
        )
        ACTIVE_TASBIH.pop(user_id, None)
        WAITING_TASBIH.discard(user_id)


def handle_tasbih_end(update: Update, context: CallbackContext):
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
    user_id = user.id
    record = get_user_record(user)
    memos = record.get("heart_memos", [])

    # تفعيل حالة قائمة المذكرات
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
    # إعادة عرض القائمة
    dummy_update = update
    open_memos_menu(dummy_update, context)


def handle_memo_edit_select(update: Update, context: CallbackContext):
    user = update.effective_user
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

    memos[idx] = text
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

    text_lines = ["احصائياتك لليوم 📊:\n"]

    # الماء
    if cups_goal:
        text_lines.append(f"- الماء: {today_cups} / {cups_goal} كوب.")
    else:
        text_lines.append("- الماء: لم يتم حساب احتياجك بعد.")

    # القرآن
    if q_goal:
        text_lines.append(f"- ورد القرآن: {q_today} / {q_goal} صفحة.")
    else:
        text_lines.append("- ورد القرآن: لم تضبط وردًا لليوم بعد.")

    # الأذكار
    text_lines.append(f"- عدد المرات التي استخدمت فيها قسم الأذكار: {adhkar_count} مرة.")

    # التسبيح
    text_lines.append(f"- مجموع التسبيحات المسجّلة عبر السبحة: {tasbih_total} تسبيحة.")

    # المذكرات
    text_lines.append(f"- عدد مذكّرات قلبك المسجّلة: {memos_count} مذكرة.")

    update.message.reply_text(
        "\n".join(text_lines),
        reply_markup=user_main_keyboard(user_id),
    )

# =================== تذكيرات الماء (JobQueue) ===================

REMINDER_HOURS_UTC = [7, 10, 13, 16, 19]


def water_reminder_job(context: CallbackContext):
    logger.info("Running water reminder job...")
    bot = context.bot

    for uid in get_all_user_ids():
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

# =================== نظام الدعم ولوحة التحكم ===================


def handle_contact_support(update: Update, context: CallbackContext):
    user = update.effective_user
    get_user_record(user)

    WAITING_SUPPORT.add(user.id)

    update.message.reply_text(
        "✉️ اكتب الآن رسالتك التي تريد إرسالها للدعم.\n"
        "اشرح ما تحتاجه بهدوء، وسيتم الاطلاع عليها بإذن الله.\n\n"
        "للإلغاء اضغط «إلغاء ❌».",
        reply_markup=CANCEL_KB,
    )


def handle_admin_panel(update: Update, context: CallbackContext):
    user = update.effective_user
    if not is_admin(user.id):
        update.message.reply_text(
            "هذا القسم خاص بالمدير فقط.",
            reply_markup=user_main_keyboard(user.id),
        )
        return

    update.message.reply_text(
        "لوحة التحكم 🛠:\n"
        "• عرض عدد المستخدمين.\n"
        "• عرض قائمة المستخدمين.\n"
        "• إرسال رسالة جماعية.",
        reply_markup=ADMIN_PANEL_KB,
    )


def handle_admin_users_count(update: Update, context: CallbackContext):
    user = update.effective_user
    if not is_admin(user.id):
        return

    total_users = len(get_all_user_ids())
    update.message.reply_text(
        f"👥 عدد المستخدمين المسجلين في البوت: {total_users}",
        reply_markup=ADMIN_PANEL_KB,
    )


def handle_admin_users_list(update: Update, context: CallbackContext):
    user = update.effective_user
    if not is_admin(user.id):
        return

    lines = []
    for uid_str, rec in data.items():
        name = rec.get("first_name") or "بدون اسم"
        username = rec.get("username")
        line = f"- {name} | ID: {uid_str}"
        if username:
            line += f" | @{username}"
        lines.append(line)

    if not lines:
        text = "لا يوجد مستخدمون مسجّلون بعد."
    else:
        text = "قائمة بعض المستخدمين:\n\n" + "\n".join(lines[:200])  # حد معقول

    update.message.reply_text(
        text,
        reply_markup=ADMIN_PANEL_KB,
    )


def handle_admin_broadcast_start(update: Update, context: CallbackContext):
    user = update.effective_user
    if not is_admin(user.id):
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

    if not is_admin(user_id):
        WAITING_BROADCAST.discard(user_id)
        update.message.reply_text(
            "هذه الميزة خاصة بالمدير فقط.",
            reply_markup=user_main_keyboard(user_id),
        )
        return

    user_ids = get_all_user_ids()
    sent = 0
    for uid in user_ids:
        try:
            update.effective_message.bot.send_message(
                chat_id=uid,
                text=f"📢 رسالة من الدعم:\n\n{text}",
            )
            sent += 1
        except Exception as e:
            logger.error(f"Error sending broadcast to {uid}: {e}")

    WAITING_BROADCAST.discard(user_id)

    update.message.reply_text(
        f"تم إرسال الرسالة إلى {sent} مستخدم.",
        reply_markup=ADMIN_PANEL_KB,
    )


def forward_support_to_admin(user, text: str, context: CallbackContext):
    """إرسال رسالة الدعم إلى الأدمن مع بيانات المستخدم."""
    if ADMIN_ID is None:
        return
    try:
        context.bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                "📩 رسالة جديدة للدعم:\n\n"
                f"الاسم: {user.full_name}\n"
                f"المعرف: @{user.username if user.username else 'لا يوجد'}\n"
                f"ID: `{user.id}`\n\n"
                f"محتوى الرسالة:\n{text}"
            ),
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error(f"Error sending support message to admin: {e}")


def try_handle_admin_reply(update: Update, context: CallbackContext) -> bool:
    """
    إذا كان الأدمن يرد على رسالة دعم / رد من المستخدم، نلتقط ID ونرسل الرد للمستخدم.
    ترجع True إذا تم التعامل مع الرسالة كـ رد أدمن.
    """
    user = update.effective_user
    msg = update.message
    text = (msg.text or "").strip()

    if not is_admin(user.id):
        return False

    if not msg.reply_to_message:
        return False

    original = msg.reply_to_message.text or ""
    m = re.search(r"ID:\s*`(\d+)`", original)
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
            reply_markup=ADMIN_PANEL_KB,
        )
    except Exception as e:
        logger.error(f"Error sending admin reply to {target_id}: {e}")
        msg.reply_text(
            "حدث خطأ أثناء إرسال الرد للمستخدم.",
            reply_markup=ADMIN_PANEL_KB,
        )
    return True

# =================== هاندلر الرسائل ===================

# =================== نظام الدعم (جديد) ===================

def support_start(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    WAITING_SUPPORT.add(user_id)

    update.message.reply_text(
        "✍️ أكتب رسالتك الآن وسيتم إرسالها للدعم.\n"
        "إذا رغبت بالإلغاء اضغط إلغاء ❌",
        reply_markup=SUPPORT_CANCEL_KB,
    )


def process_support_message(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    text = update.message.text

    WAITING_SUPPORT.discard(user_id)

    gender = get_user_record(user).get("gender")

    # إرسال للمدير (الرجال)
    context.bot.send_message(
        chat_id=ADMIN_ID,
        text=f"📩 رسالة دعم:\n\n"
             f"👤 المستخدم: {user.first_name}\n"
             f"🆔 ID: {user_id}\n"
             f"📨 الرسالة:\n{text}",
    )

    info = "📨 تم إرسال رسالتك للدعم بنجاح ✔️"

    # إرسال للمشرفة في حالة النساء فقط
    if gender == "female":
        try:
            context.bot.send_message(
                chat_id=FEMALE_ADMIN_ID,
                text=f"📩 رسالة دعم (أنثى):\n\n"
                     f"👤 المستخدم: {user.first_name}\n"
                     f"🆔 ID: {user_id}\n"
                     f"📨 الرسالة:\n{text}",
            )
            info = "📨 وصلت رسالتك للمشرفة، ستتواصل معك قريبًا 🤍"
        except:
            pass

    update.message.reply_text(
        info,
        reply_markup=user_main_keyboard(user_id),
    )
def handle_text(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    msg = update.message
    text = (msg.text or "").strip()
# ================= نظام الدعم =================
if user_id in WAITING_SUPPORT:
    return process_support_message(update, context)
    record = get_user_record(user)
    main_kb = user_main_keyboard(user_id)
    # 1️⃣ رد الأدمن على رسالة فيها ID → يرسل الرد للمستخدم
    if is_admin(user_id) and msg.reply_to_message:
        original = msg.reply_to_message.text or ""
        m = re.search(r"ID:\s*`?(\d+)`?", original)
        if m:
            target_id = int(m.group(1))
            try:
                context.bot.send_message(
                    chat_id=target_id,
                    text=f"💌 رد من الدعم:\n\n{text}",
                )
                msg.reply_text("✅ تم إرسال ردّك للمستخدم.", reply_markup=main_kb)
            except Exception as e:
                logger.error(f"Error sending admin reply to {target_id}: {e}")
                msg.reply_text("⚠️ حدث خطأ أثناء إرسال الرد.", reply_markup=main_kb)
            return
                # 2️⃣ رد المستخدم على رسالة دعم → تُرسل مباشرة للأدمن
    if (
        not is_admin(user_id)
        and msg.reply_to_message
        and msg.reply_to_message.from_user.id == context.bot.id
    ):
        original = msg.reply_to_message.text or ""

        if (
            original.startswith("💌 رد من الدعم")
            or original.startswith("📢 رسالة من الدعم")
            or "رسالتك وصلت للدعم" in original
        ):
            support_msg = (
                "📩 *رسالة جديدة من المستخدم (رد على دعم):*\n\n"
                f"👤 الاسم: {user.full_name}\n"
                f"🆔 ID: `{user_id}`\n"
                f"🔹 اسم المستخدم: @{user.username if user.username else 'لا يوجد'}\n\n"
                f"✉️ محتوى الرسالة:\n{text}"
            )
            try:
                context.bot.send_message(
                    chat_id=ADMIN_ID,
                    text=support_msg,
                    parse_mode="Markdown",
                )
            except Exception as e:
                logger.error(f"Error sending user reply to admin: {e}")

            msg.reply_text(
                "📨 رسالتك وصلت للدعم بنجاح 🤍",
                reply_markup=main_kb,
            )
            return
    # 2️⃣ رد المستخدم على رسالة دعم → تُرسل مباشرة للأدمن
    if (
        not is_admin(user_id)
        and msg.reply_to_message
        and msg.reply_to_message.from_user.id == context.bot.id
    ):
        original = msg.reply_to_message.text or ""

        if (
            original.startswith("💌 رد من الدعم")
            or original.startswith("📢 رسالة من الدعم")
            or "رسالتك وصلت للدعم" in original
        ):
            support_msg = (
                "📩 *رسالة جديدة من المستخدم (رد على دعم):*\n\n"
                f"👤 الاسم: {user.full_name}\n"
                f"🆔 ID: `{user_id}`\n"
                f"🔹 اسم المستخدم: @{user.username if user.username else 'لا يوجد'}\n\n"
                f"✉️ محتوى الرسالة:\n{text}"
            )
            try:
                context.bot.send_message(
                    chat_id=ADMIN_ID,
                    text=support_msg,
                    parse_mode="Markdown",
                )
            except Exception as e:
                logger.error(f"Error sending user reply to admin: {e}")

            msg.reply_text(
                "📨 رسالتك وصلت للدعم بنجاح 🤍",
                reply_markup=main_kb,
            )
            return
    # زر الإلغاء العام
    if text == BTN_CANCEL:
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
        WAITING_SUPPORT.discard(user_id)
        WAITING_BROADCAST.discard(user_id)

        msg.reply_text(
            "تم الإلغاء. عدنا للقائمة الرئيسية.",
            reply_markup=main_kb,
        )
        return

    # ===== رد الأدمن على رسالة دعم (بالـ reply) =====
    if try_handle_admin_reply(update, context):
        return

    # ===== حالات إدخال الماء =====
    if user_id in WAITING_GENDER:
        handle_gender_input(update, context)
        return

    if user_id in WAITING_AGE:
        handle_age_input(update, context)
        return

    if user_id in WAITING_WEIGHT:
        handle_weight_input(update, context)
        return

    # ===== حالات إدخال ورد القرآن =====
    if user_id in WAITING_QURAN_GOAL:
        handle_quran_goal_input(update, context)
        return

    if user_id in WAITING_QURAN_ADD_PAGES:
        handle_quran_add_pages_input(update, context)
        return

    # ===== حالة السبحة أثناء العدّ =====
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

    # ===== حالات مذكّرات قلبي =====
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

    # ===== حالة الدعم =====
    if user_id in WAITING_SUPPORT:
        WAITING_SUPPORT.discard(user_id)
        forward_support_to_admin(user, text, context)
        msg.reply_text(
            "تم إرسال رسالتك للدعم.\n"
            "سيتم الاطلاع عليها، والرد عليك عند الحاجة بإذن الله 🤍.",
            reply_markup=main_kb,
        )
        return

    # ===== حالة الرسالة الجماعية (نص الرسالة) =====
    if user_id in WAITING_BROADCAST:
        handle_admin_broadcast_input(update, context)
        return

    # ===== الأزرار الرئيسية =====
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
     if text == BTN_SUPPORT:
    return support_start(update, context)

    if text == BTN_WATER_MAIN:
        open_water_menu(update, context)
        return

    if text == BTN_STATS:
        handle_stats(update, context)
        return

    if text == BTN_SUPPORT:
        handle_contact_support(update, context)
        return

    if text == BTN_BACK_MAIN:
        msg.reply_text(
            "عدنا إلى القائمة الرئيسية.",
            reply_markup=main_kb,
        )
        return

    # ===== قوائم الأذكار =====
    if text == BTN_ADHKAR_MORNING:
        send_morning_adhkar(update, context)
        return

    if text == BTN_ADHKAR_EVENING:
        send_evening_adhkar(update, context)
        return

    if text == BTN_ADHKAR_GENERAL:
        send_general_adhkar(update, context)
        return

    # ===== منبّه الماء =====
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

    # لو كتب رقم → نحاول تفسيره كعدد أكواب إضافية أو صفحات قرآن حسب السياق
    if text.isdigit():
        # نحاول أولا إضافته كأكواب
        handle_add_cups(update, context)
        return

    # ===== ورد القرآن =====
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

    # ===== السبحة: اختيار الذكر =====
    for dhikr, count in TASBIH_ITEMS:
        label = f"{dhikr} ({count})"
        if text == label:
            start_tasbih_for_choice(update, context, text)
            return

    # ===== مذكّرات قلبي: زر القائمة الداخلي =====
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

    # ===== لوحة التحكم (المدير) =====
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

    # ===== ردود المستخدمين على رسائل الدعم/الرسالة الجماعية (بـ Reply) =====
    if (
        not is_admin(user_id)
        and msg.reply_to_message
        and msg.reply_to_message.from_user.id == context.bot.id
    ):
        original_text = msg.reply_to_message.text or ""
        if original_text.startswith("📢 رسالة من الدعم") or original_text.startswith("💌 رد من الدعم"):
            forward_support_to_admin(user, text, context)
            msg.reply_text(
                "تم إرسال ردّك إلى الدعم.\n"
                "شكرًا على مشاركتك 🤍.",
                reply_markup=main_kb,
            )
            return

    # ===== أي نص آخر =====
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

    # أوامر
    dp.add_handler(CommandHandler("start", start_command))
    dp.add_handler(CommandHandler("help", help_command))

    # جميع الرسائل النصية
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_text))

    # جدولة التذكيرات اليومية للماء
    for h in REMINDER_HOURS_UTC:
        job_queue.run_daily(
            water_reminder_job,
            time=time(hour=h, minute=0, tzinfo=pytz.UTC),
            name=f"water_reminder_{h}",
        )

    # تشغيل Flask في ثريد منفصل (لـ Render)
    Thread(target=run_flask, daemon=True).start()

    logger.info("Suqya Al-Kawther bot is starting...")
    updater.start_polling()
    updater.idle()


if __name__ == "__main__":
    main()
