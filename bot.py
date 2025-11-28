import os
import json
import logging
from datetime import datetime, timezone, timedelta, time
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

# ضع هنا ID حسابك في تيليجرام لتصلك إشعارات المستخدمين الجدد
ADMIN_ID = 931350292  # غيّره لو احتجت

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
    """ينشئ أو يرجع سجل المستخدم، ويحدّث آخر نشاط، ويضمن وجود الحقول الجديدة."""
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
        }
    else:
        record = data[user_id]
        record["first_name"] = user.first_name
        record["username"] = user.username
        record["last_active"] = now_iso

        # ضمان الحقول في السجلات القديمة
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

# =================== حالات الإدخال ===================

WAITING_GENDER = set()
WAITING_AGE = set()
WAITING_WEIGHT = set()

WAITING_QURAN_GOAL = set()
WAITING_QURAN_ADD_PAGES = set()

WAITING_TASBIH = set()  # أثناء العدّ
ACTIVE_TASBIH = {}      # user_id -> { "text": str, "target": int, "current": int }

# =================== الأزرار ===================

# رئيسية
BTN_ADHKAR_MAIN = "أذكاري 🤲"
BTN_QURAN_MAIN = "وردي القرآني 📖"
BTN_WATER_MAIN = "منبّه الماء 💧"
BTN_STATS = "احصائياتي 📊"
BTN_TASBIH_MAIN = "السبحة 📿"

BTN_CANCEL = "إلغاء ❌"
BTN_BACK_MAIN = "رجوع للقائمة الرئيسية ⬅️"

MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_ADHKAR_MAIN), KeyboardButton(BTN_QURAN_MAIN)],
        [KeyboardButton(BTN_WATER_MAIN), KeyboardButton(BTN_STATS)],
        [KeyboardButton(BTN_TASBIH_MAIN)],
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

WATER_MENU_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_WATER_LOG), KeyboardButton(BTN_WATER_ADD_CUPS)],
        [KeyboardButton(BTN_WATER_STATUS)],
        [KeyboardButton(BTN_WATER_SETTINGS)],
        [KeyboardButton(BTN_BACK_MAIN)],
    ],
    resize_keyboard=True,
)

WATER_SETTINGS_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_WATER_NEED)],
        [KeyboardButton(BTN_WATER_REM_ON), KeyboardButton(BTN_WATER_REM_OFF)],
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

QURAN_MENU_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_QURAN_SET_GOAL)],
        [KeyboardButton(BTN_QURAN_ADD_PAGES), KeyboardButton(BTN_QURAN_STATUS)],
        [KeyboardButton(BTN_QURAN_RESET_DAY)],
        [KeyboardButton(BTN_BACK_MAIN)],
    ],
    resize_keyboard=True,
)

# ---- أذكاري ----
BTN_ADHKAR_MORNING = "أذكار الصباح 🌅"
BTN_ADHKAR_EVENING = "أذكار المساء 🌙"
BTN_ADHKAR_GENERAL = "أذكار عامة 💭"

ADHKAR_MENU_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_ADHKAR_MORNING), KeyboardButton(BTN_ADHKAR_EVENING)],
        [KeyboardButton(BTN_ADHKAR_GENERAL)],
        [KeyboardButton(BTN_BACK_MAIN)],
    ],
    resize_keyboard=True,
)

# ---- السبحة ----
BTN_TASBIH_TICK = "تسبيحة ✅"
BTN_TASBIH_END = "إنهاء الذكر ⬅️"

TASBIH_RUN_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_TASBIH_TICK)],
        [KeyboardButton(BTN_TASBIH_END)],
        [KeyboardButton(BTN_CANCEL)],
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

TASBIH_MENU_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(f"{text} ({count})")] for text, count in TASBIH_ITEMS
    ] + [[KeyboardButton(BTN_BACK_MAIN)]],
    resize_keyboard=True,
)

# =================== دوال مساعدة عامة ===================


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

    # نتحقق هل المستخدم جديد قبل استدعاء get_user_record
    is_new = str(user.id) not in data
    get_user_record(user)

    update.message.reply_text(
        f"مرحبًا {user.first_name} 👋\n\n"
        "أهلًا بك في بوت *سُقيا الكوثر*.\n"
        "يساعدك على تنظيم شرب الماء، وضبط وردك القرآني، والمحافظة على الأذكار والتسبيح.\n\n"
        "اختر من القائمة أسفل الشاشة ما يناسبك:",
        reply_markup=MAIN_KEYBOARD,
    )

    # إشعار للأدمن عند دخول مستخدم جديد لأول مرة
    if is_new and ADMIN_ID:
        try:
            context.bot.send_message(
                chat_id=ADMIN_ID,
                text=(
                    "👤 مستخدم جديد دخل بوت سُقيا الكوثر:\n\n"
                    f"الاسم: {user.full_name}\n"
                    f"اليوزر: @{user.username if user.username else 'لا يوجد'}\n"
                    f"ID: `{user.id}`"
                ),
                parse_mode="Markdown",
            )
        except Exception as e:
            logger.error(f"Error notifying admin about new user: {e}")


def help_command(update: Update, context: CallbackContext):
    update.message.reply_text(
        "طريقة الاستخدام:\n\n"
        "• أذكاري 🤲 → أذكار الصباح والمساء وأذكار عامة.\n"
        "• وردي القرآني 📖 → تعيين عدد الصفحات التي تقرؤها يوميًا ومتابعة تقدمك.\n"
        "• منبّه الماء 💧 → حساب احتياجك من الماء، تسجيل الأكواب، وتفعيل التذكير.\n"
        "• احصائياتي 📊 → ملخّص بسيط لإنجازاتك اليوم.\n"
        "• السبحة 📿 → اختيار ذكر معيّن والعدّ عليه بعدد محدد من التسبيحات.",
        reply_markup=MAIN_KEYBOARD,
    )

# =================== قسم منبّه الماء ===================

# (كل ما تحت هذا التعليق بقي كما هو بدون أي تعديل)

def open_water_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    get_user_record(user)
    update.message.reply_text(
        "منبّه الماء 💧:\n"
        "• سجّل ما تشربه من أكواب.\n"
        "• شاهد مستواك اليوم.\n"
        "• عدّل إعداداتك وتشغيل التذكير.",
        reply_markup=WATER_MENU_KB,
    )


def open_water_settings(update: Update, context: CallbackContext):
    update.message.reply_text(
        "إعدادات الماء ⚙️:\n"
        "1) حساب احتياجك اليومي من الماء بناءً على الجنس والعمر والوزن.\n"
        "2) تشغيل أو إيقاف التذكير الدوري بالماء.",
        reply_markup=WATER_SETTINGS_KB,
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
            reply_markup=MAIN_KEYBOARD,
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
            reply_markup=MAIN_KEYBOARD,
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
            reply_markup=MAIN_KEYBOARD,
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
        reply_markup=WATER_MENU_KB,
    )


def handle_log_cup(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)

    if not record.get("cups_goal"):
        update.message.reply_text(
            "لم تقم بعد بحساب احتياجك من الماء.\n"
            "اذهب إلى «إعدادات الماء ⚙️» ثم «حساب احتياج الماء 🧮».",
            reply_markup=WATER_MENU_KB,
        )
        return

    ensure_today_water(record)
    record["today_cups"] = record.get("today_cups", 0) + 1
    save_data()

    status_text = format_water_status_text(record)
    update.message.reply_text(
        f"🥤 تم تسجيل كوب ماء.\n\n{status_text}",
        reply_markup=WATER_MENU_KB,
    )


def handle_add_cups(update: Update, context: CallbackContext):
    """إدخال عدد أكواب دفعة واحدة (بشكل بسيط: قراءة الرقم من الرسالة مباشرة)."""
    user = update.effective_user
    record = get_user_record(user)
    text = (update.message.text or "").strip()

    if not record.get("cups_goal"):
        update.message.reply_text(
            "قبل استخدام هذه الميزة، احسب احتياجك من الماء أولًا من خلال:\n"
            "«إعدادات الماء ⚙️» → «حساب احتياج الماء 🧮».",
            reply_markup=WATER_MENU_KB,
        )
        return

    # هنا سنطلب من المستخدم أن يرسل عدد الأكواب مباشرة بعد أن يضغط الزر
    # لكي لا ندخل في حالات جديدة كثيرة، سنستخدم أسلوب بسيط:
    # أول ضغطة على الزر → نشرح له الطريقة
    if text == BTN_WATER_ADD_CUPS:
        update.message.reply_text(
            "أرسل الآن عدد الأكواب التي شربتها (بالأرقام فقط)، مثال: 2 أو 3.\n"
            "وسيتم إضافتها مباشرة إلى عدّاد اليوم.",
            reply_markup=CANCEL_KB,
        )
        return

    # لو كتب رقم وهو ليس في أي حالة أخرى، سنحاول تفسيره كأكواب
    try:
        cups = int(text)
        if cups <= 0 or cups > 50:
            raise ValueError()
    except ValueError:
        update.message.reply_text(
            "لو كنت تريد إضافة عدد من الأكواب، أرسل رقمًا منطقيًا مثل: 2 أو 3.\n"
            "أو استخدم بقية الأزرار للقائمة.",
            reply_markup=WATER_MENU_KB,
        )
        return

    ensure_today_water(record)
    record["today_cups"] = record.get("today_cups", 0) + cups
    save_data()

    status_text = format_water_status_text(record)
    update.message.reply_text(
        f"🥤 تم إضافة {cups} كوب إلى عدّادك اليوم.\n\n{status_text}",
        reply_markup=WATER_MENU_KB,
    )


def handle_status(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    text = format_water_status_text(record)
    update.message.reply_text(
        text,
        reply_markup=WATER_MENU_KB,
    )


def handle_reminders_on(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)

    if not record.get("cups_goal"):
        update.message.reply_text(
            "قبل تشغيل التذكير، احسب احتياجك من الماء من خلال:\n"
            "«حساب احتياج الماء 🧮».",
            reply_markup=WATER_SETTINGS_KB,
        )
        return

    record["reminders_on"] = True
    save_data()

    update.message.reply_text(
        "تم تشغيل تذكيرات الماء ⏰\n"
        "ستصلك رسائل خلال اليوم لتذكيرك بالشرب.",
        reply_markup=WATER_SETTINGS_KB,
    )


def handle_reminders_off(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    record["reminders_on"] = False
    save_data()

    update.message.reply_text(
        "تم إيقاف تذكيرات الماء 📴\n"
        "يمكنك تشغيلها مرة أخرى وقتما شئت.",
        reply_markup=WATER_SETTINGS_KB,
    )

# =================== قسم ورد القرآن ===================

def open_quran_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    get_user_record(user)
    update.message.reply_text(
        "وردي القرآني 📖:\n"
        "• عيّن عدد صفحات اليوم.\n"
        "• سجّل ما قرأته.\n"
        "• شاهد مستوى إنجازك.\n"
        "• يمكنك إعادة تعيين ورد اليوم.",
        reply_markup=QURAN_MENU_KB,
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
            reply_markup=MAIN_KEYBOARD,
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
        reply_markup=QURAN_MENU_KB,
    )


def handle_quran_add_pages_start(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)

    if not record.get("quran_pages_goal"):
        update.message.reply_text(
            "لم تضبط بعد ورد اليوم.\n"
            "استخدم «تعيين ورد اليوم 📌» أولًا.",
            reply_markup=QURAN_MENU_KB,
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
            reply_markup=MAIN_KEYBOARD,
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
        reply_markup=QURAN_MENU_KB,
    )


def handle_quran_status(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    text = format_quran_status_text(record)
    update.message.reply_text(
        text,
        reply_markup=QURAN_MENU_KB,
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
        reply_markup=QURAN_MENU_KB,
    )

# =================== قسم الأذكار ===================


def open_adhkar_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    get_user_record(user)
    update.message.reply_text(
        "أذكاري 🤲:\n"
        "• أذكار الصباح.\n"
        "• أذكار المساء.\n"
        "• أذكار عامة تريح القلب.",
        reply_markup=ADHKAR_MENU_KB,
    )


def send_morning_adhkar(update: Update, context: CallbackContext):
    user = update.effective_user
    increment_adhkar_count(user.id, 1)
    update.message.reply_text(
        ADHKAR_MORNING_TEXT,
        reply_markup=ADHKAR_MENU_KB,
    )


def send_evening_adhkar(update: Update, context: CallbackContext):
    user = update.effective_user
    increment_adhkar_count(user.id, 1)
    update.message.reply_text(
        ADHKAR_EVENING_TEXT,
        reply_markup=ADHKAR_MENU_KB,
    )


def send_general_adhkar(update: Update, context: CallbackContext):
    user = update.effective_user
    increment_adhkar_count(user.id, 1)
    update.message.reply_text(
        ADHKAR_GENERAL_TEXT,
        reply_markup=ADHKAR_MENU_KB,
    )

# =================== قسم السبحة ===================


def open_tasbih_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    ACTIVE_TASBIH.pop(user.id, None)
    WAITING_TASBIH.discard(user.id)

    text = "اختر الذكر الذي تريد التسبيح به، وسيقوم البوت بالعدّ لك:"
    update.message.reply_text(
        text,
        reply_markup=TASBIH_MENU_KB,
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
                reply_markup=TASBIH_RUN_KB,
            )
            return

    # لو ما كان مطابقًا لأي اختيار
    update.message.reply_text(
        "رجاءً اختر من الأذكار الظاهرة في القائمة.",
        reply_markup=TASBIH_MENU_KB,
    )


def handle_tasbih_tick(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id

    state = ACTIVE_TASBIH.get(user_id)
    if not state:
        update.message.reply_text(
            "ابدأ أولًا باختيار ذكر من قائمة «السبحة 📿».",
            reply_markup=TASBIH_MENU_KB,
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
            reply_markup=TASBIH_RUN_KB,
        )
    else:
        update.message.reply_text(
            f"اكتمل التسبيح على: {dhikr}\n"
            f"وصلت إلى {target} تسبيحة. تقبّل الله منك 🤍.",
            reply_markup=MAIN_KEYBOARD,
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
        reply_markup=TASBIH_MENU_KB,
    )

# =================== احصائياتي ===================


def handle_stats(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)

    ensure_today_water(record)
    ensure_today_quran(record)

    cups_goal = record.get("cups_goal")
    today_cups = record.get("today_cups", 0)

    q_goal = record.get("quran_pages_goal")
    q_today = record.get("quran_pages_today", 0)

    tasbih_total = record.get("tasbih_total", 0)
    adhkar_count = record.get("adhkar_count", 0)

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
    text_lines.append(f"- عدد المرات التي استعنت فيها بالأذكار عبر البوت: {adhkar_count} مرة.")

    # التسبيح
    text_lines.append(f"- مجموع التسبيحات المسجّلة عبر السبحة: {tasbih_total} تسبيحة.")

    update.message.reply_text(
        "\n".join(text_lines),
        reply_markup=MAIN_KEYBOARD,
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

# =================== هاندلر الرسائل ===================


def handle_text(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    msg = update.message
    text = (msg.text or "").strip()

    record = get_user_record(user)

    # زر الإلغاء العام
    if text == BTN_CANCEL:
        WAITING_GENDER.discard(user_id)
        WAITING_AGE.discard(user_id)
        WAITING_WEIGHT.discard(user_id)
        WAITING_QURAN_GOAL.discard(user_id)
        WAITING_QURAN_ADD_PAGES.discard(user_id)
        WAITING_TASBIH.discard(user_id)
        ACTIVE_TASBIH.pop(user_id, None)

        msg.reply_text(
            "تم الإلغاء. عدنا للقائمة الرئيسية.",
            reply_markup=MAIN_KEYBOARD,
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

    # حالات إدخال ورد القرآن
    if user_id in WAITING_QURAN_GOAL:
        handle_quran_goal_input(update, context)
        return

    if user_id in WAITING_QURAN_ADD_PAGES:
        handle_quran_add_pages_input(update, context)
        return

    # حالة السبحة أثناء العدّ
    if user_id in WAITING_TASBIH:
        if text == BTN_TASBIH_TICK:
            handle_tasbih_tick(update, context)
            return
        elif text == BTN_TASBIH_END:
            handle_tasbih_end(update, context)
            return
        # أي نص آخر أثناء التسبيح → نعامل كـ تسبيحة
        else:
            handle_tasbih_tick(update, context)
            return

    # الأزرار الرئيسية
    if text == BTN_ADHKAR_MAIN:
        open_adhkar_menu(update, context)
        return

    if text == BTN_QURAN_MAIN:
        open_quran_menu(update, context)
        return

    if text == BTN_WATER_MAIN:
        open_water_menu(update, context)
        return

    if text == BTN_STATS:
        handle_stats(update, context)
        return

    if text == BTN_TASBIH_MAIN:
        open_tasbih_menu(update, context)
        return

    if text == BTN_BACK_MAIN:
        msg.reply_text(
            "عدنا إلى القائمة الرئيسية.",
            reply_markup=MAIN_KEYBOARD,
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

    # منبّه الماء: القائمة
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
        # المرحلة الأولى: شرح الطريقة
        handle_add_cups(update, context)
        return

    # لو كتب رقم بعد ما ضغط زر إضافة عدد أكواب، سيتم التقاطه هنا:
    if text.isdigit():
        # نحاول تفسير الرقم كعدد أكواب، لكن بطريقة لا تؤثر على باقي الميزات
        handle_add_cups(update, context)
        return

    # ورد القرآن: الأزرار
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

    # أي نص آخر
    msg.reply_text(
        "اختر من الأزرار الموجودة أسفل الشاشة لنكمل معًا بإذن الله 🤍",
        reply_markup=MAIN_KEYBOARD,
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
