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
DATA_FILE = "water_users.json"

# مجموعات حالات الإدخال (ماء + قرآن + تسبيح)
WAITING_GENDER = set()
WAITING_AGE = set()
WAITING_WEIGHT = set()

WAITING_QURAN_GOAL = set()          # تعيين هدف الورد القرآني

WAITING_TASBIH_GENERIC = set()      # مسبحة حرة
WAITING_TASBIH_SEQUENCE = set()     # تسبيح بعد الصلاة

# حالات التسبيح
TASBIH_GENERIC_COUNT = {}           # {user_id: count}
TASBIH_SEQUENCE_STATE = {}          # {user_id: {"sequence": [...], "index": int, "current": int}}

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
    return "Suqya AlKawther bot is running ✅"


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
    """ينشئ أو يرجع سجل المستخدم، ويحدّث آخر نشاط."""
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
            # الورد القرآني
            "quran_goal_pages": None,
            "quran_today_date": None,
            "quran_today_pages": 0,
        }
    else:
        record = data[user_id]
        record["first_name"] = user.first_name
        record["username"] = user.username
        record["last_active"] = now_iso

        # لو الحساب قديم نضيف مفاتيح الورد القرآني إن لم تكن موجودة
        if "quran_goal_pages" not in record:
            record["quran_goal_pages"] = None
        if "quran_today_date" not in record:
            record["quran_today_date"] = None
        if "quran_today_pages" not in record:
            record["quran_today_pages"] = 0

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

# =================== أزرار البوت ===================

BTN_WATER_MAIN = "منبّه الماء 💧"
BTN_STATS = "إحصائياتي 📈"
BTN_ADHKAR = "أذكاري 📿"
BTN_QURAN_WIRD = "وردي القرآني 📖"

# منبّه الماء
BTN_WATER_LOG = "سجلت كوب ماء 🥤"
BTN_WATER_STATUS = "مستواي اليوم 📊"
BTN_WATER_SETTINGS = "إعدادات الماء ⚙️"

BTN_WATER_NEED = "حساب احتياج الماء 🧮"
BTN_WATER_REM_ON = "تشغيل التذكير ⏰"
BTN_WATER_REM_OFF = "إيقاف التذكير 📴"

# الجنس
BTN_GENDER_MALE = "🧔‍♂️ ذكر"
BTN_GENDER_FEMALE = "👩 أنثى"

# عام
BTN_BACK = "رجوع ⬅"
BTN_CANCEL = "إلغاء ❌"

# الأذكار
BTN_ADHKAR_MORNING = "أذكار الصباح 🌅"
BTN_ADHKAR_EVENING = "أذكار المساء 🌙"
BTN_ADHKAR_AFTER_PRAYER = "تسبيح بعد الصلاة 🕋"
BTN_TASBIH_FREE = "مسبحة حرة 🔢"

# التسبيح
BTN_TASBIH_PLUS = "تسبيح +1 ✅"
BTN_TASBIH_RESET = "تصفير العداد 🔄"
BTN_TASBIH_DONE = "إنهاء التسبيح ⬅"

# الورد القرآني
BTN_QURAN_SET_GOAL = "تعيين هدفي القرآني 🎯"
BTN_QURAN_LOG = "سجّلت ورد اليوم 📖"
BTN_QURAN_STATUS = "حالة وردي اليوم 📊"

# لوحات المفاتيح

MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_ADHKAR), KeyboardButton(BTN_QURAN_WIRD)],
        [KeyboardButton(BTN_WATER_MAIN), KeyboardButton(BTN_STATS)],
    ],
    resize_keyboard=True,
)

WATER_MENU_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_WATER_LOG), KeyboardButton(BTN_WATER_STATUS)],
        [KeyboardButton(BTN_WATER_SETTINGS)],
        [KeyboardButton(BTN_BACK)],
    ],
    resize_keyboard=True,
)

WATER_SETTINGS_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_WATER_NEED)],
        [KeyboardButton(BTN_WATER_REM_ON), KeyboardButton(BTN_WATER_REM_OFF)],
        [KeyboardButton(BTN_BACK)],
    ],
    resize_keyboard=True,
)

CANCEL_KB = ReplyKeyboardMarkup(
    [[KeyboardButton(BTN_CANCEL)]],
    resize_keyboard=True,
)

GENDER_KB = ReplyKeyboardMarkup(
    [[KeyboardButton(BTN_GENDER_MALE), KeyboardButton(BTN_GENDER_FEMALE)]],
    resize_keyboard=True,
)

ADHKAR_MENU_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_ADHKAR_MORNING), KeyboardButton(BTN_ADHKAR_EVENING)],
        [KeyboardButton(BTN_ADHKAR_AFTER_PRAYER)],
        [KeyboardButton(BTN_TASBIH_FREE)],
        [KeyboardButton(BTN_BACK)],
    ],
    resize_keyboard=True,
)

TASBIH_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_TASBIH_PLUS)],
        [KeyboardButton(BTN_TASBIH_RESET)],
        [KeyboardButton(BTN_TASBIH_DONE)],
    ],
    resize_keyboard=True,
)

QURAN_MENU_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_QURAN_SET_GOAL)],
        [KeyboardButton(BTN_QURAN_LOG), KeyboardButton(BTN_QURAN_STATUS)],
        [KeyboardButton(BTN_BACK)],
    ],
    resize_keyboard=True,
)

# =================== دوال مساعدة (ماء) ===================


def ensure_today_progress(record):
    """تصفير عدّاد الماء إذا تغيّر اليوم."""
    today_str = datetime.now(timezone.utc).date().isoformat()
    if record.get("today_date") != today_str:
        record["today_date"] = today_str
        record["today_cups"] = 0
        save_data()


def format_status_text(record):
    """نص حالة الماء اليوم."""
    ensure_today_progress(record)
    cups_goal = record.get("cups_goal")
    today_cups = record.get("today_cups", 0)

    if not cups_goal:
        return (
            "لم تقم بعد بحساب احتياجك من الماء.\n"
            "اذهب إلى «إعدادات الماء ⚙️» ثم اختر «حساب احتياج الماء 🧮» أولاً."
        )

    remaining = max(cups_goal - today_cups, 0)
    percent = min(int(today_cups / cups_goal * 100), 100)

    text = (
        f"📊 *مستوى شرب الماء اليوم:*\n\n"
        f"الأكواب التي شربتها: {today_cups} من {cups_goal} كوب.\n"
        f"نسبة الإنجاز التقريبية: {percent}%.\n\n"
    )

    if remaining > 0:
        text += (
            f"تبقّى لك تقريبًا {remaining} كوب لتصل لهدفك اليومي 💧.\n"
            "استمر بهدوء، رشفة بعد رشفة 🤍."
        )
    else:
        text += (
            "ممتاز! وصلت لهدفك اليومي من الماء 🎉\n"
            "حافظ على هذا المستوى يوميًا قدر المستطاع."
        )

    return text

# =================== دوال مساعدة (ورد القرآن) ===================


def ensure_today_quran(record):
    """تصفير عدّاد الورد اليومي إذا تغيّر اليوم."""
    today_str = datetime.now(timezone.utc).date().isoformat()
    if record.get("quran_today_date") != today_str:
        record["quran_today_date"] = today_str
        record["quran_today_pages"] = 0
        save_data()


def format_quran_status_text(record):
    """نص حالة الورد القرآني اليوم."""
    ensure_today_quran(record)
    goal = record.get("quran_goal_pages")
    done = record.get("quran_today_pages", 0)

    if not goal:
        return (
            "لم تقم بعد بتعيين هدف لوردك القرآني.\n"
            "من «وردي القرآني 📖» اختر «تعيين هدفي القرآني 🎯» أولاً."
        )

    remaining = max(goal - done, 0)
    percent = min(int(done / goal * 100), 100)

    text = (
        f"📊 *حالة وردك اليومي من القرآن:*\n\n"
        f"ما قرأته اليوم تقريبًا: {done} صفحة من {goal} صفحة.\n"
        f"نسبة الإنجاز التقريبية: {percent}%.\n\n"
    )

    if remaining > 0:
        text += (
            f"تبقّى لك تقريبًا {remaining} صفحة لتصل لهدفك اليومي.\n"
            "قسّمها على أوقات الصلوات، صفحة أو صفحتين بعد كل صلاة مثلًا 🤍."
        )
    else:
        text += (
            "ما شاء الله، وصلت لهدفك القرآني لهذا اليوم 🌿\n"
            "ثبّتك الله على تلاوة كتابه دائمًا."
        )

    return text

# =================== أوامر البوت ===================


def start_command(update: Update, context: CallbackContext):
    user = update.effective_user
    get_user_record(user)
    update.message.reply_text(
        f"مرحبًا {user.first_name} 👋\n\n"
        "أهلاً بك في *بوت سقيا الكوثر* 💧\n\n"
        "هنا تجد مزيجًا بين العناية بجسدك وروحك:\n"
        "• منبّه الماء لتنظيم شربك للماء.\n"
        "• إحصائيات بسيطة لصحتك.\n"
        "• أذكاري لمساحة ذكر وتسبيح.\n"
        "• وردي القرآني لمتابعة قراءتك اليومية.\n\n"
        "اختر من الأزرار بالأسفل لنبدأ معًا 🤍.",
        reply_markup=MAIN_KEYBOARD,
        parse_mode="Markdown",
    )


def help_command(update: Update, context: CallbackContext):
    update.message.reply_text(
        "طريقة استخدام البوت:\n\n"
        "• «أذكاري 📿» → أذكار الصباح والمساء، وتسبيح بعد الصلاة، ومسبحة حرة.\n"
        "• «وردي القرآني 📖» → تعيين هدف يومي لقراءتك ومتابعة إنجازك.\n"
        "• «منبّه الماء 💧» → حساب احتياجك من الماء ومتابعة الأكواب التي تشربها.\n"
        "• «إحصائياتي 📈» → ملخص لبيانات الماء (وممكن لاحقًا إضافة مزيد من الإحصاءات).",
        reply_markup=MAIN_KEYBOARD,
    )

# =================== وظائف منبّه الماء ===================


def open_water_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    get_user_record(user)
    update.message.reply_text(
        "من هنا تدير منبّه الماء:\n"
        "• سجّل كل كوب تشربه 🥤\n"
        "• تابع مستواك اليومي 📊\n"
        "• اضبط إعدادات واحتياجك من الماء ⚙️",
        reply_markup=WATER_MENU_KB,
    )


def open_water_settings(update: Update, context: CallbackContext):
    update.message.reply_text(
        "هذه إعدادات الماء:\n"
        "• حساب احتياجك اليومي 🧮\n"
        "• تشغيل / إيقاف التذكير ⏰",
        reply_markup=WATER_SETTINGS_KB,
    )


def handle_water_need_start(update: Update, context: CallbackContext):
    user_id = update.effective_user.id

    # تفعيل حالة اختيار الجنس
    WAITING_GENDER.add(user_id)
    WAITING_AGE.discard(user_id)
    WAITING_WEIGHT.discard(user_id)

    update.message.reply_text(
        "أولًا: اختر جنسِك:",
        reply_markup=GENDER_KB,
    )


def handle_gender_input(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    text = update.message.text.strip()

    if text == BTN_CANCEL:
        WAITING_GENDER.discard(user_id)
        update.message.reply_text(
            "تم الإلغاء. رجعناك للقائمة الرئيسية.",
            reply_markup=MAIN_KEYBOARD,
        )
        return

    if text not in [BTN_GENDER_MALE, BTN_GENDER_FEMALE]:
        update.message.reply_text(
            "رجاءً اختر من الخيارات أمامك:",
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
        "جميل 👍\n"
        "الآن أرسل عمرك (بالسنوات)، مثال: 25",
        reply_markup=CANCEL_KB,
    )


def handle_age_input(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    text = update.message.text.strip()

    if text == BTN_CANCEL:
        WAITING_AGE.discard(user_id)
        update.message.reply_text(
            "تم الإلغاء. رجعناك للقائمة الرئيسية.",
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
        "شكرًا 🌿\n"
        "الآن أرسل وزنك بالكيلوغرام، مثال: 70",
        reply_markup=CANCEL_KB,
    )


def handle_weight_input(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    text = update.message.text.strip()

    if text == BTN_CANCEL:
        WAITING_WEIGHT.discard(user_id)
        update.message.reply_text(
            "تم الإلغاء. رجعناك للقائمة الرئيسية.",
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
        rate = 0.035  # لتر لكل كغ تقريبًا
    else:
        rate = 0.033

    water_liters = weight * rate
    cups_goal = max(int(round(water_liters * 1000 / 250)), 1)  # كوب 250 مل تقريبًا

    record["water_liters"] = round(water_liters, 2)
    record["cups_goal"] = cups_goal
    save_data()

    WAITING_WEIGHT.discard(user_id)

    update.message.reply_text(
        f"تم حساب احتياجك اليومي من الماء 💧\n\n"
        f"🔹 حوالي: {record['water_liters']} لتر في اليوم.\n"
        f"🔹 ما يعادل تقريبًا: {cups_goal} كوب (بمتوسط 250 مل للكوب).\n\n"
        "احرص على توزيعها على اليوم كامل، وسأساعدك بالتذكير والمتابعة.",
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

    ensure_today_progress(record)
    record["today_cups"] = record.get("today_cups", 0) + 1
    save_data()

    status_text = format_status_text(record)
    update.message.reply_text(
        f"🥤 تم تسجيل كوب ماء.\n\n{status_text}",
        parse_mode="Markdown",
        reply_markup=WATER_MENU_KB,
    )


def handle_status(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    text = format_status_text(record)
    update.message.reply_text(
        text,
        parse_mode="Markdown",
        reply_markup=WATER_MENU_KB,
    )


def handle_reminders_on(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)

    if not record.get("cups_goal"):
        update.message.reply_text(
            "قبل تشغيل التذكير، احسب احتياجك من الماء من خلال:\n"
            "«إعدادات الماء ⚙️» → «حساب احتياج الماء 🧮».",
            reply_markup=WATER_SETTINGS_KB,
        )
        return

    record["reminders_on"] = True
    save_data()

    update.message.reply_text(
        "تم تشغيل تذكيرات الماء ⏰\n"
        "ستصلك رسائل خلال اليوم لتذكيرك بالشرب.\n"
        "يمكنك إيقافها من نفس المكان متى أحببت.",
        reply_markup=WATER_SETTINGS_KB,
    )


def handle_reminders_off(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    record["reminders_on"] = False
    save_data()

    update.message.reply_text(
        "تم إيقاف تذكيرات الماء 📴\n"
        "يمكنك تشغيلها مرة أخرى في أي وقت.",
        reply_markup=WATER_SETTINGS_KB,
    )

# =================== تذكيرات الماء (JobQueue) ===================

REMINDER_HOURS_UTC = [7, 10, 13, 16, 19]  # أوقات تقريبية (بتوقيت UTC)


def water_reminder_job(context: CallbackContext):
    logger.info("Running water reminder job...")
    bot = context.bot
    now = datetime.now(timezone.utc)

    for uid in get_all_user_ids():
        rec = data.get(str(uid)) or {}
        if not rec.get("reminders_on"):
            continue

        # تأكد من تحديث اليوم
        ensure_today_progress(rec)
        cups_goal = rec.get("cups_goal")
        today_cups = rec.get("today_cups", 0)
        if not cups_goal:
            continue

        remaining = max(cups_goal - today_cups, 0)

        try:
            bot.send_message(
                chat_id=uid,
                text=(
                    "💧 تذكير لطيف:\n"
                    "خذ الآن رشفة أو كوب ماء إن استطعت.\n\n"
                    f"شربت حتى الآن: {today_cups} من {cups_goal} كوب.\n"
                    f"المتبقي لهذا اليوم تقريبًا: {remaining} كوب."
                ),
            )
        except Exception as e:
            logger.error(f"Error sending water reminder to {uid}: {e}")

# =================== إحصائياتي ===================


def handle_stats(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)

    ensure_today_progress(record)
    ensure_today_quran(record)

    gender = record.get("gender")
    gender_text = None
    if gender == "male":
        gender_text = "ذكر"
    elif gender == "female":
        gender_text = "أنثى"

    age = record.get("age")
    weight = record.get("weight")
    water_liters = record.get("water_liters")
    cups_goal = record.get("cups_goal")
    reminders_on = record.get("reminders_on", False)
    today_cups = record.get("today_cups", 0)

    q_goal = record.get("quran_goal_pages")
    q_done = record.get("quran_today_pages", 0)

    created_at = record.get("created_at")
    days_since = None
    if created_at:
        try:
            created_dt = datetime.fromisoformat(created_at)
            if created_dt.tzinfo is None:
                created_dt = created_dt.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            days_since = (now.date() - created_dt.date()).days
        except Exception:
            days_since = None

    text_lines = [
        "📈 *ملخص بياناتك في سقيا الكوثر*:\n",
        f"👤 الاسم: {record.get('first_name') or 'غير محدد'}",
        f"🔹 اسم المستخدم: @{record.get('username')}" if record.get("username") else "🔹 اسم المستخدم: غير متوفر",
        "",
        "⚙️ *بيانات الماء:*",
        f"• الجنس: {gender_text or 'لم يتم تحديده بعد'}",
        f"• العمر: {age if age is not None else 'غير محدد'}",
        f"• الوزن: {weight if weight is not None else 'غير محدد'}",
        f"• الاحتياج اليومي من الماء: {water_liters} لتر" if water_liters else "• الاحتياج اليومي من الماء: لم يُحسب بعد",
        f"• الهدف اليومي: {cups_goal} كوب" if cups_goal else "• الهدف اليومي: لم يُحدد بعد",
        f"• حالة التذكير: {'مفعّل ⏰' if reminders_on else 'متوقف 📴'}",
        "",
    ]

    if days_since is not None:
        text_lines.append(f"📅 مدة استخدامك للبوت تقريبًا: {days_since} يومًا.")
        text_lines.append("")

    if cups_goal:
        w_percent = min(int(today_cups / cups_goal * 100), 100)
        text_lines.append("📊 *إنجازك اليوم في الماء:*")
        text_lines.append(f"• الأكواب التي شربتها اليوم: {today_cups} من {cups_goal} كوب.")
        text_lines.append(f"• النسبة التقريبية: {w_percent}%.")
        remaining = max(cups_goal - today_cups, 0)
        if remaining > 0:
            text_lines.append(f"• المتبقي لهدفك اليومي من الماء: {remaining} كوب.")
        else:
            text_lines.append("• أحسنت، وصلت إلى هدفك اليومي من الماء اليوم 🎉.")
        text_lines.append("")
    else:
        text_lines.append(
            "لم تقم بتحديد احتياجك من الماء بعد.\n"
            "اذهب إلى: «منبّه الماء 💧» → «إعدادات الماء ⚙️» → «حساب احتياج الماء 🧮»."
        )
        text_lines.append("")

    text_lines.append("📖 *بيانات وردك القرآني:*")
    if q_goal:
        q_percent = min(int(q_done / q_goal * 100), 100)
        text_lines.append(f"• هدفك اليومي: {q_goal} صفحة.")
        text_lines.append(f"• ما قرأته اليوم: {q_done} صفحة.")
        text_lines.append(f"• النسبة التقريبية: {q_percent}%.")
        q_remaining = max(q_goal - q_done, 0)
        if q_remaining > 0:
            text_lines.append(f"• المتبقي لهذا اليوم: {q_remaining} صفحة.")
        else:
            text_lines.append("• ما شاء الله، أتممت وردك القرآني لهذا اليوم 🌿.")
    else:
        text_lines.append(
            "لم تعيّن هدفًا لوردك بعد.\n"
            "من «وردي القرآني 📖» اختر «تعيين هدفي القرآني 🎯»."
        )

    update.message.reply_text(
        "\n".join(text_lines),
        parse_mode="Markdown",
        reply_markup=MAIN_KEYBOARD,
    )

# =================== أذكاري (صباح / مساء / تسبيح) ===================


def handle_adhkar(update: Update, context: CallbackContext):
    text = (
        "📿 *قسم أذكاري:*\n\n"
        "من هنا يمكنك:\n"
        "• قراءة أذكار الصباح 🌅\n"
        "• قراءة أذكار المساء 🌙\n"
        "• عمل تسبيح بعد الصلاة بتسلسل 33/33/34 🕋\n"
        "• استخدام مسبحة حرة لعدد لا نهائي من التسبيحات 🔢\n\n"
        "اختر ما يناسبك من الأزرار بالأسفل 🤍."
    )
    update.message.reply_text(
        text,
        parse_mode="Markdown",
        reply_markup=ADHKAR_MENU_KB,
    )


def handle_adhkar_morning(update: Update, context: CallbackContext):
    """أذكار صباح مختصرة من الأذكار الصحيحة المعروفة."""
    text = (
        "🌅 *أذكار الصباح (مختارة):*\n\n"
        "1️⃣  \"أَصْبَحْنَا وَأَصْبَحَ المُلكُ لِلَّهِ، والحمدُ للَّه، "
        "لا إلهَ إلا اللَّه وحدَه لا شريكَ له، له المُلكُ وله الحمدُ، "
        "وهو على كلِّ شيءٍ قدير. ربِّ أسألُكَ خيرَ ما في هذا اليومِ "
        "وخيرَ ما بعدَه، وأعوذُ بك من شرِّ ما في هذا اليومِ وشرِّ ما بعدَه...\" مرّة واحدة.\n\n"
        "2️⃣  \"اللَّهُمَّ بك أصبحنا، وبك أمسينا، وبك نحيا، وبك نموت، وإليك النشور\" مرّة واحدة.\n\n"
        "3️⃣  \"سُبحانَ اللَّهِ وبحمدِهِ\" 100 مرّة.\n\n"
        "4️⃣  \"لا إلهَ إلا اللَّه وحدَه لا شريكَ له، له المُلك وله الحمد، "
        "وهو على كلِّ شيءٍ قدير\" 100 مرّة.\n\n"
        "يمكنك استخدام «مسبحة حرة 🔢» للمساعدة في العدّ عند الأذكار ذات العدد الكبير 🤍."
    )
    update.message.reply_text(
        text,
        parse_mode="Markdown",
        reply_markup=ADHKAR_MENU_KB,
    )


def handle_adhkar_evening(update: Update, context: CallbackContext):
    text = (
        "🌙 *أذكار المساء (مختارة):*\n\n"
        "1️⃣  \"أمسينا وأمسى المُلكُ للَّه، والحمدُ للَّه، "
        "لا إلهَ إلا اللَّه وحدَه لا شريكَ له، له المُلكُ وله الحمدُ، "
        "وهو على كلِّ شيءٍ قدير...\" مرّة واحدة.\n\n"
        "2️⃣  \"اللَّهُمَّ بك أمسينا، وبك أصبحنا، وبك نحيا، وبك نموت، وإليك المصير\" مرّة واحدة.\n\n"
        "3️⃣  آية الكرسي: {اللّهُ لا إِلَهَ إِلاّ هُوَ الْحَيّ الْقَيّومُ...} مرّة واحدة.\n\n"
        "4️⃣  المعوّذات (الإخلاص، الفلق، الناس) ثلاث مرّات.\n\n"
        "استخدِم «مسبحة حرة 🔢» للعدّ إن أحببت، وخُذ وقتك مع الذكر بهدوء 🤍."
    )
    update.message.reply_text(
        text,
        parse_mode="Markdown",
        reply_markup=ADHKAR_MENU_KB,
    )


def handle_adhkar_after_prayer(update: Update, context: CallbackContext):
    """بدء تسبيح بعد الصلاة: 33 سبحان الله، 33 الحمد لله، 34 الله أكبر."""
    user_id = update.effective_user.id

    TASBIH_SEQUENCE_STATE[user_id] = {
        "sequence": [
            {"phrase": "سبحان الله", "target": 33},
            {"phrase": "الحمد لله", "target": 33},
            {"phrase": "الله أكبر", "target": 34},
        ],
        "index": 0,
        "current": 0,
    }
    WAITING_TASBIH_SEQUENCE.add(user_id)

    seq = TASBIH_SEQUENCE_STATE[user_id]["sequence"][0]
    update.message.reply_text(
        "🕋 *تسبيح بعد الصلاة:*\n\n"
        "التسلسل الكامل:\n"
        "• سبحان الله 33 مرة\n"
        "• الحمد لله 33 مرة\n"
        "• الله أكبر 34 مرة\n\n"
        "الآن نبدأ بالجزء الأول:\n"
        f"🔹 {seq['phrase']} ({seq['target']} مرة)\n\n"
        "اضغط «تسبيح +1 ✅» لكل مرة تسبّح بها.\n"
        "يمكنك إعادة التصفير أو الإنهاء في أي وقت.",
        parse_mode="Markdown",
        reply_markup=TASBIH_KB,
    )


def handle_tasbih_free_start(update: Update, context: CallbackContext):
    """بدء مسبحة حرة بلا حد معيّن."""
    user_id = update.effective_user.id
    TASBIH_GENERIC_COUNT[user_id] = 0
    WAITING_TASBIH_GENERIC.add(user_id)

    update.message.reply_text(
        "🔢 *مسبحة حرة:*\n\n"
        "اضغط «تسبيح +1 ✅» في كل مرة تقول فيها ذكرًا (سبحان الله، الحمد لله، أو أي ذكر).\n"
        "• «تصفير العداد 🔄» لإعادته إلى الصفر.\n"
        "• «إنهاء التسبيح ⬅» للخروج والعودة للقائمة.",
        parse_mode="Markdown",
        reply_markup=TASBIH_KB,
    )

# =================== وردي القرآني ===================


def open_quran_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    ensure_today_quran(record)

    text = (
        "📖 *وردي القرآني:*\n\n"
        "من هنا تنظّم وردك اليومي من القرآن:\n"
        "• عيّن عدد الصفحات التي تريد قراءتها يوميًا.\n"
        "• سجّل كل مرة تقرأ فيها جزءًا من وردك.\n"
        "• راقب إنجازك اليومي بسهولة.\n\n"
        "اختر من الأزرار بالأسفل:"
    )
    update.message.reply_text(
        text,
        parse_mode="Markdown",
        reply_markup=QURAN_MENU_KB,
    )


def handle_quran_set_goal_start(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    WAITING_QURAN_GOAL.add(user_id)

    update.message.reply_text(
        "🎯 أرسل الآن عدد الصفحات الذي تريد جعله هدفًا يوميًا لوردك.\n"
        "مثال: 5 أو 10 أو 20.\n\n"
        "يمكنك دائمًا تعديل هذا الهدف لاحقًا.",
        reply_markup=CANCEL_KB,
    )


def handle_quran_goal_input(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    text = update.message.text.strip()

    if text == BTN_CANCEL:
        WAITING_QURAN_GOAL.discard(user_id)
        update.message.reply_text(
            "تم الإلغاء. رجعناك للقائمة الرئيسية.",
            reply_markup=MAIN_KEYBOARD,
        )
        return

    try:
        pages = int(text)
        if pages <= 0 or pages > 100:
            raise ValueError()
    except ValueError:
        update.message.reply_text(
            "رجاءً أرسل رقم صفحات منطقي (بين 1 و 100 تقريبًا)، مثال: 10",
            reply_markup=CANCEL_KB,
        )
        return

    record = get_user_record(user)
    record["quran_goal_pages"] = pages
    # تصفير تقدم اليوم
    record["quran_today_date"] = None
    record["quran_today_pages"] = 0
    save_data()

    WAITING_QURAN_GOAL.discard(user_id)

    update.message.reply_text(
        f"تم تعيين هدفك القرآني اليومي على: {pages} صفحة ✅\n\n"
        "لا تنس تسجيل التقدّم من زر «سجّلت ورد اليوم 📖».",
        reply_markup=QURAN_MENU_KB,
    )


def handle_quran_log(update: Update, context: CallbackContext):
    """كل ضغطة تسجّل صفحة واحدة تمت قراءتها."""
    user = update.effective_user
    record = get_user_record(user)

    goal = record.get("quran_goal_pages")
    if not goal:
        update.message.reply_text(
            "لم تعيّن هدفًا لوردك بعد.\n"
            "من نفس القسم اختر «تعيين هدفي القرآني 🎯».",
            reply_markup=QURAN_MENU_KB,
        )
        return

    ensure_today_quran(record)
    record["quran_today_pages"] = record.get("quran_today_pages", 0) + 1
    save_data()

    status = format_quran_status_text(record)
    update.message.reply_text(
        f"📖 تم تسجيل صفحة جديدة في وردك.\n\n{status}",
        parse_mode="Markdown",
        reply_markup=QURAN_MENU_KB,
    )


def handle_quran_status(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    text = format_quran_status_text(record)
    update.message.reply_text(
        text,
        parse_mode="Markdown",
        reply_markup=QURAN_MENU_KB,
    )

# =================== هاندلر التسبيح في الوضعين ===================


def handle_tasbih_generic(update: Update, context: CallbackContext):
    """التعامل مع مسبحة حرة."""
    user_id = update.effective_user.id
    msg_text = update.message.text.strip()

    if msg_text == BTN_TASBIH_PLUS:
        count = TASBIH_GENERIC_COUNT.get(user_id, 0) + 1
        TASBIH_GENERIC_COUNT[user_id] = count
        update.message.reply_text(
            f"العدّاد الآن: {count}",
            reply_markup=TASBIH_KB,
        )
        return

    if msg_text == BTN_TASBIH_RESET:
        TASBIH_GENERIC_COUNT[user_id] = 0
        update.message.reply_text(
            "تم تصفير العداد إلى 0.",
            reply_markup=TASBIH_KB,
        )
        return

    if msg_text == BTN_TASBIH_DONE:
        total = TASBIH_GENERIC_COUNT.get(user_id, 0)
        WAITING_TASBIH_GENERIC.discard(user_id)
        TASBIH_GENERIC_COUNT.pop(user_id, None)
        update.message.reply_text(
            f"انتهيت من المسبحة الحرة.\n"
            f"مجموع ما سبّحته في هذه الجلسة: {total} مرة.\n"
            "جعله الله في ميزان حسناتك 🤍.",
            reply_markup=ADHKAR_MENU_KB,
        )
        return

    # أي نص آخر داخل وضع المسبحة الحرة
    update.message.reply_text(
        "استخدم الأزرار الخاصة بالمسبحة:\n"
        "• تسبيح +1 ✅\n"
        "• تصفير العداد 🔄\n"
        "• إنهاء التسبيح ⬅",
        reply_markup=TASBIH_KB,
    )


def handle_tasbih_sequence(update: Update, context: CallbackContext):
    """التعامل مع تسبيح بعد الصلاة (تسلسل 33/33/34)."""
    user_id = update.effective_user.id
    msg_text = update.message.text.strip()

    state = TASBIH_SEQUENCE_STATE.get(user_id)
    if not state:
        # لخبطة بسيطة: خروج
        WAITING_TASBIH_SEQUENCE.discard(user_id)
        update.message.reply_text(
            "تم إنهاء وضع التسبيح.\nيمكنك البدء من جديد عبر «تسبيح بعد الصلاة 🕋».",
            reply_markup=ADHKAR_MENU_KB,
        )
        return

    if msg_text == BTN_TASBIH_RESET:
        state["current"] = 0
        seq = state["sequence"][state["index"]]
        update.message.reply_text(
            f"تم تصفير العداد لهذا الذكر.\n"
            f"الذكر الحالي: {seq['phrase']} ({seq['target']} مرة).",
            reply_markup=TASBIH_KB,
        )
        return

    if msg_text == BTN_TASBIH_DONE:
        WAITING_TASBIH_SEQUENCE.discard(user_id)
        TASBIH_SEQUENCE_STATE.pop(user_id, None)
        update.message.reply_text(
            "تم إنهاء وضع التسبيح بعد الصلاة.\n"
            "إن أحببت يمكنك العودة وإكماله من جديد في أي وقت 🤍.",
            reply_markup=ADHKAR_MENU_KB,
        )
        return

    if msg_text == BTN_TASBIH_PLUS:
        seq_list = state["sequence"]
        idx = state["index"]
        cur = state["current"]

        seq = seq_list[idx]
        cur += 1
        state["current"] = cur

        if cur < seq["target"]:
            update.message.reply_text(
                f"الذكر: {seq['phrase']}\n"
                f"العدّاد: {cur} / {seq['target']}",
                reply_markup=TASBIH_KB,
            )
            return
        else:
            # أنهى هذا الذكر
            idx += 1
            if idx < len(seq_list):
                # ننتقل للذكر التالي
                state["index"] = idx
                state["current"] = 0
                next_seq = seq_list[idx]
                update.message.reply_text(
                    f"أحسنت، أنهيت:\n{seq['phrase']} ({seq['target']} مرة) ✅\n\n"
                    f"الآن انتقل إلى:\n{next_seq['phrase']} ({next_seq['target']} مرة)\n"
                    "واصل على نفس الزر «تسبيح +1 ✅».",
                    reply_markup=TASBIH_KB,
                )
                return
            else:
                # انتهى من التسلسل كاملًا
                WAITING_TASBIH_SEQUENCE.discard(user_id)
                TASBIH_SEQUENCE_STATE.pop(user_id, None)
                update.message.reply_text(
                    "ما شاء الله، أتممت تسبيحك بعد الصلاة كاملًا:\n"
                    "• سبحان الله 33 مرة\n"
                    "• الحمد لله 33 مرة\n"
                    "• الله أكبر 34 مرة\n\n"
                    "نسأل الله أن يكتب لك الأجر كاملًا 🤍.",
                    reply_markup=ADHKAR_MENU_KB,
                )
                return

    # أي نص آخر داخل وضع التسبيح
    update.message.reply_text(
        "أنت الآن في وضع تسبيح بعد الصلاة.\n"
        "استخدم الأزرار:\n"
        "• تسبيح +1 ✅\n"
        "• تصفير العداد 🔄\n"
        "• إنهاء التسبيح ⬅",
        reply_markup=TASBIH_KB,
    )

# =================== هاندلر الرسائل ===================


def handle_text(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    msg = update.message
    text = (msg.text or "").strip()

    record = get_user_record(user)  # يتأكد من وجوده

    # زر الإلغاء العام
    if text == BTN_CANCEL:
        WAITING_GENDER.discard(user_id)
        WAITING_AGE.discard(user_id)
        WAITING_WEIGHT.discard(user_id)
        WAITING_QURAN_GOAL.discard(user_id)
        WAITING_TASBIH_GENERIC.discard(user_id)
        WAITING_TASBIH_SEQUENCE.discard(user_id)
        TASBIH_GENERIC_COUNT.pop(user_id, None)
        TASBIH_SEQUENCE_STATE.pop(user_id, None)

        msg.reply_text(
            "تم الإلغاء. رجعناك للقائمة الرئيسية.",
            reply_markup=MAIN_KEYBOARD,
        )
        return

    # حالات التسبيح أولًا (لأنهم في وضع خاص)
    if user_id in WAITING_TASBIH_GENERIC:
        handle_tasbih_generic(update, context)
        return

    if user_id in WAITING_TASBIH_SEQUENCE:
        handle_tasbih_sequence(update, context)
        return

    # مراحل إدخال إعدادات الماء
    if user_id in WAITING_GENDER:
        handle_gender_input(update, context)
        return

    if user_id in WAITING_AGE:
        handle_age_input(update, context)
        return

    if user_id in WAITING_WEIGHT:
        handle_weight_input(update, context)
        return

    # تعيين هدف الورد القرآني
    if user_id in WAITING_QURAN_GOAL:
        handle_quran_goal_input(update, context)
        return

    # الأزرار الرئيسية
    if text == BTN_ADHKAR:
        handle_adhkar(update, context)
        return

    if text == BTN_QURAN_WIRD:
        open_quran_menu(update, context)
        return

    if text == BTN_WATER_MAIN:
        open_water_menu(update, context)
        return

    if text == BTN_STATS:
        handle_stats(update, context)
        return

    if text == BTN_BACK:
        msg.reply_text(
            "تم الرجوع للقائمة الرئيسية.",
            reply_markup=MAIN_KEYBOARD,
        )
        return

    # قائمة منبّه الماء
    if text == BTN_WATER_LOG:
        handle_log_cup(update, context)
        return

    if text == BTN_WATER_STATUS:
        handle_status(update, context)
        return

    if text == BTN_WATER_SETTINGS:
        open_water_settings(update, context)
        return

    # إعدادات الماء
    if text == BTN_WATER_NEED:
        handle_water_need_start(update, context)
        return

    if text == BTN_WATER_REM_ON:
        handle_reminders_on(update, context)
        return

    if text == BTN_WATER_REM_OFF:
        handle_reminders_off(update, context)
        return

    # أذكاري: صباح / مساء / بعد الصلاة / مسبحة حرة
    if text == BTN_ADHKAR_MORNING:
        handle_adhkar_morning(update, context)
        return

    if text == BTN_ADHKAR_EVENING:
        handle_adhkar_evening(update, context)
        return

    if text == BTN_ADHKAR_AFTER_PRAYER:
        handle_adhkar_after_prayer(update, context)
        return

    if text == BTN_TASBIH_FREE:
        handle_tasbih_free_start(update, context)
        return

    # وردي القرآني
    if text == BTN_QURAN_SET_GOAL:
        handle_quran_set_goal_start(update, context)
        return

    if text == BTN_QURAN_LOG:
        handle_quran_log(update, context)
        return

    if text == BTN_QURAN_STATUS:
        handle_quran_status(update, context)
        return

    # أي نص آخر
    msg.reply_text(
        "اختر من الأزرار الموجودة أسفل الشاشة لنكمل معًا 🌿",
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

    logger.info("Suqya AlKawther bot is starting...")
    updater.start_polling()
    updater.idle()


if __name__ == "__main__":
    main()
