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

# مجموعات حالات الإدخال
WAITING_GENDER = set()
WAITING_AGE = set()
WAITING_WEIGHT = set()

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
    return "Water-bot is running ✅"


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
            # تقدم اليوم
            "today_date": None,
            "today_cups": 0,
            # تاريخ الاستخدام (لأجل إحصائياتي)
            "history": {},  # {"2025-01-01": 6, "2025-01-02": 4, ...}
        }
    else:
        record = data[user_id]
        record["first_name"] = user.first_name
        record["username"] = user.username
        record["last_active"] = now_iso

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

BTN_WATER_LOG = "سجلت كوب ماء 🥤"
BTN_WATER_STATUS = "مستواي اليوم 📊"
BTN_WATER_SETTINGS = "إعدادات الماء ⚙️"
BTN_STATS = "إحصائياتي 📈"

BTN_WATER_NEED = "حساب احتياج الماء 🧮"
BTN_WATER_REM_ON = "تشغيل التذكير ⏰"
BTN_WATER_REM_OFF = "إيقاف التذكير 📴"

BTN_GENDER_MALE = "🧔‍♂️ ذكر"
BTN_GENDER_FEMALE = "👩 أنثى"

BTN_BACK = "رجوع ⬅"
BTN_CANCEL = "إلغاء ❌"

MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_WATER_MAIN)],
    ],
    resize_keyboard=True,
)

WATER_MENU_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_WATER_LOG), KeyboardButton(BTN_WATER_STATUS)],
        [KeyboardButton(BTN_WATER_SETTINGS), KeyboardButton(BTN_STATS)],
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

# =================== دوال مساعدة ===================


def ensure_today_progress(record):
    """تصفير العدّاد إذا تغيّر اليوم + حفظ تاريخ اليوم السابق في الإحصائيات."""
    today_str = datetime.now(timezone.utc).date().isoformat()
    old_date = record.get("today_date")
    # لو كان فيه يوم سابق مختلف وله أكواب، نخزّنه في history
    if old_date and old_date != today_str:
        history = record.get("history", {})
        # لا نكتب إلا إذا ما تم تخزينه من قبل
        if old_date not in history:
            history[old_date] = record.get("today_cups", 0)
            record["history"] = history

    if record.get("today_date") != today_str:
        record["today_date"] = today_str
        record["today_cups"] = 0
        save_data()


def format_status_text(record):
    """نص حالة اليوم."""
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


def format_stats_text(record):
    """نص إحصائياتي 📈."""
    ensure_today_progress(record)
    cups_goal = record.get("cups_goal")
    today_cups = record.get("today_cups", 0)
    history = record.get("history", {})

    # إجمالي الأكواب
    total_past = sum(history.values()) if isinstance(history, dict) else 0
    total_all = total_past + today_cups

    # عدد الأيام المسجَّلة
    days_with_data = len([d for d, v in history.items() if v > 0]) if isinstance(history, dict) else 0
    if today_cups > 0:
        days_with_data += 1

    # أفضل يوم
    best_day_text = "لا توجد بيانات كافية بعد."
    if isinstance(history, dict) and history:
        # نبحث عن اليوم الذي شُرب فيه أعلى عدد أكواب
        best_date, best_cups = max(history.items(), key=lambda x: x[1])
        # تنسيق التاريخ للعرض
        try:
            d = datetime.fromisoformat(best_date).date()
            best_date_human = d.strftime("%Y-%m-%d")
        except Exception:
            best_date_human = best_date
        best_day_text = f"أفضل يوم كان بتاريخ {best_date_human} بعدد {best_cups} كوب تقريبًا."

    text_lines = []

    text_lines.append("📈 *ملخّص استخدامك لمنبّه الماء:*")
    text_lines.append("")

    text_lines.append(f"🔹 مجموع الأكواب المسجّلة حتى الآن: {total_all} كوب.")
    text_lines.append(f"🔹 عدد الأيام التي سجّلت فيها شرب الماء: {days_with_data} يوم تقريبًا.")

    text_lines.append("")
    text_lines.append(f"🔹 أكواب اليوم الحالي: {today_cups} كوب" + (f" من {cups_goal} كوب." if cups_goal else "."))

    text_lines.append("")
    text_lines.append(f"🔹 {best_day_text}")

    text_lines.append("")
    text_lines.append(
        "استمر في تسجيل أكوابك يوميًا، ومع الوقت ستلاحظ نمط تقدّمك وتُحفَّز أكثر على الالتزام 🤍."
    )

    return "\n".join(text_lines)

# =================== أوامر البوت ===================


def start_command(update: Update, context: CallbackContext):
    user = update.effective_user
    get_user_record(user)
    update.message.reply_text(
        f"مرحبًا {user.first_name} 👋\n\n"
        "هذا بوت منبّه الماء 💧.\n"
        "سأساعدك تحسب احتياجك من الماء وتتابع شربك خلال اليوم.\n\n"
        "اضغط على زر «منبّه الماء 💧» للبدء.",
        reply_markup=MAIN_KEYBOARD,
    )


def help_command(update: Update, context: CallbackContext):
    update.message.reply_text(
        "استخدم الأزرار أسفل الشاشة للتنقّل.\n"
        "• منبّه الماء 💧 → للدخول لجميع المزايا.\n"
        "داخل المنبّه يمكنك:\n"
        "• تسجيل كوب ماء 🥤\n"
        "• معرفة مستواك اليوم 📊\n"
        "• معرفة إحصائياتك 📈\n"
        "• إعداد احتياجك وتذكيرات الماء ⚙️",
        reply_markup=MAIN_KEYBOARD,
    )

# =================== وظائف الماء ===================


def open_water_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    get_user_record(user)
    update.message.reply_text(
        "اختر ما يناسبك من خيارات الماء:",
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
        "أولاً: اختر جنسك:",
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


def handle_stats(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    text = format_stats_text(record)
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
                    "💧 تذكير بلطف:\n"
                    "خذ الآن رشفة أو كوب ماء إن استطعت.\n\n"
                    f"شربت حتى الآن: {today_cups} من {cups_goal} كوب.\n"
                    f"المتبقي لهذا اليوم تقريبًا: {remaining} كوب."
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

    get_user_record(user)  # يتأكد من وجوده

    # زر الإلغاء العام
    if text == BTN_CANCEL:
        WAITING_GENDER.discard(user_id)
        WAITING_AGE.discard(user_id)
        WAITING_WEIGHT.discard(user_id)

        msg.reply_text(
            "تم الإلغاء. رجعناك للقائمة الرئيسية.",
            reply_markup=MAIN_KEYBOARD,
        )
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

    # الأزرار الرئيسية
    if text == BTN_WATER_MAIN:
        open_water_menu(update, context)
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

    if text == BTN_STATS:
        handle_stats(update, context)
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

    # أي نص آخر
    msg.reply_text(
        "اختر من الأزرار الموجودة أسفل الشاشة لنكمل معًا 💧",
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

    logger.info("Water bot is starting...")
    updater.start_polling()
    updater.idle()


if __name__ == "__main__":
    main()
