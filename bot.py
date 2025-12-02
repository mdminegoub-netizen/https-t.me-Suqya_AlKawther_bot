import os
import json
import logging
import re
import random
from datetime import datetime, timezone, time
from threading import Thread
from dateutil import parser as dateutil_parser

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
SUPERVISOR_ID = 8395818573

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# =================== خادم ويب لـ Render ===================
app = Flask(__name__)

@app.route("/")
def index():
    return "Suqya Al-Kawther bot is running"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
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

# =================== إعدادات الجرعة التحفيزية ===================
DEFAULT_MOTIVATION_HOURS_UTC = [6, 9, 12, 15, 18, 21]
DEFAULT_MOTIVATION_MESSAGES = [
    "تذكّر: قليلٌ دائم خيرٌ من كثير منقطع، خطوة اليوم تقرّبك من نسختك الأفضل",
    "جرعة ماء + آية من القرآن + ذكر بسيط = راحة قلب يوم كامل بإذن الله.",
    "مهما كان يومك مزدحمًا، قلبك يستحق لحظات هدوء مع ذكر الله.",
    "لو شعرت بثقل، افتح المصحف صفحة واحدة فقط… ستشعر أن همّك خفّ ولو قليلًا.",
    "لا تستصغر كوب ماء تشربه بنية حفظ الصحة، ولا صفحة قرآن تقرؤها بنية القرب من الله.",
    "قل: الحمد لله الآن… أحيانًا شكرٌ صادق يغيّر مزاج يومك كله.",
    "استعن بالله ولا تعجز، كل محاولة للالتزام خير، حتى لو تعثّرت بعدها.",
]

GLOBAL_KEY = "_global_config"
MOTIVATION_HOURS_UTC = []
MOTIVATION_MESSAGES = []

def get_global_config():
    cfg = data.get(GLOBAL_KEY, {})
    if "motivation_hours" not in cfg:
        cfg["motivation_hours"] = DEFAULT_MOTIVATION_HOURS_UTC.copy()
    if "motivation_messages" not in cfg:
        cfg["motivation_messages"] = DEFAULT_MOTIVATION_MESSAGES.copy()
    data[GLOBAL_KEY] = cfg
    save_data()
    return cfg

_global_cfg = get_global_config()
MOTIVATION_HOURS_UTC = _global_cfg["motivation_hours"]
MOTIVATION_MESSAGES = _global_cfg["motivation_messages"]

# =================== سجلات المستخدمين ===================
def get_user_record(user):
    user_id = str(user.id)
    now_iso = datetime.now(timezone.utc).isoformat()

    if user_id not in data:
        data[user_id] = {
            "user_id": user.id,
            "first_name": user.first_name,
            "username": user.username,
            "created_at": now_iso,
            "last_active": now_iso,
            "gender": None, "age": None, "weight": None,
            "water_liters": None, "cups_goal": None, "reminders_on": False,
            "today_date": None, "today_cups": 0,
            "quran_pages_goal": None, "quran_pages_today": 0, "quran_today_date": None,
            "tasbih_total": 0, "adhkar_count": 0,
            "heart_memos": [], "future_messages": [],
            "points": 0, "level": 0, "medals": [], "best_rank": None,
            "daily_full_streak": 0, "last_full_day": None,
            "motivation_on": True,
        }
    else:
        record = data[user_id]
        record["first_name"] = user.first_name
        record["username"] = user.username
        record["last_active"] = now_iso

        # تحويل المذكرات القديمة
        if record.get("heart_memos") and isinstance(record["heart_memos"], list) and record["heart_memos"] and isinstance(record["heart_memos"][0], str):
            old = record["heart_memos"]
            record["heart_memos"] = [{"text": t, "created_at": now_iso, "reminder_at": None, "reminder_sent": False} for t in old]

        for memo in record.get("heart_memos", []):
            memo.setdefault("created_at", now_iso)
            memo.setdefault("reminder_at", None)
            memo.setdefault("reminder_sent", False)

        record.setdefault("future_messages", [])

    save_data()
    return data[user_id]

# =================== حالات الانتظار ===================
WAITING_MEMO_REMINDER_CONFIRM = set()
WAITING_MEMO_REMINDER_DATETIME = set()
WAITING_FUTURE_MSG_TEXT = set()
WAITING_FUTURE_MSG_TIME = set()
FUTURE_MSG_BUFFER = {}

# أزرار جديدة
BTN_FUTURE_SELF = "رسالة إلى نفسي"
BTN_MEMO_REMINDER_YES = "نعم، أريد تذكيرًا"
BTN_MEMO_REMINDER_NO = "لا، بدون تذكير"
BTN_CANCEL = "إلغاء"

# لوحة رئيسية محدثة
MAIN_KEYBOARD_USER = ReplyKeyboardMarkup([
    ["أذكاري", "وردي القرآني"],
    ["السبحة", "مذكّرات قلبي"],
    ["منبّه الماء", "احصائياتي"],
    [BTN_FUTURE_SELF],
    ["تواصل مع الدعم", "الاشعارات"]
], resize_keyboard=True)

# =================== وظائف التذكيرات ===================
def heart_memos_reminder_job(context: CallbackContext):
    now = datetime.now(timezone.utc)
    for uid_str, user_data in list(data.items()):
        if uid_str == GLOBAL_KEY: continue
        uid = int(uid_str)
        changed = False
        for memo in user_data.get("heart_memos", []):
            if memo.get("reminder_at") and not memo.get("reminder_sent", False):
                try:
                    if dateutil_parser.parse(memo["reminder_at"]) <= now:
                        context.bot.send_message(uid, f"تذكير بمذكّرة كتبتها سابقًا:\n\n{memo['text']}")
                        memo["reminder_sent"] = True
                        changed = True
                except: pass
        if changed: save_data()

def future_messages_job(context: CallbackContext):
    now = datetime.now(timezone.utc)
    for uid_str, user_data in list(data.items()):
        if uid_str == GLOBAL_KEY: continue
        uid = int(uid_str)
        changed = False
        for msg in user_data.get("future_messages", []):
            if not msg.get("sent", False):
                try:
                    if dateutil_parser.parse(msg["send_at"]) <= now:
                        created = msg.get("created_at", "?")[:10]
                        context.bot.send_message(uid, f"رسالة من نفسك في الماضي ({created}):\n\n{msg['text']}")
                        msg["sent"] = True
                        changed = True
                except: pass
        if changed: save_data()

# =================== معالجة الرسائل ===================
def handle_text(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    text = update.message.text.strip() if update.message.text else ""

    if text == BTN_CANCEL:
        for s in [WAITING_MEMO_REMINDER_CONFIRM, WAITING_MEMO_REMINDER_DATETIME,
                  WAITING_FUTURE_MSG_TEXT, WAITING_FUTURE_MSG_TIME]:
            s.discard(user_id)
        FUTURE_MSG_BUFFER.pop(user_id, None)
        update.message.reply_text("تم الإلغاء.", reply_markup=MAIN_KEYBOARD_USER)
        return

    # رسالة إلى نفسي
    if text == BTN_FUTURE_SELF:
        WAITING_FUTURE_MSG_TEXT.add(user_id)
        update.message.reply_text(
            "اكتب الآن رسالة تريد أن تقرأها أنت في المستقبل\nاكتبها كما تحب ✨",
            reply_markup=ReplyKeyboardMarkup([[BTN_CANCEL]], resize_keyboard=True)
        )
        return

    if user_id in WAITING_FUTURE_MSG_TEXT:
        FUTURE_MSG_BUFFER[user_id] = {"text": text}
        WAITING_FUTURE_MSG_TEXT.discard(user_id)
        WAITING_FUTURE_MSG_TIME.add(user_id)
        update.message.reply_text(
            "الآن أرسل التاريخ والوقت:\nمثال: 2025-12-25 21:00",
            reply_markup=ReplyKeyboardMarkup([[BTN_CANCEL]], resize_keyboard=True)
        )
        return

    if user_id in WAITING_FUTURE_MSG_TIME:
        try:
            dt = dateutil_parser.parse(text)
            if dt.tzinfo is None:
                dt = pytz.UTC.localize(dt)
            if dt <= datetime.now(timezone.utc):
                raise ValueError()
            buffer = FUTURE_MSG_BUFFER.pop(user_id, None)
            record = get_user_record(user)
            record["future_messages"].append({
                "text": buffer["text"],
                "created_at": datetime.now(timezone.utc).isoformat(),
                "send_at": dt.isoformat(),
                "sent": False
            })
            save_data()
            WAITING_FUTURE_MSG_TIME.discard(user_id)
            update.message.reply_text(
                f"تم حفظ رسالتك!\nستصلك في: {dt.strftime('%Y-%m-%d %H:%M')} إن شاء الله",
                reply_markup=MAIN_KEYBOARD_USER
            )
        except:
            update.message.reply_text("صيغة غير صحيحة، أعد الكتابة:\nمثال: 2025-12-25 21:00")
        return

    # إضافة مذكرة مع تذكير
    if "إضافة مذكرة جديدة" in text or user_id in WAITING_MEMO_REMINDER_CONFIRM:
        if user_id not in WAITING_MEMO_REMINDER_CONFIRM:
            record = get_user_record(user)
            record["heart_memos"].append({
                "text": text,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "reminder_at": None,
                "reminder_sent": False
            })
            save_data()
        if text == BTN_MEMO_REMINDER_YES:
            WAITING_MEMO_REMINDER_CONFIRM.discard(user_id)
            WAITING_MEMO_REMINDER_DATETIME.add(user_id)
            update.message.reply_text("أرسل التاريخ والوقت: 2025-12-25 21:00")
            return
        elif text == BTN_MEMO_REMINDER_NO:
            WAITING_MEMO_REMINDER_CONFIRM.discard(user_id)
            update.message.reply_text("تم الحفظ بدون تذكير", reply_markup=MAIN_KEYBOARD_USER)
            return

    update.message.reply_text("مرحبًا! استخدم الأزرار أدناه", reply_markup=MAIN_KEYBOARD_USER)

# =================== الأوامر ===================
def start_command(update: Update, context: CallbackContext):
    user = update.effective_user
    get_user_record(user)
    update.message.reply_text(
        f"مرحبًا {user.first_name}!\nأهلاً بك في بوت سقيا الكوثر المحدث\nالآن مع ميزة 'رسالة إلى نفسي' وتذكيرات المذكرات!",
        reply_markup=MAIN_KEYBOARD_USER
    )

# =================== تشغيل البوت ===================
def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN غير موجود!")

    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher
    job_queue = updater.job_queue

    dp.add_handler(CommandHandler("start", start_command))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_text))

    # تذكيرات كل 5 دقائق
    job_queue.run_repeating(heart_memos_reminder_job, interval=300, first=10)
    job_queue.run_repeating(future_messages_job, interval=300, first=20)

    Thread(target=run_flask, daemon=True).start()
    logger.info("Suqya Al-Kawther Bot v3.0 شغال بنجاح!")
    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
