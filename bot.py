import os
import json
import logging
import re
import random
from datetime import datetime, timezone, time, timedelta
from threading import Thread
from dateutil.parser import parse as parse_date

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
ADMIN_ID = 931350292  # غيّره لو احتجت مستقبلاً

# معرف المشرفة (الأخوات)
SUPERVISOR_ID = 8395818573  # المشرفة

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

# =================== إعدادات افتراضية للجرعة التحفيزية ===================

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

CURRENT_MOTIVATION_JOBS = []

def get_global_config():
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

    data[GLOBAL_KEY] = cfg
    if changed:
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
            # مذكّرات قلبي (الصيغة الجديدة)
            "heart_memos": [],
            # رسائل إلى نفسي في المستقبل
            "future_messages": [],
            # نظام النقاط والمستويات والميداليات
            "points": 0,
            "level": 0,
            "medals": [],
            "best_rank": None,
            # الاستمرارية اليومية (ماء + قرآن)
            "daily_full_streak": 0,
            "last_full_day": None,
            # الجرعة التحفيزية
            "motivation_on": True,
        }
    else:
        record = data[user_id]
        record["first_name"] = user.first_name
        record["username"] = user.username
        record["last_active"] = now_iso

        # ضمان الحقول الأساسية
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
        record.setdefault("points", 0)
        record.setdefault("level", 0)
        record.setdefault("medals", [])
        record.setdefault("best_rank", None)
        record.setdefault("daily_full_streak", 0)
        record.setdefault("last_full_day", None)
        record.setdefault("motivation_on", True)

        # تحويل heart_memos من الصيغة القديمة (قائمة نصوص) إلى الصيغة الجديدة
        if record["heart_memos"] and isinstance(record["heart_memos"], list) and isinstance(record["heart_memos"][0], str):
            old_memos = record["heart_memos"]
            record["heart_memos"] = []
            for text in old_memos:
                record["heart_memos"].append({
                    "text": text,
                    "created_at": now_iso,
                    "reminder_at": None,
                    "reminder_sent": False
                })
        
        # ضمان أن كل مذكرة لها الحقول الصحيحة
        for memo in record["heart_memos"]:
            if not isinstance(memo, dict):
                memo = {"text": str(memo), "created_at": now_iso, "reminder_at": None, "reminder_sent": False}
            memo.setdefault("created_at", now_iso)
            memo.setdefault("reminder_at", None)
            memo.setdefault("reminder_sent", False)

        record.setdefault("future_messages", [])

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
    return [int(uid) for uid in data.keys() if uid != GLOBAL_KEY]

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
WAITING_MEMO_REMINDER_CONFIRM = set()   # جديد
WAITING_MEMO_REMINDER_DATETIME = set()  # جديد
WAITING_MEMO_EDIT_SELECT = set()
WAITING_MEMO_EDIT_TEXT = set()
WAITING_MEMO_DELETE_SELECT = set()
MEMO_EDIT_INDEX = {}

# رسالة إلى نفسي
WAITING_FUTURE_MSG_TEXT = set()
WAITING_FUTURE_MSG_TIME = set()
FUTURE_MSG_BUFFER = {}  # user_id -> {"text": str}

# دعم / إدارة
WAITING_SUPPORT_GENDER = set()
WAITING_SUPPORT = set()
WAITING_BROADCAST = set()

# إدارة الجرعة التحفيزية
WAITING_MOTIVATION_ADD = set()
WAITING_MOTIVATION_DELETE = set()
WAITING_MOTIVATION_TIMES = set()

# =================== الأزرار ===================

# رئيسية
BTN_ADHKAR_MAIN = "أذكاري"
BTN_QURAN_MAIN = "وردي القرآني"
BTN_TASBIH_MAIN = "السبحة"
BTN_MEMOS_MAIN = "مذكّرات قلبي"
BTN_WATER_MAIN = "منبّه الماء"
BTN_STATS = "احصائياتي"

BTN_SUPPORT = "تواصل مع الدعم"
BTN_NOTIFICATIONS_MAIN = "الاشعارات"
BTN_FUTURE_SELF = "رسالة إلى نفسي"  # جديد

BTN_CANCEL = "إلغاء"
BTN_BACK_MAIN = "رجوع للقائمة الرئيسية"

# أزرار تذكير المذكّرات
BTN_MEMO_REMINDER_YES = "نعم، أريد تذكيرًا"
BTN_MEMO_REMINDER_NO = "لا، بدون تذكير"

# لوحة المدير
BTN_ADMIN_PANEL = "لوحة التحكم"

MAIN_KEYBOARD_USER = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_ADHKAR_MAIN), KeyboardButton(BTN_QURAN_MAIN)],
        [KeyboardButton(BTN_TASBIH_MAIN), KeyboardButton(BTN_MEMOS_MAIN)],
        [KeyboardButton(BTN_WATER_MAIN), KeyboardButton(BTN_STATS)],
        [KeyboardButton(BTN_FUTURE_SELF)],  # جديد
        [KeyboardButton(BTN_SUPPORT), KeyboardButton(BTN_NOTIFICATIONS_MAIN)],
    ],
    resize_keyboard=True,
)

MAIN_KEYBOARD_ADMIN = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_ADHKAR_MAIN), KeyboardButton(BTN_QURAN_MAIN)],
        [KeyboardButton(BTN_TASBIH_MAIN), KeyboardButton(BTN_MEMOS_MAIN)],
        [KeyboardButton(BTN_WATER_MAIN), KeyboardButton(BTN_STATS)],
        [KeyboardButton(BTN_FUTURE_SELF)],
        [KeyboardButton(BTN_SUPPORT), KeyboardButton(BTN_NOTIFICATIONS_MAIN)],
        [KeyboardButton(BTN_ADMIN_PANEL)],
    ],
    resize_keyboard=True,
)

# باقي الأزرار تبقى كما هي (ماء، قرآن، أذكار، سبحة، إلخ)...
# (تم حذف تكرارها للاختصار، لكنها موجودة في الكود الأصلي ولا تتغير)

# =================== دوال مساعدة للمذكّرات والرسائل المستقبلية ===================

def format_memo_for_display(memo, idx):
    text = memo.get("text", "")
    created = memo.get("created_at", "")[:10] if memo.get("created_at") else "غير معروف"
    reminder = ""
    if memo.get("reminder_at"):
        r_time = memo["reminder_at"][:16].replace("T", " ")
        reminder = f" | تذكير: {r_time}"
    return f"{idx+1}. {text}\n   التاريخ: {created}{reminder}"

def format_memos_list(memos):
    if not memos:
        return "لا توجد مذكّرات بعد."
    return "\n\n".join(format_memo_for_display(m, i) for i, m in enumerate(memos))

# =================== JobQueue للتذكيرات ===================

def heart_memos_reminder_job(context: CallbackContext):
    now = datetime.now(timezone.utc)
    for uid_str, user_data in data.items():
        if uid_str == GLOBAL_KEY:
            continue
        uid = int(uid_str)
        memos = user_data.get("heart_memos", [])
        changed = False
        for memo in memos:
            if (memo.get("reminder_at") and 
                not memo.get("reminder_sent", False) and
                parse_date(memo["reminder_at"]) <= now):
                try:
                    context.bot.send_message(
                        chat_id=uid,
                        text=f"تذكير بمذكّرة كتبتها سابقًا:\n\n{memo['text']}"
                    )
                    memo["reminder_sent"] = True
                    changed = True
                except Exception as e:
                    logger.error(f"Error sending memo reminder to {uid}: {e}")
        if changed:
            save_data()

def future_messages_job(context: CallbackContext):
    now = datetime.now(timezone.utc)
    for uid_str, user_data in data.items():
        if uid_str == GLOBAL_KEY:
            continue
        uid = int(uid_str)
        msgs = user_data.get("future_messages", [])
        changed = False
        for msg in msgs:
            if not msg.get("sent", False) and parse_date(msg["send_at"]) <= now:
                created = msg.get("created_at", "")[:10] if msg.get("created_at") else "الماضي"
                try:
                    context.bot.send_message(
                        chat_id=uid,
                        text=f"رسالة من نفسك في الماضي (بتاريخ {created}):\n\n{msg['text']}"
                    )
                    msg["sent"] = True
                    changed = True
                except Exception as e:
                    logger.error(f"Error sending future message to {uid}: {e}")
        if changed:
            save_data()

# =================== معالجة النصوص (handle_text) - الجزء الجديد والمحدث ===================

def handle_text(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    msg = update.message
    text = (msg.text or "").strip()

    record = get_user_record(user)
    main_kb = user_main_keyboard(user_id)

    # === إلغاء عام ===
    if text == BTN_CANCEL:
        # تنظيف كل الحالات
        for s in [WAITING_GENDER, WAITING_AGE, WAITING_WEIGHT, WAITING_QURAN_GOAL, WAITING_QURAN_ADD_PAGES,
                  WAITING_TASBIH, WAITING_MEMO_MENU, WAITING_MEMO_ADD, WAITING_MEMO_REMINDER_CONFIRM,
                  WAITING_MEMO_REMINDER_DATETIME, WAITING_MEMO_EDIT_SELECT, WAITING_MEMO_EDIT_TEXT,
                  WAITING_MEMO_DELETE_SELECT, WAITING_FUTURE_MSG_TEXT, WAITING_FUTURE_MSG_TIME,
                  WAITING_SUPPORT_GENDER, WAITING_SUPPORT, WAITING_BROADCAST, WAITING_MOTIVATION_ADD,
                  WAITING_MOTIVATION_DELETE, WAITING_MOTIVATION_TIMES]:
            s.discard(user_id)
        ACTIVE_TASBIH.pop(user_id, None)
        MEMO_EDIT_INDEX.pop(user_id, None)
        FUTURE_MSG_BUFFER.pop(user_id, None)
        msg.reply_text("تم الإلغاء. عدنا للقائمة الرئيسية.", reply_markup=main_kb)
        return

    # === مذكّرات قلبي - تذكير بعد الإضافة ===
    if user_id in WAITING_MEMO_REMINDER_CONFIRM:
        if text == BTN_MEMO_REMINDER_YES:
            WAITING_MEMO_REMINDER_CONFIRM.discard(user_id)
            WAITING_MEMO_REMINDER_DATETIME.add(user_id)
            msg.reply_text(
                "أرسل التاريخ والوقت الذي تريد التذكير فيه:\n"
                "الصيغة: YYYY-MM-DD HH:MM\n"
                "مثال: 2025-12-25 21:30",
                reply_markup=ReplyKeyboardMarkup([[KeyboardButton(BTN_CANCEL)]], resize_keyboard=True)
            )
            return
        elif text == BTN_MEMO_REMINDER_NO:
            WAITING_MEMO_REMINDER_CONFIRM.discard(user_id)
            open_memos_menu(update, context)
            return

    if user_id in WAITING_MEMO_REMINDER_DATETIME:
        try:
            dt = parse_date(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if dt <= datetime.now(timezone.utc):
                raise ValueError("الوقت في الماضي")
            # تحديث آخر مذكرة
            memos = record.get("heart_memos", [])
            if memos:
                memos[-1]["reminder_at"] = dt.isoformat()
                memos[-1]["reminder_sent"] = False
                save_data()
            WAITING_MEMO_REMINDER_DATETIME.discard(user_id)
            msg.reply_text(
                f"تم ضبط تذكير لهذه المذكّرة في:\n{dt.strftime('%Y-%m-%d %H:%M')} ⏰",
                reply_markup=build_memos_menu_kb(is_admin(user_id))
            )
            open_memos_menu(update, context)
        except:
            msg.reply_text("الصيغة غير صحيحة أو الوقت في الماضي، أعد المحاولة:\nمثال: 2025-12-25 21:30")
        return

    # === رسالة إلى نفسي ===
    if text == BTN_FUTURE_SELF:
        WAITING_FUTURE_MSG_TEXT.add(user_id)
        msg.reply_text(
            "اكتب الآن رسالة تريد أن تقرأها أنت في المستقبل\n"
            "اكتبها كما تحب، ثم سنختار وقت الإرسال ✨",
            reply_markup=ReplyKeyboardMarkup([[KeyboardButton(BTN_CANCEL)]], resize_keyboard=True)
        )
        return

    if user_id in WAITING_FUTURE_MSG_TEXT:
        FUTURE_MSG_BUFFER[user_id] = {"text": text}
        WAITING_FUTURE_MSG_TEXT.discard(user_id)
        WAITING_FUTURE_MSG_TIME.add(user_id)
        msg.reply_text(
            "ممتاز!\nالآن أرسل التاريخ والوقت الذي تريد أن تصلك فيه الرسالة:\n"
            "الصيغة: YYYY-MM-DD HH:MM\n"
            "مثال: 2025-12-25 21:00",
            reply_markup=ReplyKeyboardMarkup([[KeyboardButton(BTN_CANCEL)]], resize_keyboard=True)
        )
        return

    if user_id in WAITING_FUTURE_MSG_TIME:
        try:
            dt = parse_date(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if dt <= datetime.now(timezone.utc):
                raise ValueError("الوقت في الماضي")
            buffer = FUTURE_MSG_BUFFER.pop(user_id, None)
            if not buffer:
                raise ValueError("لا يوجد نص")
            record["future_messages"].append({
                "text": buffer["text"],
                "created_at": datetime.now(timezone.utc).isoformat(),
                "send_at": dt.isoformat(),
                "sent": False
            })
            save_data()
            WAITING_FUTURE_MSG_TIME.discard(user_id)
            msg.reply_text(
                f"تم حفظ رسالتك إلى نفسك في المستقبل!\n"
                f"ستصلك في: {dt.strftime('%Y-%m-%d %H:%M')} إن شاء الله",
                reply_markup=main_kb
            )
        except:
            msg.reply_text("الصيغة غير صحيحة أو الوقت في الماضي، أعد المحاولة:\nمثال: 2025-12-25 21:00")
        return

    # === إضافة مذكّرة جديدة (تم تعديلها لتدعم التذكير) ===
    if user_id in WAITING_MEMO_ADD:
        record["heart_memos"].append({
            "text": text,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "reminder_at": None,
            "reminder_sent": False
        })
        save_data()
        WAITING_MEMO_ADD.discard(user_id)
        WAITING_MEMO_REMINDER_CONFIRM.add(user_id)
        created = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
        msg.reply_text(
            f"تم حفظ مذكّرتك\n"
            f"التاريخ: {created}\n\n"
            f"هل تريد أن أذكّرك بها في وقت معين؟",
            reply_markup=ReplyKeyboardMarkup([
                [KeyboardButton(BTN_MEMO_REMINDER_YES), KeyboardButton(BTN_MEMO_REMINDER_NO)]
            ], resize_keyboard=True)
        )
        return

    # باقي الكود كما هو (ماء، قرآن، أذكار، إلخ)...
    # (يتم الاحتفاظ بكل الـ handlers القديمة دون تغيير)

    # === باقي الأوامر (مثل الماء والقرآن والسبحة...) تبقى كما هي تمامًا ===
    # (تم حذفها هنا للاختصار، لكنها موجودة في الكود الأصلي ولا تتأثر)

# =================== فتح مذكّرات قلبي (محدث) ===================

def open_memos_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    record = get_user_record(user)
    memos = record.get("heart_memos", [])

    for s in [WAITING_MEMO_MENU, WAITING_MEMO_ADD, WAITING_MEMO_REMINDER_CONFIRM,
              WAITING_MEMO_REMINDER_DATETIME, WAITING_MEMO_EDIT_SELECT,
              WAITING_MEMO_EDIT_TEXT, WAITING_MEMO_DELETE_SELECT]:
        s.discard(user_id)
    WAITING_MEMO_MENU.add(user_id)

    kb = ReplyKeyboardMarkup([
        [KeyboardButton("إضافة مذكرة جديدة")],
        [KeyboardButton("تعديل مذكرة"), KeyboardButton("حذف مذكرة")],
        [KeyboardButton(BTN_BACK_MAIN)]
    ], resize_keyboard=True)

    update.message.reply_text(
        f"مذكّرات قلبي:\n\n{format_memos_list(memos)}\n\n"
        "اختر ماذا تريد:",
        reply_markup=kb
    )

# =================== main() - إضافة الـ Jobs الجديدة ===================

def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN غير موجود!")

    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher
    job_queue = updater.job_queue

    dp.add_handler(CommandHandler("start", start_command))
    dp.add_handler(CommandHandler("help", help_command))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_text))

    # تذكيرات الماء
    for h in [7, 10, 13, 16, 19]:
        job_queue.run_daily(water_reminder_job, time(hour=h, minute=0, tzinfo=pytz.UTC), name=f"water_{h}")

    # الجرعة التحفيزية
    for h in MOTIVATION_HOURS_UTC:
        job_queue.run_daily(motivation_job, time(hour=h, minute=0, tzinfo=pytz.UTC), name=f"motiv_{h}")

    # تذكيرات المذكّرات و الرسائل المستقبلية (كل 5 دقائق)
    job_queue.run_repeating(heart_memos_reminder_job, interval=300, first=10)
    job_queue.run_repeating(future_messages_job, interval=300, first=20)

    Thread(target=run_flask, daemon=True).start()

    logger.info("Suqya Al-Kawther bot v2.0 (مذكّرات + رسائل مستقبلية) is running...")
    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
