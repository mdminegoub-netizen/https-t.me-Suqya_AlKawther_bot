import os
import sys
import json
import logging
import re
import random
from collections import defaultdict
from uuid import uuid4
from datetime import datetime, timezone, time, timedelta
from threading import Thread
from typing import Dict, List, Optional, Tuple

import pytz
from flask import Flask, request
from apscheduler.schedulers.background import BackgroundScheduler
from telegram import (
    Update,
    User, # تم إضافة User هنا
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardRemove,
)

import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core.exceptions import FailedPrecondition

from telegram.ext import (
    Updater,
    MessageHandler,
    Filters,
    MessageFilter,
    CallbackContext,
    CommandHandler,
    CallbackQueryHandler,
    DispatcherHandlerStop,
)


class FuncMessageFilter(MessageFilter):
    def __init__(self, func):
        super().__init__()
        self.func = func

    def filter(self, message):
        return bool(self.func(message))

# =================== إعدادات أساسية ===================

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATA_FILE = "suqya_users.json"
PORT = int(os.getenv("PORT", 10000))
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")
AUDIO_STORAGE_CHANNEL_ID = str(os.getenv("AUDIO_STORAGE_CHANNEL_ID", "-1003269735721"))
ALLOWED_UPDATES = [
    "message",
    "edited_message",
    "channel_post",
    "edited_channel_post",
    "callback_query",
]

# معرف الأدمن (أنت)
ADMIN_ID = 931350292  # غيّره لو احتجت مستقبلاً

# معرف المشرفة (الأخوات)
SUPERVISOR_ID = 8395818573  # المشرفة

# ملف اللوج
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=getattr(logging, LOG_LEVEL, logging.INFO),
)
logger = logging.getLogger(__name__)

WEBHOOK_TIMEOUT = int(os.getenv("WEBHOOK_TIMEOUT", 15))
WEBHOOK_MAX_CONNECTIONS = int(os.getenv("WEBHOOK_MAX_CONNECTIONS", 40))

# ضبط اتصال البوت لتحمل عدد أكبر من الاتصالات
REQUEST_KWARGS = {
    "read_timeout": WEBHOOK_TIMEOUT,
    "connect_timeout": int(os.getenv("WEBHOOK_CONNECT_TIMEOUT", 10)),
}

# إعدادات الكاش لتقليل قراءات Firestore المتكررة
USER_CACHE_TTL_SECONDS = int(os.getenv("USER_CACHE_TTL_SECONDS", 60))
LAST_ACTIVE_UPDATE_INTERVAL_SECONDS = int(os.getenv("LAST_ACTIVE_UPDATE_INTERVAL_SECONDS", 60))

# =================== خادم ويب بسيط لـ Render ===================

app = Flask(__name__)


updater = None
dispatcher = None
job_queue = None
IS_RUNNING = True

@app.route("/")
def index():
    return "Suqya Al-Kawther bot is running ✅"

@app.route(f"/{BOT_TOKEN}", methods=["POST"])
def webhook_handler():
    """معالجة تحديثات الـ Webhook من Telegram"""
    if request.method == "POST":
        try:
            payload = request.get_json(force=True)
            update = Update.de_json(payload, dispatcher.bot)
            update_type = (
                "channel_post"
                if update.channel_post
                else "callback_query"
                if update.callback_query
                else "message"
                if update.message
                else "unknown"
            )
            logger.info(
                "📥 Webhook update received | type=%s | update_id=%s",
                update_type,
                getattr(update, "update_id", ""),
            )
            dispatcher.process_update(update)
            return "ok", 200
        except Exception as e:
            logger.error(f"خطأ في معالجة webhook: {e}")
            return "error", 500
    return "ok", 200

def run_flask():
    """تشغيل Flask لمعالجة Webhook (Blocking)"""
    logger.info(f"🌐 تشغيل Flask على المنفذ {PORT}...")
    try:
        app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False, threaded=True)
    except Exception as e:
        logger.error(f"❌ خطأ في Flask: {e}")


# =================== تخزين البيانات ===================


# تعريف data كـ dictionary فارغ في البداية
data = {}
# مؤشر لتتبع مصدر البيانات (Firestore أو ملف محلي)
DATA_LOADED_FROM_FIRESTORE = False
# كاش بسيط لتجنب قراءات Firestore المتكررة خلال فترة قصيرة
USER_CACHE_TIMESTAMPS: Dict[str, datetime] = {}
LAST_ACTIVE_WRITE_TRACKER: Dict[str, datetime] = {}

def load_data():
    """
    تحميل جميع المستخدمين من Firestore عند بدء البوت
    """
    global DATA_LOADED_FROM_FIRESTORE
    loaded_data = {}

    # محاولة التحميل من Firestore أولاً
    if firestore_available():
        try:
            logger.info("🔄 جاري تحميل جميع المستخدمين من Firestore...")
            users_ref = db.collection(USERS_COLLECTION)
            docs = users_ref.stream()

            count = 0
            for doc in docs:
                if str(doc.id) == str(GLOBAL_KEY):
                    continue
                user_data = doc.to_dict()
                loaded_data[doc.id] = user_data
                count += 1

            logger.info(f"✅ تم تحميل {count} مستخدم من Firestore")
            DATA_LOADED_FROM_FIRESTORE = True
            return loaded_data

        except Exception as e:
            logger.error(f"❌ خطأ في تحميل المستخدمين من Firestore: {e}")

    # Fallback: التحميل من الملف المحلي
    if not os.path.exists(DATA_FILE):
        return {}
    try:
        DATA_LOADED_FROM_FIRESTORE = False
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return {}


def save_data():
    """
    دالة متوافقة مع الكود القديم - تحفظ جميع المستخدمين في Firestore
    """
    if not firestore_available():
        # حفظ محلي كـ fallback
        try:
            with open(DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"خطأ في حفظ البيانات محلياً: {e}")
        return
    
    try:
        # حفظ جميع المستخدمين في Firestore
        saved_count = 0
        for user_id_str, user_data in data.items():
            # تجاهل المفاتيح غير الرقمية
            if user_id_str.startswith("_") or user_id_str == "GLOBAL_KEY":
                continue
            
            try:
                user_id = int(user_id_str)
                doc_ref = db.collection(USERS_COLLECTION).document(user_id_str)
                doc_ref.set(user_data, merge=True)
                saved_count += 1
                logger.info(f"✅ تم حفظ بيانات المستخدم {user_id} في Firestore (عدد الحقول: {len(user_data)})")
            except ValueError:
                continue
            except Exception as e:
                logger.error(f"❌ خطأ في حفظ المستخدم {user_id_str}: {e}")
        
        if saved_count > 0:
            logger.info(f"✅ تم حفظ {saved_count} مستخدم في Firestore")
                
    except Exception as e:
        logger.error(f"❌ خطأ في save_data: {e}", exc_info=True)


def initialize_firebase():
    try:
        secrets_path = "/etc/secrets"
        firebase_files = []
        
        if os.path.exists(secrets_path):
            for file in os.listdir(secrets_path):
                if file.startswith("soqya-") and file.endswith(".json"):
                    firebase_files.append(os.path.join(secrets_path, file))
        
        if firebase_files:
            cred_path = firebase_files[0]
            logger.info(f"تم العثور على ملف Firebase: {cred_path}")
            
            if not firebase_admin._apps:
                cred = credentials.Certificate(cred_path)
                firebase_admin.initialize_app(cred)
                logger.info("✅ تم تهيئة Firebase بنجاح")
            else:
                logger.info("✅ Firebase مفعل بالفعل")
        else:
            logger.warning("❌ لم يتم العثور على ملف Firebase")
            
    except Exception as e:
        logger.error(f"❌ خطأ في تهيئة Firebase: {e}")

initialize_firebase()

try:
    db = firestore.client()
    logger.info("✅ تم الاتصال بـ Firestore بنجاح")
except Exception as e:
    logger.error(f"❌ خطأ في الاتصال بـ Firestore: {e}")
    db = None


def firestore_available():
    """التحقق مما إذا كان Firestore متاحاً"""
    return db is not None


def _is_cache_fresh(user_id: str, now: datetime) -> bool:
    """يتحقق من صلاحية الكاش للمستخدم"""
    cached_at = USER_CACHE_TIMESTAMPS.get(user_id)
    if not cached_at:
        return False
    return (now - cached_at).total_seconds() < USER_CACHE_TTL_SECONDS


def _remember_cache(user_id: str, record: Dict, fetched_at: datetime):
    """تحديث الكاش المحلي ووقت آخر تحميل"""
    data[user_id] = record
    USER_CACHE_TIMESTAMPS[user_id] = fetched_at


def _throttled_last_active_update(user_id: str, now_iso: str, now_dt: datetime):
    """تحديث last_active في Firestore مع تقليل عدد الكتابات"""
    last_write = LAST_ACTIVE_WRITE_TRACKER.get(user_id)
    if last_write and (now_dt - last_write).total_seconds() < LAST_ACTIVE_UPDATE_INTERVAL_SECONDS:
        return

    LAST_ACTIVE_WRITE_TRACKER[user_id] = now_dt
    if not firestore_available():
        return

    try:
        db.collection(USERS_COLLECTION).document(user_id).update({"last_active": now_iso})
    except Exception as e:
        logger.debug("تعذر تحديث آخر نشاط للمستخدم %s: %s", user_id, e)

# المجموعات (Collections) في Firestore
USERS_COLLECTION = "users"
WATER_LOGS_COLLECTION = "water_logs"
TIPS_COLLECTION = "tips"
NOTES_COLLECTION = "notes"
GLOBAL_CONFIG_COLLECTION = "global_config"
# Collections جديدة للمجتمع والمنافسات
COMMUNITY_BENEFITS_COLLECTION = "community_benefits"
COMPETITION_POINTS_COLLECTION = "competition_points"
COMMUNITY_MEDALS_COLLECTION = "community_medals"
AUDIO_LIBRARY_COLLECTION = "audio_library"
AUDIO_LIBRARY_FILE = "audio_library.json"
BOOK_CATEGORIES_COLLECTION = "book_categories"
BOOKS_COLLECTION = "books"


# =================== نهاية Firebase ===================

# =================== دوال التخزين المحلي (Fallback) ===================

def get_user_record_local_by_id(user_id: int) -> Dict:
    """مساعدة للحصول على سجل محلي بواسطة ID"""
    uid = str(user_id)
    if uid not in data:
        # إنشاء سجل افتراضي
        data[uid] = {
            "user_id": user_id,
            "first_name": "مستخدم",
            "username": None,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "last_active": datetime.now(timezone.utc).isoformat(),
            "heart_memos": [],
            "saved_books": [],
            "saved_books_updated_at": None,
        }
    ensure_medal_defaults(data[uid])
    return data[uid]


def migrate_data_to_firestore():
    """ترحيل البيانات من JSON المحلي إلى Firestore"""
    if not firestore_available():
        logger.warning("Firestore غير متوفر، لا يمكن ترحيل البيانات")
        return
    
    logger.info("بدء ترحيل البيانات إلى Firestore...")
    
    # تحميل البيانات المحلية
    global data
    if not data:
        load_data_local()
    
    migrated_users = 0
    migrated_benefits = 0
    
    # ترحيل المستخدمين
    for user_id_str, user_data in data.items():
        # تجاهل المفاتيح غير الرقمية (مثل GLOBAL_KEY أو _global_config)
        if user_id_str == "GLOBAL_KEY" or user_id_str == GLOBAL_KEY or user_id_str.startswith("_"):
            continue
            
        try:
            user_id = int(user_id_str)
            
            # تحديث سجل المستخدم في Firestore
            doc_ref = db.collection(USERS_COLLECTION).document(user_id_str)
            
            # تحويل heart_memos إلى تنسيق Firestore
            heart_memos = user_data.get("heart_memos", [])
            if heart_memos and isinstance(heart_memos, list) and len(heart_memos) > 0:
                # حفظ كل مذكرة كوثيقة منفصلة
                for memo in heart_memos:
                    if memo.strip():  # تجاهل المذكرات الفارغة
                        save_note(user_id, memo)
                
                # إزالة المذكرات من بيانات المستخدم
                user_data.pop("heart_memos", None)

            # تجاهل بيانات الرسائل القديمة إن وجدت
            user_data.pop("letters_to_self", None)
            
            # حفظ بيانات المستخدم
            doc_ref.set(user_data)
            migrated_users += 1
            
        except Exception as e:
            logger.error(f"خطأ في ترحيل المستخدم {user_id_str}: {e}")
    
    # ترحيل الفوائد والنصائح
    if "GLOBAL_KEY" in data:
        global_config = data["GLOBAL_KEY"]
        benefits = global_config.get("benefits", [])
        
        for benefit in benefits:
            try:
                save_benefit(benefit)
                migrated_benefits += 1
            except Exception as e:
                logger.error(f"خطأ في ترحيل الفائدة: {e}")
        
        # حفظ الإعدادات العامة
        config_doc_ref = db.collection(GLOBAL_CONFIG_COLLECTION).document("config")
        config_doc_ref.set({
            "motivation_times": _normalize_times(
                global_config.get("motivation_times")
                or global_config.get("motivation_hours"),
                DEFAULT_MOTIVATION_TIMES_UTC.copy(),
            ),
            "motivation_messages": global_config.get("motivation_messages", []),
            "benefits": []  # الفوائد محفوظة منفصلة الآن
        })
    
    logger.info(f"✅ تم ترحيل {migrated_users} مستخدم و {migrated_benefits} فائدة إلى Firestore")
    
    # نسخة احتياطية من الملف المحلي
    try:
        backup_file = f"{DATA_FILE}.backup"
        with open(backup_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"تم إنشاء نسخة احتياطية في {backup_file}")
    except Exception as e:
        logger.error(f"خطأ في إنشاء النسخة الاحتياطية: {e}")


def get_user_record_local(user: User) -> Dict:
    """نسخة محلية من get_user_record"""
    user_id = str(user.id)
    now_iso = datetime.now(timezone.utc).isoformat()
    
    if user_id not in data:
        data[user_id] = {
            "user_id": user.id,
            "first_name": user.first_name,
            "username": user.username,
            "created_at": now_iso,
            "last_active": now_iso,
            "is_new_user": True,
            "is_banned": False,
            "banned_by": None,
            "banned_at": None,
            "ban_reason": None,
            "gender": None,
            "age": None,
            "weight": None,
            "water_liters": None,
            "cups_goal": None,
            "reminders_on": False,
            "water_enabled": False,
            "today_date": None,
            "today_cups": 0,
            "quran_pages_goal": None,
            "quran_pages_today": 0,
            "quran_today_date": None,
            "tasbih_total": 0,
            "adhkar_count": 0,
            "heart_memos": [],
            "saved_books": [],
            "saved_books_updated_at": None,
            "points": 0,
            "level": 0,
            "medals": [],
            "best_rank": None,
            "course_full_name": None,
            "daily_full_streak": 0,
            "last_full_day": None,
            "daily_full_count": 0,
            "motivation_on": True,
        }
    else:
        record = data[user_id]
        record["first_name"] = user.first_name
        record["username"] = user.username
        record["last_active"] = now_iso
        
        # ضمان الحقول
        default_fields = {
            "is_banned": False,
            "banned_by": None,
            "banned_at": None,
            "ban_reason": None,
            "gender": None,
            "country": None,
            "age": None,
            "weight": None,
            "water_liters": None,
            "cups_goal": None,
            "reminders_on": False,
            "water_enabled": False,
            "today_date": None,
            "today_cups": 0,
            "quran_pages_goal": None,
            "quran_pages_today": 0,
            "quran_today_date": None,
            "tasbih_total": 0,
            "adhkar_count": 0,
            "heart_memos": [],
            "saved_books": [],
            "saved_books_updated_at": None,
            "points": 0,
            "level": 0,
            "medals": [],
            "best_rank": None,
            "daily_full_streak": 0,
            "last_full_day": None,
            "daily_full_count": 0,
            "motivation_on": True,
            "course_full_name": None,
            "is_new_user": False
        }
        
        for field, default_value in default_fields.items():
            if field not in record:
                record[field] = default_value

    ensure_medal_defaults(record)
    save_data_local()
    return data[user_id]


def update_user_record_local(user_id: int, **kwargs):
    """نسخة محلية من update_user_record"""
    uid = str(user_id)
    if uid not in data:
        return
    
    data[uid].update(kwargs)
    data[uid]["last_active"] = datetime.now(timezone.utc).isoformat()
    save_data_local()


def get_all_user_ids_local() -> List[int]:
    """نسخة محلية من get_all_user_ids"""
    return [int(uid) for uid in data.keys() if uid != "GLOBAL_KEY"]

def get_active_user_ids_local() -> List[int]:
    """نسخة محلية من get_active_user_ids"""
    return [
        int(uid)
        for uid, rec in data.items()
        if uid != "GLOBAL_KEY" and not rec.get("is_banned", False)
    ]

def get_banned_user_ids_local() -> List[int]:
    """نسخة محلية من get_banned_user_ids"""
    return [
        int(uid)
        for uid, rec in data.items()
        if uid != "GLOBAL_KEY" and rec.get("is_banned", False)
    ]

def get_users_sorted_by_points_local() -> List[Dict]:
    """نسخة محلية من get_users_sorted_by_points"""
    return sorted(
        [r for k, r in data.items() if k != "GLOBAL_KEY"],
        key=lambda r: r.get("points", 0),
        reverse=True,
    )

# دالة المساعدة للفوائد (محلية)
def get_benefits_local() -> List[Dict]:
    """نسخة محلية من get_benefits"""
    config = get_global_config_local()
    return config.get("benefits", [])

def save_benefit_local(benefit_data: Dict) -> str:
    """نسخة محلية من save_benefit"""
    config = get_global_config_local()
    benefits = config.get("benefits", [])
    
    if "id" not in benefit_data:
        benefit_data["id"] = get_next_benefit_id_local()
    
    if "date" not in benefit_data:
        benefit_data["date"] = datetime.now(timezone.utc).isoformat()
    
    benefits.append(benefit_data)
    config["benefits"] = benefits
    update_global_config_local(config)
    
    return str(benefit_data["id"])

def update_benefit_local(benefit_id: int, benefit_data: Dict):
    """نسخة محلية من update_benefit"""
    config = get_global_config_local()
    benefits = config.get("benefits", [])
    
    for i, benefit in enumerate(benefits):
        if benefit.get("id") == benefit_id:
            benefits[i].update(benefit_data)
            break
    
    config["benefits"] = benefits
    update_global_config_local(config)

# =================== نهاية دوال التخزين المحلي ===================







# =================== إعدادات افتراضية للجرعة التحفيزية (على مستوى البوت) ===================

DEFAULT_MOTIVATION_TIMES_UTC = [
    "06:00",
    "09:00",
    "12:00",
    "15:00",
    "18:00",
    "21:00",
]

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

def _time_to_minutes(time_str: str) -> int:
    try:
        parts = time_str.split(":")
        return int(parts[0]) * 60 + int(parts[1])
    except Exception:
        return -1


def _normalize_times(raw_times, fallback: List[str]) -> List[str]:
    times = []

    for t in raw_times or []:
        hour = None
        minute = None

        if isinstance(t, int):
            hour = t
            minute = 0
        elif isinstance(t, str):
            match = re.match(r"^(\d{1,2}):(\d{2})$", t.strip())
            if match:
                hour = int(match.group(1))
                minute = int(match.group(2))

        if hour is None or minute is None:
            continue

        if 0 <= hour <= 23 and 0 <= minute <= 59:
            times.append(f"{hour:02d}:{minute:02d}")

    normalized = sorted(set(times), key=_time_to_minutes)
    return normalized or fallback


MOTIVATION_TIMES_UTC = []
MOTIVATION_MESSAGES = []


def get_global_config():
    """
    يرجع (أو ينشئ) الإعدادات العامة للبوت (مثل أوقات الجرعة التحفيزية ورسائلها).
    تُخزَّن تحت مفتاح خاص في نفس ملف JSON.
    """
    cfg = {}
    changed = False

    # حاول القراءة من Firestore أولاً
    if firestore_available():
        try:
            doc_ref = db.collection(GLOBAL_CONFIG_COLLECTION).document("config")
            doc = doc_ref.get()
            if doc.exists:
                cfg = doc.to_dict() or {}
        except Exception as e:
            logger.error(f"❌ خطأ في قراءة الإعدادات العامة من Firestore: {e}")

    # fallback إلى البيانات المحملة محليًا
    if not cfg:
        cfg = data.get(GLOBAL_KEY)

    if not cfg or not isinstance(cfg, dict):
        cfg = {}
        changed = True

    if "motivation_times" not in cfg or not cfg.get("motivation_times"):
        legacy_hours = cfg.get("motivation_hours")
        cfg["motivation_times"] = _normalize_times(
            legacy_hours if legacy_hours is not None else [], DEFAULT_MOTIVATION_TIMES_UTC.copy()
        )
        changed = True

    if "motivation_messages" not in cfg or not cfg.get("motivation_messages"):
        cfg["motivation_messages"] = DEFAULT_MOTIVATION_MESSAGES.copy()
        changed = True

    if "benefits" not in cfg or not isinstance(cfg.get("benefits"), list):
        cfg["benefits"] = []
        changed = True

    data[GLOBAL_KEY] = cfg

    if changed:
        save_global_config(cfg)

    return cfg


def save_global_config(cfg: Dict):
    """حفظ الإعدادات العامة في Firestore أو محليًا عند عدم توفره"""
    data[GLOBAL_KEY] = cfg

    if firestore_available():
        try:
            db.collection(GLOBAL_CONFIG_COLLECTION).document("config").set(cfg, merge=True)
            logger.info("✅ تم حفظ الإعدادات العامة في Firestore")
        except Exception as e:
            logger.error(f"❌ خطأ في حفظ الإعدادات العامة في Firestore: {e}")
    else:
        save_data()


_global_cfg = get_global_config()
MOTIVATION_TIMES_UTC = _global_cfg["motivation_times"]
MOTIVATION_MESSAGES = _global_cfg["motivation_messages"]


# =================== نصوص الأذكار ===================

MORNING_ADHKAR_ITEMS = [
    {
        "title": "آية الكرسي",
        "text": "«اللّه لا إله إلا هو الحيّ القيّوم...»",
        "repeat": "مرة واحدة بعد الفجر حتى ارتفاع الشمس.",
    },
    {
        "title": "المعوّذات",
        "text": "قل هو الله أحد، قل أعوذ برب الفلق، قل أعوذ برب الناس.",
        "repeat": "تُقرأ ثلاث مرات.",
    },
    {
        "title": "دعاء الصباح",
        "text": "«أصبحنا وأصبح الملك لله، والحمد لله، لا إله إلا الله وحده لا شريك له، له الملك وله الحمد وهو على كل شيء قدير».",
        "repeat": "مرة واحدة.",
    },
    {
        "title": "شكر النعمة",
        "text": "«اللهم ما أصبح بي من نعمة أو بأحد من خلقك فمنك وحدك لا شريك لك، لك الحمد ولك الشكر».",
        "repeat": "مرة واحدة.",
    },
    {
        "title": "شهادة التوحيد",
        "text": "«اللهم إني أصبحت أشهدك وأشهد حملة عرشك وملائكتك وجميع خلقك، أنك أنت الله لا إله إلا أنت وحدك لا شريك لك، وأن محمدًا عبدك ورسولك».",
        "repeat": "أربع مرات.",
    },
    {
        "title": "حسبي الله",
        "text": "«حسبي الله لا إله إلا هو عليه توكلت وهو رب العرش العظيم».",
        "repeat": "سبع مرات.",
    },
    {
        "title": "الصلاة على النبي ﷺ",
        "text": "«اللهم صل وسلم على سيدنا محمد».",
        "repeat": "عددًا كثيرًا طوال الصباح.",
    },
]

EVENING_ADHKAR_ITEMS = [
    {
        "title": "آية الكرسي",
        "text": "«اللّه لا إله إلا هو الحيّ القيّوم...»",
        "repeat": "مرة واحدة بعد العصر حتى الليل.",
    },
    {
        "title": "المعوّذات",
        "text": "قل هو الله أحد، قل أعوذ برب الفلق، قل أعوذ برب الناس.",
        "repeat": "تُقرأ ثلاث مرات.",
    },
    {
        "title": "دعاء المساء",
        "text": "«أمسينا وأمسى الملك لله، والحمد لله، لا إله إلا الله وحده لا شريك له، له الملك وله الحمد وهو على كل شيء قدير».",
        "repeat": "مرة واحدة.",
    },
    {
        "title": "شكر النعمة",
        "text": "«اللهم ما أمسى بي من نعمة أو بأحد من خلقك فمنك وحدك لا شريك لك، لك الحمد ولك الشكر».",
        "repeat": "مرة واحدة.",
    },
    {
        "title": "شهادة التوحيد",
        "text": "«اللهم إني أمسيت أشهدك وأشهد حملة عرشك وملائكتك وجميع خلقك، أنك أنت الله لا إله إلا أنت وحدك لا شريك لك، وأن محمدًا عبدك ورسولك».",
        "repeat": "أربع مرات.",
    },
    {
        "title": "ذكر الحفظ",
        "text": "«باسم الله الذي لا يضر مع اسمه شيء في الأرض ولا في السماء وهو السميع العليم».",
        "repeat": "ثلاث مرات.",
    },
    {
        "title": "الصلاة على النبي ﷺ",
        "text": "«اللهم صل وسلم على سيدنا محمد».",
        "repeat": "عددًا كثيرًا طوال المساء.",
    },
]

GENERAL_ADHKAR_ITEMS = [
    {
        "title": "الاستغفار",
        "text": "«أستغفر الله العظيم وأتوب إليه».",
        "repeat": "كررها ما استطعت.",
    },
    {
        "title": "توحيد الله",
        "text": "«لا إله إلا الله وحده لا شريك له، له الملك وله الحمد وهو على كل شيء قدير».",
        "repeat": "قلها مرارًا ليثبت قلبك.",
    },
    {
        "title": "تسبيح الأربعة",
        "text": "«سبحان الله، والحمد لله، ولا إله إلا الله، والله أكبر».",
        "repeat": "اختر العدد الذي يشرح صدرك.",
    },
    {
        "title": "لا حول ولا قوة إلا بالله",
        "text": "«لا حول ولا قوة إلا بالله».",
        "repeat": "رددها كلما شعرت بالحاجة إلى العون.",
    },
    {
        "title": "الصلاة على النبي ﷺ",
        "text": "«اللهم صل وسلم على سيدنا محمد».",
        "repeat": "أكثر منها في كل وقت.",
    },
]

STRUCTURED_ADHKAR_SECTIONS = {
    "morning": {"title": "🌅 أذكار الصباح", "items": MORNING_ADHKAR_ITEMS},
    "evening": {"title": "🌙 أذكار المساء", "items": EVENING_ADHKAR_ITEMS},
    "general": {"title": "أذكار عامة 💭", "items": GENERAL_ADHKAR_ITEMS},
}

STRUCTURED_ADHKAR_DONE_MESSAGES = {
    "morning": "🌿 بارك الله فيك… جعل الله صباحك نورًا وطمأنينة، وكتب لك حفظًا ورزقًا وتوفيقًا. 🤍",
    "evening": "🌙 أحسن الله مساءك… جعل الله ليلك سكينة، وغفر ذنبك، وحفظك من كل سوء. 🤲",
    "general": "طيب الله قلبك… وشرح صدرك، وملأ حياتك ذكرًا وبركة، ورزقك الثبات. 🌿",
}

SLEEP_ADHKAR_ITEMS = [
    {
        "title": "آية الكرسي",
        "text": "﴿اللَّهُ لَا إِلَٰهَ إِلَّا هُوَ الْحَيُّ الْقَيُّومُ... وَهُوَ الْعَلِيُّ الْعَظِيمُ﴾ (البقرة: 255)",
        "repeat": "مرة واحدة قبل النوم.",
    },
    {
        "title": "خواتيم سورة البقرة",
        "text": "﴿آمَنَ الرَّسُولُ بِمَا أُنزِلَ إِلَيْهِ مِن رَّبِّهِ... وَانصُرْنَا عَلَى الْقَوْمِ الْكَافِرِينَ﴾ (البقرة: 285-286)",
        "repeat": "مرة واحدة تكفي عن قيام الليل بإذن الله.",
    },
    {
        "title": "النفث بالمعوّذات",
        "text": "جمع الكفين ثم قراءة: قل هو الله أحد، قل أعوذ برب الفلق، قل أعوذ برب الناس، ثم النفث والمسح على الجسد. تُكرر ثلاث مرات.",
        "repeat": "ثلاث مرات مع المسح بعد كل مرة.",
    },
    {
        "title": "دعاء البراء بن عازب",
        "text": "«باسمك ربي وضعت جنبي وبك أرفعه، فإن أمسكت نفسي فارحمها، وإن أرسلتها فاحفظها بما تحفظ به عبادك الصالحين».",
        "repeat": "مرة واحدة مع وضع اليد تحت الخد الأيمن.",
    },
    {
        "title": "ذكر التسليم واليقين",
        "text": "«اللهم أسلمت نفسي إليك، وفوّضت أمري إليك، وألجأت ظهري إليك، رغبة ورهبة إليك، لا ملجأ ولا منجى منك إلا إليك، آمنت بكتابك الذي أنزلت، وبنبيك الذي أرسلت».",
        "repeat": "مرة واحدة قبل إغلاق العينين.",
    },
    {
        "title": "تسبيح خاتمة اليوم",
        "text": "«سبحان الله» 33، «الحمد لله» 33، «الله أكبر» 34 مرة.",
        "repeat": "يُقال بالترتيب قبل النوم.",
    },
]


# =================== سجلات المستخدمين ===================


def get_next_benefit_id():
    """يرجع معرف فريد للفائدة الجديدة"""
    benefits = get_benefits_from_firestore()
    if not benefits:
        return 1
    max_id = max(b.get("id", 0) for b in benefits)
    return max_id + 1


def get_benefits_from_firestore():
    """قراءة الفوائد من Firestore"""
    if not firestore_available():
        cfg = get_global_config()
        return cfg.get("benefits", [])
    
    try:
        benefits_ref = db.collection(COMMUNITY_BENEFITS_COLLECTION)
        docs = benefits_ref.stream()
        benefits = []
        for doc in docs:
            benefit_data = doc.to_dict()
            benefit_data['firestore_id'] = doc.id
            benefits.append(benefit_data)
        return benefits
    except Exception as e:
        logger.error(f"❌ خطأ في قراءة الفوائد من Firestore: {e}")
        return []

def save_benefit_to_firestore(benefit_data: Dict) -> str:
    """حفظ فائدة جديدة في Firestore"""
    if not firestore_available():
        logger.warning("Firestore غير متوفر")
        return ""
    
    try:
        benefit_ref = db.collection(COMMUNITY_BENEFITS_COLLECTION).add(benefit_data)
        logger.info(f"✅ تم حفظ الفائدة في Firestore: {benefit_ref[1].id}")
        return benefit_ref[1].id
    except Exception as e:
        logger.error(f"❌ خطأ في حفظ الفائدة في Firestore: {e}")
        return ""

def update_benefit_in_firestore(firestore_id: str, benefit_data: Dict):
    """تحديث فائدة في Firestore"""
    if not firestore_available():
        return
    
    try:
        db.collection(COMMUNITY_BENEFITS_COLLECTION).document(firestore_id).set(benefit_data, merge=True)
        logger.info(f"✅ تم تحديث الفائدة في Firestore: {firestore_id}")
    except Exception as e:
        logger.error(f"❌ خطأ في تحديث الفائدة: {e}")

def delete_benefit_from_firestore(firestore_id: str):
    """حذف فائدة من Firestore"""
    if not firestore_available():
        return
    
    try:
        db.collection(COMMUNITY_BENEFITS_COLLECTION).document(firestore_id).delete()
        logger.info(f"✅ تم حذف الفائدة من Firestore: {firestore_id}")
    except Exception as e:
        logger.error(f"❌ خطأ في حذف الفائدة: {e}")

def get_benefits():
    """يرجع قائمة الفوائد من Firestore أو الإعدادات العامة"""
    return get_benefits_from_firestore()

def save_benefits(benefits_list):
    """حفظ قائمة الفوائد - يتم الحفظ في Firestore مباشرة"""
    if not firestore_available():
        return
    
    try:
        # حذف جميع الفوائد القديمة
        docs = db.collection(COMMUNITY_BENEFITS_COLLECTION).stream()
        for doc in docs:
            doc.reference.delete()
            
        # إضافة الفوائد الجديدة
        batch = db.batch()
        for benefit in benefits_list:
            doc_ref = db.collection(COMMUNITY_BENEFITS_COLLECTION).document(str(benefit["id"]))
            batch.set(doc_ref, benefit)
        
        batch.commit()
        logger.info(f"✅ تم حفظ {len(benefits_list)} فائدة في Firestore")
    except Exception as e:
        logger.error(f"❌ خطأ في حفظ الفوائد: {e}")


def get_user_record(user):
    """
    ينشئ أو يرجع سجل المستخدم من Firestore
    """
    user_id = str(user.id)
    now_dt = datetime.now(timezone.utc)
    now_iso = now_dt.isoformat()

    # محاولة استخدام الكاش لتجنب قراءات Firestore المتكررة في نفس الجلسة
    cached_record = data.get(user_id)
    if cached_record and _is_cache_fresh(user_id, now_dt):
        cached_record["last_active"] = now_iso
        _throttled_last_active_update(user_id, now_iso, now_dt)
        ensure_medal_defaults(cached_record)
        ensure_water_defaults(cached_record)
        return cached_record
    
    if not firestore_available():
        logger.warning("Firestore غير متوفر، استخدام التخزين المحلي")
        return get_user_record_local(user)
    
    try:
        # قراءة من Firestore
        doc_ref = db.collection(USERS_COLLECTION).document(user_id)
        doc = doc_ref.get()

        if doc.exists:
            record = doc.to_dict()
            ensure_water_defaults(record)
            # تحميل المذكرات من Subcollections إذا كانت غير موجودة في السجل
            try:
                if not record.get("heart_memos"):
                    memos_data = []
                    for memo_doc in doc_ref.collection("heart_memos").stream():
                        memo_data = memo_doc.to_dict()
                        if memo_data.get("note"):
                            memos_data.append(memo_data)
                    if memos_data:
                        memos_data.sort(key=lambda m: m.get("created_at") or "")
                        record["heart_memos"] = [m.get("note") for m in memos_data]
            except Exception as e:
                logger.warning(f"⚠️ تعذر تحميل المذكرات الفرعية للمستخدم {user_id}: {e}")

            # تحديث آخر نشاط مع تقليل الكتابات المتكررة
            _throttled_last_active_update(user_id, now_iso, now_dt)
            # إضافة المستخدم إلى data المحلي
            ensure_medal_defaults(record)
            _remember_cache(user_id, record, now_dt)
            logger.debug("قراءة بيانات المستخدم %s من Firestore", user_id)
            return record
        else:
            # إنشاء سجل جديد
            new_record = {
                "user_id": user.id,
                "first_name": user.first_name,
                "username": user.username,
                "created_at": now_iso,
                "last_active": now_iso,
                "is_new_user": True,
                "is_banned": False,
                "banned_by": None,
                "banned_at": None,
                "ban_reason": None,
                "gender": None,
                "country": None,
                "age": None,
                "weight": None,
                "water_liters": None,
                "cups_goal": None,
                "reminders_on": False,
                "water_enabled": False,
                "today_date": None,
                "today_cups": 0,
                "quran_pages_goal": None,
                "quran_pages_today": 0,
                "quran_today_date": None,
                "tasbih_total": 0,
                "adhkar_count": 0,
                "heart_memos": [],
                "saved_books": [],
                "saved_books_updated_at": None,
                "points": 0,
                "level": 1,
                "streak_days": 0,
                "last_streak_date": None,
                "medals": [],
                "daily_full_count": 0,
                "saved_benefits": [],
                "motivation_on": True,
                "motivation_times": DEFAULT_MOTIVATION_TIMES_UTC.copy(),
            }
            doc_ref.set(new_record)
            # إضافة المستخدم إلى data المحلي
            ensure_medal_defaults(new_record)
            _remember_cache(user_id, new_record, now_dt)
            logger.info(f"✅ تم إنشاء مستخدم جديد {user_id} في Firestore")
            return new_record
            
    except Exception as e:
        logger.error(f"❌ خطأ في قراءة/إنشاء المستخدم {user_id} من Firestore: {e}")
        return get_user_record_local(user)


def update_user_record(user_id: int, **kwargs):
    """تحديث سجل المستخدم في Firestore"""
    user_id_str = str(user_id)
    
    if not firestore_available():
        logger.warning("Firestore غير متوفر، استخدام التخزين المحلي")
        return update_user_record_local(user_id, **kwargs)
    
    try:
        doc_ref = db.collection(USERS_COLLECTION).document(user_id_str)
        
        # إضافة last_active تلقائياً
        kwargs["last_active"] = datetime.now(timezone.utc).isoformat()
        
        # تحديث في Firestore
        doc_ref.update(kwargs)

        # تحديث data المحلي أيضاً
        if user_id_str in data:
            data[user_id_str].update(kwargs)
            _remember_cache(user_id_str, data[user_id_str], datetime.now(timezone.utc))
        else:
            # إذا لم يكن في data، قراءته من Firestore
            doc = doc_ref.get()
            if doc.exists:
                _remember_cache(user_id_str, doc.to_dict(), datetime.now(timezone.utc))

        logger.debug("تم تحديث بيانات المستخدم %s في Firestore: %s", user_id, list(kwargs.keys()))
        
    except Exception as e:
        logger.error(f"❌ خطأ في تحديث المستخدم {user_id} في Firestore: {e}", exc_info=True)
        # Fallback للتخزين المحلي
        if user_id_str in data:
            data[user_id_str].update(kwargs)


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

WAITING_WATER_ADD_CUPS = set()

WAITING_QURAN_GOAL = set()
WAITING_QURAN_ADD_PAGES = set()

WAITING_TASBIH = set()
ACTIVE_TASBIH = {}      # user_id -> { "text": str, "target": int, "current": int }

# مكتبة الكتب
WAITING_BOOK_SEARCH = set()
WAITING_BOOK_CATEGORY_NAME = set()
WAITING_BOOK_CATEGORY_ORDER = set()
WAITING_BOOK_ADD_CATEGORY = set()
WAITING_BOOK_ADD_TITLE = set()
WAITING_BOOK_ADD_AUTHOR = set()
WAITING_BOOK_ADD_DESCRIPTION = set()
WAITING_BOOK_ADD_TAGS = set()
WAITING_BOOK_ADD_COVER = set()
WAITING_BOOK_ADD_PDF = set()
WAITING_BOOK_EDIT_FIELD = set()
WAITING_BOOK_EDIT_COVER = set()
WAITING_BOOK_EDIT_PDF = set()
WAITING_BOOK_ADMIN_SEARCH = set()
BOOK_CREATION_CONTEXT: Dict[int, Dict] = {}
BOOK_CATEGORY_EDIT_CONTEXT: Dict[int, Dict] = {}
BOOK_EDIT_CONTEXT: Dict[int, Dict] = {}
BOOK_SEARCH_CACHE: Dict[str, Dict] = {}
BOOK_NAV_CACHE: Dict[str, Dict] = {}
BOOKS_PAGE_SIZE = 5
BOOK_SEARCH_PAGE_SIZE = 5
BOOK_LATEST_LIMIT = 20

# مذكّرات قلبي
WAITING_MEMO_MENU = set()
WAITING_MEMO_ADD = set()
WAITING_MEMO_EDIT_SELECT = set()
WAITING_MEMO_EDIT_TEXT = set()
WAITING_MEMO_DELETE_SELECT = set()
MEMO_EDIT_INDEX = {}

# رسائل إلى نفسي
# دعم / إدارة
WAITING_SUPPORT_GENDER = set()
WAITING_SUPPORT = set()
WAITING_BROADCAST = set()
SUPPORT_MSG_MAP: Dict[Tuple[int, int], int] = {}  # (admin_id, msg_id) -> user_id

# فلاتر مساعدة
def _user_in_support_session(user) -> bool:
    return bool(user and user.id in WAITING_SUPPORT)


def _user_waiting_book_media(user) -> bool:
    if not user:
        return False
    uid = user.id
    return uid in (
        WAITING_BOOK_ADD_COVER
        | WAITING_BOOK_EDIT_COVER
        | WAITING_BOOK_ADD_PDF
        | WAITING_BOOK_EDIT_PDF
    )

# فوائد ونصائح
WAITING_BENEFIT_TEXT = set()
WAITING_BENEFIT_EDIT_TEXT = set()
WAITING_BENEFIT_DELETE_CONFIRM = set()
BENEFIT_EDIT_ID = {} # user_id -> benefit_id

# إدارة الدورات
WAITING_NEW_COURSE = set()
COURSE_CREATION_CONTEXT: Dict[int, Dict] = {}
WAITING_NEW_LESSON = set()
LESSON_CREATION_CONTEXT: Dict[int, Dict] = {}
WAITING_NEW_QUIZ = set()
QUIZ_CREATION_CONTEXT: Dict[int, Dict] = {}
WAITING_QUIZ_ANSWER = set()
ACTIVE_QUIZ_STATE: Dict[int, Dict] = {}
WAITING_LESSON_TITLE = set()
WAITING_LESSON_CONTENT = set()
WAITING_LESSON_AUDIO = set()
WAITING_LESSON_CURRICULUM_NAME = set()
WAITING_QUIZ_TITLE = set()
WAITING_QUIZ_QUESTION = set()
WAITING_QUIZ_ANSWER_TEXT = set()
WAITING_QUIZ_ANSWER_POINTS = set()
WAITING_COURSE_COUNTRY = set()
WAITING_COURSE_AGE = set()
WAITING_COURSE_GENDER = set()
WAITING_COURSE_FULL_NAME = set()
COURSE_SUBSCRIPTION_CONTEXT: Dict[int, Dict] = {}
WAITING_PROFILE_EDIT_NAME = set()
WAITING_PROFILE_EDIT_AGE = set()
WAITING_PROFILE_EDIT_COUNTRY = set()
PROFILE_EDIT_CONTEXT: Dict[int, Dict] = {}
# Staff Reply bridge: staff_received_message_id -> routing info
# IMPORTANT: key must be (chat_id, message_id) لأن message_id مو عالمي
STAFF_REPLY_BRIDGE: Dict[Tuple[int, int], Dict] = {}
# نظام العرض داخل الدورات (معزول عن الدعم)
WAITING_COURSE_PRESENTATION_MEDIA: Dict[int, str] = {}
PRESENTATION_MEDIA_TIMEOUTS: Dict[int, object] = {}
# نظام الفائدة داخل الدورات (معزول عن العرض والدعم)
WAITING_COURSE_BENEFIT_MEDIA: Dict[int, Dict] = {}
COURSE_BENEFIT_TIMEOUTS: Dict[int, object] = {}


def _lessons_back_keyboard(course_id: str):
    if course_id:
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton("🔙 رجوع", callback_data=f"COURSES:lessons_{course_id}")]]
        )
    return COURSES_ADMIN_MENU_KB


def _quizzes_back_keyboard(course_id: str):
    if course_id:
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton("🔙 رجوع", callback_data=f"COURSES:quizzes_{course_id}")]]
        )
    return COURSES_ADMIN_MENU_KB


def _reset_lesson_creation(user_id: int):
    WAITING_NEW_LESSON.discard(user_id)
    WAITING_LESSON_TITLE.discard(user_id)
    WAITING_LESSON_CONTENT.discard(user_id)
    WAITING_LESSON_AUDIO.discard(user_id)
    WAITING_LESSON_CURRICULUM_NAME.discard(user_id)
    LESSON_CREATION_CONTEXT.pop(user_id, None)


def _reset_course_creation(user_id: int):
    WAITING_NEW_COURSE.discard(user_id)
    COURSE_CREATION_CONTEXT.pop(user_id, None)


def _reset_course_subscription_flow(user_id: int):
    WAITING_COURSE_COUNTRY.discard(user_id)
    WAITING_COURSE_FULL_NAME.discard(user_id)
    WAITING_COURSE_AGE.discard(user_id)
    WAITING_COURSE_GENDER.discard(user_id)
    COURSE_SUBSCRIPTION_CONTEXT.pop(user_id, None)


def _course_creation_keyboard():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("❌ إلغاء", callback_data="COURSES:create_cancel")],
            [InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:admin_back")],
        ]
    )


def _reset_profile_edit_flow(user_id: int):
    WAITING_PROFILE_EDIT_NAME.discard(user_id)
    WAITING_PROFILE_EDIT_AGE.discard(user_id)
    WAITING_PROFILE_EDIT_COUNTRY.discard(user_id)
    PROFILE_EDIT_CONTEXT.pop(user_id, None)


def _reset_quiz_creation(user_id: int):
    WAITING_NEW_QUIZ.discard(user_id)
    WAITING_QUIZ_TITLE.discard(user_id)
    WAITING_QUIZ_QUESTION.discard(user_id)
    WAITING_QUIZ_ANSWER_TEXT.discard(user_id)
    WAITING_QUIZ_ANSWER_POINTS.discard(user_id)
    QUIZ_CREATION_CONTEXT.pop(user_id, None)


def _save_lesson(
    user_id: int,
    course_id: str,
    title: str,
    content_type: str,
    msg,
    content_value: str = "",
    audio_file_id: str = None,
    audio_file_unique_id: str = None,
    audio_kind: str = None,
    source_chat_id: int = None,
    source_message_id: int = None,
):
    try:
        lesson_payload = {
            "course_id": course_id,
            "title": title,
            "content": content_value if content_type != "audio" else "",
            "content_type": content_type,
            "has_presentation": False,
            "audio_file_id": audio_file_id,
            "audio_file_unique_id": audio_file_unique_id,
            "audio_kind": audio_kind,
            "source_chat_id": source_chat_id,
            "source_message_id": source_message_id,
            "created_at": firestore.SERVER_TIMESTAMP,
        }
        db.collection(COURSE_LESSONS_COLLECTION).add(lesson_payload)
        msg.reply_text(
            "✅ تم إضافة الدرس.",
            reply_markup=_lessons_back_keyboard(course_id),
        )
    except Exception as e:
        logger.error(f"خطأ في إضافة الدرس: {e}")
        msg.reply_text(
            "❌ تعذر حفظ الدرس حالياً.",
            reply_markup=COURSES_ADMIN_MENU_KB,
        )
    finally:
        _reset_lesson_creation(user_id)


def _update_lesson(
    user_id: int,
    lesson_id: str,
    course_id: str,
    title: str,
    content_type: str,
    msg,
    content_value: str = "",
    audio_meta: Dict = None,
):
    try:
        doc_ref = db.collection(COURSE_LESSONS_COLLECTION).document(lesson_id)
        doc = doc_ref.get()
        if not doc.exists:
            msg.reply_text("❌ الدرس غير موجود.", reply_markup=_lessons_back_keyboard(course_id))
            return

        update_payload = {
            "title": title,
            "content_type": content_type,
            "updated_at": firestore.SERVER_TIMESTAMP,
        }

        if content_type == "audio":
            update_payload.update(
                {
                    "content": "",
                    "audio_file_id": (audio_meta or {}).get("file_id"),
                    "audio_file_unique_id": (audio_meta or {}).get("file_unique_id"),
                    "audio_kind": (audio_meta or {}).get("audio_kind"),
                    "source_chat_id": (audio_meta or {}).get("source_chat_id"),
                    "source_message_id": (audio_meta or {}).get("source_message_id"),
                }
            )
        else:
            update_payload.update(
                {
                    "content": content_value,
                    "audio_file_id": None,
                    "audio_file_unique_id": None,
                    "audio_kind": None,
                    "source_chat_id": None,
                    "source_message_id": None,
                }
            )

        doc_ref.update(update_payload)
        msg.reply_text(
            "✅ تم تحديث الدرس.",
            reply_markup=_lessons_back_keyboard(course_id),
        )
    except Exception as e:
        logger.error(f"خطأ في تعديل الدرس: {e}")
        msg.reply_text("❌ تعذر تعديل الدرس حالياً.", reply_markup=_lessons_back_keyboard(course_id))
    finally:
        _reset_lesson_creation(user_id)


def _is_audio_document(document) -> bool:
    if not document:
        return False
    mime = (getattr(document, "mime_type", "") or "").lower()
    if mime.startswith("audio/"):
        return True
    filename = (getattr(document, "file_name", "") or "").lower()
    audio_ext = (".mp3", ".wav", ".ogg", ".oga", ".opus", ".m4a", ".flac", ".aac")
    return any(filename.endswith(ext) for ext in audio_ext)


def _extract_audio_metadata(message) -> Dict:
    meta: Dict = {}
    audio_obj = None
    audio_kind = None
    forward_id = getattr(message, "forward_from_message_id", None)

    if message.voice:
        audio_kind = "voice"; audio_obj = message.voice
    elif message.audio:
        audio_kind = "audio"; audio_obj = message.audio
    elif message.document and _is_audio_document(message.document):
        audio_kind = "document_audio"; audio_obj = message.document

    if audio_obj:
        meta["file_id"] = getattr(audio_obj, "file_id", None)
        meta["file_unique_id"] = getattr(audio_obj, "file_unique_id", None)
        meta["audio_kind"] = audio_kind

    # forward from channel/group
    if message.forward_from_chat:
        meta["source_chat_id"] = message.forward_from_chat.id
        if forward_id:
            meta["source_message_id"] = forward_id

    # forward from user (عند ظهور forward_from)
    elif message.forward_from:
        meta["source_chat_id"] = message.forward_from.id
        if forward_id:
            meta["source_message_id"] = forward_id

    return meta


def _finalize_quiz_creation_from_message(user_id: int, msg):
    ctx = QUIZ_CREATION_CONTEXT.get(user_id, {})
    course_id = ctx.get("course_id")
    quiz_id = ctx.get("quiz_id")
    is_edit_mode = ctx.get("mode") == "edit" and quiz_id
    if not course_id:
        msg.reply_text("❌ الدورة غير معروفة.", reply_markup=COURSES_ADMIN_MENU_KB)
        _reset_quiz_creation(user_id)
        return

    answers = ctx.get("answers", [])
    if len(answers) < 2 or not ctx.get("title") or not ctx.get("question"):
        WAITING_QUIZ_ANSWER_TEXT.add(user_id)
        msg.reply_text(
            "❌ يجب إضافة إجابتين على الأقل قبل إنهاء الاختبار.",
            reply_markup=_quizzes_back_keyboard(course_id),
        )
        return

    try:
        quiz_payload = {
            "course_id": course_id,
            "title": ctx.get("title"),
            "question": ctx.get("question"),
            "options": answers,
        }
        if is_edit_mode:
            quiz_payload["updated_at"] = firestore.SERVER_TIMESTAMP
            db.collection(COURSE_QUIZZES_COLLECTION).document(quiz_id).update(quiz_payload)
            msg.reply_text(
                "✅ تم تعديل الاختبار.",
                reply_markup=_quizzes_back_keyboard(course_id),
            )
        else:
            quiz_payload["created_at"] = firestore.SERVER_TIMESTAMP
            db.collection(COURSE_QUIZZES_COLLECTION).add(quiz_payload)
            msg.reply_text(
                "✅ تم إضافة الاختبار.",
                reply_markup=_quizzes_back_keyboard(course_id),
            )
    except Exception as e:
        logger.error(f"خطأ في إضافة الاختبار: {e}")
        msg.reply_text("❌ تعذر حفظ الاختبار حالياً.", reply_markup=COURSES_ADMIN_MENU_KB)
    finally:
        _reset_quiz_creation(user_id)


def _finalize_quiz_creation_from_callback(user_id: int, query: Update.callback_query):
    ctx = QUIZ_CREATION_CONTEXT.get(user_id, {})
    course_id = ctx.get("course_id")
    quiz_id = ctx.get("quiz_id")
    is_edit_mode = ctx.get("mode") == "edit" and quiz_id
    if not course_id:
        safe_edit_message_text(query, "❌ الدورة غير معروفة.", reply_markup=COURSES_ADMIN_MENU_KB)
        _reset_quiz_creation(user_id)
        return

    answers = ctx.get("answers", [])
    if len(answers) < 2 or not ctx.get("title") or not ctx.get("question"):
        safe_edit_message_text(
            query,
            "❌ أضف إجابتين على الأقل قبل الإنهاء.",
            reply_markup=_quizzes_back_keyboard(course_id),
        )
        WAITING_QUIZ_ANSWER_TEXT.add(user_id)
        return

    try:
        quiz_payload = {
            "course_id": course_id,
            "title": ctx.get("title"),
            "question": ctx.get("question"),
            "options": answers,
        }
        if is_edit_mode:
            quiz_payload["updated_at"] = firestore.SERVER_TIMESTAMP
            db.collection(COURSE_QUIZZES_COLLECTION).document(quiz_id).update(quiz_payload)
            safe_edit_message_text(
                query,
                "✅ تم تعديل الاختبار.",
                reply_markup=_quizzes_back_keyboard(course_id),
            )
        else:
            quiz_payload["created_at"] = firestore.SERVER_TIMESTAMP
            db.collection(COURSE_QUIZZES_COLLECTION).add(quiz_payload)
            safe_edit_message_text(
                query,
                "✅ تم إضافة الاختبار.",
                reply_markup=_quizzes_back_keyboard(course_id),
            )
    except Exception as e:
        logger.error(f"خطأ في إضافة الاختبار: {e}")
        safe_edit_message_text(query, "❌ تعذر حفظ الاختبار حالياً.", reply_markup=COURSES_ADMIN_MENU_KB)
    finally:
        _reset_quiz_creation(user_id)


def handle_audio_message(update: Update, context: CallbackContext):
    if update.effective_user is None or update.effective_chat.type == "channel":
        return

    user_id = update.effective_user.id
    if user_id not in WAITING_LESSON_AUDIO:
        return

    ctx = LESSON_CREATION_CONTEXT.get(user_id, {}) or {}
    course_id = ctx.get("course_id")
    title = ctx.get("title")
    lesson_id = ctx.get("lesson_id")
    edit_action = ctx.get("edit_action")
    if not course_id or not title:
        _reset_lesson_creation(user_id)
        update.message.reply_text("❌ البيانات غير مكتملة.", reply_markup=COURSES_ADMIN_MENU_KB)
        return

    document_obj = update.message.document
    if document_obj and not _is_audio_document(document_obj):
        update.message.reply_text(
            "أرسل ملف صوتي فقط",
            reply_markup=_lessons_back_keyboard(course_id),
        )
        return

    meta = _extract_audio_metadata(update.message)
    file_id = meta.get("file_id")

    if not file_id:
        update.message.reply_text("❌ لم يتم استقبال ملف صوتي صالح.", reply_markup=_lessons_back_keyboard(course_id))
        return

    if edit_action == "edit_content":
        if not lesson_id:
            _reset_lesson_creation(user_id)
            update.message.reply_text("❌ الدرس غير معروف.", reply_markup=COURSES_ADMIN_MENU_KB)
            return
        _update_lesson(
            user_id,
            lesson_id,
            course_id,
            title,
            "audio",
            update.message,
            audio_meta=meta,
        )
    else:
        _save_lesson(
            user_id,
            course_id,
            title,
            "audio",
            update.message,
            audio_file_id=file_id,
            audio_file_unique_id=meta.get("file_unique_id"),
            audio_kind=meta.get("audio_kind"),
            source_chat_id=meta.get("source_chat_id"),
            source_message_id=meta.get("source_message_id"),
        )

# أذكار النوم
SLEEP_ADHKAR_STATE = {}  # user_id -> current_index
STRUCTURED_ADHKAR_STATE = {}  # user_id -> {"category": str, "index": int}

# إدارة الجرعة التحفيزية (من لوحة التحكم)
WAITING_MOTIVATION_ADD = set()
WAITING_MOTIVATION_DELETE = set()
WAITING_MOTIVATION_TIMES = set()

# مكتبة الصوتيات
LOCAL_AUDIO_LIBRARY: List[Dict] = []
AUDIO_USER_STATE: Dict[int, Dict] = {}


def _load_local_audio_library():
    """تحميل المكتبة الصوتية من ملف محلي عند غياب Firestore."""

    global LOCAL_AUDIO_LIBRARY

    if not os.path.exists(AUDIO_LIBRARY_FILE):
        LOCAL_AUDIO_LIBRARY = []
        return

    try:
        with open(AUDIO_LIBRARY_FILE, "r", encoding="utf-8") as f:
            LOCAL_AUDIO_LIBRARY = json.load(f) or []
            if not isinstance(LOCAL_AUDIO_LIBRARY, list):
                LOCAL_AUDIO_LIBRARY = []
        logger.info(
            "💾 تم تحميل %s مقطعًا من الملف المحلي للمكتبة الصوتية",
            len(LOCAL_AUDIO_LIBRARY),
        )
    except Exception as e:
        logger.error(f"❌ خطأ في قراءة المكتبة الصوتية المحلية: {e}")
        LOCAL_AUDIO_LIBRARY = []


def _persist_local_audio_library():
    """حفظ نسخة محلية من المكتبة الصوتية لاستخدامها دون Firestore."""

    try:
        with open(AUDIO_LIBRARY_FILE, "w", encoding="utf-8") as f:
            json.dump(LOCAL_AUDIO_LIBRARY, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"❌ خطأ في حفظ المكتبة الصوتية محليًا: {e}")

# نظام الحظر
WAITING_BAN_USER = set()
WAITING_UNBAN_USER = set()
WAITING_BAN_REASON = set()
BAN_TARGET_ID = {}  # user_id -> target_user_id

# إدارة المنافسات والمجتمع
WAITING_DELETE_USER_POINTS = set()
WAITING_DELETE_USER_MEDALS = set()
# متغيرات التأكيد الجديدة
WAITING_CONFIRM_RESET_POINTS = set()
WAITING_CONFIRM_RESET_MEDALS = set()

# =================== الأزرار ===================

# رئيسية
BTN_ADHKAR_MAIN = "أذكاري 📿"
BTN_QURAN_MAIN = "وردي القرآني 📖"
BTN_TASBIH_MAIN = "السبحة 📿"
BTN_MEMOS_MAIN = "مذكرات قلبي 🗓️"
BTN_WATER_MAIN = "منبه الماء 💧"
BTN_STATS = "احصائياتي 📊"
BTN_STATS_ONLY = "إحصائياتي 📊"
BTN_MEDALS_ONLY = "ميدالياتي 🏅"
BTN_STATS_BACK_MAIN = "↩️ رجوع للقائمة الرئيسية"
BTN_MEDALS = "ميدالياتي 🏵️"
BTN_BOOKS_MAIN = "مكتبة طالب العلم 📚"
BTN_BOOKS_ADMIN = "إدارة مكتبة الكتب 📚"
BTN_BOOKS_MANAGE_CATEGORIES = "إدارة التصنيفات 🗂"
BTN_BOOKS_ADD_BOOK = "إضافة كتاب ➕"
BTN_BOOKS_MANAGE_BOOKS = "إدارة الكتب 📋"
BTN_BOOKS_BACKFILL = "تهيئة بيانات الكتب ♻️"
BTN_BOOKS_BACK_MENU = "🔙 رجوع إلى مكتبة الكتب"

BTN_SUPPORT = "تواصل مع الدعم ✉️"
BTN_NOTIFICATIONS_MAIN = "الاشعارات 🔔"
# =================== أزرار قسم الدورات ===================
BTN_COURSES_SECTION = "قسم الدورات 🧩"
BTN_MANAGE_COURSES = "إدارة الدورات 📋"
BTN_AUDIO_LIBRARY = "مكتبة صوتية 🎧"

BTN_CANCEL = "إلغاء ❌"
BTN_BACK_MAIN = "رجوع للقائمة الرئيسية ⬅️"
BTN_SLEEP_ADHKAR_BACK = "⬅️ رجوع للقائمة الرئيسية"
BTN_ADHKAR_NEXT = "➡️ التالي"
BTN_ADHKAR_PREV = "⬅️ السابق"
BTN_ADHKAR_DONE = "✅ إنهاء الأذكار"
BTN_ADHKAR_BACK_MENU = "🔙 الرجوع إلى قائمة الأذكار"
BTN_ADHKAR_BACK_MAIN = "🔝 الرجوع إلى القائمة الرئيسية"

BTN_AUDIO_BACK = "↩️ رجوع"
BTN_AUDIO_NEXT = "التالي ▶️"
BTN_AUDIO_PREV = "⬅️ السابق"

AUDIO_PAGE_SIZE = 10
AUDIO_SECTIONS = {
    "fatawa": {"button": "📌 فتاوى", "hashtag": "#فتاوى", "title": "فتاوى 🎧"},
    "mawaedh": {"button": "📌 مواعظ", "hashtag": "#مواعظ", "title": "مواعظ 🎧"},
    "aqeeda": {"button": "📌 العقيدة", "hashtag": "#العقيدة", "title": "العقيدة 🎧"},
    "faith_trip": {"button": "📌 رحلة إيمانية", "hashtag": "#رحلة_إيمانية", "title": "رحلة إيمانية 🎧"},
}
AUDIO_SECTION_BY_BUTTON = {cfg["button"]: key for key, cfg in AUDIO_SECTIONS.items()}

# أسماء الدورات التي يجب تجاهلها لأنها ليست دورات حقيقية بل أزرار رجوع خاطئة
COURSE_NAME_BLACKLIST = {
    BTN_BACK_MAIN,
    BTN_STATS_BACK_MAIN,
    BTN_SLEEP_ADHKAR_BACK,
    "رجوع للقائمة الرئيسية",
    "↩️ رجوع للقائمة الرئيسية",
    "⬅️ رجوع للقائمة الرئيسية",
}


def _is_back_placeholder_course(course_name: str) -> bool:
    """تحديد إن كان الاسم يمثل زر رجوع تمت إضافته بالخطأ كدورة."""

    if not course_name:
        return False

    normalized_name = course_name.strip()
    return normalized_name in COURSE_NAME_BLACKLIST


# المنافسات و المجتمع
BTN_COMP_MAIN = "المنافسات و المجتمع 🏆"
BTN_MY_PROFILE = "ملفي التنافسي 🎯"
BTN_TOP10 = "أفضل 10 🏅"
BTN_TOP100 = "أفضل 100 🏆"

# فوائد و نصائح
BTN_BENEFITS_MAIN = "مجتمع الفوائد و النصائح 💡"
BTN_BENEFIT_ADD = "✍️ أضف فائدة / نصيحة"
BTN_BENEFIT_VIEW = "📖 استعراض الفوائد"
BTN_BENEFIT_TOP10 = "🎆 أفضل 10 فوائد"
BTN_BENEFIT_TOP100 = "🏆 أفضل 100 فائدة"
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
# أزرار إدارة المنافسات والمجتمع
BTN_ADMIN_MANAGE_COMPETITION = "🔹 إدارة المنافسات والمجتمع"

# الأزرار الجديدة للتأكيد
BTN_ADMIN_RESET_POINTS = "تصفير نقاط المنافسات والمجتمع 🔴"
BTN_ADMIN_RESET_MEDALS = "تصفير ميداليات المنافسات والمجتمع 🎆"

# جرعة تحفيزية للمستخدم
BTN_MOTIVATION_ON = "تشغيل الجرعة التحفيزية ⚡"
BTN_MOTIVATION_OFF = "إيقاف الجرعة التحفيزية 😴"

# الميداليات
MEDAL_BEGINNING = "ميدالية بداية الطريق 🌱"
MEDAL_PERSISTENCE = "ميدالية الاستمرار 🚀"
MEDAL_HIGH_SPIRIT = "ميدالية الهمة العالية 💪"
MEDAL_HERO = "ميدالية بطل سُقيا الكوثر 🥇"
MEDAL_DAILY_ACTIVITY = "ميدالية النشاط اليومي ✨"
MEDAL_STREAK = "ميدالية الاستمرارية (ستريك الأيام) 🗓️"
MEDAL_TOP_BENEFIT = "وسام صاحب فائدة من العشرة الأوائل 💡🥇"

LEVEL_MEDAL_RULES = [
    (3, MEDAL_BEGINNING),
    (8, MEDAL_PERSISTENCE),
    (15, MEDAL_HIGH_SPIRIT),
    (25, MEDAL_HERO),
]

DAILY_FULL_MEDAL_THRESHOLD = 3
DAILY_STREAK_MEDAL_THRESHOLD = 14

MEDAL_RENAMES = {
    "ميدالية بداية الطريق 🟢": MEDAL_BEGINNING,
    "ميدالية الاستمرار 🎓": MEDAL_PERSISTENCE,
    "ميدالية الهمة العالية 🔥": MEDAL_HIGH_SPIRIT,
    "ميدالية بطل سُقيا الكوثر 🏆": MEDAL_HERO,
    "ميدالية النشاط اليومي ⚡": MEDAL_DAILY_ACTIVITY,
    "ميدالية الاستمرارية 📅": MEDAL_STREAK,
    "وسام صاحب فائدة من العشرة الأوائل 💡🏅": MEDAL_TOP_BENEFIT,
}

MAIN_KEYBOARD_USER = ReplyKeyboardMarkup(
    [
        # السطر الأول: وردي القرآني في العمود الأيسر وأذكاري في العمود الأيمن
        [KeyboardButton(BTN_ADHKAR_MAIN), KeyboardButton(BTN_QURAN_MAIN)],
        # السطر الثاني: مكتبة طالب العلم في العمود الأيسر وقسم الدورات في العمود الأيمن
        [KeyboardButton(BTN_COURSES_SECTION), KeyboardButton(BTN_BOOKS_MAIN)],
        # السطر الثالث: مكتبة صوتية في العمود الأيسر ومذكرات قلبي في العمود الأيمن
        [KeyboardButton(BTN_MEMOS_MAIN), KeyboardButton(BTN_AUDIO_LIBRARY)],
        # السطر الرابع: مجتمع الفوائد والنصائح في العمود الأيسر والمنافسات والمجتمع في العمود الأيمن
        [KeyboardButton(BTN_COMP_MAIN), KeyboardButton(BTN_BENEFITS_MAIN)],
        # السطر الخامس: منبه الماء في العمود الأيسر واحصائياتي في العمود الأيمن
        [KeyboardButton(BTN_STATS), KeyboardButton(BTN_WATER_MAIN)],
        # السطر السادس: التواصل مع الدعم في العمود الأيسر والاشعارات في العمود الأيمن
        [KeyboardButton(BTN_NOTIFICATIONS_MAIN), KeyboardButton(BTN_SUPPORT)],
    ],
    resize_keyboard=True,
)

MAIN_KEYBOARD_ADMIN = ReplyKeyboardMarkup(
    [
        # السطر الأول: وردي القرآني في العمود الأيسر وأذكاري في العمود الأيمن
        [KeyboardButton(BTN_ADHKAR_MAIN), KeyboardButton(BTN_QURAN_MAIN)],
        # السطر الثاني: مكتبة طالب العلم في العمود الأيسر وقسم الدورات في العمود الأيمن
        [KeyboardButton(BTN_COURSES_SECTION), KeyboardButton(BTN_BOOKS_MAIN)],
        # السطر الثالث: مكتبة صوتية في العمود الأيسر ومذكرات قلبي في العمود الأيمن
        [KeyboardButton(BTN_MEMOS_MAIN), KeyboardButton(BTN_AUDIO_LIBRARY)],
        # السطر الرابع: مجتمع الفوائد والنصائح في العمود الأيسر والمنافسات والمجتمع في العمود الأيمن
        [KeyboardButton(BTN_COMP_MAIN), KeyboardButton(BTN_BENEFITS_MAIN)],
        # السطر الخامس: منبه الماء في العمود الأيسر واحصائياتي في العمود الأيمن
        [KeyboardButton(BTN_STATS), KeyboardButton(BTN_WATER_MAIN)],
        # السطر السادس: التواصل مع الدعم في العمود الأيسر والاشعارات في العمود الأيمن
        [KeyboardButton(BTN_NOTIFICATIONS_MAIN), KeyboardButton(BTN_SUPPORT)],
        # السطر السابع: لوحة التحكم (فقط للمدير)
        [KeyboardButton(BTN_ADMIN_PANEL)],
    ],
    resize_keyboard=True,
)

MAIN_KEYBOARD_SUPERVISOR = ReplyKeyboardMarkup(
    [
        # السطر الأول: وردي القرآني في العمود الأيسر وأذكاري في العمود الأيمن
        [KeyboardButton(BTN_ADHKAR_MAIN), KeyboardButton(BTN_QURAN_MAIN)],
        # السطر الثاني: مكتبة طالب العلم في العمود الأيسر وقسم الدورات في العمود الأيمن
        [KeyboardButton(BTN_COURSES_SECTION), KeyboardButton(BTN_BOOKS_MAIN)],
        # السطر الثالث: مكتبة صوتية في العمود الأيسر ومذكرات قلبي في العمود الأيمن
        [KeyboardButton(BTN_MEMOS_MAIN), KeyboardButton(BTN_AUDIO_LIBRARY)],
        # السطر الرابع: مجتمع الفوائد والنصائح في العمود الأيسر والمنافسات والمجتمع في العمود الأيمن
        [KeyboardButton(BTN_COMP_MAIN), KeyboardButton(BTN_BENEFITS_MAIN)],
        # السطر الخامس: منبه الماء في العمود الأيسر واحصائياتي في العمود الأيمن
        [KeyboardButton(BTN_STATS), KeyboardButton(BTN_WATER_MAIN)],
        # السطر السادس: التواصل مع الدعم في العمود الأيسر والاشعارات في العمود الأيمن
        [KeyboardButton(BTN_NOTIFICATIONS_MAIN), KeyboardButton(BTN_SUPPORT)],
        # السطر السابع: لوحة التحكم (للمشرفة)
        [KeyboardButton(BTN_ADMIN_PANEL)],
    ],
    resize_keyboard=True,
)

PRESENTATION_SESSION_KB = ReplyKeyboardMarkup(
    [[KeyboardButton("🚪 خروج من العرض")]],
    resize_keyboard=True,
)

BENEFIT_SESSION_KB = ReplyKeyboardMarkup(
    [[KeyboardButton("🚪 خروج من الفائدة")]],
    resize_keyboard=True,
)

SESSION_EXIT_PRESENTATION_TEXTS = {"🚪 خروج من العرض", "🚪 خروج من العَرْض"}
SESSION_EXIT_BENEFIT_TEXTS = {"🚪 خروج من الفائدة"}

MAIN_MENU_BUTTON_TEXTS = {
    BTN_ADHKAR_MAIN,
    BTN_QURAN_MAIN,
    BTN_COURSES_SECTION,
    BTN_BOOKS_MAIN,
    BTN_MEMOS_MAIN,
    BTN_AUDIO_LIBRARY,
    BTN_COMP_MAIN,
    BTN_BENEFITS_MAIN,
    BTN_STATS,
    BTN_WATER_MAIN,
    BTN_NOTIFICATIONS_MAIN,
    BTN_SUPPORT,
    BTN_ADMIN_PANEL,
    "أذكاري",
    "وردي القرآني",
    "قسم الدورات",
    "مكتبة طالب العلم",
    "مذكرات قلبي",
    "مكتبة صوتية",
    "المنافسات و المجتمع",
    "مجتمع الفوائد و النصائح",
    "احصائياتي",
    "إحصائياتي",
    "منبه الماء",
    "الاشعارات",
    "تواصل مع الدعم",
    "لوحة التحكم",
}

BTN_SUPPORT_END = "🔚 إنهاء التواصل"

CANCEL_KB = ReplyKeyboardMarkup(
    [[KeyboardButton(BTN_CANCEL)]],
    resize_keyboard=True,
)

SUPPORT_SESSION_KB = ReplyKeyboardMarkup(
    [[KeyboardButton(BTN_SUPPORT_END)]],
    resize_keyboard=True,
)
SUPPORT_REPLY_INLINE_KB = InlineKeyboardMarkup(
    [[InlineKeyboardButton("✉️ اضغط هنا للرد", callback_data="support_open")]]
)

SUPPORT_PROMPT_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_SUPPORT)],
        [KeyboardButton(BTN_CANCEL)],
    ],
    resize_keyboard=True,
)

AUDIO_LIBRARY_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(AUDIO_SECTIONS["fatawa"]["button"]), KeyboardButton(AUDIO_SECTIONS["mawaedh"]["button"])],
        [KeyboardButton(AUDIO_SECTIONS["aqeeda"]["button"]), KeyboardButton(AUDIO_SECTIONS["faith_trip"]["button"])],
        [KeyboardButton(BTN_BACK_MAIN)],
    ],
    resize_keyboard=True,
)

STATS_MENU_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_STATS_ONLY), KeyboardButton(BTN_MEDALS_ONLY)],
        [KeyboardButton(BTN_STATS_BACK_MAIN)],
    ],
    resize_keyboard=True,
)

# ---- منبّه الماء ----
BTN_WATER_LOG = "سجلت كوب ماء 🥤"
BTN_WATER_ADD_CUPS = "إضافة عدد أكواب 🧮🥤"
BTN_WATER_STATUS = "مستواي اليوم 📊"
BTN_WATER_SETTINGS = "إعدادات الماء ⚙️"

BTN_WATER_NEED = "حساب احتياج الماء 🧘"
BTN_WATER_REM_ON = "تشغيل تذكير الماء ⏰"
BTN_WATER_REM_OFF = "إيقاف تذكير الماء 📴"
BTN_WATER_RESET = "تصفير عداد الماء 🔄"

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
        [KeyboardButton(BTN_WATER_RESET)],
        [KeyboardButton(BTN_WATER_BACK_MENU)],
        [KeyboardButton(BTN_BACK_MAIN), KeyboardButton(BTN_ADMIN_PANEL)],
    ],
    resize_keyboard=True,
)

WATER_SETTINGS_KB_USER = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_WATER_NEED)],
        [KeyboardButton(BTN_WATER_RESET)],
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
        [KeyboardButton(BTN_BENEFIT_TOP10), KeyboardButton(BTN_BENEFIT_TOP100)],
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
BTN_ADHKAR_SLEEP = "💤 أذكار النوم"
BTN_SLEEP_ADHKAR_NEXT = "⬅️ التالي"

ADHKAR_MENU_KB_USER = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_ADHKAR_MORNING), KeyboardButton(BTN_ADHKAR_EVENING)],
        [KeyboardButton(BTN_ADHKAR_GENERAL), KeyboardButton(BTN_ADHKAR_SLEEP)],
        [KeyboardButton(BTN_BACK_MAIN)],
    ],
    resize_keyboard=True,
)

ADHKAR_MENU_KB_ADMIN = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_ADHKAR_MORNING), KeyboardButton(BTN_ADHKAR_EVENING)],
        [KeyboardButton(BTN_ADHKAR_GENERAL), KeyboardButton(BTN_ADHKAR_SLEEP)],
        [KeyboardButton(BTN_BACK_MAIN), KeyboardButton(BTN_ADMIN_PANEL)],
    ],
    resize_keyboard=True,
)

def build_structured_adhkar_kb(has_prev: bool, show_next: bool) -> ReplyKeyboardMarkup:
    rows = []
    nav_row = []

    if has_prev:
        nav_row.append(KeyboardButton(BTN_ADHKAR_PREV))
    if show_next:
        nav_row.append(KeyboardButton(BTN_ADHKAR_NEXT))

    if nav_row:
        rows.append(nav_row)

    rows.append([KeyboardButton(BTN_ADHKAR_BACK_MENU)])
    rows.append([KeyboardButton(BTN_ADHKAR_BACK_MAIN)])

    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

SLEEP_ADHKAR_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_SLEEP_ADHKAR_NEXT)],
        [KeyboardButton(BTN_SLEEP_ADHKAR_BACK)],
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

BOOKS_ADMIN_MENU_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_BOOKS_MANAGE_CATEGORIES)],
        [KeyboardButton(BTN_BOOKS_ADD_BOOK)],
        [KeyboardButton(BTN_BOOKS_MANAGE_BOOKS)],
        [KeyboardButton(BTN_BOOKS_BACKFILL)],
        [KeyboardButton(BTN_BACK_MAIN), KeyboardButton(BTN_ADMIN_PANEL)],
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
        [KeyboardButton(BTN_BOOKS_ADMIN)],
        [KeyboardButton(BTN_ADMIN_MANAGE_COMPETITION)],
        [KeyboardButton(BTN_MANAGE_COURSES)],
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
        [KeyboardButton(BTN_BOOKS_ADMIN)],
        [KeyboardButton(BTN_MANAGE_COURSES)],
        [KeyboardButton(BTN_BACK_MAIN)],
    ],
    resize_keyboard=True,
)

ADMIN_COMPETITION_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_ADMIN_RESET_POINTS)],
        [KeyboardButton(BTN_ADMIN_RESET_MEDALS)],

        [KeyboardButton(BTN_BACK_MAIN), KeyboardButton(BTN_ADMIN_PANEL)],
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
def notifications_menu_keyboard(user_id: int, record: Dict = None) -> ReplyKeyboardMarkup:
    record = record or get_user_record_by_id(user_id) or {}
    reminders_on = bool(record.get("water_enabled", False))
    water_button = KeyboardButton(BTN_WATER_REM_OFF if reminders_on else BTN_WATER_REM_ON)

    rows = [
        [KeyboardButton(BTN_MOTIVATION_ON)],
        [KeyboardButton(BTN_MOTIVATION_OFF)],
        [water_button],
    ]

    if is_admin(user_id):
        rows.append([KeyboardButton(BTN_BACK_MAIN), KeyboardButton(BTN_ADMIN_PANEL)])
    else:
        rows.append([KeyboardButton(BTN_BACK_MAIN)])

    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

# =================== نظام النقاط ===================

POINTS_PER_WATER_CUP = 1
POINTS_WATER_DAILY_BONUS = 20

POINTS_PER_QURAN_PAGE = 3
POINTS_QURAN_DAILY_BONUS = 30


def tasbih_points_for_session(target_count: int) -> int:
    return max(target_count // 10, 1)

# =================== الميداليات ===================


def normalize_medals_list(medals: List[str]) -> List[str]:
    normalized = []
    for medal in medals or []:
        new_name = MEDAL_RENAMES.get(medal, medal)
        if new_name not in normalized:
            normalized.append(new_name)
    return normalized


def ensure_medal_defaults(record: dict):
    record["medals"] = normalize_medals_list(record.get("medals", []))
    record.setdefault("daily_full_count", 0)
    record.setdefault("daily_full_streak", 0)
    record.setdefault("last_full_day", None)
    record.setdefault("saved_books", [])
    record.setdefault("saved_books_updated_at", None)

# =================== مكتبة الكتب ===================

BOOKS_CALLBACK_PREFIX = "BOOKS"
BOOKS_HOME_BACK = "BOOKS:home"
BOOKS_LATEST_CALLBACK = "BOOKS:latest:0"
BOOKS_SAVED_CALLBACK = "BOOKS:saved:0"
BOOKS_SEARCH_PROMPT_CALLBACK = "BOOKS:search_prompt"
BOOKS_EXIT_CALLBACK = "BOOKS:exit"
BOOKS_ADMIN_MANAGE_CATEGORIES = "BOOKS:admin_categories"
BOOKS_ADMIN_MANAGE_BOOKS = "BOOKS:admin_books"
BOOKS_ADMIN_ADD_BOOK = "BOOKS:admin_add_book"
BOOKS_BACK_CALLBACK = "BOOKS:back"
BOOKS_CATEGORY_SELECT_PREFIX = "BOOKS:cat"
BOOKS_SEARCH_RESULTS_PREFIX = "BOOKS:search_results"
BOOKS_ADMIN_EDIT_CATEGORY_PREFIX = "BOOKS:edit_category"
BOOKS_ADMIN_EDIT_BOOK_PREFIX = "BOOKS:edit_book"
BOOKS_BACKFILL_BATCH_SIZE = 200
BOOKS_DEFAULT_ROUTE = "home:none:0"


def _book_timestamp_value():
    if firestore_available():
        return firestore.SERVER_TIMESTAMP
    return datetime.now(timezone.utc).isoformat()


def _normalize_book_bool(value, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y", "on"}:
            return True
        if normalized in {"false", "0", "no", "n", "off", ""}:
            return False
    if isinstance(value, (int, float)):
        return bool(value)
    return default


def _normalize_book_text(value: str) -> str:
    return (value or "").strip().lower()


def _normalize_category_id(val):
    if val is None:
        return ""
    try:
        # Firestore DocumentReference
        if hasattr(val, "id"):
            return str(val.id).strip()
    except Exception:
        pass
    return str(val).strip()


def _as_bool(v, default):
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes", "y", "on")
    if isinstance(v, (int, float)):
        return bool(v)
    return default


def _parse_tags_input(text: str) -> List[str]:
    if not text:
        return []
    tags = [t.strip() for t in text.split(",") if t.strip()]
    normalized = []
    for tag in tags:
        normalized_tag = tag.replace("#", "").strip()
        if normalized_tag:
            normalized.append(normalized_tag)
    return normalized


def _book_category_sort_key(cat: Dict) -> Tuple:
    return (
        cat.get("order") if cat.get("order") is not None else 0,
        cat.get("name") or "",
    )


def _book_created_at_value(raw_value) -> datetime:
    if isinstance(raw_value, datetime):
        return raw_value
    if hasattr(raw_value, "to_datetime"):
        try:
            return raw_value.to_datetime()
        except Exception:
            pass
    if hasattr(raw_value, "timestamp") and not isinstance(raw_value, (int, float)):
        try:
            return datetime.fromtimestamp(raw_value.timestamp(), tz=timezone.utc)
        except Exception:
            pass
    if isinstance(raw_value, (int, float)):
        try:
            return datetime.fromtimestamp(raw_value, tz=timezone.utc)
        except Exception:
            pass
    if isinstance(raw_value, str):
        try:
            return datetime.fromisoformat(raw_value)
        except Exception:
            pass
    return None


def _sort_books_by_created_at(books: List[Dict]) -> List[Dict]:
    fallback = datetime.fromtimestamp(0, tz=timezone.utc)
    return sorted(
        books,
        key=lambda b: _book_created_at_value(b.get("created_at")) or fallback,
        reverse=True,
    )


def _log_book_skip(book_id: str, reason: str):
    logger.info("[BOOKS][SKIP] book_id=%s reason=%s", book_id, reason)


def _normalize_category_key(value: str) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _build_category_lookup(include_inactive: bool = True) -> Dict[str, Dict[str, str]]:
    if not firestore_available():
        return {"id_map": {}, "name_map": {}, "slug_map": {}, "id_to_name": {}}
    categories = fetch_book_categories(include_inactive=include_inactive)
    lookup = {"id_map": {}, "name_map": {}, "slug_map": {}, "id_to_name": {}}
    for cat in categories:
        cat_id = (cat.get("id") or "").strip()
        if not cat_id:
            continue
        normalized_id = _normalize_category_key(cat_id)
        lookup["id_map"][normalized_id] = cat_id
        lookup["id_to_name"][cat_id] = cat.get("name")
        name_norm = _normalize_category_key(cat.get("name"))
        if name_norm and name_norm not in lookup["name_map"]:
            lookup["name_map"][name_norm] = cat_id
        slug_norm = _normalize_category_key(cat.get("slug"))
        if slug_norm and slug_norm not in lookup["slug_map"]:
            lookup["slug_map"][slug_norm] = cat_id
    return lookup


def _resolve_category_id(category_lookup: Dict[str, Dict[str, str]], category_id: str = None, category_name: str = None) -> str:
    if not category_lookup:
        return (category_id or "").strip()
    has_lookup_data = any(category_lookup.get(key) for key in ("id_map", "name_map", "slug_map"))
    if not has_lookup_data:
        return (category_id or "").strip()
    normalized_id = _normalize_category_key(category_id)
    if normalized_id and normalized_id in category_lookup.get("id_map", {}):
        return category_lookup["id_map"][normalized_id]
    for candidate in (category_name, category_id):
        norm = _normalize_category_key(candidate)
        if norm and norm in category_lookup.get("name_map", {}):
            return category_lookup["name_map"][norm]
        if norm and norm in category_lookup.get("slug_map", {}):
            return category_lookup["slug_map"][norm]
    return ""


def _prepare_book_backfill_updates(book: Dict, category_lookup: Dict[str, Dict[str, str]] = None) -> Tuple[Dict, List[str]]:
    updates: Dict = {}
    reasons: List[str] = []
    category_lookup = category_lookup or {}

    if not isinstance(book.get("is_active"), bool):
        updates["is_active"] = True
        reasons.append("is_active_defaulted")

    if not isinstance(book.get("is_deleted"), bool):
        updates["is_deleted"] = False
        reasons.append("is_deleted_defaulted")

    created_missing = "created_at" not in book or book.get("created_at") is None
    if created_missing:
        updated_value = _book_created_at_value(book.get("updated_at"))
        updates["created_at"] = updated_value or firestore.SERVER_TIMESTAMP
        reasons.append("created_at_added")

    if "downloads_count" not in book:
        updates["downloads_count"] = 0
        reasons.append("downloads_defaulted")

    if category_lookup:
        current_category_raw = (book.get("category_id") or "").strip()
        resolved_category_id = _resolve_category_id(
            category_lookup,
            category_id=current_category_raw,
            category_name=book.get("category_name_snapshot"),
        )
        if resolved_category_id and resolved_category_id != current_category_raw:
            updates["category_id"] = resolved_category_id
            reasons.append("category_id_corrected")
        elif not resolved_category_id and current_category_raw and _normalize_category_key(current_category_raw) not in category_lookup.get("id_map", {}):
            reasons.append("category_id_unmapped")
        desired_snapshot = category_lookup.get("id_to_name", {}).get(resolved_category_id or current_category_raw)
        if desired_snapshot and desired_snapshot != book.get("category_name_snapshot"):
            updates["category_name_snapshot"] = desired_snapshot
            reasons.append("category_snapshot_synced")

    if updates:
        updates["updated_at"] = firestore.SERVER_TIMESTAMP

    return updates, reasons


def _flush_books_backfill_batch(batch_items: List[Tuple[str, Dict]], errors: List[str]) -> int:
    if not batch_items:
        return 0
    batch = db.batch()
    doc_ids = []
    for doc_id, payload in batch_items:
        doc_ids.append(doc_id)
        ref = db.collection(BOOKS_COLLECTION).document(doc_id)
        batch.update(ref, payload)
    try:
        batch.commit()
        return len(batch_items)
    except Exception as e:
        errors.append(f"{','.join(doc_ids)} | {e}")
        return 0


def run_books_backfill() -> Dict:
    stats = {"total": 0, "updated": 0, "skipped": 0}
    skipped_reasons = defaultdict(int)
    errors: List[str] = []

    if not firestore_available():
        return {
            "total": 0,
            "updated": 0,
            "skipped": 0,
            "errors": ["firestore_unavailable"],
            "skipped_reasons": {},
        }

    category_lookup = _build_category_lookup(include_inactive=True)
    docs = db.collection(BOOKS_COLLECTION).stream()
    batch_items: List[Tuple[str, Dict]] = []

    for doc in docs:
        stats["total"] += 1
        book = doc.to_dict() or {}
        book_id = doc.id or book.get("id") or "unknown"

        try:
            updates, reasons = _prepare_book_backfill_updates(book, category_lookup)
        except Exception as prep_err:
            errors.append(f"{book_id} | prep_error | {prep_err}")
            continue

        if not updates:
            stats["skipped"] += 1
            reason_key = "no_changes"
            if "category_id_unmapped" in reasons:
                reason_key = "category_id_unmapped"
            skipped_reasons[reason_key] += 1
            continue

        batch_items.append((book_id, updates))

        if len(batch_items) >= BOOKS_BACKFILL_BATCH_SIZE:
            stats["updated"] += _flush_books_backfill_batch(batch_items, errors)
            batch_items = []

    stats["updated"] += _flush_books_backfill_batch(batch_items, errors)

    stats["skipped_reasons"] = dict(skipped_reasons)
    stats["errors"] = errors
    return stats


def _format_books_backfill_report(result: Dict) -> str:
    lines = [
        "♻️ تقرير تهيئة بيانات الكتب",
        f"- إجمالي السجلات: {result.get('total', 0)}",
        f"- تم التحديث: {result.get('updated', 0)}",
        f"- تم التخطي: {result.get('skipped', 0)}",
    ]

    skipped = result.get("skipped_reasons") or {}
    if skipped:
        lines.append("أسباب التخطي:")
        for reason, count in skipped.items():
            lines.append(f"  • {reason}: {count}")

    errors = result.get("errors") or []
    if errors:
        lines.append(f"الأخطاء ({len(errors)}):")
        for err in errors[:10]:
            lines.append(f"  • {err}")
        if len(errors) > 10:
            lines.append(f"  • ... (+{len(errors) - 10} أخطاء إضافية)")

    return "\n".join(lines)


def fetch_book_categories(include_inactive: bool = False) -> List[Dict]:
    if not firestore_available():
        logger.warning("[BOOKS] Firestore غير متاح - لا يمكن جلب التصنيفات")
        return []
    try:
        query = db.collection(BOOK_CATEGORIES_COLLECTION)
        if not include_inactive:
            query = query.where("is_active", "==", True)
        docs = query.stream()
        categories = []
        for doc in docs:
            data = doc.to_dict()
            data["id"] = doc.id
            categories.append(data)
        categories.sort(key=_book_category_sort_key)
        return categories
    except Exception as e:
        logger.error(f"[BOOKS] خطأ في جلب التصنيفات: {e}", exc_info=True)
        return []


def get_book_category(category_id: str) -> Dict:
    if not firestore_available():
        return {}
    try:
        doc = db.collection(BOOK_CATEGORIES_COLLECTION).document(category_id).get()
        if doc.exists:
            data = doc.to_dict()
            data["id"] = doc.id
            return data
    except Exception as e:
        logger.error(f"[BOOKS] خطأ في قراءة التصنيف {category_id}: {e}")
    return {}


def save_book_category(name: str, order: int = None, created_by: int = None) -> str:
    if not firestore_available():
        logger.warning("[BOOKS] Firestore غير متاح - لن يتم حفظ التصنيف")
        return ""
    payload = {
        "name": name.strip(),
        "slug": re.sub(r"\s+", "-", name.strip().lower()),
        "order": order if order is not None else 0,
        "is_active": True,
        "created_by": created_by,
        "created_at": _book_timestamp_value(),
        "updated_at": _book_timestamp_value(),
    }
    try:
        doc_ref = db.collection(BOOK_CATEGORIES_COLLECTION).add(payload)[1]
        logger.info("[BOOKS] تم إنشاء تصنيف جديد %s", doc_ref.id)
        return doc_ref.id
    except Exception as e:
        logger.error(f"[BOOKS] خطأ في إنشاء التصنيف: {e}")
        return ""


def update_book_category(category_id: str, **fields):
    if not firestore_available():
        return False
    try:
        fields["updated_at"] = _book_timestamp_value()
        db.collection(BOOK_CATEGORIES_COLLECTION).document(category_id).update(fields)
        logger.info("[BOOKS] تم تحديث التصنيف %s", category_id)
        return True
    except Exception as e:
        logger.error(f"[BOOKS] خطأ في تحديث التصنيف {category_id}: {e}")
        return False


def deactivate_book_category(category_id: str) -> bool:
    return update_book_category(category_id, is_active=False)


def category_has_books(category_id: str) -> bool:
    if not firestore_available():
        return False
    try:
        books = fetch_books_list(include_inactive=True, include_deleted=False)
        return any(str(book.get("category_id")) == str(category_id) for book in books)
    except Exception as e:
        logger.error(f"[BOOKS] خطأ في فحص كتب التصنيف {category_id}: {e}")
        return False


def delete_book_category(category_id: str) -> bool:
    if category_has_books(category_id):
        return False
    if not firestore_available():
        return False
    try:
        db.collection(BOOK_CATEGORIES_COLLECTION).document(category_id).delete()
        logger.info("[BOOKS] تم حذف التصنيف نهائياً %s", category_id)
        return True
    except Exception as e:
        logger.error(f"[BOOKS] خطأ في حذف التصنيف {category_id}: {e}")
        return False


def _book_query(include_inactive=False, include_deleted=False):
    query = db.collection(BOOKS_COLLECTION)
    if not include_inactive:
        query = query.where("is_active", "==", True)
    if not include_deleted:
        query = query.where("is_deleted", "==", False)
    return query


def _ensure_admin_book_defaults(payload: Dict, *, existing: Dict = None, is_creation: bool = False) -> Dict:
    data = payload.copy()
    existing = existing or {}

    active_value = _normalize_book_bool(
        data.get("is_active") if "is_active" in data else (None if is_creation else existing.get("is_active")),
        True,
    )
    deleted_value = _normalize_book_bool(
        data.get("is_deleted") if "is_deleted" in data else (None if is_creation else existing.get("is_deleted")),
        False,
    )
    created_source = data.get("created_at") if is_creation else data.get("created_at", existing.get("created_at"))
    created_value = _book_created_at_value(created_source)
    if isinstance(created_value, datetime) and created_value.tzinfo is None:
        created_value = created_value.replace(tzinfo=timezone.utc)
    if created_value is None:
        created_value = _book_timestamp_value()

    data["is_active"] = active_value
    data["is_deleted"] = deleted_value
    data["created_at"] = created_value
    data["updated_at"] = _book_timestamp_value()
    return data


def _fetch_books_raw() -> List[Dict]:
    docs = db.collection(BOOKS_COLLECTION).stream()
    books = []
    for doc in docs:
        book = doc.to_dict() or {}
        book["id"] = doc.id
        books.append(book)
    logger.info("[BOOKS][RAW] total=%s", len(books))
    return books


def _filter_books_pythonically(books: List[Dict], include_inactive: bool, include_deleted: bool) -> List[Dict]:
    visible = []
    for book in books:
        book_id = book.get("id") or "unknown"
        is_deleted = _as_bool(book.get("is_deleted"), False)
        is_active = _as_bool(book.get("is_active"), True)
        if not include_deleted and is_deleted:
            logger.info("[BOOKS][RAW_SKIP] %s is_deleted_true", book_id)
            continue
        if not include_inactive and not is_active:
            logger.info("[BOOKS][RAW_SKIP] %s is_active_false", book_id)
            continue
        visible.append(book)
    logger.info("[BOOKS][VISIBLE] total=%s", len(visible))
    return visible


def fetch_books_list(
    category_id: str = None,
    include_inactive: bool = False,
    include_deleted: bool = False,
) -> List[Dict]:
    if not firestore_available():
        logger.warning("[BOOKS] Firestore غير متاح - تعذر جلب الكتب")
        return []
    try:
        category_filter = _normalize_category_id(category_id)
        all_books = _fetch_books_raw()
        if category_filter:
            logger.info("[BOOKS][CAT_FILTER] wanted=%s total_before=%s", category_filter, len(all_books))
            sample = [b.get("category_id") for b in all_books[:10]]
            logger.info("[BOOKS][CAT_FILTER] sample_category_ids=%s", sample)
            filtered = [b for b in all_books if _normalize_category_id(b.get("category_id")) == category_filter]
            logger.info("[BOOKS][CAT_FILTER] total_after=%s", len(filtered))
            all_books = filtered
        books = _filter_books_pythonically(all_books, include_inactive, include_deleted)
        for book in books:
            missing_required = [field for field in ("title", "category_id", "pdf_file_id", "created_at") if not book.get(field)]
            if missing_required:
                logger.warning(
                    "[BOOKS][LIST][MISSING] book_id=%s missing=%s",
                    book.get("id"),
                    ",".join(missing_required),
                )
        books = _sort_books_by_created_at(books)
        logger.info(
            "[BOOKS][LIST] fetched=%s filters=category:%s include_inactive=%s include_deleted=%s",
            len(books),
            category_filter or "all",
            include_inactive,
            include_deleted,
        )
        return books
    except Exception as e:
        logger.error(f"[BOOKS] خطأ في جلب الكتب: {e}", exc_info=True)
        return []


def fetch_latest_books(limit: int = BOOK_LATEST_LIMIT) -> List[Dict]:
    if not firestore_available():
        return []
    try:
        all_books = _fetch_books_raw()
        books = _filter_books_pythonically(all_books, include_inactive=False, include_deleted=False)
        books = _sort_books_by_created_at(books)
        return books[:limit]
    except Exception as e:
        logger.error(f"[BOOKS] خطأ في جلب آخر الإضافات: {e}", exc_info=True)
        return []


def get_book_by_id(book_id: str) -> Dict:
    if not firestore_available():
        return {}
    try:
        doc = db.collection(BOOKS_COLLECTION).document(book_id).get()
        if doc.exists:
            book = doc.to_dict()
            book["id"] = doc.id
            return book
    except Exception as e:
        logger.error(f"[BOOKS] خطأ في قراءة الكتاب {book_id}: {e}")
    return {}


def create_book_record(payload: Dict) -> str:
    if not firestore_available():
        logger.warning("[BOOKS] Firestore غير متاح - لن يتم حفظ الكتاب")
        return ""
    payload = _ensure_admin_book_defaults(payload, is_creation=True)
    payload.setdefault("downloads_count", 0)
    try:
        doc_ref = db.collection(BOOKS_COLLECTION).add(payload)[1]
        book_id = doc_ref.id
        logger.info("[BOOKS] تم إنشاء كتاب جديد %s", book_id)
        try:
            stored_doc = doc_ref.get()
            stored_data = stored_doc.to_dict() or {}
            stored_data["id"] = book_id
            logger.info(
                "[BOOKS][NEW_RECORD] %s",
                json.dumps(stored_data, ensure_ascii=False, default=str),
            )
        except Exception as log_err:
            logger.warning("[BOOKS] تعذر قراءة السجل بعد الإنشاء: %s", log_err, exc_info=True)
        return book_id
    except Exception as e:
        logger.error(f"[BOOKS] خطأ في إنشاء الكتاب: {e}", exc_info=True)
        return ""


def update_book_record(book_id: str, **fields) -> bool:
    if not firestore_available():
        return False
    try:
        existing = {}
        try:
            existing_doc = db.collection(BOOKS_COLLECTION).document(book_id).get()
            if existing_doc.exists:
                existing = existing_doc.to_dict() or {}
        except Exception as fetch_err:
            logger.warning("[BOOKS] تعذر قراءة الكتاب قبل التحديث %s: %s", book_id, fetch_err)
        data = _ensure_admin_book_defaults(fields, existing=existing, is_creation=False)
        db.collection(BOOKS_COLLECTION).document(book_id).update(data)
        logger.info("[BOOKS] تم تحديث الكتاب %s", book_id)
        return True
    except Exception as e:
        logger.error(f"[BOOKS] خطأ في تحديث الكتاب {book_id}: {e}", exc_info=True)
        return False


def soft_delete_book(book_id: str) -> bool:
    return update_book_record(book_id, is_deleted=True)


def increment_book_download(book_id: str):
    if not firestore_available():
        return
    try:
        db.collection(BOOKS_COLLECTION).document(book_id).update(
            {
                "downloads_count": firestore.Increment(1),
                "updated_at": _book_timestamp_value(),
            }
        )
        logger.info("[BOOKS] زيادة عداد التحميل للكتاب %s", book_id)
    except Exception as e:
        logger.error(f"[BOOKS] خطأ في زيادة عداد التحميل للكتاب {book_id}: {e}")


def _book_matches_query(book: Dict, term: str) -> bool:
    search_texts = [
        book.get("title", ""),
        book.get("author", ""),
        book.get("description", ""),
    ]
    tags = book.get("tags", [])
    search_texts.extend(tags if isinstance(tags, list) else [])
    normalized_term = _normalize_book_text(term)
    for txt in search_texts:
        if normalized_term in _normalize_book_text(str(txt)):
            return True
    return False


def search_books(term: str) -> List[Dict]:
    if not term:
        return []
    if not firestore_available():
        return []
    try:
        books = fetch_books_list(include_inactive=False, include_deleted=False)
        matches = [b for b in books if _book_matches_query(b, term)]
        matches.sort(key=lambda b: b.get("title", ""))
        return matches
    except Exception as e:
        logger.error(f"[BOOKS] خطأ في البحث عن الكتب: {e}", exc_info=True)
        return []


def _fetch_books_by_ids(book_ids: List[str]) -> List[Dict]:
    books: List[Dict] = []
    for bid in book_ids:
        book = get_book_by_id(bid)
        if not book:
            _log_book_skip(bid, "not_found")
            continue
        if book.get("is_deleted"):
            _log_book_skip(bid, "is_deleted")
            continue
        if not book.get("is_active", True):
            _log_book_skip(bid, "is_active_false")
            continue
        books.append(book)
    return books


def _paginate_items(items: List[Dict], page: int, page_size: int):
    total = len(items)
    total_pages = max((total - 1) // page_size + 1, 1) if total else 1
    safe_page = max(0, min(page, total_pages - 1))
    start = safe_page * page_size
    return items[start : start + page_size], safe_page, total_pages


def _book_caption(book: Dict, category_name: str = None) -> str:
    title = str(book.get("title") or "كتاب")
    author = str(book.get("author") or "غير محدد")
    cat = str(category_name or book.get("category_name_snapshot") or "غير مصنف")
    desc = str(book.get("description") or "").strip()
    downloads = str(book.get("downloads_count") or 0)

    lines = [
        f"📖 {title}",
        f"✍️ المؤلف: {author}",
        f"🗂 التصنيف: {cat}",
    ]
    if desc:
        lines.append(f"📝 الوصف:\n{desc}")
    lines.append(f"⬇️ عدد التحميلات: {downloads}")
    return "\n\n".join(lines)


def _book_detail_keyboard(book_id: str, is_saved: bool) -> InlineKeyboardMarkup:
    save_button = InlineKeyboardButton(
        "❌ إزالة من المحفوظات" if is_saved else "⭐ احفظ للقراءة لاحقًا",
        callback_data=f"{BOOKS_CALLBACK_PREFIX}:toggle_save:{book_id}",
    )
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "⬇️ تحميل PDF",
                    callback_data=f"{BOOKS_CALLBACK_PREFIX}:download:{book_id}",
                )
            ],
            [save_button],
            [InlineKeyboardButton("🔙 رجوع", callback_data=BOOKS_BACK_CALLBACK)],
        ]
    )


def _book_list_keyboard(
    items: List[Dict],
    page: int,
    total_pages: int,
    source: str,
    category_id: str = None,
    search_token: str = None,
) -> InlineKeyboardMarkup:
    rows = []
    for book in items:
        title = book.get("title", "كتاب")
        button_text = f"📘 {title}"
        rows.append(
            [
                InlineKeyboardButton(
                    button_text,
                    callback_data=f"{BOOKS_CALLBACK_PREFIX}:book:{book.get('id')}",
                )
            ]
        )
    nav_row = []
    if page > 0:
        nav_row.append(
            InlineKeyboardButton(
                "⬅️ السابق",
                callback_data=f"{BOOKS_CALLBACK_PREFIX}:list:{_encode_route(source, category_id, search_token, page - 1)}",
            )
        )
    if page < total_pages - 1:
        nav_row.append(
            InlineKeyboardButton(
                "التالي ➡️",
                callback_data=f"{BOOKS_CALLBACK_PREFIX}:list:{_encode_route(source, category_id, search_token, page + 1)}",
            )
        )
    if nav_row:
        rows.append(nav_row)
    rows.append([InlineKeyboardButton("↩️ رجوع للقائمة", callback_data=BOOKS_HOME_BACK)])
    return InlineKeyboardMarkup(rows)


def _encode_route(source: str, category_id: str, search_token: str, page: int) -> str:
    parts = [source, category_id or "none", str(page)]
    if source == "search" and search_token:
        parts.append(search_token)
    return ":".join(parts)


def _parse_route(route: str) -> Dict:
    parts = (route or "").split(":")
    if len(parts) < 3:
        return {"source": "home", "category_id": None, "page": 0, "search_token": None}
    source, category_id, page_str = parts[0], parts[1], parts[2]
    search_token = parts[3] if len(parts) > 3 else None
    try:
        page = int(page_str)
    except Exception:
        page = 0
    return {
        "source": source,
        "category_id": None if category_id == "none" else category_id,
        "page": page,
        "search_token": search_token,
    }


def _render_books_route(update: Update, context: CallbackContext, route: str, from_callback: bool = False):
    route_info = _parse_route(route or BOOKS_DEFAULT_ROUTE)
    source = route_info.get("source")
    page = route_info.get("page", 0)
    if source == "cat":
        show_books_by_category(update, context, route_info.get("category_id"), page=page, from_callback=from_callback)
    elif source == "latest":
        show_latest_books(update, context, page=page, from_callback=from_callback)
    elif source == "saved":
        show_saved_books(update, context, page=page, from_callback=from_callback)
    elif source == "search":
        token = route_info.get("search_token")
        if token:
            _render_search_results(update, context, token, page=page, from_callback=from_callback)
    else:
        open_books_home(update, context, from_callback=from_callback)


def _ensure_saved_books_defaults(record: Dict):
    if "saved_books" not in record:
        record["saved_books"] = []
    if "saved_books_updated_at" not in record:
        record["saved_books_updated_at"] = None


def add_book_to_saved(user_id: int, book_id: str) -> bool:
    record = get_user_record_by_id(user_id) or {}
    _ensure_saved_books_defaults(record)
    if book_id in record.get("saved_books", []):
        return True
    saved = record.get("saved_books", [])
    saved.append(book_id)
    update_user_record(
        user_id,
        saved_books=saved,
        saved_books_updated_at=datetime.now(timezone.utc).isoformat(),
    )
    return True


def books_home_keyboard() -> InlineKeyboardMarkup:
    categories = fetch_book_categories(include_inactive=False)
    rows = []
    for cat in categories:
        rows.append(
            [
                InlineKeyboardButton(
                    f"🗂 {cat.get('name', 'تصنيف')}",
                    callback_data=f"{BOOKS_CALLBACK_PREFIX}:cat:{cat.get('id')}:0",
                )
            ]
        )
    search_button = InlineKeyboardButton("🔎 بحث داخل المكتبة", callback_data=BOOKS_SEARCH_PROMPT_CALLBACK)
    rows.append([InlineKeyboardButton("🆕 آخر الإضافات", callback_data=BOOKS_LATEST_CALLBACK)])
    rows.append([search_button])
    rows.append([InlineKeyboardButton("📌 محفوظاتي", callback_data=BOOKS_SAVED_CALLBACK)])
    rows.append([InlineKeyboardButton("🔙 رجوع", callback_data=BOOKS_EXIT_CALLBACK)])
    return InlineKeyboardMarkup(rows)


def open_books_home(update: Update, context: CallbackContext, from_callback: bool = False):
    if not firestore_available():
        if from_callback and update.callback_query:
            update.callback_query.answer()
            update.callback_query.message.reply_text(
                "خدمة مكتبة الكتب غير متاحة حالياً. حاول لاحقاً.",
                reply_markup=user_main_keyboard(update.effective_user.id),
            )
            return
        update.message.reply_text(
            "خدمة مكتبة الكتب غير متاحة حالياً. حاول لاحقاً.",
            reply_markup=user_main_keyboard(update.effective_user.id),
        )
        return
    categories = fetch_book_categories()
    text = "مكتبة طالب العلم 📘\nاختر تصنيفًا أو خيارًا من القائمة:"
    kb = books_home_keyboard()
    if from_callback and update.callback_query:
        try:
            update.callback_query.edit_message_text(text, reply_markup=kb)
        except Exception:
            update.callback_query.message.reply_text(text, reply_markup=kb)
        return
    update.message.reply_text(text, reply_markup=kb)


def _get_books_for_search_token(token: str) -> Tuple[List[Dict], str]:
    entry = BOOK_SEARCH_CACHE.get(token)
    if not entry:
        return [], ""
    if not entry.get("book_ids"):
        books = search_books(entry.get("query", ""))
        entry["book_ids"] = [b.get("id") for b in books if b.get("id")]
    books = _fetch_books_by_ids(entry.get("book_ids", []))
    return books, entry.get("query", "")


def _send_books_list_message(
    update: Update,
    context: CallbackContext,
    books: List[Dict],
    title: str,
    source: str,
    category_id: str = None,
    search_token: str = None,
    page: int = 0,
    empty_message: str = None,
    from_callback: bool = False,
):
    page_items, safe_page, total_pages = _paginate_items(books, page, BOOKS_PAGE_SIZE)
    try:
        context.user_data["books_last_route"] = _encode_route(source, category_id, search_token, safe_page)
    except Exception:
        pass
    if not books:
        message_text = empty_message or "لا توجد كتب متاحة هنا بعد."
        if from_callback and update.callback_query:
            update.callback_query.edit_message_text(message_text, reply_markup=books_home_keyboard())
        else:
            update.message.reply_text(message_text, reply_markup=books_home_keyboard())
        return

    lines = [title, f"الصفحة {safe_page + 1} من {total_pages}", ""]
    start_index = safe_page * BOOKS_PAGE_SIZE
    for idx, book in enumerate(page_items, start=1 + start_index):
        lines.append(f"{idx}. {book.get('title', 'كتاب')} — {book.get('author', 'مؤلف غير معروف')}")
    keyboard = _book_list_keyboard(page_items, safe_page, total_pages, source, category_id, search_token)

    text = "\n".join(lines)
    if from_callback and update.callback_query:
        try:
            update.callback_query.edit_message_text(text, reply_markup=keyboard)
        except Exception:
            update.callback_query.message.reply_text(text, reply_markup=keyboard)
    else:
        update.message.reply_text(text, reply_markup=keyboard)


def show_books_by_category(update: Update, context: CallbackContext, category_id: str, page: int = 0, from_callback: bool = False):
    category = get_book_category(category_id)
    if not category or not category.get("is_active", True):
        msg = update.callback_query.message if from_callback and update.callback_query else update.message
        if msg:
            msg.reply_text("هذا التصنيف غير متاح حالياً.", reply_markup=books_home_keyboard())
        return
    books_list = fetch_books_list(include_inactive=False, include_deleted=False)
    requested = _normalize_category_id(category_id)
    logger.info("[BOOKS][CAT] requested=%s", requested)
    logger.info(
        "[BOOKS][CAT] sample=%s",
        [
            {"id": b.get("id"), "cat": repr(b.get("category_id")), "norm": _normalize_category_id(b.get("category_id"))}
            for b in books_list[:20]
        ],
    )
    books = [book for book in books_list if _normalize_category_id(book.get("category_id")) == requested]
    logger.info(
        "[BOOKS][LIST][DISPLAY] category=%s page=%s total=%s",
        category_id,
        page,
        len(books),
    )
    title = f"🗂 كتب تصنيف «{category.get('name', 'غير مسمى')}»"
    _send_books_list_message(
        update,
        context,
        books,
        title,
        source="cat",
        category_id=category_id,
        page=page,
        empty_message="لا توجد كتب في هذا التصنيف حتى الآن.",
        from_callback=from_callback,
    )


def show_latest_books(update: Update, context: CallbackContext, page: int = 0, from_callback: bool = False):
    books = fetch_latest_books(limit=BOOK_LATEST_LIMIT)
    logger.info("[BOOKS][LATEST][DISPLAY] page=%s total=%s", page, len(books))
    _send_books_list_message(
        update,
        context,
        books,
        "🆕 آخر الإضافات",
        source="latest",
        page=page,
        empty_message="لا توجد إضافات حديثة حتى الآن.",
        from_callback=from_callback,
    )


def show_saved_books(update: Update, context: CallbackContext, page: int = 0, from_callback: bool = False):
    record = get_user_record(update.effective_user)
    _ensure_saved_books_defaults(record)
    saved_ids = record.get("saved_books", [])
    books = _fetch_books_by_ids(saved_ids)
    _send_books_list_message(
        update,
        context,
        books,
        "📌 كتبك المحفوظة",
        source="saved",
        page=page,
        empty_message="لا توجد كتب محفوظة حالياً.",
        from_callback=from_callback,
    )


def _render_search_results(update: Update, context: CallbackContext, token: str, page: int = 0, from_callback: bool = False):
    books, query_text = _get_books_for_search_token(token)
    _send_books_list_message(
        update,
        context,
        books,
        f"نتائج البحث عن: {query_text}",
        source="search",
        search_token=token,
        page=page,
        empty_message="لا توجد نتائج مطابقة.",
        from_callback=from_callback,
    )


def handle_book_search_input(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    text = (update.message.text or "").strip()

    WAITING_BOOK_SEARCH.discard(user_id)

    update_user_record(
        user_id,
        book_search_waiting=False,
        book_search_waiting_at=None,
    )

    logger.info("[BOOKS][SEARCH_INPUT] user=%s text=%r", user_id, text)
    if not text:
        update.message.reply_text(
            "الرجاء كتابة كلمة بحث صالحة.",
            reply_markup=books_home_keyboard(),
        )
        return
    normalized_query = _normalize_book_text(text)
    books = fetch_books_list(include_inactive=False, include_deleted=False)
    results = []
    for book in books:
        tags = book.get("tags") or book.get("keywords") or []
        if isinstance(tags, str):
            tags = [tags]
        elif not isinstance(tags, list):
            tags = []
        search_fields = [
            _normalize_book_text(book.get("title", "")),
            _normalize_book_text(book.get("author", "")),
            _normalize_book_text(book.get("description", "")),
            _normalize_book_text(book.get("category_name_snapshot", "")),
        ]
        search_fields.extend([_normalize_book_text(str(t)) for t in tags])
        if any(normalized_query in field for field in search_fields):
            results.append(book)
    token = uuid4().hex[:8]
    BOOK_SEARCH_CACHE[token] = {"query": text, "book_ids": [b.get("id") for b in results if b.get("id")]}
    _render_search_results(update, context, token, page=0, from_callback=False)


def _mark_admin_books_mode(context: CallbackContext, active: bool):
    """تخزين حالة تواجد الأدمن داخل إدارة الكتب لمنع تداخل الراوترات."""

    if active:
        context.user_data["books_admin_mode"] = True
        # تأكد من إزالة أي لوحة رد سابقة مرة واحدة فقط في جلسة الإدارة
        context.user_data.pop("admin_books_reply_kb_removed", None)
    else:
        context.user_data.pop("books_admin_mode", None)
        context.user_data.pop("admin_books_reply_kb_removed", None)


def books_search_text_router(update: Update, context: CallbackContext):
    if not update.message or not update.message.text:
        return

    user_id = update.effective_user.id
    text = (update.message.text or "").strip()

    # اقرأ الحالة من Firestore مباشرة (بدون كاش)
    rec = get_user_record_by_id(user_id) or {}

    # تجاهل نصوص الأدمن أثناء وجوده في وضع إدارة الكتب حتى لا تُعامل كبحث عام
    if _ensure_is_admin_or_supervisor(user_id) and context.user_data.get("books_admin_mode"):
        return

    if rec.get("book_search_waiting", False):
        logger.info("[BOOKS][ROUTER] user=%s text=%r", user_id, text)
        handle_book_search_input(update, context)
        raise DispatcherHandlerStop()


def prompt_book_search(update: Update, context: CallbackContext):
    user_id = update.effective_user.id

    WAITING_BOOK_SEARCH.add(user_id)
    update_user_record(
        user_id,
        book_search_waiting=True,
        book_search_waiting_at=datetime.now(timezone.utc).isoformat(),
    )

    update.callback_query.answer()
    update.callback_query.message.reply_text(
        "أرسل الآن كلمة البحث.\nسأبحث في العنوان، المؤلف، الوصف والكلمات المفتاحية.",
        reply_markup=CANCEL_KB,
    )
    logger.info("[BOOKS][SEARCH_PROMPT] user=%s firestore_waiting=True", user_id)


MAX_CAPTION = 1000


def _send_book_detail(update: Update, context: CallbackContext, book_id: str, route_str: str, from_callback: bool = False):
    book = get_book_by_id(book_id)
    if not book or book.get("is_deleted") or not book.get("is_active", True):
        _log_book_skip(book_id, "detail_not_available")
        msg = update.callback_query.message if from_callback and update.callback_query else update.message
        if msg:
            msg.reply_text("هذا الكتاب غير متاح حالياً.", reply_markup=books_home_keyboard())
        return

    category_name = None
    if book.get("category_id"):
        category = get_book_category(book.get("category_id"))
        category_name = category.get("name") if category else book.get("category_name_snapshot")
    record = get_user_record_by_id(update.effective_user.id) or {}
    _ensure_saved_books_defaults(record)
    is_saved = book_id in record.get("saved_books", [])
    context.user_data["books_last_route"] = route_str or BOOKS_DEFAULT_ROUTE
    caption = _book_caption(book, category_name=category_name)
    keyboard = _book_detail_keyboard(book_id, is_saved)

    if from_callback and update.callback_query:
        update.callback_query.answer()
    chat_id = update.effective_chat.id if update.effective_chat else update.callback_query.message.chat_id
    cover_id = book.get("cover_file_id")
    if cover_id:
        try:
            context.bot.send_photo(
                chat_id=chat_id,
                photo=cover_id,
                caption=caption[:MAX_CAPTION],
                reply_markup=keyboard,
            )
            return
        except Exception as e:
            logger.warning(
                "[BOOKS] send_photo failed book=%s err=%s",
                book_id,
                e,
                exc_info=True,
            )
    try:
        context.bot.send_message(
            chat_id=chat_id,
            text=caption[:3900],
            reply_markup=keyboard,
        )
    except Exception as e:
        logger.error("[BOOKS] send_message failed err=%r", e, exc_info=True)
        if update.callback_query:
            update.callback_query.message.reply_text("تعذر عرض الكتاب حالياً.", reply_markup=books_home_keyboard())


def handle_book_download(update: Update, context: CallbackContext, book_id: str):
    query = update.callback_query
    book = get_book_by_id(book_id)
    if not book or book.get("is_deleted") or not book.get("is_active", True):
        _log_book_skip(book_id, "download_not_available")
        if query:
            query.answer("الكتاب غير متاح.", show_alert=True)
        return
    file_id = book.get("pdf_file_id")
    if not file_id:
        if query:
            query.answer("ملف الكتاب غير متوفر.", show_alert=True)
        return
    try:
        context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=file_id,
            filename=book.get("pdf_filename") or None,
            caption=book.get("title") or "",
        )
        increment_book_download(book_id)
        if query:
            query.answer("تم إرسال الكتاب ✅")
            try:
                query.edit_message_reply_markup(
                    reply_markup=_book_detail_keyboard(
                        book_id,
                        book_id in (get_user_record_by_id(query.from_user.id) or {}).get("saved_books", []),
                    )
                )
            except Exception:
                pass
    except Exception as e:
        logger.error(f"[BOOKS] خطأ في إرسال الكتاب: {e}")
        if query:
            query.answer("تعذر إرسال الكتاب الآن.", show_alert=True)


def handle_toggle_saved(update: Update, context: CallbackContext, book_id: str):
    query = update.callback_query
    user_id = query.from_user.id
    record = get_user_record_by_id(user_id) or {}
    _ensure_saved_books_defaults(record)
    is_saved = book_id in record.get("saved_books", [])
    if is_saved:
        remove_book_from_saved(user_id, book_id)
        query.answer("تمت إزالته من محفوظاتك.", show_alert=False)
    else:
        add_book_to_saved(user_id, book_id)
        query.answer("تم حفظ الكتاب للقراءة لاحقًا.", show_alert=False)
    try:
        updated_saved = not is_saved
        query.edit_message_reply_markup(reply_markup=_book_detail_keyboard(book_id, updated_saved))
    except Exception:
        pass


# =================== إدارة المكتبة (أدمن/مشرفة) ===================


def _ensure_is_admin_or_supervisor(user_id: int) -> bool:
    return is_admin(user_id) or is_supervisor(user_id)


def _run_books_backfill_for_admin(update: Update, context: CallbackContext):
    user = update.effective_user
    if not user or not _ensure_is_admin_or_supervisor(user.id):
        if update.message:
            update.message.reply_text("هذا الأمر مخصص للأدمن فقط.")
        return

    if not firestore_available():
        update.message.reply_text("Firestore غير متاح حالياً. تعذر تشغيل التهيئة.")
        return

    progress_msg = update.message.reply_text("🔄 جارٍ تهيئة بيانات الكتب...")
    result = run_books_backfill()
    report = _format_books_backfill_report(result)

    try:
        progress_msg.edit_text(report)
    except Exception:
        update.message.reply_text(report)


def open_books_admin_menu(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    if not _ensure_is_admin_or_supervisor(user_id):
        return
    _mark_admin_books_mode(context, True)
    update.message.reply_text(
        "📚 إدارة مكتبة الكتب\nاختر العملية المطلوبة:",
        reply_markup=BOOKS_ADMIN_MENU_KB,
    )


def _admin_categories_keyboard(categories: List[Dict]) -> InlineKeyboardMarkup:
    rows = []
    for cat in categories:
        status = "✅" if cat.get("is_active", True) else "⛔️"
        rows.append(
            [
                InlineKeyboardButton(
                    f"{status} {cat.get('name', 'تصنيف')}",
                    callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_category:{cat.get('id')}",
                )
            ]
        )
    rows.append([InlineKeyboardButton("➕ إضافة تصنيف", callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_category_add")])
    rows.append([InlineKeyboardButton("🔙 رجوع", callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_back")])
    return InlineKeyboardMarkup(rows)


def open_book_categories_admin(update_or_query, context: CallbackContext, notice: str = None, use_callback: bool = False):
    user_obj = getattr(update_or_query, "effective_user", None) or getattr(update_or_query, "from_user", None) or getattr(getattr(update_or_query, "callback_query", None), "from_user", None)
    user_id = getattr(user_obj, "id", None)
    if user_id and not _ensure_is_admin_or_supervisor(user_id):
        return
    if not firestore_available():
        message_obj = getattr(update_or_query, "message", None) or getattr(getattr(update_or_query, "callback_query", None), "message", None)
        if message_obj:
            message_obj.reply_text("قاعدة البيانات غير متاحة حالياً.", reply_markup=BOOKS_ADMIN_MENU_KB)
        return
    categories = fetch_book_categories(include_inactive=True)
    text_lines = ["🗂 إدارة التصنيفات"]
    if notice:
        text_lines.append(notice)
    if not categories:
        text_lines.append("لا توجد تصنيفات بعد. أضف تصنيفًا جديدًا للبدء.")
    kb = _admin_categories_keyboard(categories)
    text = "\n".join(text_lines)
    query = getattr(update_or_query, "callback_query", None)
    message_obj = getattr(update_or_query, "message", None) or getattr(query, "message", None)
    if use_callback and query:
        try:
            query.edit_message_text(text, reply_markup=kb)
            return
        except Exception:
            pass
    if message_obj:
        message_obj.reply_text(text, reply_markup=kb)


def start_add_book_category(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    if not _ensure_is_admin_or_supervisor(user_id):
        return
    BOOK_CATEGORY_EDIT_CONTEXT[user_id] = {"mode": "create"}
    WAITING_BOOK_CATEGORY_NAME.add(user_id)
    update.message.reply_text(
        "أرسل اسم التصنيف الجديد:",
        reply_markup=CANCEL_KB,
    )


def _start_category_rename(query: Update.callback_query, category_id: str):
    user_id = query.from_user.id
    BOOK_CATEGORY_EDIT_CONTEXT[user_id] = {"mode": "rename", "category_id": category_id}
    WAITING_BOOK_CATEGORY_NAME.add(user_id)
    query.answer()
    query.message.reply_text("أرسل الاسم الجديد للتصنيف:", reply_markup=CANCEL_KB)


def _start_category_order_edit(query: Update.callback_query, category_id: str):
    user_id = query.from_user.id
    BOOK_CATEGORY_EDIT_CONTEXT[user_id] = {"mode": "order", "category_id": category_id}
    WAITING_BOOK_CATEGORY_ORDER.add(user_id)
    query.answer()
    query.message.reply_text(
        "أرسل رقم الترتيب (استخدم الأرقام فقط).",
        reply_markup=CANCEL_KB,
    )


def _category_options_keyboard(category_id: str, is_active: bool) -> InlineKeyboardMarkup:
    toggle_text = "👁️ إخفاء" if is_active else "✅ إظهار"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✏️ تعديل الاسم", callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_category_rename:{category_id}")],
            [InlineKeyboardButton("🔢 تعديل الترتيب", callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_category_order:{category_id}")],
            [InlineKeyboardButton(toggle_text, callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_category_toggle:{category_id}")],
            [InlineKeyboardButton("🗑 حذف نهائي", callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_category_delete:{category_id}")],
            [InlineKeyboardButton("🔙 رجوع", callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_categories")],
        ]
    )


def _show_category_options(query: Update.callback_query, category_id: str):
    cat = get_book_category(category_id)
    if not cat:
        query.answer("التصنيف غير موجود.", show_alert=True)
        return
    text = (
        f"التصنيف: {cat.get('name', 'غير مسمى')}\n"
        f"الحالة: {'مفعل' if cat.get('is_active', True) else 'مخفي'}\n"
        f"الترتيب: {cat.get('order', 0)}"
    )
    kb = _category_options_keyboard(category_id, cat.get("is_active", True))
    try:
        query.edit_message_text(text, reply_markup=kb)
    except Exception:
        query.message.reply_text(text, reply_markup=kb)


def _handle_category_toggle(query: Update.callback_query, category_id: str):
    cat = get_book_category(category_id)
    if not cat:
        query.answer("التصنيف غير موجود.", show_alert=True)
        return
    new_state = not cat.get("is_active", True)
    update_book_category(category_id, is_active=new_state)
    query.answer("تم تحديث حالة التصنيف.")
    _show_category_options(query, category_id)


def _handle_category_delete(update: Update, context: CallbackContext, query: Update.callback_query, category_id: str):
    if category_has_books(category_id):
        query.answer("لا يمكن حذف تصنيف يحتوي على كتب. أخفِه بدلاً من ذلك.", show_alert=True)
        return
    if delete_book_category(category_id):
        query.answer("تم حذف التصنيف.", show_alert=True)
        open_book_categories_admin(update, context, use_callback=True)
    else:
        query.answer("تعذر حذف التصنيف.", show_alert=True)


def _reset_book_creation(user_id: int):
    WAITING_BOOK_ADD_CATEGORY.discard(user_id)
    WAITING_BOOK_ADD_TITLE.discard(user_id)
    WAITING_BOOK_ADD_AUTHOR.discard(user_id)
    WAITING_BOOK_ADD_DESCRIPTION.discard(user_id)
    WAITING_BOOK_ADD_TAGS.discard(user_id)
    WAITING_BOOK_ADD_COVER.discard(user_id)
    WAITING_BOOK_ADD_PDF.discard(user_id)
    BOOK_CREATION_CONTEXT.pop(user_id, None)


def start_add_book_flow(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    if not _ensure_is_admin_or_supervisor(user_id):
        return
    if not firestore_available():
        update.message.reply_text("قاعدة البيانات غير متاحة حالياً.", reply_markup=BOOKS_ADMIN_MENU_KB)
        return
    categories = fetch_book_categories()
    if not categories:
        update.message.reply_text("لا توجد تصنيفات نشطة. أضف تصنيفًا أولاً.", reply_markup=BOOKS_ADMIN_MENU_KB)
        return
    BOOK_CREATION_CONTEXT[user_id] = {"mode": "create"}
    WAITING_BOOK_ADD_CATEGORY.add(user_id)
    buttons = [
        [
            InlineKeyboardButton(
                cat.get("name", "تصنيف"),
                callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_select_category:{cat.get('id')}",
            )
        ]
        for cat in categories
    ]
    buttons.append([InlineKeyboardButton("إلغاء", callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_cancel_creation")])
    update.message.reply_text(
        "اختر التصنيف للكتاب الجديد:",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


def _finalize_book_creation(update: Update, context: CallbackContext, ctx: Dict):
    user_id = update.effective_user.id
    category_lookup = _build_category_lookup(include_inactive=True)
    resolved_category_id = _resolve_category_id(
        category_lookup,
        category_id=ctx.get("category_id"),
        category_name=ctx.get("category_name_snapshot"),
    )
    if not resolved_category_id:
        update.message.reply_text("تعذر تحديد التصنيف المختار. يرجى إعادة المحاولة.", reply_markup=BOOKS_ADMIN_MENU_KB)
        _reset_book_creation(user_id)
        return
    if resolved_category_id != (ctx.get("category_id") or "").strip():
        logger.info("[BOOKS][CREATE] normalized_category_id old=%s new=%s", ctx.get("category_id"), resolved_category_id)
    ctx["category_id"] = resolved_category_id
    required_fields = ["category_id", "title", "author", "pdf_file_id"]
    if any(not ctx.get(f) for f in required_fields):
        update.message.reply_text("البيانات غير مكتملة. يرجى إعادة المحاولة.", reply_markup=BOOKS_ADMIN_MENU_KB)
        _reset_book_creation(user_id)
        return
    category_snapshot = None
    if ctx.get("category_id"):
        cat = get_book_category(ctx.get("category_id"))
        if cat:
            category_snapshot = cat.get("name")
        else:
            category_snapshot = category_lookup.get("id_to_name", {}).get(ctx.get("category_id")) or ctx.get("category_name_snapshot")
    payload = {
        "title": ctx.get("title"),
        "author": ctx.get("author"),
        "category_id": ctx.get("category_id"),
        "category_name_snapshot": category_snapshot,
        "description": ctx.get("description") or "",
        "tags": ctx.get("tags") or [],
        "cover_file_id": ctx.get("cover_file_id"),
        "pdf_file_id": ctx.get("pdf_file_id"),
        "pdf_filename": ctx.get("pdf_filename"),
        "created_by": user_id,
    }
    book_id = create_book_record(payload)
    _reset_book_creation(user_id)
    if book_id:
        update.message.reply_text(f"تم حفظ الكتاب بنجاح (ID: {book_id}).", reply_markup=BOOKS_ADMIN_MENU_KB)
    else:
        update.message.reply_text("تعذر حفظ الكتاب حالياً.", reply_markup=BOOKS_ADMIN_MENU_KB)


def _get_admin_route(context: CallbackContext) -> str:
    return context.user_data.get("admin_last_route", "admin_all:none:0")


def _set_admin_route(context: CallbackContext, route: str):
    try:
        context.user_data["admin_last_route"] = route
    except Exception:
        pass


def _admin_books_keyboard(
    items: List[Dict],
    page: int,
    total_pages: int,
) -> InlineKeyboardMarkup:
    rows = []
    for book in items:
        book_id = book.get("id")
        title = book.get("title", "كتاب")
        rows.append(
            [
                InlineKeyboardButton(
                    f"✏️ {title}",
                    callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_book:{book_id}",
                )
            ]
        )
    nav_row = []
    if page > 0:
        nav_row.append(
            InlineKeyboardButton("⬅️ السابق", callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_list_prev")
        )
    if page < total_pages - 1:
        nav_row.append(
            InlineKeyboardButton("التالي ➡️", callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_list_next")
        )
    if nav_row:
        rows.append(nav_row)
    rows.append([InlineKeyboardButton("🔍 بحث إداري", callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_search_prompt")])
    rows.append([InlineKeyboardButton("🔙 رجوع", callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_back")])
    return InlineKeyboardMarkup(rows)


def _send_admin_books_list(
    update_or_query,
    context: CallbackContext,
    books: List[Dict],
    title: str,
    source: str,
    category_id: str = None,
    search_token: str = None,
    page: int = 0,
    from_callback: bool = False,
):
    page_items, safe_page, total_pages = _paginate_items(books, page, BOOKS_PAGE_SIZE)
    start_index = safe_page * BOOKS_PAGE_SIZE
    text_lines = [title, f"الصفحة {safe_page + 1} من {total_pages}", ""]
    route = _encode_route(source, category_id, search_token, safe_page)
    _set_admin_route(context, route)
    if not books:
        text_lines.append("لا توجد كتب متاحة.")
    else:
        for idx, book in enumerate(page_items, start=1 + start_index):
            if book.get("is_deleted"):
                status_label = "🗑 محذوف"
            else:
                status_label = "✅" if book.get("is_active", True) else "⛔️ مخفي"
            text_lines.append(f"{idx}. {book.get('title', 'كتاب')} — {book.get('author', 'مؤلف')} ({status_label})")
    kb = _admin_books_keyboard(page_items if books else [], safe_page, total_pages)
    # إبقاء وضع الإدارة مفعّل فقط لمنع تداخل الراوترات النصية، بدون التأثير على الـ callbacks
    context.user_data["books_admin_mode"] = True
    text = "\n".join(text_lines)
    query = getattr(update_or_query, "callback_query", None)
    message_obj = getattr(update_or_query, "message", None) or getattr(query, "message", None)
    if message_obj and not from_callback:
        # تأكد من إزالة أي ReplyKeyboard قديم قبل عرض لوحة Inline الحالية مرة واحدة فقط
        kb_removed = context.user_data.get("admin_books_reply_kb_removed")
        if not kb_removed:
            try:
                message_obj.reply_text("\u200b", reply_markup=ReplyKeyboardRemove())
                context.user_data["admin_books_reply_kb_removed"] = True
            except Exception:
                pass
    if from_callback and query:
        try:
            query.edit_message_text(text, reply_markup=kb)
            return
        except Exception:
            pass
    if message_obj:
        message_obj.reply_text(text, reply_markup=kb)


def open_books_admin_list(update_or_query, context: CallbackContext, category_id: str = None, page: int = 0, search_token: str = None, from_callback: bool = False):
    user_obj = getattr(update_or_query, "effective_user", None) or getattr(update_or_query, "from_user", None) or getattr(getattr(update_or_query, "callback_query", None), "from_user", None)
    user_id = getattr(user_obj, "id", None)
    if user_id and not _ensure_is_admin_or_supervisor(user_id):
        return
    _mark_admin_books_mode(context, True)
    if not firestore_available():
        target = getattr(update_or_query, "message", None) or getattr(getattr(update_or_query, "callback_query", None), "message", None)
        if target:
            target.reply_text("قاعدة البيانات غير متاحة حالياً.", reply_markup=BOOKS_ADMIN_MENU_KB)
        return
    if search_token:
        books, query_text = _get_books_for_search_token(search_token)
        title = f"نتائج البحث الإداري: {query_text}"
        _send_admin_books_list(update_or_query, context, books, title, source="admin_search", search_token=search_token, page=page, from_callback=from_callback)
        return
    books = fetch_books_list(category_id=category_id, include_inactive=True, include_deleted=True)
    title = "📋 إدارة الكتب"
    if category_id:
        cat = get_book_category(category_id)
        if cat:
            title += f" — {cat.get('name', '')}"
    _send_admin_books_list(update_or_query, context, books, title, source="admin_cat" if category_id else "admin_all", category_id=category_id, page=page, from_callback=from_callback)


def _book_admin_detail_keyboard(book_id: str, is_active: bool, is_deleted: bool) -> InlineKeyboardMarkup:
    toggle_text = "👁️ إخفاء" if is_active else "✅ تفعيل"
    delete_text = "🗑 حذف منطقي" if not is_deleted else "♻️ استرجاع"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✏️ تعديل العنوان", callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_book_field:title:{book_id}")],
            [InlineKeyboardButton("✍️ تعديل المؤلف", callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_book_field:author:{book_id}")],
            [InlineKeyboardButton("📝 تعديل الوصف", callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_book_field:description:{book_id}")],
            [InlineKeyboardButton("🏷️ تعديل الكلمات المفتاحية", callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_book_field:tags:{book_id}")],
            [InlineKeyboardButton("🗂 تغيير التصنيف", callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_book_field:category:{book_id}")],
            [InlineKeyboardButton("🖼 تغيير الغلاف", callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_book_field:cover:{book_id}")],
            [InlineKeyboardButton("📄 تغيير ملف PDF", callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_book_field:pdf:{book_id}")],
            [InlineKeyboardButton(toggle_text, callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_book_toggle:{book_id}")],
            [InlineKeyboardButton(delete_text, callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_book_delete:{book_id}")],
            [InlineKeyboardButton("🔙 رجوع للقائمة", callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_list_back")],
        ]
    )


def _send_admin_book_detail(update: Update, context: CallbackContext, book_id: str, route: str = None):
    book = get_book_by_id(book_id)
    if not book:
        q = getattr(update, "callback_query", None)
        if q:
            q.answer("الكتاب غير موجود.", show_alert=True)
        else:
            msg = getattr(update, "message", None)
            if msg:
                msg.reply_text("الكتاب غير موجود.")
        return
    category_name = None
    if book.get("category_id"):
        cat = get_book_category(book.get("category_id"))
        category_name = cat.get("name") if cat else book.get("category_name_snapshot")
    caption = _book_caption(book, category_name=category_name)
    active_route = route or _get_admin_route(context)
    _set_admin_route(context, active_route)
    kb = _book_admin_detail_keyboard(book_id, book.get("is_active", True), book.get("is_deleted", False))
    q = getattr(update, "callback_query", None)

    # 1) لو جاء من Inline button
    if q:
        try:
            q.edit_message_text(caption, reply_markup=kb, parse_mode="HTML")
        except Exception:
            logger.exception(
                "[BOOKS][ADMIN_BOOK] Failed to edit message for book_id=%s route=%s",
                book_id,
                route,
            )
            # fallback
            try:
                q.message.reply_text(caption, reply_markup=kb, parse_mode="HTML")
            except Exception:
                try:
                    # جرب مرة بدون parse_mode لو كان التنسيق هو المشكلة
                    q.message.reply_text(caption, reply_markup=kb)
                except Exception:
                    logger.exception(
                        "[BOOKS][ADMIN_BOOK] Fallback send failed for book_id=%s route=%s",
                        book_id,
                        route,
                    )
        return

    # 2) لو جاء من ReplyKeyboard / رسالة نصية
    msg = getattr(update, "message", None)
    if msg:
        try:
            msg.reply_text(caption, reply_markup=kb, parse_mode="HTML")
        except Exception:
            try:
                msg.reply_text(caption, reply_markup=kb)
            except Exception:
                logger.exception(
                    "[BOOKS][ADMIN_BOOK] Failed to send detail via message for book_id=%s route=%s",
                    book_id,
                    route,
                )
        return


def _admin_set_book_category(update: Update, context: CallbackContext, book_id: str, category_id: str, route: str = None):
    cat = get_book_category(category_id)
    if not cat or not cat.get("is_active", True):
        update.callback_query.answer("التصنيف غير متاح.", show_alert=True)
        return
    update_book_record(book_id, category_id=category_id, category_name_snapshot=cat.get("name"))
    update.callback_query.answer("تم تحديث التصنيف.")
    _send_admin_book_detail(update, context, book_id, route or _get_admin_route(context))


def _start_book_field_edit(
    query: Update.callback_query, context: CallbackContext, field: str, book_id: str, route: str = None
):
    user_id = query.from_user.id
    BOOK_EDIT_CONTEXT[user_id] = {
        "book_id": book_id,
        "field": field,
        "route": route or _get_admin_route(context),
    }
    if field in {"title", "author", "description", "tags"}:
        WAITING_BOOK_EDIT_FIELD.add(user_id)
        prompt = {
            "title": "أرسل العنوان الجديد:",
            "author": "أرسل اسم المؤلف الجديد:",
            "description": "أرسل الوصف الجديد (أو اكتب تخطي لمسح الوصف):",
            "tags": "أرسل الكلمات المفتاحية مفصولة بفواصل:",
        }.get(field, "أرسل القيمة الجديدة:")
        query.answer()
        query.message.reply_text(prompt, reply_markup=CANCEL_KB)
    elif field == "category":
        query.answer()
        categories = fetch_book_categories()
        if not categories:
            query.message.reply_text("لا توجد تصنيفات متاحة.", reply_markup=BOOKS_ADMIN_MENU_KB)
            return
        buttons = [
            [
                InlineKeyboardButton(
                    cat.get("name", "تصنيف"),
                    callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_book_category:{book_id}:{cat.get('id')}",
                )
            ]
            for cat in categories
        ]
        buttons.append([InlineKeyboardButton("🔙 رجوع", callback_data=f"{BOOKS_CALLBACK_PREFIX}:admin_book:{book_id}")])
        query.message.reply_text("اختر التصنيف الجديد:", reply_markup=InlineKeyboardMarkup(buttons))
    elif field == "cover":
        query.answer()
        WAITING_BOOK_EDIT_COVER.add(user_id)
        query.message.reply_text("أرسل صورة الغلاف الجديدة:", reply_markup=CANCEL_KB)
    elif field == "pdf":
        query.answer()
        WAITING_BOOK_EDIT_PDF.add(user_id)
        query.message.reply_text("أرسل ملف الـ PDF الجديد:", reply_markup=CANCEL_KB)


def _handle_admin_book_toggle(update: Update, context: CallbackContext, book_id: str, route: str):
    book = get_book_by_id(book_id)
    if not book:
        update.callback_query.answer("الكتاب غير موجود.", show_alert=True)
        return
    new_state = not book.get("is_active", True)
    update_book_record(book_id, is_active=new_state)
    update.callback_query.answer("تم تحديث حالة الكتاب.")
    _send_admin_book_detail(update, context, book_id, route)


def _handle_admin_book_delete(update: Update, context: CallbackContext, book_id: str, route: str):
    book = get_book_by_id(book_id)
    if not book:
        update.callback_query.answer("الكتاب غير موجود.", show_alert=True)
        return
    new_deleted = not book.get("is_deleted", False)
    update_book_record(book_id, is_deleted=new_deleted)
    update.callback_query.answer("تم تحديث حالة الحذف.")
    _send_admin_book_detail(update, context, book_id, route)


def start_admin_book_search_prompt(query: Update.callback_query):
    user_id = query.from_user.id
    WAITING_BOOK_ADMIN_SEARCH.add(user_id)
    query.answer()
    query.message.reply_text("أرسل الآن عبارة البحث للبحث الإداري:", reply_markup=CANCEL_KB)


def prompt_admin_books_search_text(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    WAITING_BOOK_ADMIN_SEARCH.add(user_id)
    update.message.reply_text("أرسل الآن عبارة البحث للبحث الإداري:", reply_markup=CANCEL_KB)


def handle_admin_book_search_input(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    text = (update.message.text or "").strip()
    WAITING_BOOK_ADMIN_SEARCH.discard(user_id)
    if not text:
        update.message.reply_text("الرجاء كتابة عبارة بحث صالحة.", reply_markup=BOOKS_ADMIN_MENU_KB)
        return
    results = search_books(text)
    token = uuid4().hex
    BOOK_SEARCH_CACHE[token] = {"query": text, "book_ids": [b.get("id") for b in results if b.get("id")]}
    open_books_admin_list(update, context, search_token=token, page=0, from_callback=False)


def handle_book_media_message(update: Update, context: CallbackContext):
    user = update.effective_user
    if not _user_waiting_book_media(user):
        return

    user_id = user.id
    if user_id in WAITING_BOOK_ADD_COVER or user_id in WAITING_BOOK_EDIT_COVER:
        photo_list = update.message.photo or []
        if not photo_list:
            update.message.reply_text("أرسل صورة غلاف صالحة أو اكتب تخطي.", reply_markup=CANCEL_KB)
            return
        file_id = photo_list[-1].file_id
        if user_id in WAITING_BOOK_ADD_COVER:
            ctx = BOOK_CREATION_CONTEXT.get(user_id, {})
            ctx["cover_file_id"] = file_id
            BOOK_CREATION_CONTEXT[user_id] = ctx
            WAITING_BOOK_ADD_COVER.discard(user_id)
            WAITING_BOOK_ADD_PDF.add(user_id)
            update.message.reply_text("تم حفظ الغلاف. الآن أرسل ملف الـ PDF للكتاب.", reply_markup=CANCEL_KB)
        else:
            ctx = BOOK_EDIT_CONTEXT.get(user_id, {})
            book_id = ctx.get("book_id")
            route = ctx.get("route")
            update_book_record(book_id, cover_file_id=file_id)
            WAITING_BOOK_EDIT_COVER.discard(user_id)
            BOOK_EDIT_CONTEXT.pop(user_id, None)
            update.message.reply_text("تم تحديث الغلاف.", reply_markup=BOOKS_ADMIN_MENU_KB)
            if book_id and route:
                try:
                    _send_admin_book_detail(update, context, book_id, route)
                except Exception:
                    pass
        raise DispatcherHandlerStop()

    if user_id in WAITING_BOOK_ADD_PDF or user_id in WAITING_BOOK_EDIT_PDF:
        doc = update.message.document
        mime_type = (doc.mime_type or "").lower() if doc else ""
        filename = (doc.file_name or "").lower() if doc else ""
        if not doc or not (mime_type.startswith("application/pdf") or filename.endswith(".pdf")):
            update.message.reply_text("أرسل ملف PDF صالح.", reply_markup=CANCEL_KB)
            return
        file_id = doc.file_id
        filename = doc.file_name
        if user_id in WAITING_BOOK_ADD_PDF:
            ctx = BOOK_CREATION_CONTEXT.get(user_id, {})
            ctx["pdf_file_id"] = file_id
            ctx["pdf_filename"] = filename
            BOOK_CREATION_CONTEXT[user_id] = ctx
            WAITING_BOOK_ADD_PDF.discard(user_id)
            _finalize_book_creation(update, context, ctx)
        else:
            ctx = BOOK_EDIT_CONTEXT.get(user_id, {})
            book_id = ctx.get("book_id")
            route = ctx.get("route")
            update_book_record(book_id, pdf_file_id=file_id, pdf_filename=filename)
            WAITING_BOOK_EDIT_PDF.discard(user_id)
            BOOK_EDIT_CONTEXT.pop(user_id, None)
            update.message.reply_text("تم تحديث ملف الكتاب.", reply_markup=BOOKS_ADMIN_MENU_KB)
            if book_id and route:
                try:
                    _send_admin_book_detail(update, context, book_id, route)
                except Exception:
                    pass
        raise DispatcherHandlerStop()


def handle_books_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    data = query.data or ""
    user_id = query.from_user.id
    is_privileged = _ensure_is_admin_or_supervisor(user_id)

    logger.info("[BOOKS][CB] data=%s user=%s", data, user_id)

    # إدارة المكتبة للأدمن/المشرفة
    if data.startswith(f"{BOOKS_CALLBACK_PREFIX}:admin"):
        if not is_privileged:
            query.answer("غير مصرح لك باستخدام هذه الخيارات.", show_alert=True)
            return
    if data == f"{BOOKS_CALLBACK_PREFIX}:admin_back":
        query.answer()
        query.message.reply_text("رجعنا لقائمة إدارة المكتبة.", reply_markup=BOOKS_ADMIN_MENU_KB)
        return

    if data == f"{BOOKS_CALLBACK_PREFIX}:admin_categories":
        open_book_categories_admin(update, context, use_callback=True)
        return

    if data == f"{BOOKS_CALLBACK_PREFIX}:admin_category_add":
        BOOK_CATEGORY_EDIT_CONTEXT[user_id] = {"mode": "create"}
        WAITING_BOOK_CATEGORY_NAME.add(user_id)
        query.answer()
        query.message.reply_text("أرسل اسم التصنيف الجديد:", reply_markup=CANCEL_KB)
        return

    if data.startswith(f"{BOOKS_CALLBACK_PREFIX}:admin_category_rename:"):
        cat_id = data.split(":")[2]
        _start_category_rename(query, cat_id)
        return

    if data.startswith(f"{BOOKS_CALLBACK_PREFIX}:admin_category_order:"):
        cat_id = data.split(":")[2]
        _start_category_order_edit(query, cat_id)
        return

    if data.startswith(f"{BOOKS_CALLBACK_PREFIX}:admin_category_toggle:"):
        cat_id = data.split(":")[2]
        _handle_category_toggle(query, cat_id)
        return

    if data.startswith(f"{BOOKS_CALLBACK_PREFIX}:admin_category_delete:"):
        cat_id = data.split(":")[2]
        _handle_category_delete(update, context, query, cat_id)
        return

    if data.startswith(f"{BOOKS_CALLBACK_PREFIX}:admin_category:"):
        parts = data.split(":")
        cat_id = parts[2] if len(parts) > 2 else None
        if not cat_id:
            query.answer("تصنيف غير معروف.", show_alert=True)
            return
        _show_category_options(query, cat_id)
        return

    if data.startswith(f"{BOOKS_CALLBACK_PREFIX}:admin_select_category:"):
        cat_id = data.split(":")[2]
        cat = get_book_category(cat_id)
        if not cat or not cat.get("is_active", True):
            query.answer("التصنيف غير متاح.", show_alert=True)
            return
        ctx = BOOK_CREATION_CONTEXT.get(user_id, {"mode": "create"})
        ctx["category_id"] = cat_id
        ctx["category_name_snapshot"] = cat.get("name")
        BOOK_CREATION_CONTEXT[user_id] = ctx
        WAITING_BOOK_ADD_CATEGORY.discard(user_id)
        WAITING_BOOK_ADD_TITLE.add(user_id)
        query.answer()
        query.message.reply_text("أرسل عنوان الكتاب:", reply_markup=CANCEL_KB)
        return

    if data == f"{BOOKS_CALLBACK_PREFIX}:admin_cancel_creation":
        _reset_book_creation(user_id)
        query.answer("تم الإلغاء.")
        query.message.reply_text("تم إلغاء إضافة الكتاب.", reply_markup=BOOKS_ADMIN_MENU_KB)
        return

    if data == f"{BOOKS_CALLBACK_PREFIX}:admin_list_prev":
        route_info = _parse_route(_get_admin_route(context))
        new_page = max(route_info.get("page", 0) - 1, 0)
        category_id = route_info.get("category_id")
        search_token = route_info.get("search_token")
        source = route_info.get("source")
        open_books_admin_list(
            update,
            context,
            category_id=category_id if source == "admin_cat" else None,
            page=new_page,
            search_token=search_token if source == "admin_search" else None,
            from_callback=True,
        )
        return

    if data == f"{BOOKS_CALLBACK_PREFIX}:admin_list_next":
        route_info = _parse_route(_get_admin_route(context))
        new_page = route_info.get("page", 0) + 1
        category_id = route_info.get("category_id")
        search_token = route_info.get("search_token")
        source = route_info.get("source")
        open_books_admin_list(
            update,
            context,
            category_id=category_id if source == "admin_cat" else None,
            page=new_page,
            search_token=search_token if source == "admin_search" else None,
            from_callback=True,
        )
        return

    if data == f"{BOOKS_CALLBACK_PREFIX}:admin_list_back":
        route_info = _parse_route(_get_admin_route(context))
        source = route_info.get("source")
        page = route_info.get("page", 0)
        category_id = route_info.get("category_id")
        search_token = route_info.get("search_token")
        open_books_admin_list(
            update,
            context,
            category_id=category_id if source == "admin_cat" else None,
            page=page,
            search_token=search_token if source == "admin_search" else None,
            from_callback=True,
        )
        return

    if data.startswith(f"{BOOKS_CALLBACK_PREFIX}:admin_list:"):
        route = data.split(":", 2)[2]
        _set_admin_route(context, route)
        route_info = _parse_route(route)
        source = route_info.get("source")
        page = route_info.get("page", 0)
        category_id = route_info.get("category_id")
        search_token = route_info.get("search_token")
        open_books_admin_list(update, context, category_id=category_id if source == "admin_cat" else None, page=page, search_token=search_token if source == "admin_search" else None, from_callback=True)
        return

    if data.startswith(f"{BOOKS_CALLBACK_PREFIX}:admin_book:"):
        parts = data.split(":", 3)
        if len(parts) < 3:
            return
        book_id = parts[2]
        route = parts[3] if len(parts) > 3 else _get_admin_route(context)
        query.answer()
        try:
            _send_admin_book_detail(update, context, book_id, route)
        except Exception:
            logger.exception(
                "[BOOKS][ADMIN_BOOK] Failed to render book detail | book_id=%s route=%s",
                book_id,
                route,
            )
            try:
                query.message.reply_text("تعذر فتح تفاصيل الكتاب حاليًا.")
            except Exception:
                pass
        return

    if data.startswith(f"{BOOKS_CALLBACK_PREFIX}:admin_book_field:"):
        parts = data.split(":")
        if len(parts) < 4:
            return
        field = parts[2]
        book_id = parts[3]
        route = ":".join(parts[4:]) if len(parts) > 4 else _get_admin_route(context)
        logger.info("[BOOKS][FIELD] field=%s book_id=%s route=%s", field, book_id, route)
        _start_book_field_edit(query, context, field, book_id, route)
        return

    if data.startswith(f"{BOOKS_CALLBACK_PREFIX}:admin_book_category:"):
        parts = data.split(":")
        if len(parts) < 4:
            return
        book_id = parts[2]
        category_id = parts[3]
        route = ":".join(parts[4:]) if len(parts) > 4 else _get_admin_route(context)
        _admin_set_book_category(update, context, book_id, category_id, route)
        return

    if data.startswith(f"{BOOKS_CALLBACK_PREFIX}:admin_book_toggle:"):
        parts = data.split(":")
        if len(parts) < 3:
            return
        book_id = parts[2]
        route = ":".join(parts[3:]) if len(parts) > 3 else _get_admin_route(context)
        logger.info("[BOOKS][TOGGLE] book_id=%s route=%s", book_id, route)
        _handle_admin_book_toggle(update, context, book_id, route)
        return

    if data.startswith(f"{BOOKS_CALLBACK_PREFIX}:admin_book_delete:"):
        parts = data.split(":")
        if len(parts) < 3:
            return
        book_id = parts[2]
        route = ":".join(parts[3:]) if len(parts) > 3 else _get_admin_route(context)
        logger.info("[BOOKS][DELETE] book_id=%s route=%s", book_id, route)
        _handle_admin_book_delete(update, context, book_id, route)
        return

    if data == f"{BOOKS_CALLBACK_PREFIX}:admin_search_prompt":
        start_admin_book_search_prompt(query)
        return

    if data == BOOKS_EXIT_CALLBACK:
        query.answer()
        query.message.reply_text(
            "تم الرجوع للقائمة الرئيسية.",
            reply_markup=user_main_keyboard(user_id),
        )
        return

    if data == BOOKS_HOME_BACK:
        open_books_home(update, context, from_callback=True)
        return

    if data == BOOKS_SEARCH_PROMPT_CALLBACK:
        prompt_book_search(update, context)
        return

    if data == BOOKS_BACK_CALLBACK:
        last_route = context.user_data.get("books_last_route", BOOKS_DEFAULT_ROUTE)
        _render_books_route(update, context, last_route, from_callback=True)
        return

    if data.startswith(f"{BOOKS_CALLBACK_PREFIX}:cat:"):
        try:
            _, _, cat_id, page_str = data.split(":", 3)
            page = int(page_str)
        except Exception:
            cat_id = None
            page = 0
        if cat_id:
            show_books_by_category(update, context, cat_id, page=page, from_callback=True)
        return

    if data.startswith(f"{BOOKS_CALLBACK_PREFIX}:latest:"):
        try:
            page = int(data.split(":")[2])
        except Exception:
            page = 0
        show_latest_books(update, context, page=page, from_callback=True)
        return

    if data.startswith(f"{BOOKS_CALLBACK_PREFIX}:saved:"):
        try:
            page = int(data.split(":")[2])
        except Exception:
            page = 0
        show_saved_books(update, context, page=page, from_callback=True)
        return

    if data.startswith(f"{BOOKS_CALLBACK_PREFIX}:list:"):
        route = data.split(":", 2)[2]
        _render_books_route(update, context, route, from_callback=True)
        return

    if data.startswith(f"{BOOKS_CALLBACK_PREFIX}:book:"):
        parts = data.split(":")
        if len(parts) < 3:
            return
        book_id = parts[2]
        route = context.user_data.get("books_last_route", BOOKS_DEFAULT_ROUTE)
        _send_book_detail(update, context, book_id, route, from_callback=True)
        return

    if data.startswith(f"{BOOKS_CALLBACK_PREFIX}:download:"):
        parts = data.split(":")
        if len(parts) < 3:
            return
        book_id = parts[2]
        handle_book_download(update, context, book_id)
        return

    if data.startswith(f"{BOOKS_CALLBACK_PREFIX}:toggle_save:"):
        parts = data.split(":")
        if len(parts) < 3:
            return
        book_id = parts[2]
        handle_toggle_saved(update, context, book_id)
        return

    query.answer()


def remove_book_from_saved(user_id: int, book_id: str) -> bool:
    record = get_user_record_by_id(user_id) or {}
    _ensure_saved_books_defaults(record)
    saved = record.get("saved_books", [])
    if book_id not in saved:
        return True
    saved = [bid for bid in saved if bid != book_id]
    update_user_record(
        user_id,
        saved_books=saved,
        saved_books_updated_at=datetime.now(timezone.utc).isoformat(),
    )
    return True


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


def ensure_water_defaults(record: Dict):
    """ضبط الإعدادات الافتراضية لتذكير الماء"""
    if "water_enabled" not in record:
        record["water_enabled"] = False
    if "reminders_on" not in record:
        record["reminders_on"] = False


def perform_initial_water_cleanup():
    """تعطيل تذكيرات الماء للمستخدمين الحاليين لمرة واحدة"""
    cfg = get_global_config()
    if cfg.get("water_cleanup_done"):
        return

    updated = 0
    try:
        if firestore_available():
            batch = db.batch()
            for doc in db.collection(USERS_COLLECTION).stream():
                if str(doc.id) == str(GLOBAL_KEY):
                    continue
                doc_data = doc.to_dict() or {}
                updates = {}

                if doc_data.get("water_enabled") is not False:
                    updates["water_enabled"] = False
                if doc_data.get("reminders_on"):
                    updates["reminders_on"] = False

                if updates:
                    batch.update(doc.reference, updates)
                    updated += 1
                    if updated % 400 == 0:
                        batch.commit()
                        batch = db.batch()

            if updated % 400 != 0:
                batch.commit()

        for uid, rec in data.items():
            if str(uid) == str(GLOBAL_KEY):
                continue
            if rec.get("water_enabled") or rec.get("reminders_on"):
                rec["water_enabled"] = False
                rec["reminders_on"] = False

        logger.info(f"✅ تم تعطيل تذكيرات الماء افتراضيًا لـ {updated} مستخدم")
    except Exception as e:
        logger.error(f"❌ خطأ في تعطيل تذكيرات الماء: {e}")

    cfg["water_cleanup_done"] = True
    save_global_config(cfg)


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
    """جلب جميع المستخدمين من Firestore وفرزهم حسب النقاط"""
    if not firestore_available():
        # Fallback to local data
        return sorted(
            [r for k, r in data.items() if k != GLOBAL_KEY],
            key=lambda r: r.get("points", 0),
            reverse=True,
        )
        
    try:
        users_ref = db.collection(USERS_COLLECTION)
        # جلب جميع الوثائق
        docs = users_ref.stream()
        
        users_list = []
        for doc in docs:
            if str(doc.id) == str(GLOBAL_KEY):
                continue
            users_list.append(doc.to_dict())
            
        # فرز القائمة
        return sorted(
            users_list,
            key=lambda r: r.get("points", 0),
            reverse=True,
        )
        
    except Exception as e:
        logger.error(f"❌ خطأ في جلب المستخدمين وفرزهم من Firestore: {e}")
        # Fallback to local data
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
    ensure_medal_defaults(record)
    old_level = record.get("level", 0)
    points = record.get("points", 0)

    new_level = points // 20

    if new_level == old_level:
        check_rank_improvement(user_id, record, context)
        return

    record["level"] = new_level
    medals = record.get("medals", [])
    new_medals = []

    for lvl, name in LEVEL_MEDAL_RULES:
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
    ensure_medal_defaults(record)
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
    total_full_days = record.get("daily_full_count", 0) or 0

    got_new_daily_medal = False
    got_new_streak_medal = False

    is_new_completion = last_full_day != today_str

    if is_new_completion:
        total_full_days += 1
        if last_full_day:
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

    if total_full_days >= DAILY_FULL_MEDAL_THRESHOLD and MEDAL_DAILY_ACTIVITY not in medals:
        medals.append(MEDAL_DAILY_ACTIVITY)
        got_new_daily_medal = True

    record["daily_full_count"] = total_full_days
    record["daily_full_streak"] = streak

    if is_new_completion:
        record["last_full_day"] = today_str

    if streak >= DAILY_STREAK_MEDAL_THRESHOLD and MEDAL_STREAK not in medals:
        medals.append(MEDAL_STREAK)
        got_new_streak_medal = True

    record["medals"] = medals
    save_data()

    if context is not None:
        try:
            if got_new_daily_medal:
                context.bot.send_message(
                    chat_id=user_id,
                    text=(
                        "✨ مبروك! أنجزت هدف الماء وهدف القرآن لعدة أيام.\n"
                        f"هذه *{MEDAL_DAILY_ACTIVITY}* بعد الوصول إلى {DAILY_FULL_MEDAL_THRESHOLD} أيام مكتملة. استمر! 🤍"
                    ),
                    parse_mode="Markdown",
                )
            if got_new_streak_medal:
                context.bot.send_message(
                    chat_id=user_id,
                    text=(
                        f"🗓️ ما شاء الله! حافظت على نشاطك اليومي (ماء + قرآن) لمدة {DAILY_STREAK_MEDAL_THRESHOLD} أيام متتالية.\n"
                        f"حصلت على *{MEDAL_STREAK}*\n"
                        "استمر، فالقليل الدائم أحبّ إلى الله من الكثير المنقطع 🤍"
                    ),
                    parse_mode="Markdown",
                )
        except Exception as e:
            logger.error(f"Error sending daily activity medals messages to {user_id}: {e}")


def add_points(user_id: int, amount: int, context: CallbackContext = None, reason: str = ""):
    """إضافة نقاط للمستخدم في Firestore"""
    user_id_str = str(user_id)
    
    if not firestore_available():
        logger.warning("Firestore غير متوفر")
        return
    
    try:
        doc_ref = db.collection(USERS_COLLECTION).document(user_id_str)
        doc = doc_ref.get()
        
        if doc.exists:
            record = doc.to_dict()
            current_points = record.get("points", 0)
            new_points = current_points + amount
            
            # تحديث النقاط
            doc_ref.update({
                "points": new_points,
                "last_active": datetime.now(timezone.utc).isoformat()
            })
            
            # تحديث record للمستوى والميداليات
            record["points"] = new_points
            data[user_id_str] = record
            
            # فحص المستوى ومنح الميداليات
            update_level_and_medals(user_id, record, context)
            
            logger.info(f"✅ تم إضافة {amount} نقطة للمستخدم {user_id} (السبب: {reason}). المجموع: {new_points}")
            
            # إرسال إشعار للمستخدم
            if context and amount > 0:
                try:
                    context.bot.send_message(
                        chat_id=user_id,
                        text=f"🎉 رائع! حصلت على {amount} نقطة\n{reason}\n\nمجموع نقاطك الآن: {new_points} 🌟"
                    )
                except Exception as e:
                    logger.error(f"خطأ في إرسال إشعار النقاط: {e}")
                    
    except Exception as e:
        logger.error(f"❌ خطأ في إضافة نقاط للمستخدم {user_id}: {e}")





def save_note(user_id: int, note_text: str):
    """حفظ مذكرة قلبي في Firestore"""
    user_id_str = str(user_id)
    
    if not firestore_available():
        logger.warning("Firestore غير متوفر")
        return
    
    try:
        # حفظ المذكرة في subcollection
        note_data = {
            "user_id": user_id,
            "note": note_text,
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        
        db.collection(USERS_COLLECTION).document(user_id_str).collection("heart_memos").add(note_data)
        logger.info(f"✅ تم حفظ مذكرة قلبي للمستخدم {user_id} في Firestore")
        
    except Exception as e:
        logger.error(f"❌ خطأ في حفظ المذكرة للمستخدم {user_id}: {e}")


def save_benefit(benefit_data: Dict):
    """حفظ فائدة/نصيحة في Firestore"""
    
    if not firestore_available():
        logger.warning("Firestore غير متوفر")
        return None
    
    try:
        # إضافة معلومات إضافية
        if "created_at" not in benefit_data:
            benefit_data["created_at"] = datetime.now(timezone.utc).isoformat()
        if "likes" not in benefit_data:
            benefit_data["likes"] = 0
        
        # حفظ الفائدة
        doc_ref = db.collection(BENEFITS_COLLECTION).add(benefit_data)
        benefit_id = doc_ref[1].id
        logger.info(f"✅ تم حفظ فائدة جديدة في Firestore (ID: {benefit_id})")
        return benefit_id
        
    except Exception as e:
        logger.error(f"❌ خطأ في حفظ الفائدة: {e}")
        return None


def start_command(update: Update, context: CallbackContext):
    """معالج أمر /start مع ضمان الإرسال الفوري وتنظيف حالات الانتظار."""
    user = update.effective_user
    user_id = user.id
    
    # الخطوة 1: تنظيف جميع حالات الانتظار للمستخدم الحالي
    # هذا يضمن أن /start يقطع أي عملية جارية ويعيد المستخدم للقائمة الرئيسية
    WAITING_GENDER.discard(user_id)
    WAITING_AGE.discard(user_id)
    WAITING_WEIGHT.discard(user_id)
    WAITING_QURAN_GOAL.discard(user_id)
    WAITING_QURAN_ADD_PAGES.discard(user_id)
    WAITING_TASBIH.discard(user_id)
    WAITING_MEMO_MENU.discard(user_id)
    WAITING_MEMO_ADD.discard(user_id)
    WAITING_MEMO_EDIT_SELECT.discard(user_id)
    WAITING_MEMO_EDIT_TEXT.discard(user_id)
    WAITING_MEMO_DELETE_SELECT.discard(user_id)
    WAITING_BOOK_SEARCH.discard(user_id)
    WAITING_BOOK_ADMIN_SEARCH.discard(user_id)
    WAITING_BOOK_CATEGORY_NAME.discard(user_id)
    WAITING_BOOK_CATEGORY_ORDER.discard(user_id)
    WAITING_BOOK_ADD_CATEGORY.discard(user_id)
    WAITING_BOOK_ADD_TITLE.discard(user_id)
    WAITING_BOOK_ADD_AUTHOR.discard(user_id)
    WAITING_BOOK_ADD_DESCRIPTION.discard(user_id)
    WAITING_BOOK_ADD_TAGS.discard(user_id)
    WAITING_BOOK_ADD_COVER.discard(user_id)
    WAITING_BOOK_ADD_PDF.discard(user_id)
    WAITING_BOOK_EDIT_FIELD.discard(user_id)
    WAITING_BOOK_EDIT_COVER.discard(user_id)
    WAITING_BOOK_EDIT_PDF.discard(user_id)
    BOOK_CREATION_CONTEXT.pop(user_id, None)
    BOOK_EDIT_CONTEXT.pop(user_id, None)
    BOOK_CATEGORY_EDIT_CONTEXT.pop(user_id, None)
    WAITING_SUPPORT_GENDER.discard(user_id)
    WAITING_BROADCAST.discard(user_id)
    WAITING_WATER_ADD_CUPS.discard(user_id)
    WAITING_BENEFIT_TEXT.discard(user_id)
    WAITING_BENEFIT_EDIT_TEXT.discard(user_id)
    WAITING_BENEFIT_DELETE_CONFIRM.discard(user_id)
    WAITING_MOTIVATION_ADD.discard(user_id)
    WAITING_MOTIVATION_DELETE.discard(user_id)
    WAITING_MOTIVATION_TIMES.discard(user_id)
    WAITING_BAN_USER.discard(user_id)
    WAITING_UNBAN_USER.discard(user_id)
    WAITING_BAN_REASON.discard(user_id)
    
    # الخطوة 2: قراءة أو إنشاء سجل المستخدم
    record = get_user_record(user)
    
    # الخطوة 3: التحقق إذا كان المستخدم محظورًا
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
    
    # الخطوة 4: إرسال رسالة الترحيب بالكيبورد الرئيسي
    welcome_message = (
        "🤍 أهلاً بك في سقيا الكوثر\n"
        "هنا تُسقى أرواحنا بالذكر والطمأنينة…\n"
        "ونتشارك نُصحًا ينفع القلب ويُرضي الله 🌿"
    )
    
    try:
        update.message.reply_text(
            welcome_message,
            reply_markup=user_main_keyboard(user_id),
        )
    except Exception as e:
        logger.error(f"Error sending welcome message to user {user_id}: {e}")
    
    # الخطوة 5: إرسال إشعار دخول للأدمن والمشرفة عند كل /start
    if ADMIN_ID is not None or SUPERVISOR_ID is not None:
        username_text = f"@{user.username}" if user.username else "غير متوفر"
        
        # تنسيق وقت الدخول بتوقيت الجزائر
        now_utc = datetime.now(timezone.utc)
        try:
            local_tz = pytz.timezone("Africa/Algiers")
        except:
            local_tz = timezone.utc
        
        now_local = now_utc.astimezone(local_tz)
        login_time_str = now_local.strftime("%d-%m-%Y | %H:%M:%S")
        
        # التحقق من كون المستخدم جديداً أم قديماً
        is_new = record.get("is_new_user", False)
        user_status = "🆕 مستخدم جديد" if is_new else "👤 مستخدم قديم"
        
        notification_message = (
            f"🔔 {user_status} دخل البوت\n\n"
            f"👤 الاسم: {user.first_name}\n"
            f"🆔 User ID: {user.id}\n"
            f"🧑‍💻 Username: {username_text}\n"
            f"🕒 وقت الدخول: {login_time_str} (توقيت الجزائر)\n\n"
            "📝 ملاحظة: معلومات الجهاز والموقع الجغرافي غير متوفرة من Telegram API"
        )
        
        # إرسال الإشعار للأدمن
        if ADMIN_ID is not None:
            try:
                context.bot.send_message(
                    chat_id=ADMIN_ID,
                    text=notification_message,
                )
            except Exception as e:
                logger.error(f"Error sending login notification to admin {ADMIN_ID}: {e}")
        
        # إرسال الإشعار للمشرفة
        if SUPERVISOR_ID is not None:
            try:
                context.bot.send_message(
                    chat_id=SUPERVISOR_ID,
                    text=notification_message,
                )
            except Exception as e:
                logger.error(f"Error sending login notification to supervisor {SUPERVISOR_ID}: {e}")
    
    # الخطوة 6: إذا كان مستخدم جديد، تحديث العلامة
    if record.get("is_new_user", False):
        update_user_record(user_id, is_new_user=False)


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
        "• مكتبة الكتب 📚 → تصفّح الكتب الموثوقة، التحميل، البحث، والحفظ للقراءة لاحقًا.\n"
        "• منبّه الماء 💧 → حساب احتياجك من الماء، تسجيل الأكواب، وتفعيل التذكير.\n"
        "• احصائياتي 📊 → ملخّص بسيط لإنجازاتك اليوم.\n"
        "• تواصل مع الدعم ✉️ → لإرسال رسالة للدعم والرد عليك لاحقًا.\n"
        "• المنافسات و المجتمع 🏅 → لرؤية مستواك ونقاطك ولوحات الشرف.\n"
        "• الاشعارات 🔔 → تشغيل أو إيقاف الجرعة التحفيزية خلال اليوم.",
        reply_markup=kb,
    )

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
        "• عدّل إعداداتك وتابع احتياجك اليومي.\n"
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
        "2) تصفير العداد والرجوع إلى منبّه الماء مباشرة.",
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
        open_water_menu(update, context)
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
    
    # حفظ في Firestore
    update_user_record(user.id, gender=record["gender"])
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
        open_water_menu(update, context)
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
    
    # حفظ في Firestore
    update_user_record(user.id, age=record["age"])
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
        open_water_menu(update, context)
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
    new_cups = before + 1

    # حفظ في Firestore
    update_user_record(user.id, today_cups=new_cups)
    logger.info(f"✅ تم حفظ كوب ماء للمستخدم {user.id} في Firestore")

    add_points(user.id, POINTS_PER_WATER_CUP, context, reason="شرب كوب ماء")

    cups_goal = record.get("cups_goal")
    if cups_goal and before < cups_goal <= new_cups:
        add_points(user.id, POINTS_WATER_DAILY_BONUS, context, reason="إكمال هدف الماء اليومي")

    # تحديث record المحلي
    record["today_cups"] = new_cups
    check_daily_full_activity(user.id, record, context)

    check_daily_full_activity(user.id, record, context)

    status_text = format_water_status_text(record)
    update.message.reply_text(
        f"🥤 تم تسجيل كوب ماء.\n\n{status_text}",
        reply_markup=water_menu_keyboard(user.id),
    )


def handle_add_cups(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    record = get_user_record(user)
    text = (update.message.text or "").strip()

    if not record.get("cups_goal"):
        WAITING_WATER_ADD_CUPS.discard(user_id)
        update.message.reply_text(
            "قبل استخدام هذه الميزة، احسب احتياجك من الماء أولًا من خلال:\n"
            "«إعدادات الماء ⚙️» → «حساب احتياج الماء 🧮».",
            reply_markup=water_menu_keyboard(user.id),
        )
        return

    if text == BTN_WATER_ADD_CUPS:
        WAITING_WATER_ADD_CUPS.add(user_id)
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
    new_total = before + cups

    update_user_record(user.id, today_cups=new_total)
    record["today_cups"] = new_total

    add_points(user.id, cups * POINTS_PER_WATER_CUP, context, reason="إضافة أكواب ماء")

    cups_goal = record.get("cups_goal")
    if cups_goal and before < cups_goal <= new_total:
        add_points(user.id, POINTS_WATER_DAILY_BONUS, context, reason="إكمال هدف الماء اليومي")

    check_daily_full_activity(user.id, record, context)

    WAITING_WATER_ADD_CUPS.discard(user_id)

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

    record["water_enabled"] = True
    record["reminders_on"] = True

    # حفظ في Firestore
    update_user_record(
        user.id,
        reminders_on=record["reminders_on"],
        water_enabled=record["water_enabled"],
    )
    save_data()

    refresh_water_jobs()

    update.message.reply_text(
        "تم تشغيل تذكيرات الماء ⏰\n"
        "ستصلك رسائل خلال اليوم لتذكيرك بالشرب.",
        reply_markup=notifications_menu_keyboard(user.id, record),
    )


def handle_reminders_off(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    record = get_user_record(user)
    record["water_enabled"] = False
    record["reminders_on"] = False

    # حفظ في Firestore
    update_user_record(
        user.id,
        reminders_on=record["reminders_on"],
        water_enabled=record["water_enabled"],
    )
    save_data()

    refresh_water_jobs()

    update.message.reply_text(
        "تم إيقاف تذكيرات الماء 📴\n"
        "يمكنك تشغيلها مرة أخرى وقتما شئت.",
        reply_markup=notifications_menu_keyboard(user.id, record),
    )


def handle_water_reset(update: Update, context: CallbackContext):
    """تصفير عداد الماء يدوياً"""
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id
    
    # حفظ الاستهلاك اليومي قبل التصفير
    today_cups = record.get("today_cups", 0)
    
    # تصفير العداد
    record["today_cups"] = 0
    
    # حفظ في Firestore
    update_user_record(user_id, today_cups=0)
    save_data()
    
    logger.info(f"✅ تم تصفير عداد الماء للمستخدم {user_id} (كان: {today_cups} كوب)")
    
    update.message.reply_text(
        f"تم تصفير عداد الماء 🔄\n"
        f"كان عدد الأكواب: {today_cups} كوب\n"
        f"الآن: 0 كوب\n\n"
        "يمكنك البدء من جديد!",
        reply_markup=water_settings_keyboard(user_id),
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
            "تم الإلغاء، عدنا إلى قائمة وردي القرآني.",
            reply_markup=quran_menu_keyboard(user_id),
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
    
    # حفظ في Firestore
    update_user_record(user.id, quran_pages_goal=record["quran_pages_goal"])
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
            "تم الإلغاء، عدنا إلى قائمة وردي القرآني.",
            reply_markup=quran_menu_keyboard(user_id),
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
    # تحديث Firestore مباشرة
    update_user_record(user_id, quran_pages_today=record["quran_pages_today"], quran_today_date=record.get("quran_today_date"))

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
    
    # حفظ في Firestore
    update_user_record(user.id, quran_pages_today=record["quran_pages_today"])
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

    STRUCTURED_ADHKAR_STATE.pop(user.id, None)
    get_user_record(user)
    kb = adhkar_menu_keyboard(user.id)
    update.message.reply_text(
        "أذكاري 🤲:\n"
        "• أذكار الصباح.\n"
        "• أذكار المساء.\n"
        "• أذكار عامة تريح القلب.\n"
        "• أذكار النوم الموثوقة.",
        reply_markup=kb,
    )


def format_structured_adhkar_text(category_key: str, index: int) -> str:
    section = STRUCTURED_ADHKAR_SECTIONS.get(category_key, {})
    items = section.get("items", [])

    if index < 0 or index >= len(items):
        return ""

    item = items[index]
    total = len(items)
    return (
        f"{section.get('title', 'الأذكار')} ({index + 1}/{total}):\n\n"
        f"{item['title']}:\n{item['text']}\n\n"
        f"التكرار: {item['repeat']}"
    )


def send_structured_adhkar_step(update: Update, user_id: int, category_key: str, index: int):
    section = STRUCTURED_ADHKAR_SECTIONS.get(category_key, {})
    items = section.get("items", [])

    if not items:
        update.message.reply_text(
            "تعذّر تحميل الأذكار حاليًا، حاول لاحقًا.",
            reply_markup=adhkar_menu_keyboard(user_id),
        )
        return

    index = max(0, min(index, len(items) - 1))
    STRUCTURED_ADHKAR_STATE[user_id] = {"category": category_key, "index": index}
    kb = build_structured_adhkar_kb(index > 0, bool(items))
    update.message.reply_text(
        format_structured_adhkar_text(category_key, index),
        reply_markup=kb,
    )


def start_structured_adhkar(update: Update, context: CallbackContext, category_key: str):
    user = update.effective_user
    record = get_user_record(user)

    if record.get("is_banned", False):
        return

    if category_key not in STRUCTURED_ADHKAR_SECTIONS:
        open_adhkar_menu(update, context)
        return

    increment_adhkar_count(user.id, 1)
    send_structured_adhkar_step(update, user.id, category_key, 0)


def send_morning_adhkar(update: Update, context: CallbackContext):
    start_structured_adhkar(update, context, "morning")


def send_evening_adhkar(update: Update, context: CallbackContext):
    start_structured_adhkar(update, context, "evening")


def send_general_adhkar(update: Update, context: CallbackContext):
    start_structured_adhkar(update, context, "general")


def handle_structured_adhkar_next(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    record = get_user_record(user)

    if record.get("is_banned", False):
        return

    state = STRUCTURED_ADHKAR_STATE.get(user_id)
    if not state:
        open_adhkar_menu(update, context)
        return

    category = state["category"]
    index = state["index"]
    items = STRUCTURED_ADHKAR_SECTIONS.get(category, {}).get("items", [])

    if index >= len(items) - 1:
        done_msg = STRUCTURED_ADHKAR_DONE_MESSAGES.get(category, "✅ بارك الله فيك وتقبّل الله ذكرك. 🤍")
        STRUCTURED_ADHKAR_STATE.pop(user_id, None)
        update.message.reply_text(
            done_msg,
            reply_markup=adhkar_menu_keyboard(user_id),
        )
        return

    send_structured_adhkar_step(update, user_id, category, index + 1)


def handle_structured_adhkar_done(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    record = get_user_record(user)

    if record.get("is_banned", False):
        return

    state = STRUCTURED_ADHKAR_STATE.get(user_id)
    if not state:
        open_adhkar_menu(update, context)
        return

    category = state["category"]
    done_msg = STRUCTURED_ADHKAR_DONE_MESSAGES.get(
        category, "✅ بارك الله فيك وتقبّل الله ذكرك. 🤍"
    )

    STRUCTURED_ADHKAR_STATE.pop(user_id, None)
    update.message.reply_text(
        done_msg,
        reply_markup=adhkar_menu_keyboard(user_id),
    )


def handle_structured_adhkar_prev(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    record = get_user_record(user)

    if record.get("is_banned", False):
        return

    state = STRUCTURED_ADHKAR_STATE.get(user_id)
    if not state:
        open_adhkar_menu(update, context)
        return

    category = state["category"]
    index = state["index"]

    if index <= 0:
        send_structured_adhkar_step(update, user_id, category, 0)
        return

    send_structured_adhkar_step(update, user_id, category, index - 1)


def handle_structured_adhkar_back_to_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    STRUCTURED_ADHKAR_STATE.pop(user.id, None)
    open_adhkar_menu(update, context)


def handle_structured_adhkar_back_main(update: Update, context: CallbackContext):
    user = update.effective_user
    STRUCTURED_ADHKAR_STATE.pop(user.id, None)
    update.message.reply_text(
        "عدنا إلى القائمة الرئيسية.",
        reply_markup=user_main_keyboard(user.id),
    )


def format_sleep_adhkar_text(index: int) -> str:
    total = len(SLEEP_ADHKAR_ITEMS)
    item = SLEEP_ADHKAR_ITEMS[index]
    return (
        f"💤 أذكار النوم ({index + 1}/{total}):\n\n"
        f"{item['title']}:\n{item['text']}\n\n"
        f"التكرار: {item['repeat']}"
    )


def start_sleep_adhkar(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)

    if record.get("is_banned", False):
        return

    increment_adhkar_count(user.id, 1)
    SLEEP_ADHKAR_STATE[user.id] = 0
    update.message.reply_text(
        format_sleep_adhkar_text(0),
        reply_markup=SLEEP_ADHKAR_KB,
    )


def handle_sleep_adhkar_next(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    record = get_user_record(user)

    if record.get("is_banned", False):
        return

    if user_id not in SLEEP_ADHKAR_STATE:
        start_sleep_adhkar(update, context)
        return

    current_index = SLEEP_ADHKAR_STATE[user_id]

    if current_index >= len(SLEEP_ADHKAR_ITEMS) - 1:
        SLEEP_ADHKAR_STATE.pop(user_id, None)
        update.message.reply_text(
            "🤍 تمّت أذكارك قبل النوم،\n"
            "نسأل الله أن يحفظك بعينه التي لا تنام،\n"
            "وأن يجعل ليلك سكينة، ونومك راحة، وأحلامك طمأنينة،\n"
            "ويكتب لك أجر الذاكرين، ويغمر قلبك بالطمأنينة والبركة. 🌙",
            reply_markup=adhkar_menu_keyboard(user_id),
        )
        return

    next_index = current_index + 1
    SLEEP_ADHKAR_STATE[user_id] = next_index
    update.message.reply_text(
        format_sleep_adhkar_text(next_index),
        reply_markup=SLEEP_ADHKAR_KB,
    )


def handle_sleep_adhkar_back(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    record = get_user_record(user)

    if record.get("is_banned", False):
        return

    if user_id in SLEEP_ADHKAR_STATE:
        increment_adhkar_count(user_id, 1)
    SLEEP_ADHKAR_STATE.pop(user_id, None)
    update.message.reply_text(
        "عدنا إلى القائمة الرئيسية.",
        reply_markup=user_main_keyboard(user_id),
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
    
    # حفظ في Firestore
    update_user_record(user.id, heart_memos=memos)
    save_data()
    logger.info(f"✅ تم حفظ مذكرة جديدة للمستخدم {user.id} في Firestore")

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

    memos[idx] = text
    record["heart_memos"] = memos
    
    # حفظ في Firestore
    update_user_record(user.id, heart_memos=record["heart_memos"])
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
    
    # حفظ في Firestore
    update_user_record(user.id, heart_memos=record["heart_memos"])
    save_data()

    WAITING_MEMO_DELETE_SELECT.discard(user_id)

    update.message.reply_text(
        f"🗑 تم حذف المذكرة:\n\n{deleted}",
        reply_markup=build_memos_menu_kb(is_admin(user_id)),
    )
    open_memos_menu(update, context)

# =================== احصائياتي ===================


def build_medals_overview_lines(record: dict) -> List[str]:
    ensure_medal_defaults(record)

    medals = record.get("medals", [])
    level = record.get("level", 0)
    total_full_days = record.get("daily_full_count", 0) or 0
    streak = record.get("daily_full_streak", 0) or 0

    lines = ["🏵️ لوحة الميداليات:\n"]

    if medals:
        lines.append("ميدالياتك الحالية:")
        lines.extend(f"- {medal}" for medal in medals)
    else:
        lines.append("لا توجد ميداليات حالياً. اجمع النقاط لتبدأ رحلتك 🤍")

    lines.append("\nالشروط الحالية:")
    lines.append("• ميداليات المستوى:")
    for lvl, name in LEVEL_MEDAL_RULES:
        status = "✅" if name in medals else "⏳" if level >= lvl else "⌛"
        lines.append(f"  {status} {name} — تبدأ من المستوى {lvl}.")

    daily_status = "✅" if MEDAL_DAILY_ACTIVITY in medals else "⏳"
    lines.append(
        f"• {daily_status} {MEDAL_DAILY_ACTIVITY}: بعد {DAILY_FULL_MEDAL_THRESHOLD} أيام مكتملة (أنجزت {total_full_days})."
    )

    streak_status = "✅" if MEDAL_STREAK in medals else "⏳"
    lines.append(
        f"• {streak_status} {MEDAL_STREAK}: تتطلب {DAILY_STREAK_MEDAL_THRESHOLD} يومًا متتاليًا (سلسلتك الحالية {streak})."
    )

    benefit_status = "✅" if MEDAL_TOP_BENEFIT in medals else "⏳"
    lines.append(
        f"• {benefit_status} {MEDAL_TOP_BENEFIT}: حافظ على فائدة ضمن أفضل 10 بالإعجابات."
    )

    return lines


def open_stats_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)

    if record.get("is_banned", False):
        return

    update.message.reply_text(
        "من فضلك اختر:\n- إحصائياتي\n- ميدالياتي",
        reply_markup=STATS_MENU_KB,
    )


def send_stats_overview(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)

    if record.get("is_banned", False):
        return

    record = get_user_record(user)

    ensure_today_water(record)
    ensure_today_quran(record)
    ensure_medal_defaults(record)

    cups_goal = record.get("cups_goal")
    today_cups = record.get("today_cups", 0)

    q_goal = record.get("quran_pages_goal")
    q_today = record.get("quran_pages_today", 0)

    adhkar_count = record.get("adhkar_count", 0)

    memos_count = len(record.get("heart_memos", []))
    saved_books_count = len(record.get("saved_books", []))

    points = record.get("points", 0)
    level = record.get("level", 0)

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
    text_lines.append(f"- عدد مذكّرات قلبك المسجّلة: {memos_count} مذكرة.")
    text_lines.append(f"- عدد الكتب المحفوظة لديك: {saved_books_count} كتاب.")

    text_lines.append(f"- مجموع نقاطك: {points} نقطة.")
    if level <= 0:
        text_lines.append("- مستواك الحالي: 0 (أول مستوى فعلي يبدأ من 20 نقطة).")
    else:
        text_lines.append(f"- المستوى الحالي: {level}.")

    update.message.reply_text(
        "\n".join(text_lines),
        reply_markup=STATS_MENU_KB,
    )


def open_medals_overview(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)

    if record.get("is_banned", False):
        return

    user_id = user.id
    medal_lines = build_medals_overview_lines(record)

    update.message.reply_text(
        "\n".join(medal_lines),
        reply_markup=STATS_MENU_KB,
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

    # حفظ الفائدة في Firestore مباشرة
    save_benefit_to_firestore(new_benefit)

    # 2. منح النقاط
    add_points(user_id, 2)

    # 3. إرسال رسالة تأكيد
    update.message.reply_text(
        "✅ تم إضافة فائدتك بنجاح! شكرًا لمشاركتك.\n"
        f"لقد حصلت على 2 نقطة مكافأة.",
        reply_markup=BENEFITS_MENU_KB,
    )


def handle_view_benefits(update: Update, context: CallbackContext):
    """عرض آخر الفوائد مع عرض الإعجابات بشكل صحيح"""
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

    # عرض آخر 5 فوائد مرتبة حسب التاريخ
    latest_benefits = sorted(benefits, key=lambda b: b.get("date", ""), reverse=True)[:5]
    
    # التحقق من صلاحيات المدير/المشرف
    is_privileged = is_admin(user.id) or is_supervisor(user.id)
    user_id = user.id
    
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
            
        # التأكد من وجود حقل likes_count
        likes_count = benefit.get("likes_count", 0)
        
        text_benefit = (
            f"• *{benefit['text']}*\n"
            f"  - من: {benefit['first_name']} | الإعجابات: {likes_count} 👍\n"
            f"  - تاريخ الإضافة: {date_str}\n"
        )
        
        # إضافة زر الإعجاب مع العدد الصحيح
        liked_by = benefit.get("liked_by", [])
        
        # التحقق مما إذا كان المستخدم الحالي قد أعجب بالفعل
        if user_id in liked_by:
            like_button_text = f"✅ أعجبتني ({likes_count})"
        else:
            like_button_text = f"👍 أعجبني ({likes_count})"
        
        # بناء اللوحة مع زر الإعجاب
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
        
    # إرسال رسالة ختامية
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


def handle_top100_benefits(update: Update, context: CallbackContext):
    """عرض أفضل 100 فائدة مرتبة حسب الإعجابات"""
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
    
    text = "🏆 أفضل 100 فائدة ونصيحة (حسب الإعجابات):\n\n"
    
    for i, benefit in enumerate(sorted_benefits[:100], start=1):
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
        
    for user_id in top_10_user_ids:
        uid_str = str(user_id)
        if uid_str in data:
            record = data[uid_str]
            ensure_medal_defaults(record)
            medals = record.get("medals", [])

            if MEDAL_TOP_BENEFIT not in medals:
                medals.append(MEDAL_TOP_BENEFIT)
                record["medals"] = medals
                save_data()

                # إرسال رسالة تهنئة
                try:
                    context.bot.send_message(
                        chat_id=user_id,
                        text=f"تهانينا! 🎉\n"
                             f"لقد حصلت على وسام جديد: *{MEDAL_TOP_BENEFIT}*\n"
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
    """معالجة الإعجاب بالفائدة مع حفظ صحيح في Firestore"""
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
        firestore_id = None
        
        for i, b in enumerate(benefits):
            if b.get("id") == benefit_id:
                benefit_index = i
                benefit = b
                firestore_id = b.get("firestore_id")
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
        
        # 3. حفظ التغييرات في Firestore بشكل مباشر
        if firestore_id and firestore_available():
            try:
                update_benefit_in_firestore(firestore_id, {
                    "likes_count": benefit["likes_count"],
                    "liked_by": liked_by
                })
                logger.info(f"✅ تم حفظ الإعجاب للفائدة {benefit_id} في Firestore")
            except Exception as e:
                logger.error(f"❌ خطأ في حفظ الإعجاب في Firestore: {e}")
        
        # 4. تحديث قائمة الفوائد المحلية
        benefits[benefit_index] = benefit
        save_benefits(benefits)
        
        # 5. تحديث زر الإعجاب
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
        
        # 6. فحص ومنح الوسام
        check_and_award_medal(context)


# =================== الاشعارات / الجرعة التحفيزية للمستخدم ===================


def open_notifications_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    kb = notifications_menu_keyboard(user.id, record)

    status = "مفعّلة ✅" if record.get("motivation_on", True) else "موقفة ⛔️"
    water_status = "مفعّل ✅" if record.get("water_enabled") else "متوقف ⛔️"

    update.message.reply_text(
        "الاشعارات 🔔:\n"
        f"• حالة الجرعة التحفيزية الحالية: {status}\n\n"
        f"• حالة تذكير الماء: {water_status}\n\n"
        "الجرعة التحفيزية هي رسائل قصيرة ولطيفة خلال اليوم تشرح القلب "
        "وتعينك على الاستمرار في الماء والقرآن والذكر 🤍\n\n"
        "يمكنك التحكم في الجرعة والتحكم في تذكير الماء من الأزرار بالأسفل.",
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
    
    # حفظ في Firestore
    update_user_record(user.id, motivation_on=record["motivation_on"])
    save_data()

    update.message.reply_text(
        "تم تشغيل الجرعة التحفيزية ✨\n"
        "ستصلك رسائل تحفيزية في أوقات مختلفة من اليوم 🤍",
        reply_markup=notifications_menu_keyboard(user.id, record),
    )


def handle_motivation_off(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    record = get_user_record(user)
    record["motivation_on"] = False
    
    # حفظ في Firestore
    update_user_record(user.id, motivation_on=record["motivation_on"])
    save_data()

    update.message.reply_text(
        "تم إيقاف الجرعة التحفيزية 😴\n"
        "يمكنك تشغيلها مرة أخرى من نفس المكان متى أحببت.",
        reply_markup=notifications_menu_keyboard(user.id, record),
    )

# =================== تذكيرات الماء ===================

REMINDER_HOURS_UTC = [7, 10, 13, 16, 19]


def water_reminder_job(context: CallbackContext):
    logger.info("Running water reminder job...")
    bot = context.bot
    current_hour = context.job.context if hasattr(context, "job") else None

    for uid in get_active_user_ids():
        rec = data.get(str(uid)) or {}
        if rec.get("water_enabled") is not True:
            continue

        user_hours = _normalize_hours(rec.get("water_reminder_hours"), REMINDER_HOURS_UTC)
        if current_hour is not None and current_hour not in user_hours:
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


# =================== التصفير اليومي ===================

def daily_reset_water():
    """تصفير عداد الماء يومياً عند منتصف الليل"""
    logger.info("🔄 بدء تصفير عداد الماء اليومي...")
    
    if not firestore_available():
        logger.warning("Firestore غير متوفر للتصفير اليومي")
        return
    
    try:
        # قراءة جميع المستخدمين من Firestore
        users_ref = db.collection(USERS_COLLECTION)
        docs = users_ref.stream()
        
        reset_count = 0
        for doc in docs:
            if str(doc.id) == str(GLOBAL_KEY):
                continue
            user_data = doc.to_dict()
            today_cups = user_data.get("today_cups", 0)
            
            if today_cups > 0:
                # تصفير العداد
                doc.reference.update({"today_cups": 0})
                
                # تحديث data المحلي
                if doc.id in data:
                    data[doc.id]["today_cups"] = 0
                
                reset_count += 1
        
        logger.info(f"✅ تم تصفير عداد الماء لـ {reset_count} مستخدم")
        
    except Exception as e:
        logger.error(f"❌ خطأ في تصفير عداد الماء: {e}", exc_info=True)


def daily_reset_quran():
    """تصفير ورد القرآن يومياً عند منتصف الليل"""
    logger.info("🔄 بدء تصفير ورد القرآن اليومي...")
    
    if not firestore_available():
        logger.warning("Firestore غير متوفر للتصفير اليومي")
        return
    
    try:
        # قراءة جميع المستخدمين من Firestore
        users_ref = db.collection(USERS_COLLECTION)
        docs = users_ref.stream()
        
        reset_count = 0
        for doc in docs:
            if str(doc.id) == str(GLOBAL_KEY):
                continue
            user_data = doc.to_dict()
            quran_today = user_data.get("quran_pages_today", 0)
            
            if quran_today > 0:
                # تصفير ورد اليوم
                doc.reference.update({"quran_pages_today": 0})
                
                # تحديث data المحلي
                if doc.id in data:
                    data[doc.id]["quran_pages_today"] = 0
                
                reset_count += 1
        
        logger.info(f"✅ تم تصفير ورد القرآن لـ {reset_count} مستخدم")
        
    except Exception as e:
        logger.error(f"❌ خطأ في تصفير ورد القرآن: {e}", exc_info=True)


def daily_reset_competition():
    """تصفير نقاط المنافسة اليومية (دون التأثير على النقاط الإجمالية)"""
    logger.info("🔄 بدء تصفير نقاط المنافسة اليومية...")
    
    if not firestore_available():
        logger.warning("Firestore غير متوفر للتصفير اليومي")
        return
    
    try:
        # قراءة جميع المستخدمين من Firestore
        users_ref = db.collection(USERS_COLLECTION)
        docs = users_ref.stream()
        
        reset_count = 0
        for doc in docs:
            if str(doc.id) == str(GLOBAL_KEY):
                continue
            user_data = doc.to_dict()
            daily_points = user_data.get("daily_competition_points", 0)
            
            if daily_points > 0:
                # تصفير نقاط المنافسة اليومية والترتيب
                doc.reference.update({
                    "daily_competition_points": 0,
                    "community_rank": 0
                })
                
                # تحديث data المحلي
                if doc.id in data:
                    data[doc.id]["daily_competition_points"] = 0
                    data[doc.id]["community_rank"] = 0
                
                reset_count += 1
        
        logger.info(f"✅ تم تصفير نقاط المنافسة اليومية والترتيب لـ {reset_count} مستخدم")
        logger.info("ℹ️ النقاط الإجمالية والميداليات الدائمة لم تتأثر")
        
    except Exception as e:
        logger.error(f"❌ خطأ في تصفير نقاط المنافسة: {e}", exc_info=True)


def daily_reset_all(context: CallbackContext = None):
    """تصفير جميع البيانات اليومية عند منتصف الليل"""
    logger.info("🌙 بدء التصفير اليومي الشامل (00:00 توقيت الجزائر)...")
    
    # تصفير عداد الماء
    daily_reset_water()
    
    # تصفير ورد القرآن
    daily_reset_quran()
    
    # تصفير نقاط المنافسة اليومية
    daily_reset_competition()
    
    logger.info("✅ اكتمل التصفير اليومي الشامل")


# =================== الجرعة التحفيزية (JobQueue + إدارة) ===================


def _normalize_hours(raw_hours, fallback: List[int]) -> List[int]:
    hours = []
    for h in raw_hours or []:
        try:
            h_int = int(h)
            if 0 <= h_int <= 23:
                hours.append(h_int)
        except (TypeError, ValueError):
            continue

    return sorted(set(hours)) or fallback


def _all_motivation_times() -> List[str]:
    times = set()
    for uid in get_active_user_ids():
        rec = data.get(str(uid)) or {}
        if rec.get("motivation_on") is False:
            continue
        times.update(
            _normalize_times(
                rec.get("motivation_times") or rec.get("motivation_hours"),
                MOTIVATION_TIMES_UTC,
            )
        )

    return sorted(times, key=_time_to_minutes) or MOTIVATION_TIMES_UTC


def _all_water_hours() -> List[int]:
    hours = set()
    for uid in get_active_user_ids():
        rec = data.get(str(uid)) or {}
        if not rec.get("water_enabled"):
            continue
        hours.update(_normalize_hours(rec.get("water_reminder_hours"), REMINDER_HOURS_UTC))

    return sorted(hours)


def refresh_water_jobs():
    """إعادة مزامنة مهام تذكير الماء مع إعدادات المستخدمين"""
    if not job_queue:
        return

    desired_hours = _all_water_hours()

    current_jobs = [
        job for job in job_queue.jobs() if job.name and job.name.startswith("water_reminder_")
    ]
    current_hours = set()

    for job in current_jobs:
        try:
            hour = int(str(job.name).split("_")[-1])
            current_hours.add(hour)
            if hour not in desired_hours:
                job.schedule_removal()
        except Exception:
            continue

    for hour in desired_hours:
        if hour in current_hours:
            continue
        try:
            job_queue.run_daily(
                water_reminder_job,
                time=time(hour=hour, minute=0, second=random.randint(0, 45), tzinfo=pytz.UTC),
                name=f"water_reminder_{hour}",
                context=hour,
                job_kwargs={"misfire_grace_time": 300, "coalesce": True},
            )
        except Exception as e:
            logger.warning(f"⚠️ خطأ في جدولة تذكير الماء للساعة {hour}: {e}")


def motivation_job(context: CallbackContext):
    now_utc = datetime.now(timezone.utc)
    current_time_str = now_utc.strftime("%H:%M")
    logger.info("Running motivation job for %s...", current_time_str)

    bot = context.bot
    active_users = get_active_user_ids()
    logger.info("📨 سيتم فحص %s مستخدم نشط لإرسال الجرعة التحفيزية.", len(active_users))

    for uid in active_users:
        rec = data.get(str(uid)) or {}

        if rec.get("motivation_on") is False:
            logger.debug("⏭️ المستخدم %s أوقف الجرعة التحفيزية، سيتم التجاوز.", uid)
            continue

        user_times = _normalize_times(
            rec.get("motivation_times") or rec.get("motivation_hours"),
            MOTIVATION_TIMES_UTC,
        )
        if current_time_str not in set(user_times):
            logger.debug(
                "⏭️ المستخدم %s لا يملك الوقت %s ضمن أوقاته (%s).",
                uid,
                current_time_str,
                user_times,
            )
            continue

        if not MOTIVATION_MESSAGES:
            logger.warning("⚠️ لا توجد رسائل جرعة تحفيزية لإرسالها.")
            continue

        msg = random.choice(MOTIVATION_MESSAGES)

        try:
            logger.info("🚀 إرسال جرعة تحفيزية للمستخدم %s", uid)
            bot.send_message(
                chat_id=uid,
                text=msg,
            )
        except Exception as e:
            logger.error(f"Error sending motivation message to {uid}: {e}")


def _seconds_until_next_minute() -> float:
    now = datetime.now(timezone.utc)
    remaining_seconds = 60 - now.second - now.microsecond / 1_000_000
    return max(0.0, remaining_seconds)

# ======== لوحة التحكم لإدارة الجرعة التحفيزية (أدمن + مشرفة) ========


def open_admin_motivation_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    if not (is_admin(user.id) or is_supervisor(user.id)):
        update.message.reply_text(
            "هذا القسم خاص بالإدارة فقط.",
            reply_markup=user_main_keyboard(user.id),
        )
        return

    hours_text = ", ".join(MOTIVATION_TIMES_UTC) if MOTIVATION_TIMES_UTC else "لا توجد أوقات مضبوطة"
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
    save_global_config(cfg)

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
    save_global_config(cfg)

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

    current = ", ".join(MOTIVATION_TIMES_UTC) if MOTIVATION_TIMES_UTC else "لا توجد"
    update.message.reply_text(
        "تعديل أوقات الجرعة التحفيزية ⏰\n\n"
        f"الأوقات الحالية (بتوقيت UTC): {current}\n\n"
        "أرسل الأوقات الجديدة بصيغة الساعات والدقائق (24h) مثل:\n"
        "`06:30 , 12:00 , 18:45` أو `21:10 — 18:45 — 09:05`\n\n"
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

    matches = re.findall(r"(\d{1,2}):(\d{2})", text)
    times = []
    for h_str, m_str in matches:
        hour = int(h_str)
        minute = int(m_str)
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            times.append(f"{hour:02d}:{minute:02d}")

    times = sorted(set(times), key=_time_to_minutes)

    if not times:
        msg.reply_text(
            "رجاءً أرسل الأوقات بصيغة صحيحة مثل: 06:30, 12:00, 18:45",
            reply_markup=CANCEL_KB,
        )
        return

    global MOTIVATION_TIMES_UTC
    MOTIVATION_TIMES_UTC = times

    cfg = get_global_config()
    cfg["motivation_times"] = MOTIVATION_TIMES_UTC
    save_global_config(cfg)

    WAITING_MOTIVATION_TIMES.discard(user_id)

    hours_text = ", ".join(MOTIVATION_TIMES_UTC)
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

        # تعديل العرض: إذا كانت النقاط والميداليات صفر/فارغة، اعرض اسم المستخدم فقط مع 0 نقطة ولا توجد ميداليات
        if points == 0 and not medals:
            lines.append(f"{idx}) {name} — 🎯 0 نقطة")
            lines.append("(لا توجد ميداليات متاحة)")
        else:
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

        # تعديل العرض: إذا كانت النقاط والميداليات صفر/فارغة، اعرض اسم المستخدم فقط مع 0 نقطة ولا توجد ميداليات
        if points == 0 and not medals:
            lines.append(f"{idx}) {name} — 🎯 0 نقطة")
            lines.append("(لا توجد ميداليات متاحة)")
        else:
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

def _send_support_session_opened_message(reply_func, gender: Optional[str] = None):
    is_female = gender == "female"
    text = (
        "حياكِ الله يا طيبة، تم فتح المحادثة مع الدعم.\n\n"
        "🤍 تفضلي بالكتابة، رسالتك تصل للدعم مباشرة"
        if is_female
        else "حياك الله، تم فتح المحادثة مع الدعم.\n\n"
             "📥يمكنك الآن الكتابة بكل راحة وخصوصية،"
    )
    reply_func(text, reply_markup=SUPPORT_SESSION_KB)


def _open_support_session(update: Update, user_id: int, gender: Optional[str]):
    WAITING_SUPPORT.add(user_id)
    _send_support_session_opened_message(update.message.reply_text, gender)


def handle_contact_support(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    record = get_user_record(user)
    user_id = user.id

    gender = record.get("gender")

    if user_id in WAITING_SUPPORT:
        update.message.reply_text(
            "المحادثة مع الدعم مفتوحة بالفعل.\n"
            "أرسل رسالتك مباشرة أو اضغط «🔚 إنهاء التواصل» عند الانتهاء.",
            reply_markup=SUPPORT_SESSION_KB,
        )
        return

    if gender in ["male", "female"]:
        _open_support_session(update, user_id, gender)
        return

    WAITING_SUPPORT_GENDER.add(user_id)
    update.message.reply_text(
        "قبل إرسال رسالتك للدعم، اختر الجنس:\n\n"
        "🧔‍♂️ لو كنت رجلًا → تصل رسالتك للمشرف.\n"
        "👩 لو كنت امرأة → تصل رسالتك للمشرفة.\n\n"
        "اختر من الأزرار بالأسفل 👇",
        reply_markup=GENDER_KB,
    )


def handle_support_open_callback(update: Update, context: CallbackContext):
    q = update.callback_query
    if not q:
        return

    message = q.message
    if not message:
        q.answer()
        return

    user = q.from_user
    record = get_user_record(user)
    user_id = user.id

    if record.get("is_banned", False):
        q.answer()
        return

    if user_id in WAITING_SUPPORT:
        q.answer()
        message.reply_text(
            "المحادثة مع الدعم مفتوحة بالفعل.\n"
            "أرسل رسالتك مباشرة أو اضغط «🔚 إنهاء التواصل» عند الانتهاء.",
            reply_markup=SUPPORT_SESSION_KB,
        )
        return

    gender = record.get("gender")
    q.answer()

    if gender in ["male", "female"]:
        WAITING_SUPPORT.add(user_id)
        _send_support_session_opened_message(message.reply_text, gender)
        return

    WAITING_SUPPORT_GENDER.add(user_id)
    message.reply_text(
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
            "لوحة التحكم 🔧:\n"
            "• عرض عدد المستخدمين.\n"
            "• عرض قائمة المستخدمين.\n"
            "• إرسال رسالة جماعية.\n"
            "• عرض ترتيب المنافسة تفصيلياً.\n"
            "• حظر وفك حظر المستخدمين.\n"
            "• عرض قائمة المحظورين.\n"
            "• إدارة رسائل وأوقات الجرعة التحفيزية 💡.\n"
            "• التحكم في المنافسات والمجتمع (حذف نقاط وميداليات)."
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
        if str(uid_str) == str(GLOBAL_KEY):
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
            sent = context.bot.send_message(
                chat_id=ADMIN_ID,
                text=admin_msg,
                parse_mode="Markdown",
            )
            _remember_support_message(ADMIN_ID, sent, user.id)
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
            sent = context.bot.send_message(
                chat_id=SUPERVISOR_ID,
                text=supervisor_msg,
            )
            _remember_support_message(SUPERVISOR_ID, sent, user.id)
        except Exception as e:
            logger.error(f"Error sending support message to supervisor: {e}")


def _support_confirmation_text(gender: Optional[str], session_open: bool) -> str:
    is_female = gender == "female"

    if session_open:
        if is_female:
            return (
                "🤍 📨 تم إرسال رسالتك إلى الدعم النسائي (المشرفة).\n\n"
                "يمكنكِ متابعة الكتابة وإرسال رسائل أخرى،\n"
                "أو الضغط على «🔚 إنهاء التواصل» عند الانتهاء."
            )
        return (
            "🤍 📨 تم إرسال رسالتك إلى الدعم.\n\n"
            "يمكنك متابعة الكتابة وإرسال رسائل أخرى،\n"
            "أو الضغط على «🔚 إنهاء التواصل» عند الانتهاء."
        )

    if is_female:
        return "📨 تم إرسال رسالتك إلى الدعم النسائي (المشرفة) 🤍"

    return "📨 تم إرسال رسالتك إلى الدعم 🤍"


def _support_header(user: User) -> str:
    record = data.get(str(user.id), {})
    gender = record.get("gender")
    gender_label = "ذكر" if gender == "male" else "أنثى" if gender == "female" else "غير محدد"

    return (
        "📩 رسالة جديدة للدعم:\n\n"
        f"الاسم: {user.full_name}\n"
        f"اسم المستخدم: @{user.username if user.username else 'لا يوجد'}\n"
        f"ID: {user.id}\n"
        f"الجنس: {gender_label}"
    )


def _remember_support_message(admin_id: Optional[int], sent_message, target_user_id: int):
    if admin_id is None or sent_message is None:
        return

    try:
        SUPPORT_MSG_MAP[(admin_id, sent_message.message_id)] = target_user_id
    except Exception as e:
        logger.debug("تعذر حفظ ربط رسالة الدعم: %s", e)


def _extract_target_id_from_support_message(msg) -> Optional[int]:
    src = ""
    if msg.text:
        src = msg.text
    elif msg.caption:
        src = msg.caption
    else:
        return None

    m = re.search(r"ID:\s*`?(\d+)`?", src)
    return int(m.group(1)) if m else None


def handle_support_open_callback(update: Update, context: CallbackContext):
    q = update.callback_query
    if not q:
        return
    q.answer()

    user_id = q.from_user.id

    WAITING_SUPPORT.add(user_id)
    WAITING_SUPPORT_GENDER.discard(user_id)

    q.message.reply_text(
        "✅ تم فتح المحادثة مع الدعم الآن.\n"
        "يمكنك إرسال (نص/صورة/صوت/فيديو).\n"
        "ستبقى المحادثة مفتوحة حتى تضغط زر (🔚 إنهاء التواصل).",
        reply_markup=SUPPORT_SESSION_KB,
    )


def handle_support_admin_reply_any(update: Update, context: CallbackContext):
    msg = update.message
    if not msg or not msg.reply_to_message:
        return

    sender = msg.from_user
    sender_id = sender.id if sender else None
    if not sender_id:
        return

    if not (is_admin(sender_id) or is_supervisor(sender_id)):
        return

    replied_id = msg.reply_to_message.message_id
    replied_chat_id = getattr(msg.reply_to_message, "chat_id", None)
    if replied_chat_id is None:
        return
    bridge = STAFF_REPLY_BRIDGE.get((replied_chat_id, replied_id))
    if bridge:
        target_chat_id = bridge.get("user_chat_id")
        target_user_id = bridge.get("user_id")
        thread_id = bridge.get("thread_id")
        kind = bridge.get("kind")
        user_gender = bridge.get("user_gender")
        if not target_chat_id:
            return

        # 🔁 جهّز “فتح الشات” للمستخدم بعد الرد (عرض/فائدة)
        def _reopen_user_mode_after_staff_reply():
            try:
                if not target_user_id or not thread_id:
                    return
                if kind == "presentation":
                    WAITING_COURSE_PRESENTATION_MEDIA[target_user_id] = thread_id
                    if job_queue:
                        _schedule_presentation_media_timeout(
                            user_id=target_user_id,
                            chat_id=target_chat_id,
                            thread_id=thread_id,
                        )
                elif kind == "benefit":
                    # نعيد بناء سياق الفائدة من Firestore لضمان نفس سلوك المستخدم
                    if firestore_available():
                        tdoc = db.collection(COURSE_BENEFIT_THREADS_COLLECTION).document(thread_id).get()
                        if tdoc.exists:
                            t = tdoc.to_dict() or {}
                            WAITING_COURSE_BENEFIT_MEDIA[target_user_id] = {
                                "session_id": thread_id,
                                "thread_id": thread_id,
                                "user_id": t.get("user_id", target_user_id),
                                "user_name": t.get("user_name"),
                                "user_username": t.get("user_username"),
                                "user_gender": t.get("user_gender"),
                                "course_id": t.get("course_id"),
                                "course_title": t.get("course_title"),
                                "lesson_id": t.get("lesson_id"),
                                "lesson_title": t.get("lesson_title"),
                                "curriculum_section": t.get("curriculum_section"),
                            }
                            if job_queue:
                                _schedule_course_benefit_timeout(
                                    user_id=target_user_id,
                                    chat_id=target_chat_id,
                                    session_id=thread_id,
                                )
            except Exception as e:
                logger.debug("[STAFF_REPLY_BRIDGE] reopen mode failed: %s", e)

        if not user_gender and thread_id and kind in {"presentation", "benefit"}:
            try:
                collection = (
                    COURSE_PRESENTATIONS_THREADS_COLLECTION
                    if kind == "presentation"
                    else COURSE_BENEFIT_THREADS_COLLECTION
                )
                thread_doc = db.collection(collection).document(thread_id).get()
                if thread_doc.exists:
                    user_gender = (thread_doc.to_dict() or {}).get("user_gender")
            except Exception as e:
                logger.debug("[STAFF_REPLY_BRIDGE] failed to load gender: %s", e)
        if not user_gender and target_user_id:
            user_gender = (get_user_record_by_id(target_user_id) or {}).get("gender")

        staff_title = "المشرفة" if user_gender != "male" else "المشرف"
        prefix = "💬 رد من المشرفة/الأدمن"
        if kind == "presentation":
            prefix = f"💬 رد على العرض من {staff_title}"
        elif kind == "benefit":
            prefix = f"💬 رد على الفائدة من {staff_title}"

        reply_markup = None
        if kind in {"presentation", "benefit"} and bridge.get("course_id") and bridge.get("lesson_id"):
            button_label = "💬 اضغط للرد على المشرفة"
            if user_gender == "male":
                button_label = "💬 اضغط للرد على المشرف"
            callback_prefix = "COURSE:PRES:OPEN" if kind == "presentation" else "COURSE:BEN:OPEN"
            reply_markup = InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            button_label,
                            callback_data=f"{callback_prefix}:{bridge.get('course_id')}:{bridge.get('lesson_id')}",
                        )
                    ]
                ]
            )

        sent_to_user = None
        if msg.text:
            sent_to_user = context.bot.send_message(
                chat_id=target_chat_id,
                text=f"{prefix}:\n\n{msg.text}",
                reply_markup=reply_markup,
            )
        elif msg.photo:
            sent_to_user = context.bot.send_message(
                chat_id=target_chat_id, text=prefix, reply_markup=reply_markup
            )
            context.bot.send_photo(
                chat_id=target_chat_id,
                photo=msg.photo[-1].file_id,
                caption=msg.caption,
            )
        elif msg.voice:
            sent_to_user = context.bot.send_message(
                chat_id=target_chat_id, text=prefix, reply_markup=reply_markup
            )
            context.bot.send_voice(
                chat_id=target_chat_id,
                voice=msg.voice.file_id,
                caption=msg.caption,
            )
        elif msg.audio:
            sent_to_user = context.bot.send_message(
                chat_id=target_chat_id, text=prefix, reply_markup=reply_markup
            )
            context.bot.send_audio(
                chat_id=target_chat_id,
                audio=msg.audio.file_id,
                caption=msg.caption,
            )
        elif msg.document:
            sent_to_user = context.bot.send_message(
                chat_id=target_chat_id, text=prefix, reply_markup=reply_markup
            )
            context.bot.send_document(
                chat_id=target_chat_id,
                document=msg.document.file_id,
                caption=msg.caption,
            )
        elif msg.video_note:
            sent_to_user = context.bot.send_message(
                chat_id=target_chat_id, text=prefix, reply_markup=reply_markup
            )
            context.bot.send_video_note(
                chat_id=target_chat_id, video_note=msg.video_note.file_id
            )
        else:
            msg.reply_text("⚠️ نوع الرسالة غير مدعوم.")
            return

        _reopen_user_mode_after_staff_reply()
        msg.reply_text("✅ تم إرسال الرد للمتعلم.")
        if (
            sent_to_user
            and is_supervisor(sender_id)
            and ADMIN_ID is not None
            and ADMIN_ID != sender_id
        ):
            try:
                if msg.text:
                    context.bot.send_message(
                        chat_id=ADMIN_ID,
                        text=f"{prefix}:\n\n{msg.text}",
                    )
                elif msg.photo:
                    context.bot.send_message(chat_id=ADMIN_ID, text=prefix)
                    context.bot.send_photo(
                        chat_id=ADMIN_ID,
                        photo=msg.photo[-1].file_id,
                        caption=msg.caption,
                    )
                elif msg.voice:
                    context.bot.send_message(chat_id=ADMIN_ID, text=prefix)
                    context.bot.send_voice(
                        chat_id=ADMIN_ID,
                        voice=msg.voice.file_id,
                        caption=msg.caption,
                    )
                elif msg.audio:
                    context.bot.send_message(chat_id=ADMIN_ID, text=prefix)
                    context.bot.send_audio(
                        chat_id=ADMIN_ID,
                        audio=msg.audio.file_id,
                        caption=msg.caption,
                    )
                elif msg.document:
                    context.bot.send_message(chat_id=ADMIN_ID, text=prefix)
                    context.bot.send_document(
                        chat_id=ADMIN_ID,
                        document=msg.document.file_id,
                        caption=msg.caption,
                    )
                elif msg.video_note:
                    context.bot.send_message(chat_id=ADMIN_ID, text=prefix)
                    context.bot.send_video_note(
                        chat_id=ADMIN_ID, video_note=msg.video_note.file_id
                    )
            except Exception as e:
                logger.error("Error mirroring supervisor reply to admin: %s", e)
        return

    user = sender

    target_id = _extract_target_id_from_support_message(msg.reply_to_message)
    if not target_id:
        target_id = SUPPORT_MSG_MAP.get((user.id, msg.reply_to_message.message_id))
    if not target_id:
        return

    reply_prefix = "💌 رد من الدعم"
    if is_supervisor(user.id):
        reply_prefix = "💌 رد من المشرفة"

    reply_markup = None if (target_id in WAITING_SUPPORT) else SUPPORT_REPLY_INLINE_KB

    try:
        if msg.text:
            context.bot.send_message(
                chat_id=target_id,
                text=f"{reply_prefix}:\n\n{msg.text}",
                reply_markup=reply_markup,
            )
        elif msg.photo:
            context.bot.send_photo(
                chat_id=target_id,
                photo=msg.photo[-1].file_id,
                caption=msg.caption or reply_prefix,
                reply_markup=reply_markup,
            )
        elif msg.video:
            context.bot.send_video(
                chat_id=target_id,
                video=msg.video.file_id,
                caption=msg.caption or reply_prefix,
                reply_markup=reply_markup,
            )
        elif msg.voice:
            context.bot.send_voice(
                chat_id=target_id,
                voice=msg.voice.file_id,
                caption=msg.caption or reply_prefix,
                reply_markup=reply_markup,
            )
        elif msg.audio:
            context.bot.send_audio(
                chat_id=target_id,
                audio=msg.audio.file_id,
                caption=msg.caption or reply_prefix,
                reply_markup=reply_markup,
            )
        elif msg.video_note:
            context.bot.send_video_note(
                chat_id=target_id,
                video_note=msg.video_note.file_id,
                reply_markup=reply_markup,
            )
        else:
            return
    except Exception as e:
        logger.error(f"Error sending support reply to {target_id}: {e}")
        return

    try:
        ack_markup = (
            admin_panel_keyboard_for(user.id)
            if is_admin(user.id)
            else user_main_keyboard(user.id)
        )
        msg.reply_text("تم إرسال ردّك للمستخدم.", reply_markup=ack_markup)
    except Exception as e:
        logger.error(f"Error sending ack for support reply: {e}")

    if is_supervisor(user.id) and ADMIN_ID is not None:
        target_record = get_user_record_by_id(target_id) or {}
        if target_record.get("gender") == "female":
            try:
                if msg.text:
                    context.bot.send_message(
                        chat_id=ADMIN_ID,
                        text=(
                            "📨 نسخة من رد المشرفة:\n\n"
                            f"إلى ID: {target_id}\n"
                            f"نص الرد:\n{msg.text}"
                        ),
                    )
                elif msg.photo:
                    context.bot.send_photo(
                        chat_id=ADMIN_ID,
                        photo=msg.photo[-1].file_id,
                        caption=msg.caption
                        or f"نسخة من رد المشرفة إلى ID: {target_id}",
                    )
                elif msg.video:
                    context.bot.send_video(
                        chat_id=ADMIN_ID,
                        video=msg.video.file_id,
                        caption=msg.caption
                        or f"نسخة من رد المشرفة إلى ID: {target_id}",
                    )
                elif msg.voice:
                    context.bot.send_voice(
                        chat_id=ADMIN_ID,
                        voice=msg.voice.file_id,
                        caption=msg.caption
                        or f"نسخة من رد المشرفة إلى ID: {target_id}",
                    )
                elif msg.audio:
                    context.bot.send_audio(
                        chat_id=ADMIN_ID,
                        audio=msg.audio.file_id,
                        caption=msg.caption
                        or f"نسخة من رد المشرفة إلى ID: {target_id}",
                    )
                elif msg.video_note:
                    context.bot.send_video_note(
                        chat_id=ADMIN_ID,
                        video_note=msg.video_note.file_id,
                    )
            except Exception as e:
                logger.error(f"Error sending supervisor reply copy to admin: {e}")


def _is_reply_to_support_message(msg, bot_id: int) -> bool:
    if not msg or not msg.reply_to_message:
        return False
    if msg.reply_to_message.from_user.id != bot_id:
        return False
    src = (msg.reply_to_message.text or msg.reply_to_message.caption or "").strip()
    return (
        src.startswith("💌 رد من الدعم")
        or src.startswith("📢 رسالة من الدعم")
        or src.startswith("💌 رد من المشرفة")
        or "رسالتك وصلت للدعم" in src
    )


def handle_support_photo(update: Update, context: CallbackContext):
    user = update.effective_user
    if not _user_in_support_session(user):
        user_id = user.id if user else None
        is_reply = _is_reply_to_support_message(update.message, context.bot.id)
        if user_id and is_reply and not (is_admin(user_id) or is_supervisor(user_id)):
            update.message.reply_text(
                "للتواصل مع الدعم اضغط على زر التواصل مع الدعم فقط.",
                reply_markup=user_main_keyboard(user_id),
            )
        return  # لا تمس أي مسار آخر

    user_id = user.id
    is_reply = _is_reply_to_support_message(update.message, context.bot.id)

    photos = update.message.photo or []

    # ✅ إذا ما كانت Photo، جرّب Document (صورة بدون ضغط)
    doc = getattr(update.message, "document", None)
    if (not photos) and doc and (doc.mime_type or "").startswith("image/"):
        # نعاملها كصورة/ملف صورة
        best_file_id = doc.file_id
        caption = update.message.caption or ""
        text = _support_header(user) + (f"\n\n📝 تعليق المستخدم:\n{caption}" if caption else "")

        record = data.get(str(user_id), {})
        gender = record.get("gender")

        if gender == "female":
            targets = [admin_id for admin_id in [SUPERVISOR_ID, ADMIN_ID] if admin_id]
        else:
            targets = [ADMIN_ID] if ADMIN_ID else []

        for admin_id in targets:
            try:
                sent = context.bot.send_document(chat_id=admin_id, document=best_file_id, caption=text)
                _remember_support_message(admin_id, sent, user_id)
            except Exception as e:
                logger.exception("Support image-document forward failed", exc_info=e)

        update.message.reply_text(
            _support_confirmation_text(record.get("gender"), True),
            reply_markup=SUPPORT_SESSION_KB,
        )
        raise DispatcherHandlerStop()

    # ✅ المسار العادي للـ Photo
    if not photos:
        return

    best_photo = photos[-1]
    caption = update.message.caption or ""
    text = _support_header(user) + (f"\n\n📝 تعليق المستخدم:\n{caption}" if caption else "")

    record = data.get(str(user_id), {})
    gender = record.get("gender")

    if gender == "female":
        targets = [admin_id for admin_id in [SUPERVISOR_ID, ADMIN_ID] if admin_id]
    else:
        targets = [ADMIN_ID] if ADMIN_ID else []

    for admin_id in targets:
        try:
            sent = context.bot.send_photo(chat_id=admin_id, photo=best_photo.file_id, caption=text)
            _remember_support_message(admin_id, sent, user_id)
        except Exception as e:
            logger.warning(f"Support photo forward failed to {admin_id}: {e}")

    update.message.reply_text(
        _support_confirmation_text(record.get("gender"), True),
        reply_markup=SUPPORT_SESSION_KB,
    )
    raise DispatcherHandlerStop()


def handle_support_audio(update: Update, context: CallbackContext):
    user = update.effective_user
    if not _user_in_support_session(user):
        user_id = user.id if user else None
        is_reply = _is_reply_to_support_message(update.message, context.bot.id)
        if user_id and is_reply and not (is_admin(user_id) or is_supervisor(user_id)):
            update.message.reply_text(
                "للتواصل مع الدعم اضغط على زر التواصل مع الدعم فقط.",
                reply_markup=user_main_keyboard(user_id),
            )
        return  # لا تمس أي مسار آخر

    user_id = user.id
    is_reply = _is_reply_to_support_message(update.message, context.bot.id)

    audio = update.message.audio or update.message.voice
    if not audio:
        return

    caption = update.message.caption or ""
    text = _support_header(user) + (f"\n\n📝 تعليق المستخدم:\n{caption}" if caption else "")

    record = data.get(str(user_id), {})
    gender = record.get("gender")

    if gender == "female":
        targets = [admin_id for admin_id in [SUPERVISOR_ID, ADMIN_ID] if admin_id]
    else:
        targets = [ADMIN_ID] if ADMIN_ID else []

    for admin_id in targets:
        try:
            if update.message.voice:
                sent = context.bot.send_voice(chat_id=admin_id, voice=audio.file_id, caption=text)
            else:
                sent = context.bot.send_audio(chat_id=admin_id, audio=audio.file_id, caption=text)
            _remember_support_message(admin_id, sent, user_id)
        except Exception as e:
            logger.warning(f"Support audio forward failed to {admin_id}: {e}")

    update.message.reply_text(
        _support_confirmation_text(record.get("gender"), True),
        reply_markup=SUPPORT_SESSION_KB,
    )
    raise DispatcherHandlerStop()


def handle_support_video(update: Update, context: CallbackContext):
    user = update.effective_user
    if not _user_in_support_session(user):
        user_id = user.id if user else None
        is_reply = _is_reply_to_support_message(update.message, context.bot.id)
        if user_id and is_reply and not (is_admin(user_id) or is_supervisor(user_id)):
            update.message.reply_text(
                "للتواصل مع الدعم اضغط على زر التواصل مع الدعم فقط.",
                reply_markup=user_main_keyboard(user_id),
            )
        return  # لا تمس أي مسار آخر

    user_id = user.id
    is_reply = _is_reply_to_support_message(update.message, context.bot.id)

    video = update.message.video
    if not video:
        return

    caption = update.message.caption or ""
    text = _support_header(user) + (f"\n\n📝 تعليق المستخدم:\n{caption}" if caption else "")

    record = data.get(str(user_id), {})
    gender = record.get("gender")

    if gender == "female":
        targets = [admin_id for admin_id in [SUPERVISOR_ID, ADMIN_ID] if admin_id]
    else:
        targets = [ADMIN_ID] if ADMIN_ID else []

    for admin_id in targets:
        try:
            sent = context.bot.send_video(
                chat_id=admin_id,
                video=video.file_id,
                caption=text
            )
            _remember_support_message(admin_id, sent, user_id)
        except Exception as e:
            logger.warning(f"Support video forward failed to {admin_id}: {e}")

    update.message.reply_text(
        _support_confirmation_text(record.get("gender"), True),
        reply_markup=SUPPORT_SESSION_KB,
    )
    raise DispatcherHandlerStop()


def handle_support_video_note(update: Update, context: CallbackContext):
    user = update.effective_user
    if not _user_in_support_session(user):
        user_id = user.id if user else None
        is_reply = _is_reply_to_support_message(update.message, context.bot.id)
        if user_id and is_reply and not (is_admin(user_id) or is_supervisor(user_id)):
            update.message.reply_text(
                "للتواصل مع الدعم اضغط على زر التواصل مع الدعم فقط.",
                reply_markup=user_main_keyboard(user_id),
            )
        return

    user_id = user.id
    is_reply = _is_reply_to_support_message(update.message, context.bot.id)

    video_note = update.message.video_note
    if not video_note:
        return

    text = _support_header(user)

    record = data.get(str(user_id), {})
    gender = record.get("gender")

    if gender == "female":
        targets = [admin_id for admin_id in [SUPERVISOR_ID, ADMIN_ID] if admin_id]
    else:
        targets = [ADMIN_ID] if ADMIN_ID else []

    for admin_id in targets:
        try:
            context.bot.send_message(chat_id=admin_id, text=text)
            context.bot.send_video_note(chat_id=admin_id, video_note=video_note.file_id)
        except Exception as e:
            logger.warning(f"Support video note forward failed to {admin_id}: {e}")

    update.message.reply_text(
        _support_confirmation_text(record.get("gender"), True),
        reply_markup=SUPPORT_SESSION_KB,
    )
    raise DispatcherHandlerStop()

# =================== دوال جديدة للميزات المطلوبة ===================

# حالات الانتظار الجديدة
WAITING_MANAGE_POINTS_USER_ID = set()
WAITING_MANAGE_POINTS_ACTION = {}  # user_id -> target_user_id
WAITING_MANAGE_POINTS_VALUE = set()

def get_user_record_by_id(user_id: int) -> Dict:
    """الحصول على سجل المستخدم بناءً على المعرف"""
    user_id_str = str(user_id)
    if not firestore_available():
        return data.get(user_id_str)
    try:
        doc_ref = db.collection(USERS_COLLECTION).document(user_id_str)
        doc = doc_ref.get()
        if doc.exists:
            record = doc.to_dict()
            ensure_water_defaults(record)
            data[user_id_str] = record
            ensure_medal_defaults(record)
            return record
        return None
    except Exception as e:
        logger.error(f"خطأ في الحصول على سجل المستخدم {user_id}: {e}")
        return data.get(user_id_str)


def handle_supervisor_new_users(update: Update, context: CallbackContext):
    """عرض الحسابات الجديدة للمشرفة"""
    user = update.effective_user
    if not is_supervisor(user.id):
        return
    all_users = get_all_user_ids()
    if not all_users:
        update.message.reply_text("لا توجد حسابات مسجلة.", reply_markup=SUPERVISOR_PANEL_KB)
        return
    users_with_dates = []
    for uid in all_users:
        record = get_user_record_by_id(uid)
        if record:
            created_at = record.get("created_at", "")
            users_with_dates.append((uid, record, created_at))
    users_with_dates.sort(key=lambda x: x[2], reverse=True)
    latest_users = users_with_dates[:50]
    if not latest_users:
        update.message.reply_text("لا توجد بيانات.", reply_markup=SUPERVISOR_PANEL_KB)
        return
    message = "📊 الحسابات الجديدة (آخر 50):\n\n"
    for idx, (uid, record, created_at) in enumerate(latest_users, 1):
        first_name = record.get("first_name", "مجهول")
        username = record.get("username", "-")
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(created_at)
            date_str = dt.strftime("%Y-%m-%d %H:%M")
        except:
            date_str = created_at
        message += f"{idx}. **ID:** `{uid}` | **{first_name}** | @{username} | {date_str}\n"
    if len(message) > 4096:
        update.message.reply_text(message[:4000], reply_markup=SUPERVISOR_PANEL_KB, parse_mode="Markdown")
    else:
        update.message.reply_text(message, reply_markup=SUPERVISOR_PANEL_KB, parse_mode="Markdown")

# =================== هاندلر الرسالل ===================


def handle_text(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id

    msg = update.message
    text = (msg.text or "").strip()

    record = get_user_record(user) or {}
    fresh_record = get_user_record_by_id(user_id) or record
    in_admin_books_mode = _ensure_is_admin_or_supervisor(user_id) and context.user_data.get("books_admin_mode")

    if user_id in WAITING_BOOK_EDIT_FIELD:
        ctx = BOOK_EDIT_CONTEXT.get(user_id, {})
        book_id = ctx.get("book_id")
        field = ctx.get("field")
        route = ctx.get("route")
        if not book_id or not field:
            WAITING_BOOK_EDIT_FIELD.discard(user_id)
            BOOK_EDIT_CONTEXT.pop(user_id, None)
            msg.reply_text("حدث خطأ أثناء التعديل.", reply_markup=BOOKS_ADMIN_MENU_KB)
            return
        update_data = {}
        if field == "tags":
            update_data["tags"] = _parse_tags_input(text)
        elif field == "description" and text.strip().lower() in {"تخطي", "skip"}:
            update_data["description"] = ""
        else:
            update_data[field] = text
        update_book_record(book_id, **update_data)
        WAITING_BOOK_EDIT_FIELD.discard(user_id)
        BOOK_EDIT_CONTEXT.pop(user_id, None)
        msg.reply_text("تم تحديث البيانات.", reply_markup=BOOKS_ADMIN_MENU_KB)
        try:
            _send_admin_book_detail(update, context, book_id, route)
        except Exception:
            pass
        return

    if in_admin_books_mode:
        if text == "🔎 بحث إداري":
            prompt_admin_books_search_text(update, context)
            return

        if text == "🔙 رجوع":
            # رجوع لقائمة إدارة الكتب
            open_books_admin_menu(update, context)
            return

    # ✅ بحث مكتبة طالب العلم: يعتمد على Firestore
    if not in_admin_books_mode and (user_id in WAITING_BOOK_SEARCH or fresh_record.get("book_search_waiting", False)):
        WAITING_BOOK_SEARCH.discard(user_id)
        logger.info("[BOOKS][SEARCH_ROUTE] user=%s text=%r", user_id, text)
        handle_book_search_input(update, context)
        return
    
    # التحقق إذا كان المستخدم محظورًا في بداية كل رسالة
    if record.get("is_banned", False):
        # السماح فقط بالرد على رسائل الدعم إذا كان محظوراً
        if msg.reply_to_message and msg.reply_to_message.from_user.id == context.bot.id:
            original = (msg.reply_to_message.text or msg.reply_to_message.caption or "").strip()
            if "لقد تم حظرك" in original or "رد من الدعم" in original or "رد من المشرفة" in original:
                forward_support_to_admin(user, text, context)
                msg.reply_text(
                    _support_confirmation_text(record.get("gender"), False),
                )
                return
        
        # منع أي استخدام آخر للبوت
        return

    main_kb = user_main_keyboard(user_id)
    support_session_active = user_id in WAITING_SUPPORT

    if user_id in WAITING_WATER_ADD_CUPS and not text.isdigit() and text != BTN_WATER_ADD_CUPS:
        WAITING_WATER_ADD_CUPS.discard(user_id)

    # بيانات التسجيل في الدورات
    if user_id in WAITING_COURSE_COUNTRY:
        if text == BTN_CANCEL:
            _reset_course_subscription_flow(user_id)
            msg.reply_text("تم إلغاء التسجيل في الدورة.", reply_markup=COURSES_USER_MENU_KB)
            return

        COURSE_SUBSCRIPTION_CONTEXT.setdefault(user_id, {})["country"] = text
        WAITING_COURSE_COUNTRY.discard(user_id)
        saved_name = _get_saved_course_full_name(user_id)
        if saved_name:
            COURSE_SUBSCRIPTION_CONTEXT[user_id]["full_name"] = saved_name
            WAITING_COURSE_AGE.add(user_id)
            msg.reply_text("كم عمرك؟", reply_markup=ReplyKeyboardRemove())
        else:
            WAITING_COURSE_FULL_NAME.add(user_id)
            msg.reply_text(
                "ادخل اسمك الكامل الذي توده أن يظهر على الشهادة",
                reply_markup=ReplyKeyboardMarkup([[KeyboardButton(BTN_CANCEL)]], resize_keyboard=True),
            )
        return

    if user_id in WAITING_COURSE_FULL_NAME:
        if text == BTN_CANCEL:
            _reset_course_subscription_flow(user_id)
            msg.reply_text("تم إلغاء التسجيل في الدورة.", reply_markup=COURSES_USER_MENU_KB)
            return

        full_name_value = text.strip()
        if not full_name_value:
            msg.reply_text(
                "⚠️ الرجاء إدخال اسم كامل صالح.",
                reply_markup=ReplyKeyboardMarkup([[KeyboardButton(BTN_CANCEL)]], resize_keyboard=True),
            )
            return

        ctx = COURSE_SUBSCRIPTION_CONTEXT.setdefault(user_id, {})
        ctx["full_name"] = full_name_value
        WAITING_COURSE_FULL_NAME.discard(user_id)
        if ctx.get("age") is not None and ctx.get("gender"):
            WAITING_COURSE_AGE.discard(user_id)
            WAITING_COURSE_GENDER.discard(user_id)
            _finalize_course_subscription(user, context)
        elif ctx.get("age") is not None:
            WAITING_COURSE_AGE.discard(user_id)
            WAITING_COURSE_GENDER.add(user_id)
            msg.reply_text("اختر الجنس:", reply_markup=GENDER_KB)
        else:
            WAITING_COURSE_AGE.add(user_id)
            msg.reply_text("كم عمرك؟", reply_markup=ReplyKeyboardRemove())
        return

    if user_id in WAITING_COURSE_AGE:
        if text == BTN_CANCEL:
            _reset_course_subscription_flow(user_id)
            msg.reply_text("تم إلغاء التسجيل في الدورة.", reply_markup=COURSES_USER_MENU_KB)
            return

        if not text.isdigit():
            msg.reply_text("⚠️ أرسل عمرك كرقم صحيح.", reply_markup=ReplyKeyboardRemove())
            return

        age_val = int(text)
        if age_val <= 0 or age_val > 120:
            msg.reply_text("⚠️ الرجاء إدخال عمر صالح.", reply_markup=ReplyKeyboardRemove())
            return

        COURSE_SUBSCRIPTION_CONTEXT.setdefault(user_id, {})["age"] = age_val
        WAITING_COURSE_AGE.discard(user_id)
        WAITING_COURSE_GENDER.add(user_id)
        msg.reply_text("اختر الجنس:", reply_markup=GENDER_KB)
        return

    if user_id in WAITING_COURSE_GENDER:
        if text == BTN_CANCEL:
            _reset_course_subscription_flow(user_id)
            msg.reply_text("تم إلغاء التسجيل في الدورة.", reply_markup=COURSES_USER_MENU_KB)
            return

        if text == BTN_GENDER_MALE:
            COURSE_SUBSCRIPTION_CONTEXT.setdefault(user_id, {})["gender"] = "male"
        elif text == BTN_GENDER_FEMALE:
            COURSE_SUBSCRIPTION_CONTEXT.setdefault(user_id, {})["gender"] = "female"
        else:
            msg.reply_text("رجاءً اختر من الأزرار الموجودة 👇", reply_markup=GENDER_KB)
            return

        WAITING_COURSE_GENDER.discard(user_id)
        _finalize_course_subscription(user, context)
        return

    if user_id in WAITING_PROFILE_EDIT_NAME:
        if text == BTN_CANCEL:
            _reset_profile_edit_flow(user_id)
            msg.reply_text("تم إلغاء تعديل البيانات.", reply_markup=user_main_keyboard(user_id))
            return

        name_value = text.strip()
        if not name_value:
            msg.reply_text(
                "⚠️ الرجاء إدخال اسم كامل صالح.",
                reply_markup=ReplyKeyboardMarkup([[KeyboardButton(BTN_CANCEL)]], resize_keyboard=True),
            )
            return

        PROFILE_EDIT_CONTEXT.setdefault(user_id, {})["full_name"] = name_value
        WAITING_PROFILE_EDIT_NAME.discard(user_id)
        WAITING_PROFILE_EDIT_AGE.add(user_id)
        current_age = PROFILE_EDIT_CONTEXT[user_id].get("age")
        age_hint = f"العمر الحالي: {current_age}" if current_age is not None else "العمر غير محدد"
        msg.reply_text(f"{age_hint}\n\nكم عمرك الآن؟", reply_markup=ReplyKeyboardRemove())
        return

    if user_id in WAITING_PROFILE_EDIT_AGE:
        if text == BTN_CANCEL:
            _reset_profile_edit_flow(user_id)
            msg.reply_text("تم إلغاء تعديل البيانات.", reply_markup=user_main_keyboard(user_id))
            return

        if not text.isdigit():
            msg.reply_text("⚠️ أرسل عمرك كرقم صحيح.", reply_markup=ReplyKeyboardRemove())
            return

        age_val = int(text)
        if age_val <= 0 or age_val > 120:
            msg.reply_text("⚠️ الرجاء إدخال عمر صالح.", reply_markup=ReplyKeyboardRemove())
            return

        PROFILE_EDIT_CONTEXT.setdefault(user_id, {})["age"] = age_val
        WAITING_PROFILE_EDIT_AGE.discard(user_id)
        WAITING_PROFILE_EDIT_COUNTRY.add(user_id)
        current_country = PROFILE_EDIT_CONTEXT[user_id].get("country") or "غير محدد"
        msg.reply_text(
            f"الدولة الحالية: {current_country}\n\nاكتب دولتك الآن.",
            reply_markup=ReplyKeyboardMarkup([[KeyboardButton(BTN_CANCEL)]], resize_keyboard=True),
        )
        return

    if user_id in WAITING_PROFILE_EDIT_COUNTRY:
        if text == BTN_CANCEL:
            _reset_profile_edit_flow(user_id)
            msg.reply_text("تم إلغاء تعديل البيانات.", reply_markup=user_main_keyboard(user_id))
            return

        country_val = text.strip()
        if not country_val:
            msg.reply_text(
                "⚠️ الرجاء إدخال اسم دولة صحيح.",
                reply_markup=ReplyKeyboardMarkup([[KeyboardButton(BTN_CANCEL)]], resize_keyboard=True),
            )
            return

        PROFILE_EDIT_CONTEXT.setdefault(user_id, {})["country"] = country_val
        WAITING_PROFILE_EDIT_COUNTRY.discard(user_id)
        _finalize_profile_edit(user_id, msg.chat_id, context)
        return

    # إجابات الاختبارات الخاصة بالدورات
    if user_id in WAITING_QUIZ_ANSWER:
        if _complete_quiz_answer(user_id, text, update, context):
            return

    # إنشاء دورة جديدة
    if user_id in WAITING_NEW_COURSE:
        if not (is_admin(user_id) or is_supervisor(user_id)):
            _reset_course_creation(user_id)
            msg.reply_text(
                "❌ ليس لديك صلاحية لإنشاء الدورات.",
                reply_markup=COURSES_ADMIN_MENU_KB,
            )
            return

        if text == BTN_CANCEL:
            _reset_course_creation(user_id)
            msg.reply_text("تم الإلغاء بنجاح", reply_markup=COURSES_ADMIN_MENU_KB)
            return

        course_name = text.strip()
        if not course_name:
            msg.reply_text(
                "⚠️ اسم الدورة لا يمكن أن يكون فارغاً.",
                reply_markup=_course_creation_keyboard(),
            )
            return

        if len(course_name) < COURSE_NAME_MIN_LENGTH:
            msg.reply_text(
                f"⚠️ اسم الدورة قصير جداً. الحد الأدنى {COURSE_NAME_MIN_LENGTH} حروف.",
                reply_markup=_course_creation_keyboard(),
            )
            return

        if len(course_name) > COURSE_NAME_MAX_LENGTH:
            msg.reply_text(
                f"⚠️ اسم الدورة طويل جداً. الحد الأقصى {COURSE_NAME_MAX_LENGTH} حرفاً.",
                reply_markup=_course_creation_keyboard(),
            )
            return

        normalized = course_name.lower()
        try:
            existing = list(
                db.collection(COURSES_COLLECTION)
                .where("name_lower", "==", normalized)
                .stream()
            )
            if not existing:
                existing = list(
                    db.collection(COURSES_COLLECTION)
                    .where("name", "==", course_name)
                    .stream()
                )
            if existing:
                msg.reply_text(
                    "⚠️ توجد دورة بنفس الاسم بالفعل. استخدم اسماً مختلفاً.",
                    reply_markup=_course_creation_keyboard(),
                )
                return

            db.collection(COURSES_COLLECTION).add(
                {
                    "name": course_name,
                    "name_lower": normalized,
                    "description": COURSE_CREATION_CONTEXT.get(user_id, {}).get(
                        "description", ""
                    ),
                    "status": "active",
                    "created_at": firestore.SERVER_TIMESTAMP,
                }
            )
            _reset_course_creation(user_id)
            msg.reply_text(
                f"✅ تم إنشاء دورة ({course_name}) بنجاح",
                reply_markup=COURSES_ADMIN_MENU_KB,
            )
        except Exception as e:
            logger.error(f"خطأ في إنشاء الدورة: {e}")
            _reset_course_creation(user_id)
            msg.reply_text(
                "❌ تعذر إنشاء الدورة حالياً.",
                reply_markup=COURSES_ADMIN_MENU_KB,
            )
        return

    # إنشاء درس جديد
    if user_id in WAITING_LESSON_TITLE:
        ctx = LESSON_CREATION_CONTEXT.get(user_id, {}) or {}
        course_id = ctx.get("course_id")
        lesson_id = ctx.get("lesson_id")
        edit_action = ctx.get("edit_action")
        if text == BTN_CANCEL:
            _reset_lesson_creation(user_id)
            msg.reply_text("تم الإلغاء.", reply_markup=_lessons_back_keyboard(course_id))
            return

        if edit_action == "edit_title":
            try:
                doc_ref = db.collection(COURSE_LESSONS_COLLECTION).document(lesson_id)
                if not doc_ref.get().exists:
                    msg.reply_text("❌ الدرس غير موجود.", reply_markup=_lessons_back_keyboard(course_id))
                else:
                    doc_ref.update(
                        {
                            "title": text,
                            "updated_at": firestore.SERVER_TIMESTAMP,
                        }
                    )
                    msg.reply_text("✅ تم تعديل العنوان.", reply_markup=_lessons_back_keyboard(course_id))
            except Exception as e:
                logger.error(f"خطأ في تعديل عنوان الدرس: {e}")
                msg.reply_text("❌ تعذر تعديل العنوان حالياً.", reply_markup=_lessons_back_keyboard(course_id))
            finally:
                _reset_lesson_creation(user_id)
            return

        LESSON_CREATION_CONTEXT.setdefault(user_id, {})["title"] = text
        WAITING_LESSON_TITLE.discard(user_id)
        lesson_type_kb = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("📝 نص", callback_data=f"COURSES:lesson_type_text_{course_id}")],
                [InlineKeyboardButton("🔊 ملف صوتي", callback_data=f"COURSES:lesson_type_audio_{course_id}")],
                [InlineKeyboardButton("🔗 رابط", callback_data=f"COURSES:lesson_type_link_{course_id}")],
                [InlineKeyboardButton("🔙 رجوع", callback_data=f"COURSES:lessons_{course_id}")],
            ]
        )
        msg.reply_text("اختر نوع محتوى الدرس:", reply_markup=lesson_type_kb)
        return

    if user_id in WAITING_LESSON_CONTENT:
        ctx = LESSON_CREATION_CONTEXT.get(user_id, {}) or {}
        course_id = ctx.get("course_id")
        content_type = ctx.get("content_type")
        title = ctx.get("title")
        lesson_id = ctx.get("lesson_id")
        edit_action = ctx.get("edit_action")
        if text == BTN_CANCEL:
            _reset_lesson_creation(user_id)
            msg.reply_text("تم الإلغاء.", reply_markup=_lessons_back_keyboard(course_id))
            return

        if not course_id or not title or content_type not in {"text", "link"}:
            _reset_lesson_creation(user_id)
            msg.reply_text("❌ البيانات غير مكتملة.", reply_markup=COURSES_ADMIN_MENU_KB)
            return

        if edit_action == "edit_content":
            if not lesson_id:
                _reset_lesson_creation(user_id)
                msg.reply_text("❌ الدرس غير معروف.", reply_markup=COURSES_ADMIN_MENU_KB)
                return
            _update_lesson(
                user_id,
                lesson_id,
                course_id,
                title,
                content_type,
                msg,
                content_value=text,
            )
        else:
            _save_lesson(user_id, course_id, title, content_type, msg, text)
        return

    if user_id in WAITING_LESSON_CURRICULUM_NAME:
        ctx = LESSON_CREATION_CONTEXT.get(user_id, {}) or {}
        course_id = ctx.get("course_id")
        lesson_id = ctx.get("lesson_id")
        if text == BTN_CANCEL:
            _reset_lesson_creation(user_id)
            msg.reply_text("تم الإلغاء.", reply_markup=_lessons_back_keyboard(course_id))
            return
        try:
            db.collection(COURSE_LESSONS_COLLECTION).document(lesson_id).update(
                {"curriculum_section": text, "updated_at": firestore.SERVER_TIMESTAMP}
            )
            msg.reply_text("✅ تم حفظ باب المقرر للدرس.", reply_markup=_lessons_back_keyboard(course_id))
        except Exception as e:
            logger.error(f"خطأ في حفظ باب المقرر: {e}")
            msg.reply_text("❌ تعذر الحفظ حالياً.", reply_markup=_lessons_back_keyboard(course_id))
        finally:
            _reset_lesson_creation(user_id)
        return

    # إنشاء اختبار جديد
    if user_id in WAITING_QUIZ_TITLE:
        course_id = QUIZ_CREATION_CONTEXT.get(user_id, {}).get("course_id")
        if text == BTN_CANCEL:
            _reset_quiz_creation(user_id)
            msg.reply_text("تم الإلغاء.", reply_markup=_quizzes_back_keyboard(course_id))
            return

        QUIZ_CREATION_CONTEXT.setdefault(user_id, {})["title"] = text
        WAITING_QUIZ_TITLE.discard(user_id)
        WAITING_QUIZ_QUESTION.add(user_id)
        msg.reply_text(
            "✏️ اكتب سؤال الاختبار الآن.",
            reply_markup=_quizzes_back_keyboard(course_id),
        )
        return

    if user_id in WAITING_QUIZ_QUESTION:
        course_id = QUIZ_CREATION_CONTEXT.get(user_id, {}).get("course_id")
        if text == BTN_CANCEL:
            _reset_quiz_creation(user_id)
            msg.reply_text("تم الإلغاء.", reply_markup=_quizzes_back_keyboard(course_id))
            return

        QUIZ_CREATION_CONTEXT.setdefault(user_id, {})["question"] = text
        QUIZ_CREATION_CONTEXT.setdefault(user_id, {}).setdefault("answers", [])
        WAITING_QUIZ_QUESTION.discard(user_id)
        WAITING_QUIZ_ANSWER_TEXT.add(user_id)
        msg.reply_text(
            "اكتب الإجابة الأولى.",
            reply_markup=_quizzes_back_keyboard(course_id),
        )
        return

    if user_id in WAITING_QUIZ_ANSWER_TEXT:
        course_id = QUIZ_CREATION_CONTEXT.get(user_id, {}).get("course_id")
        if text == BTN_CANCEL:
            _reset_quiz_creation(user_id)
            msg.reply_text("تم الإلغاء.", reply_markup=_quizzes_back_keyboard(course_id))
            return

        QUIZ_CREATION_CONTEXT.setdefault(user_id, {})["pending_answer_text"] = text
        WAITING_QUIZ_ANSWER_TEXT.discard(user_id)
        WAITING_QUIZ_ANSWER_POINTS.add(user_id)
        msg.reply_text(
            "كم عدد النقاط لهذه الإجابة؟",
            reply_markup=_quizzes_back_keyboard(course_id),
        )
        return

    if user_id in WAITING_QUIZ_ANSWER_POINTS:
        course_id = QUIZ_CREATION_CONTEXT.get(user_id, {}).get("course_id")
        ctx = QUIZ_CREATION_CONTEXT.setdefault(user_id, {})
        if text == BTN_CANCEL:
            _reset_quiz_creation(user_id)
            msg.reply_text("تم الإلغاء.", reply_markup=_quizzes_back_keyboard(course_id))
            return

        try:
            points = int(text)
        except Exception:
            msg.reply_text("❌ يرجى إرسال رقم صالح للنقاط.", reply_markup=_quizzes_back_keyboard(course_id))
            return

        answer_text = ctx.pop("pending_answer_text", None)
        if not answer_text or not course_id:
            _reset_quiz_creation(user_id)
            msg.reply_text("❌ البيانات غير مكتملة.", reply_markup=COURSES_ADMIN_MENU_KB)
            return

        ctx.setdefault("answers", []).append({"text": answer_text, "points": points})
        WAITING_QUIZ_ANSWER_POINTS.discard(user_id)

        if len(ctx.get("answers", [])) >= 4:
            _finalize_quiz_creation_from_message(user_id, msg)
            return

        options_kb = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("➕ إضافة إجابة أخرى", callback_data=f"COURSES:quiz_more_{course_id}")],
                [InlineKeyboardButton("✅ إنهاء", callback_data=f"COURSES:quiz_finish_{course_id}")],
            ]
        )
        msg.reply_text(
            "تم حفظ الإجابة. اختر التالي أو أضف إجابة أخرى.",
            reply_markup=options_kb,
        )
        return
    # تحديد الجنس للدعم
    if user_id in WAITING_SUPPORT_GENDER:
        if text == BTN_GENDER_MALE:
            record["gender"] = "male"
            update_user_record(user.id, gender="male")
            save_data()
            WAITING_SUPPORT_GENDER.discard(user_id)
            _open_support_session(update, user_id)
            return
        elif text == BTN_GENDER_FEMALE:
            record["gender"] = "female"
            update_user_record(user.id, gender="female")
            save_data()
            WAITING_SUPPORT_GENDER.discard(user_id)
            _open_support_session(update, user_id)
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

    # رد المستخدم على ردود الدعم
    if (
        not is_admin(user_id)
        and not is_supervisor(user_id)
        and msg.reply_to_message
        and msg.reply_to_message.from_user.id == context.bot.id
    ):
        original = (msg.reply_to_message.text or msg.reply_to_message.caption or "").strip()
        if (
            original.startswith("💌 رد من الدعم")
            or original.startswith("📢 رسالة من الدعم")
            or original.startswith("💌 رد من المشرفة")
            or "رسالتك وصلت للدعم" in original
        ):
            if user_id in WAITING_SUPPORT:
                forward_support_to_admin(user, text, context)
                msg.reply_text(
                    _support_confirmation_text(record.get("gender"), True),
                    reply_markup=SUPPORT_SESSION_KB,
                )
            else:
                msg.reply_text(
                    "للتواصل مع الدعم اضغط على زر التواصل مع الدعم فقط.",
                    reply_markup=main_kb,
                )
            return

    if text == BTN_SUPPORT_END:
        if user_id in WAITING_SUPPORT:
            WAITING_SUPPORT.discard(user_id)
            WAITING_SUPPORT_GENDER.discard(user_id)
            msg.reply_text(
                "تم إنهاء التواصل مع الدعم ✅",
                reply_markup=main_kb,
            )
        else:
            msg.reply_text(
                "لا توجد محادثة دعم مفتوحة حالياً.",
                reply_markup=main_kb,
            )
        return

    # زر إلغاء عام
    if text == BTN_CANCEL:
        if support_session_active:
            update.message.reply_text(
                "جلسة الدعم ما زالت مفتوحة. اضغط «🔚 إنهاء التواصل» لإغلاقها.",
                reply_markup=SUPPORT_SESSION_KB,
            )
            return
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
        WAITING_BOOK_SEARCH.discard(user_id)
        WAITING_BOOK_ADMIN_SEARCH.discard(user_id)
        WAITING_BOOK_CATEGORY_NAME.discard(user_id)
        WAITING_BOOK_CATEGORY_ORDER.discard(user_id)
        WAITING_BOOK_ADD_CATEGORY.discard(user_id)
        WAITING_BOOK_ADD_TITLE.discard(user_id)
        WAITING_BOOK_ADD_AUTHOR.discard(user_id)
        WAITING_BOOK_ADD_DESCRIPTION.discard(user_id)
        WAITING_BOOK_ADD_TAGS.discard(user_id)
        WAITING_BOOK_ADD_COVER.discard(user_id)
        WAITING_BOOK_ADD_PDF.discard(user_id)
        WAITING_BOOK_EDIT_FIELD.discard(user_id)
        WAITING_BOOK_EDIT_COVER.discard(user_id)
        WAITING_BOOK_EDIT_PDF.discard(user_id)
        BOOK_CREATION_CONTEXT.pop(user_id, None)
        BOOK_EDIT_CONTEXT.pop(user_id, None)
        BOOK_CATEGORY_EDIT_CONTEXT.pop(user_id, None)
        WAITING_SUPPORT_GENDER.discard(user_id)
        WAITING_BROADCAST.discard(user_id)
        WAITING_MOTIVATION_ADD.discard(user_id)
        WAITING_MOTIVATION_DELETE.discard(user_id)
        WAITING_MOTIVATION_TIMES.discard(user_id)
        _reset_course_subscription_flow(user_id)
        WAITING_BAN_USER.discard(user_id)
        WAITING_UNBAN_USER.discard(user_id)
        WAITING_BAN_REASON.discard(user_id)
        BAN_TARGET_ID.pop(user_id, None)
        SLEEP_ADHKAR_STATE.pop(user_id, None)
        STRUCTURED_ADHKAR_STATE.pop(user_id, None)
        AUDIO_USER_STATE.pop(user_id, None)
        WAITING_WATER_ADD_CUPS.discard(user_id)
        _reset_lesson_creation(user_id)
        _reset_quiz_creation(user_id)
        update_user_record(user_id, book_search_waiting=False, book_search_waiting_at=None)
        
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

    if user_id in WAITING_SUPPORT:
        forward_support_to_admin(user, text, context)

        msg.reply_text(
            _support_confirmation_text(record.get("gender"), True),
            reply_markup=SUPPORT_SESSION_KB,
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

    # مكتبة الكتب - حالات الإدخال النصي
    if user_id in WAITING_BOOK_ADMIN_SEARCH:
        handle_admin_book_search_input(update, context)
        return

    if user_id in WAITING_BOOK_CATEGORY_NAME:
        ctx = BOOK_CATEGORY_EDIT_CONTEXT.get(user_id, {})
        name = text.strip()
        if not name:
            msg.reply_text("الرجاء إدخال اسم تصنيف صالح.", reply_markup=CANCEL_KB)
            return
        mode = ctx.get("mode")
        if mode == "create":
            ctx["name"] = name
            BOOK_CATEGORY_EDIT_CONTEXT[user_id] = ctx
            WAITING_BOOK_CATEGORY_NAME.discard(user_id)
            WAITING_BOOK_CATEGORY_ORDER.add(user_id)
            msg.reply_text("أرسل ترتيب العرض (رقم). اكتب تخطي للإبقاء على الترتيب الافتراضي.", reply_markup=CANCEL_KB)
        elif mode == "rename" and ctx.get("category_id"):
            slug_value = re.sub(r"\s+", "-", name.lower())
            update_book_category(ctx["category_id"], name=name, slug=slug_value)
            WAITING_BOOK_CATEGORY_NAME.discard(user_id)
            BOOK_CATEGORY_EDIT_CONTEXT.pop(user_id, None)
            msg.reply_text("تم تحديث اسم التصنيف.", reply_markup=BOOKS_ADMIN_MENU_KB)
            open_book_categories_admin(update, context)
        else:
            WAITING_BOOK_CATEGORY_NAME.discard(user_id)
            BOOK_CATEGORY_EDIT_CONTEXT.pop(user_id, None)
            msg.reply_text("تم إلغاء العملية.", reply_markup=BOOKS_ADMIN_MENU_KB)
        return

    if user_id in WAITING_BOOK_CATEGORY_ORDER:
        ctx = BOOK_CATEGORY_EDIT_CONTEXT.get(user_id, {})
        order_val = 0
        normalized = text.strip().lower()
        if normalized not in {"تخطي", "skip", ""}:
            try:
                order_val = int(text)
            except Exception:
                msg.reply_text("الرجاء إدخال رقم صحيح للترتيب أو اكتب تخطي.", reply_markup=CANCEL_KB)
                return
        mode = ctx.get("mode")
        if mode == "create" and ctx.get("name"):
            slug_value = re.sub(r"\s+", "-", ctx.get("name").lower())
            cat_id = save_book_category(ctx.get("name"), order_val, created_by=user_id)
            WAITING_BOOK_CATEGORY_ORDER.discard(user_id)
            BOOK_CATEGORY_EDIT_CONTEXT.pop(user_id, None)
            if cat_id:
                msg.reply_text(f"تم إنشاء التصنيف بنجاح (ID: {cat_id}).", reply_markup=BOOKS_ADMIN_MENU_KB)
            else:
                msg.reply_text("تعذر إنشاء التصنيف حالياً.", reply_markup=BOOKS_ADMIN_MENU_KB)
            open_book_categories_admin(update, context)
        elif mode == "order" and ctx.get("category_id"):
            update_book_category(ctx["category_id"], order=order_val)
            WAITING_BOOK_CATEGORY_ORDER.discard(user_id)
            BOOK_CATEGORY_EDIT_CONTEXT.pop(user_id, None)
            msg.reply_text("تم تحديث ترتيب التصنيف.", reply_markup=BOOKS_ADMIN_MENU_KB)
            open_book_categories_admin(update, context)
        else:
            WAITING_BOOK_CATEGORY_ORDER.discard(user_id)
            BOOK_CATEGORY_EDIT_CONTEXT.pop(user_id, None)
            msg.reply_text("تم إلغاء العملية.", reply_markup=BOOKS_ADMIN_MENU_KB)
        return

    if user_id in WAITING_BOOK_ADD_TITLE:
        ctx = BOOK_CREATION_CONTEXT.get(user_id, {})
        ctx["title"] = text
        BOOK_CREATION_CONTEXT[user_id] = ctx
        WAITING_BOOK_ADD_TITLE.discard(user_id)
        WAITING_BOOK_ADD_AUTHOR.add(user_id)
        msg.reply_text("أرسل اسم المؤلف:", reply_markup=CANCEL_KB)
        return

    if user_id in WAITING_BOOK_ADD_CATEGORY:
        msg.reply_text("اختر التصنيف من الأزرار المعروضة.", reply_markup=CANCEL_KB)
        return

    if user_id in WAITING_BOOK_ADD_AUTHOR:
        ctx = BOOK_CREATION_CONTEXT.get(user_id, {})
        ctx["author"] = text
        BOOK_CREATION_CONTEXT[user_id] = ctx
        WAITING_BOOK_ADD_AUTHOR.discard(user_id)
        WAITING_BOOK_ADD_DESCRIPTION.add(user_id)
        msg.reply_text("أرسل وصفًا مختصرًا (أو اكتب تخطي لتجاوز الوصف):", reply_markup=CANCEL_KB)
        return

    if user_id in WAITING_BOOK_ADD_DESCRIPTION:
        ctx = BOOK_CREATION_CONTEXT.get(user_id, {})
        if text.strip().lower() in {"تخطي", "skip"}:
            ctx["description"] = ""
        else:
            ctx["description"] = text
        BOOK_CREATION_CONTEXT[user_id] = ctx
        WAITING_BOOK_ADD_DESCRIPTION.discard(user_id)
        WAITING_BOOK_ADD_TAGS.add(user_id)
        msg.reply_text("أرسل الكلمات المفتاحية مفصولة بفواصل (أو اكتب تخطي):", reply_markup=CANCEL_KB)
        return

    if user_id in WAITING_BOOK_ADD_TAGS:
        ctx = BOOK_CREATION_CONTEXT.get(user_id, {})
        if text.strip().lower() in {"تخطي", "skip"}:
            ctx["tags"] = []
        else:
            ctx["tags"] = _parse_tags_input(text)
        BOOK_CREATION_CONTEXT[user_id] = ctx
        WAITING_BOOK_ADD_TAGS.discard(user_id)
        WAITING_BOOK_ADD_COVER.add(user_id)
        msg.reply_text("أرسل صورة الغلاف (اختياري) أو اكتب تخطي:", reply_markup=CANCEL_KB)
        return

    if user_id in WAITING_BOOK_ADD_COVER:
        if text.strip().lower() in {"تخطي", "skip"}:
            WAITING_BOOK_ADD_COVER.discard(user_id)
            WAITING_BOOK_ADD_PDF.add(user_id)
            msg.reply_text("أرسل ملف الـ PDF للكتاب (إجباري):", reply_markup=CANCEL_KB)
        else:
            msg.reply_text("أرسل صورة غلاف صالحة أو اكتب تخطي.", reply_markup=CANCEL_KB)
        return

    if user_id in WAITING_BOOK_ADD_PDF:
        msg.reply_text("أرسل ملف الـ PDF للكتاب.", reply_markup=CANCEL_KB)
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

    # حذف نقاط وميداليات
    if user_id in WAITING_DELETE_USER_POINTS:
        handle_delete_user_points_input(update, context)
        return

    if user_id in WAITING_DELETE_USER_MEDALS:
        handle_delete_user_medals_input(update, context)
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

    # أذكار النوم
    if text == BTN_SLEEP_ADHKAR_NEXT:
        handle_sleep_adhkar_next(update, context)
        return

    if text == BTN_SLEEP_ADHKAR_BACK:
        handle_sleep_adhkar_back(update, context)
        return

    # مكتبة الصوتيات
    if text == BTN_AUDIO_LIBRARY:
        open_audio_library_menu(update, context)
        return

    if text in AUDIO_SECTION_BY_BUTTON:
        open_audio_section(update, context, AUDIO_SECTION_BY_BUTTON[text])
        return

    if text == BTN_AUDIO_BACK:
        open_audio_library_menu(update, context)
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

    if text == BTN_BOOKS_MAIN:
        _mark_admin_books_mode(context, False)
        open_books_home(update, context)
        return

    if text == BTN_MEMOS_MAIN:
        open_memos_menu(update, context)
        return

    if text == BTN_BOOKS_ADMIN:
        open_books_admin_menu(update, context)
        return

    if text == BTN_WATER_MAIN:
        open_water_menu(update, context)
        return

    if text == BTN_STATS:
        open_stats_menu(update, context)
        return

    if text == BTN_STATS_ONLY:
        send_stats_overview(update, context)
        return

    if text == BTN_MEDALS_ONLY or text == BTN_MEDALS:
        open_medals_overview(update, context)
        return

    if text == BTN_STATS_BACK_MAIN:
        msg.reply_text(
            "عدنا إلى القائمة الرئيسية.",
            reply_markup=user_main_keyboard(user_id),
        )
        return

    if text == BTN_SUPPORT:
        handle_contact_support(update, context)
        return

    if text == BTN_COMP_MAIN:
        open_comp_menu(update, context)
        return

    if text == BTN_COURSES_SECTION:
        open_courses_menu(update, context)
        return

    if text == BTN_MANAGE_COURSES:
        open_courses_admin_menu(update, context)
        return

    if text == BTN_BENEFITS_MAIN:
        open_benefits_menu(update, context)
        return

    if text == BTN_NOTIFICATIONS_MAIN:
        open_notifications_menu(update, context)
        return

    if text == BTN_BOOKS_MANAGE_CATEGORIES:
        open_book_categories_admin(update, context)
        return

    if text == BTN_BOOKS_ADD_BOOK:
        start_add_book_flow(update, context)
        return

    if text == BTN_BOOKS_MANAGE_BOOKS:
        open_books_admin_list(update, context)
        return

    if text == BTN_BOOKS_BACKFILL:
        _run_books_backfill_for_admin(update, context)
        return

    if text == BTN_BACK_MAIN:
        _mark_admin_books_mode(context, False)
        STRUCTURED_ADHKAR_STATE.pop(user_id, None)
        msg.reply_text(
            "عدنا إلى القائمة الرئيسية.",
            reply_markup=main_kb,
        )
        return

    # قوائم الأذكار
    if text == BTN_ADHKAR_NEXT:
        handle_structured_adhkar_next(update, context)
        return
    if text == BTN_ADHKAR_DONE:
        handle_structured_adhkar_done(update, context)
        return
    if text == BTN_ADHKAR_PREV:
        handle_structured_adhkar_prev(update, context)
        return
    if text == BTN_ADHKAR_BACK_MENU:
        handle_structured_adhkar_back_to_menu(update, context)
        return
    if text == BTN_ADHKAR_BACK_MAIN:
        handle_structured_adhkar_back_main(update, context)
        return
    if text == BTN_ADHKAR_MORNING:
        send_morning_adhkar(update, context)
        return

    if text == BTN_ADHKAR_EVENING:
        send_evening_adhkar(update, context)
        return

    if text == BTN_ADHKAR_GENERAL:
        send_general_adhkar(update, context)
        return

    if text == BTN_ADHKAR_SLEEP:
        start_sleep_adhkar(update, context)
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

    if text == BTN_WATER_RESET:
        handle_water_reset(update, context)
        return

    if text == BTN_WATER_ADD_CUPS:
        handle_add_cups(update, context)
        return

    if text == BTN_WATER_BACK_MENU:
        open_water_menu(update, context)
        return

    if text.isdigit() and user_id in WAITING_WATER_ADD_CUPS:
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

    if text == BTN_BENEFIT_TOP100:
        handle_top100_benefits(update, context)
        return

    if text == BTN_MY_BENEFITS:
        handle_my_benefits(update, context)
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

    if text == BTN_ADMIN_MANAGE_COMPETITION:
        update.message.reply_text(
            "🔹 التحكم في المنافسات والمجتمع:\n"
            "اختر العملية المطلوبة:",
            reply_markup=ADMIN_COMPETITION_KB,
        )
        return

    # معالجات الأزرار الجديدة للتأكيد
    if text == BTN_ADMIN_RESET_POINTS:
        handle_admin_confirm_reset_points(update, context)
        return

    if text == BTN_ADMIN_RESET_MEDALS:
        handle_admin_confirm_reset_medals(update, context)
        return

    # معالجات الإدخال للتأكيد
    if user_id in WAITING_CONFIRM_RESET_POINTS:
        handle_confirm_reset_points_input(update, context)
        return

    if user_id in WAITING_CONFIRM_RESET_MEDALS:
        handle_confirm_reset_medals_input(update, context)
        return






    # أي نص آخر
    if not support_session_active and not is_admin(user_id) and not is_supervisor(user_id):
        msg.reply_text(
            "رسالتك لم تُرسل للدعم. إذا أردت التواصل مع الدعم اضغط زر (تواصل مع الدعم ✉️).",
            reply_markup=SUPPORT_PROMPT_KB,
        )

# =================== دوال إدارة المنافسات والمجتمع ===================

def delete_user_competition_points(user_id: int):
    """حذف نقاط المنافسة لمستخدم معين"""
    if not firestore_available():
        return
    
    try:
        user_id_str = str(user_id)
        doc_ref = db.collection(USERS_COLLECTION).document(user_id_str)
        doc_ref.update({
            "daily_competition_points": 0,
            "community_rank": 0
        })
        logger.info(f"✅ تم حذف نقاط المنافسة للمستخدم {user_id}")
    except Exception as e:
        logger.error(f"❌ خطأ في حذف نقاط المنافسة: {e}")

def reset_competition_points():
    """تصفير جميع نقاط المنافسات والمجتمع من جميع المستخدمين"""
    if not firestore_available():
        logger.warning("Firestore غير متوفر للتصفير")
        return
    
    try:
        users_ref = db.collection(USERS_COLLECTION)
        docs = users_ref.stream()
        batch = db.batch()
        
        count = 0
        for doc in docs:
            if str(doc.id) == str(GLOBAL_KEY):
                continue
            # تصفير جميع النقاط والترتيب المتعلقة بالمنافسات والمجتمع
            batch.update(doc.reference, {
                "daily_competition_points": 0,
                "community_rank": 0,
                "points": 0,  # تصفير النقاط الإجمالية المستخدمة في التصنيف
                "total_points": 0, # تصفير النقاط الكلية (إذا كانت تستخدم في التصنيف)
            })
            count += 1
        
        batch.commit()
        
        logger.info(f"✅ تم تصفير نقاط المنافسات والمجتمع لـ {count} مستخدم")
    except Exception as e:
        logger.error(f"❌ خطأ في تصفير نقاط المنافسات والمجتمع: {e}", exc_info=True)

def delete_user_medals(user_id: int):
    """حذف ميداليات مستخدم معين من المجتمع فقط"""
    if not firestore_available():
        return
    
    try:
        user_id_str = str(user_id)
        doc_ref = db.collection(USERS_COLLECTION).document(user_id_str)
        doc_ref.update({
            "community_medals": []
        })
        logger.info(f"✅ تم حذف ميداليات المجتمع للمستخدم {user_id}")
    except Exception as e:
        logger.error(f"❌ خطأ في حذف الميداليات: {e}")

def reset_competition_medals():
    """تصفير جميع ميداليات المنافسات والمجتمع فقط (الميداليات الأخرى تبقى)"""
    if not firestore_available():
        logger.warning("Firestore غير متوفر للتصفير")
        return
    
    try:
        users_ref = db.collection(USERS_COLLECTION)
        docs = users_ref.stream()
        batch = db.batch()
        
        count = 0
        for doc in docs:
            if str(doc.id) == str(GLOBAL_KEY):
                continue
            # تصفير فقط ميداليات المنافسات والمجتمع
            # الميداليات الأخرى (الإنجازات الدائمة) تبقى كما هي
            batch.update(doc.reference, {
                "community_medals": [],
                "medals": [] # تصفير الميداليات الإجمالية المستخدمة في التصنيف
            })
            count += 1
            
        batch.commit()
        
        logger.info(f"✅ تم تصفير ميداليات المنافسات والمجتمع لـ {count} مستخدم")
    except Exception as e:
        logger.error(f"❌ خطأ في تصفير ميداليات المنافسات والمجتمع: {e}", exc_info=True)



# =================== تشغيل البوت ===================



def handle_admin_confirm_reset_points(update: Update, context: CallbackContext):
    """طلب تأكيد تصفير نقاط المنافسات والمجتمع"""
    user = update.effective_user
    if not is_admin(user.id):
        return
    
    user_id = user.id
    WAITING_CONFIRM_RESET_POINTS.add(user_id)
    
    # إنشاء لوحة مفاتيح للتأكيد
    confirm_kb = ReplyKeyboardMarkup(
        [
            [KeyboardButton("✅ نعم، تصفير الآن"), KeyboardButton("❌ إلغاء")],
        ],
        resize_keyboard=True,
    )
    
    update.message.reply_text(
        "⚠️ تحذير مهم!\n\n"
        "هل أنت متأكد من تصفير كل نقاط المنافسات والمجتمع لجميع المستخدمين؟\n\n"
        "هذه العملية لا يمكن التراجع عنها!",
        reply_markup=confirm_kb,
    )

def handle_admin_confirm_reset_medals(update: Update, context: CallbackContext):
    """طلب تأكيد تصفير ميداليات المنافسات والمجتمع"""
    user = update.effective_user
    if not is_admin(user.id):
        return
    
    user_id = user.id
    WAITING_CONFIRM_RESET_MEDALS.add(user_id)
    
    # إنشاء لوحة مفاتيح للتأكيد
    confirm_kb = ReplyKeyboardMarkup(
        [
            [KeyboardButton("✅ نعم، تصفير الآن"), KeyboardButton("❌ إلغاء")],
        ],
        resize_keyboard=True,
    )
    
    update.message.reply_text(
        "⚠️ تحذير مهم!\n\n"
        "هل أنت متأكد من تصفير كل ميداليات المنافسات والمجتمع لجميع المستخدمين؟\n\n"
        "هذه العملية لا يمكن التراجع عنها!",
        reply_markup=confirm_kb,
    )

def handle_confirm_reset_points_input(update: Update, context: CallbackContext):
    """معالجة تأكيد تصفير النقاط"""
    user = update.effective_user
    user_id = user.id
    
    if user_id not in WAITING_CONFIRM_RESET_POINTS:
        return
    
    text = (update.message.text or "").strip()
    WAITING_CONFIRM_RESET_POINTS.discard(user_id)
    
    if text == "✅ نعم، تصفير الآن":
        reset_competition_points()
        update.message.reply_text(
            "✅ تم تصفير كل نقاط المنافسات والمجتمع بنجاح.",
            reply_markup=ADMIN_PANEL_KB,
        )
    elif text == "❌ إلغاء":
        update.message.reply_text(
            "تم الإلغاء.",
            reply_markup=ADMIN_PANEL_KB,
        )

def handle_confirm_reset_medals_input(update: Update, context: CallbackContext):
    """معالجة تأكيد تصفير الميداليات"""
    user = update.effective_user
    user_id = user.id
    
    if user_id not in WAITING_CONFIRM_RESET_MEDALS:
        return
    
    text = (update.message.text or "").strip()
    WAITING_CONFIRM_RESET_MEDALS.discard(user_id)
    
    if text == "✅ نعم، تصفير الآن":
        reset_competition_medals()
        update.message.reply_text(
            "✅ تم تصفير كل ميداليات المنافسات والمجتمع بنجاح.",
            reply_markup=ADMIN_PANEL_KB,
        )
    elif text == "❌ إلغاء":
        update.message.reply_text(
            "تم الإلغاء.",
            reply_markup=ADMIN_PANEL_KB,
        )

# =================== مكتبة الصوتيات ===================

ARABIC_LETTER_NORMALIZATION = str.maketrans({
    "أ": "ا",
    "إ": "ا",
    "آ": "ا",
    "ٱ": "ا",
    "ى": "ي",
    "ؤ": "و",
    "ئ": "ي",
    "ة": "ه",
    "ـ": "",
})


def _normalize_hashtag(tag: str) -> str:
    """Normalize hashtags for robust matching across Arabic variants."""

    if not tag:
        return ""

    text = tag.strip().lstrip("#")
    # إزالة العلامات الشائعة الملاصقة للهاشتاق
    text = text.rstrip(".,،؛؛!！?？✨⭐️🌟🥇🥈🥉🎖️🏅")
    # إزالة التشكيل والعلامات الزخرفية
    text = re.sub(r"[\u064B-\u065F\u0617-\u061A\u06D6-\u06ED]", "", text)
    text = text.translate(ARABIC_LETTER_NORMALIZATION)
    text = text.replace("_", " ")
    # إزالة أي رموز غير حروف/أرقام/مسافات (مثل الإيموجي أو الرموز الأخرى)
    text = re.sub(r"[^\w\s\u0600-\u06FF]", "", text)
    text = re.sub(r"\s+", "", text)
    return text.lower()


def extract_hashtags_from_message(message) -> Tuple[List[str], List[str]]:
    hashtags: List[str] = []

    caption_entities = getattr(message, "caption_entities", None) or []
    caption_text = message.caption or ""
    for entity in caption_entities:
        if getattr(entity, "type", "") == "hashtag":
            try:
                tag_text = caption_text[entity.offset : entity.offset + entity.length]
                hashtags.append(tag_text)
            except Exception:
                continue

    text_based_hashtags = re.findall(r"#\S+", (message.caption or message.text or ""))
    hashtags.extend(text_based_hashtags)

    normalized = [_normalize_hashtag(tag) for tag in hashtags if _normalize_hashtag(tag)]
    logger.debug(
        "🏷️ تم استخراج هاشتاقات من الرسالة | raw=%s | normalized=%s",
        hashtags,
        normalized,
    )
    return normalized, hashtags


def _match_audio_section(hashtags: List[str]) -> str:
    normalized = {_normalize_hashtag(tag) for tag in hashtags}
    matched_sections = [
        key
        for key, cfg in AUDIO_SECTIONS.items()
        if _normalize_hashtag(cfg["hashtag"]) in normalized
    ]

    if len(matched_sections) == 1:
        return matched_sections[0]
    return ""


def _audio_title_from_message(message) -> str:
    caption = message.caption or message.text or ""
    caption = re.sub(r"#\S+", "", caption)

    audio_obj = getattr(message, "audio", None)
    doc_obj = getattr(message, "document", None)

    possible_title = None
    if audio_obj and getattr(audio_obj, "title", None):
        possible_title = audio_obj.title
    elif doc_obj and getattr(doc_obj, "file_name", None):
        possible_title = doc_obj.file_name

    return (caption.strip() or possible_title or "مقطع صوتي").strip()


def _extract_audio_file(message):
    file_id = None
    file_type = ""
    file_unique_id = None

    if message.audio:
        file_id = message.audio.file_id
        file_unique_id = getattr(message.audio, "file_unique_id", None)
        file_type = "audio"
    elif message.voice:
        file_id = message.voice.file_id
        file_unique_id = getattr(message.voice, "file_unique_id", None)
        file_type = "voice"
    elif message.document:
        doc = message.document
        file_name = (doc.file_name or "").lower()
        mime_type = (doc.mime_type or "").lower()
        audio_exts = (".mp3", ".wav", ".m4a", ".ogg", ".oga", ".opus", ".mp4")
        if mime_type.startswith("audio/") or mime_type.startswith("video/") or file_name.endswith(audio_exts):
            file_id = doc.file_id
            file_unique_id = getattr(doc, "file_unique_id", None)
            file_type = "document"

    return file_id, file_type, file_unique_id


def _is_audio_storage_channel(message) -> bool:
    """تحقق مرن من قناة التخزين باستخدام المعرف الرقمي أو اسم المستخدم."""

    try:
        target = (AUDIO_STORAGE_CHANNEL_ID or "").lstrip("@")
        if not target:
            return False

        chat = getattr(message, "chat", None)
        if not chat:
            return False

        chat_id_match = str(chat.id) == target
        username_match = (
            getattr(chat, "username", None)
            and chat.username.lstrip("@").lower() == target.lower()
        )
        is_match = chat_id_match or username_match

        logger.info(
            "🛰️ فحص قناة التخزين | chat.id=%s (match=%s) | chat.username=%s (match=%s) | target=%s | final_match=%s",
            getattr(chat, "id", ""),
            chat_id_match,
            getattr(chat, "username", ""),
            username_match,
            target,
            is_match,
        )

        return is_match
    except Exception:
        return False


def delete_audio_clip_by_message_id(message_id: int):
    global LOCAL_AUDIO_LIBRARY

    if not message_id:
        return

    if firestore_available():
        try:
            doc_id = str(message_id)
            db.collection(AUDIO_LIBRARY_COLLECTION).document(doc_id).delete()

            docs = db.collection(AUDIO_LIBRARY_COLLECTION).where("message_id", "==", message_id).stream()
            for doc in docs:
                doc.reference.delete()
        except Exception as e:
            logger.error(f"❌ خطأ في حذف المقطع الصوتي: {e}")

    LOCAL_AUDIO_LIBRARY = [clip for clip in LOCAL_AUDIO_LIBRARY if clip.get("message_id") != message_id]
    _persist_local_audio_library()


def _attempt_delete_storage_message(bot, clip: Dict) -> bool:
    channel_id = clip.get("channel_id") or AUDIO_STORAGE_CHANNEL_ID
    message_id = clip.get("message_id")

    if not channel_id or not message_id:
        return False

    try:
        chat_ref = int(channel_id) if str(channel_id).lstrip("-").isdigit() else channel_id
        bot.delete_message(chat_id=chat_ref, message_id=message_id)
        logger.info(
            "🗑️ تم حذف المنشور من قناة التخزين | chat_id=%s | msg_id=%s",
            chat_ref,
            message_id,
        )
        return True
    except Exception as e:
        logger.warning("⚠️ تعذر حذف منشور قناة التخزين: %s", e)
        return False


def _upsert_local_audio_clip(record: Dict):
    """حفظ نسخة محلية محدثة من المقطع لضمان توفره حتى عند فشل Firestore."""

    global LOCAL_AUDIO_LIBRARY

    message_id = record.get("message_id")
    LOCAL_AUDIO_LIBRARY = [c for c in LOCAL_AUDIO_LIBRARY if c.get("message_id") != message_id]
    LOCAL_AUDIO_LIBRARY.append(record)
    _persist_local_audio_library()


def _cleanup_audio_duplicates(record: Dict):
    if not firestore_available():
        return

    message_id = record.get("message_id")
    file_id = record.get("file_id")
    file_unique_id = record.get("file_unique_id")
    doc_id = str(message_id)

    try:
        # إزالة أي نسخ بنفس message_id حتى لو كانت محفوظة بمعرف آخر
        message_duplicates = db.collection(AUDIO_LIBRARY_COLLECTION).where("message_id", "==", message_id).stream()
        for doc in message_duplicates:
            if doc.id != doc_id:
                doc.reference.delete()

        duplicate_query = None
        if file_unique_id:
            duplicate_query = db.collection(AUDIO_LIBRARY_COLLECTION).where("file_unique_id", "==", file_unique_id)
        elif file_id:
            duplicate_query = db.collection(AUDIO_LIBRARY_COLLECTION).where("file_id", "==", file_id)

        if duplicate_query:
            duplicates = duplicate_query.stream()
            for doc in duplicates:
                if doc.id != doc_id:
                    doc.reference.delete()
    except Exception as e:
        logger.error(f"❌ خطأ في تنظيف التكرار للمقطع الصوتي: {e}")


def save_audio_clip_record(record: Dict):
    section_key = record.get("section")
    if not section_key or section_key not in AUDIO_SECTIONS:
        logger.warning(
            "UNMATCHED_HASHTAG | رفض حفظ المقطع لعدم مطابقة قسم صحيح | section=%s | message_id=%s",
            section_key,
            record.get("message_id"),
        )
        return

    message_id = record.get("message_id")
    file_id = record.get("file_id")
    file_unique_id = record.get("file_unique_id")
    delete_audio_clip_by_message_id(message_id)

    if firestore_available() and (file_id or file_unique_id):
        _cleanup_audio_duplicates(record)

    if firestore_available():
        try:
            doc_id = str(message_id)
            db.collection(AUDIO_LIBRARY_COLLECTION).document(doc_id).set(record, merge=True)
            _upsert_local_audio_clip(record)
            logger.info(
                "💾 تم حفظ/تحديث المقطع في Firestore والمحلي | message_id=%s | section=%s",
                message_id,
                record.get("section"),
            )
            return
        except Exception as e:
            logger.error(f"❌ خطأ في حفظ المقطع الصوتي: {e}")

    # fallback محلي
    _upsert_local_audio_clip(record)
    logger.info(
        "💾 تم حفظ المقطع محليًا (Firestore غير متاح) | message_id=%s | section=%s",
        message_id,
        record.get("section"),
    )


def fetch_audio_clips(section_key: str) -> List[Dict]:
    if section_key not in AUDIO_SECTIONS:
        logger.warning(
            "UNMATCHED_HASHTAG | محاولة استعلام قسم غير معروف | section=%s",
            section_key,
        )
        return []

    clips_by_message: Dict[str, Dict] = {}
    firestore_count = 0

    if firestore_available():
        try:
            docs = (
                db.collection(AUDIO_LIBRARY_COLLECTION)
                .where("section", "==", section_key)
                .stream()
            )
            for doc in docs:
                clip_data = doc.to_dict() or {}
                clip_data.setdefault("message_id", int(doc.id) if str(doc.id).isdigit() else doc.id)
                key = str(clip_data.get("message_id") or doc.id)
                clips_by_message[key] = clip_data
                firestore_count += 1
        except Exception as e:
            logger.error(f"❌ خطأ في قراءة مكتبة الصوتيات: {e}")

    local_count = 0
    for clip in [c for c in LOCAL_AUDIO_LIBRARY if c.get("section") == section_key]:
        key = str(clip.get("message_id"))
        current = clips_by_message.get(key)
        if not current or _is_newer_audio_record(clip, current):
            clips_by_message[key] = clip
        local_count += 1

    clips = list(clips_by_message.values())

    clips.sort(
        key=lambda c: (
            c.get("created_at") or "",
            c.get("message_id") or 0,
        ),
        reverse=True,
    )

    logger.info(
        "📊 جلب مكتبة الصوتيات | section=%s | firestore=%s | local=%s | total=%s",
        section_key,
        firestore_count,
        local_count,
        len(clips),
    )
    return clips


def clean_audio_library_records() -> Dict[str, int]:
    invalid_message_ids = set()
    firestore_scanned = 0
    local_scanned = 0

    if firestore_available():
        try:
            docs = db.collection(AUDIO_LIBRARY_COLLECTION).stream()
            for doc in docs:
                firestore_scanned += 1
                clip = doc.to_dict() or {}
                message_id = clip.get("message_id") or (
                    int(doc.id) if str(doc.id).lstrip("-").isdigit() else doc.id
                )
                section = clip.get("section")
                file_id = clip.get("file_id")
                file_type = clip.get("file_type")

                if not message_id:
                    continue

                is_section_valid = bool(section) and section in AUDIO_SECTIONS
                has_file = bool(file_id)
                has_basic_fields = bool(file_type)

                if not (is_section_valid and has_file and has_basic_fields):
                    invalid_message_ids.add(message_id)
        except Exception as e:
            logger.error("❌ خطأ أثناء فحص مكتبة الصوتيات في Firestore: %s", e)

    for clip in LOCAL_AUDIO_LIBRARY:
        local_scanned += 1
        message_id = clip.get("message_id")
        section = clip.get("section")
        file_id = clip.get("file_id")
        file_type = clip.get("file_type")

        if not message_id:
            continue

        is_section_valid = bool(section) and section in AUDIO_SECTIONS
        has_file = bool(file_id)
        has_basic_fields = bool(file_type)

        if not (is_section_valid and has_file and has_basic_fields):
            invalid_message_ids.add(message_id)

    deleted = 0
    for message_id in invalid_message_ids:
        delete_audio_clip_by_message_id(message_id)
        deleted += 1

    logger.info(
        "🧹 تنظيف المكتبة الصوتية | scanned_firestore=%s | scanned_local=%s | deleted=%s",
        firestore_scanned,
        local_scanned,
        deleted,
    )

    return {
        "firestore_scanned": firestore_scanned,
        "local_scanned": local_scanned,
        "deleted": deleted,
    }


def _parse_audio_datetime(value):
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except Exception:
            return None
    return None


def _is_newer_audio_record(candidate: Dict, reference: Dict) -> bool:
    cand_dt = _parse_audio_datetime(candidate.get("created_at"))
    ref_dt = _parse_audio_datetime(reference.get("created_at"))

    if cand_dt and ref_dt:
        return cand_dt >= ref_dt
    try:
        return int(candidate.get("message_id") or 0) >= int(reference.get("message_id") or 0)
    except Exception:
        return True


def reconcile_audio_library_uniqueness():
    """تنظيف التكرارات في مكتبة الصوتيات لضمان ارتباط كل مقطع بهوية واحدة"""

    if not firestore_available():
        return

    try:
        docs = list(db.collection(AUDIO_LIBRARY_COLLECTION).stream())
        entries = []

        for doc in docs:
            data = doc.to_dict() or {}
            data.setdefault("message_id", int(doc.id) if str(doc.id).isdigit() else doc.id)
            data.setdefault("file_unique_id", data.get("file_unique_id"))
            data.setdefault("file_id", data.get("file_id"))
            entries.append({"id": doc.id, "ref": doc.reference, "data": data})

        latest_by_message: Dict[str, Dict] = {}
        latest_by_unique: Dict[str, Dict] = {}

        def consider(target: Dict[str, Dict], key: str, entry: Dict):
            if not key:
                return
            current = target.get(key)
            if not current or _is_newer_audio_record(entry["data"], current["data"]):
                target[key] = entry

        for entry in entries:
            consider(latest_by_message, str(entry["data"].get("message_id")), entry)
            unique_key = entry["data"].get("file_unique_id") or entry["data"].get("file_id")
            consider(latest_by_unique, unique_key, entry)

        keep_ids = set()
        for entry in latest_by_message.values():
            keep_ids.add(entry["id"])
        for entry in latest_by_unique.values():
            keep_ids.add(entry["id"])

        removed = 0
        for entry in entries:
            if entry["id"] not in keep_ids:
                entry["ref"].delete()
                removed += 1

        if removed:
            logger.info("🧹 تم تنظيف %s من المقاطع الصوتية المكررة", removed)
    except Exception as e:
        logger.error(f"❌ خطأ في تنظيف مكتبة الصوتيات: {e}")


def handle_channel_post(update: Update, context: CallbackContext):
    logger.error("🔥 CHANNEL POST RECEIVED 🔥")
    message = update.channel_post
    process_channel_audio_message(message)


def handle_edited_channel_post(update: Update, context: CallbackContext):
    message = update.edited_channel_post
    process_channel_audio_message(message, is_edit=True)


def handle_deleted_channel_post(update: Update, context: CallbackContext):
    message = update.effective_message
    if not message or not _is_audio_storage_channel(message):
        return

    delete_audio_clip_by_message_id(message.message_id)
    logger.info(
        "🗑️ تم حذف منشور من قناة التخزين | chat_id=%s | msg_id=%s",
        message.chat.id,
        message.message_id,
    )


def process_channel_audio_message(message, is_edit: bool = False):
    chat = getattr(message, "chat", None)
    chat_id = getattr(chat, "id", "")
    chat_username = getattr(chat, "username", "")
    message_id = getattr(message, "message_id", "")

    normalized_hashtags, raw_hashtags = extract_hashtags_from_message(message) if message else ([], [])
    section_key = _match_audio_section(normalized_hashtags)
    file_id, file_type, file_unique_id = _extract_audio_file(message) if message else (None, "", None)
    is_storage_channel = _is_audio_storage_channel(message) if message else False

    logger.info(
        "🛰️ CHANNEL_POST_LOG | chat.id=%s | chat.username=%s | msg_id=%s | storage_channel=%s | file_type=%s | file_id=%s | raw_hashtags=%s | normalized_hashtags=%s | section_key=%s",
        chat_id,
        chat_username,
        message_id,
        is_storage_channel,
        file_type or "",
        file_id or "",
        raw_hashtags,
        normalized_hashtags,
        section_key,
    )

    if not message:
        return

    if not is_storage_channel:
        logger.debug(
            "📭 تم تجاهل رسالة قناة خارج قناة التخزين | chat_id=%s | msg_id=%s",
            chat_id,
            message_id,
        )
        return

    logger.info(
        "📥 رسالة قناة مستلمة | chat_id=%s | msg_id=%s | type=%s | has_caption=%s | is_auto_forward=%s",
        message.chat.id,
        message.message_id,
        "audio" if message.audio else "voice" if message.voice else "document" if message.document else getattr(message, "content_type", "unknown"),
        bool(message.caption),
        getattr(message, "is_automatic_forward", False),
    )

    available_hashtags = {
        key: _normalize_hashtag(cfg.get("hashtag", "")) for key, cfg in AUDIO_SECTIONS.items()
    }
    logger.info(
        "🧭 AUDIO_UPLOAD_DIAG | chat.id=%s | chat.username=%s | storage_target=%s | raw_hashtags=%s | normalized_hashtags=%s | available_sections=%s | section_key=%s",
        getattr(message.chat, "id", ""),
        getattr(message.chat, "username", ""),
        AUDIO_STORAGE_CHANNEL_ID,
        raw_hashtags,
        normalized_hashtags,
        available_hashtags,
        section_key,
    )

    logger.info(
        "🏷️ بيانات الهاشتاقات | chat.id=%s | chat.username=%s | msg_id=%s | raw=%s | normalized=%s",
        getattr(message.chat, "id", ""),
        getattr(message.chat, "username", ""),
        message.message_id,
        raw_hashtags,
        normalized_hashtags,
    )

    if not file_id:
        delete_audio_clip_by_message_id(message.message_id)
        logger.info(
            "📥 تم إزالة المقطع لعدم وجود ملف صوتي صالح | chat_id=%s | msg_id=%s | hashtags=%s",
            message.chat.id,
            message.message_id,
            raw_hashtags,
        )
        return

    if not section_key or section_key not in AUDIO_SECTIONS:
        logger.warning(
            "UNMATCHED_HASHTAG | chat_id=%s | msg_id=%s | raw_hashtags=%s | normalized_hashtags=%s",
            message.chat.id,
            message.message_id,
            raw_hashtags,
            normalized_hashtags,
        )
        if is_edit:
            delete_audio_clip_by_message_id(message.message_id)
            logger.info(
                "🗑️ تمت إزالة المقطع بسبب تعديل بدون هاشتاق صالح | chat_id=%s | msg_id=%s",
                message.chat.id,
                message.message_id,
            )
        return

    logger.info(
        "🎧 %s قناة التخزين | chat_id=%s | msg_id=%s | file_type=%s | hashtags=%s",
        "تعديل" if is_edit else "رسالة",
        message.chat.id,
        message.message_id,
        file_type or "unknown",
        raw_hashtags,
    )

    if normalized_hashtags:
        logger.debug(
            "🏷️ تم اكتشاف الهاشتاقات بعد التطبيع | normalized=%s | raw=%s",
            normalized_hashtags,
            raw_hashtags,
        )

    record = {
        "section": section_key,
        "title": _audio_title_from_message(message),
        "file_id": file_id,
        "file_type": file_type,
        "file_unique_id": file_unique_id,
        "hashtags": raw_hashtags,
        "normalized_hashtags": normalized_hashtags,
        "channel_id": message.chat.id,
        "message_id": message.message_id,
        "caption": message.caption or message.text or "",
        "created_at": (message.date or datetime.now(timezone.utc)).isoformat(),
    }
    save_audio_clip_record(record)


def _audio_section_inline_keyboard(
    section_key: str, clips: List[Dict], page: int, show_delete: bool
) -> InlineKeyboardMarkup:
    start = max(page, 0) * AUDIO_PAGE_SIZE
    end = start + AUDIO_PAGE_SIZE
    sliced = clips[start:end]

    rows: List[List[InlineKeyboardButton]] = []
    for clip in sliced:
        title = clip.get("title") or "مقطع صوتي"
        mid = clip.get("message_id")

        rows.append(
            [
                InlineKeyboardButton(
                    f"🔹 {title}",
                    callback_data=f"audio_play:{section_key}:{mid}",
                )
            ]
        )

        if show_delete:
            rows.append(
                [
                    InlineKeyboardButton(
                        "🗑️",
                        callback_data=f"audio_delete:{section_key}:{mid}",
                    )
                ]
            )

    nav_row: List[InlineKeyboardButton] = []
    if start > 0:
        nav_row.append(
            InlineKeyboardButton(
                "⏮ السابق",
                callback_data=f"audio_page:{section_key}:{max(page - 1, 0)}",
            )
        )
    if end < len(clips):
        nav_row.append(
            InlineKeyboardButton(
                "التالي ▶️",
                callback_data=f"audio_page:{section_key}:{page + 1}",
            )
        )
    if nav_row:
        rows.append(nav_row)

    rows.append([InlineKeyboardButton("↩️ رجوع إلى الأقسام", callback_data="audio_back_sections")])
    return InlineKeyboardMarkup(rows)


def open_audio_library_menu(update: Update, context: CallbackContext):
    AUDIO_USER_STATE.pop(update.effective_user.id, None)
    update.message.reply_text(
        "اختر قسمًا من المكتبة الصوتية:",
        reply_markup=AUDIO_LIBRARY_KB,
    )


def _send_audio_section_page(
    update: Update,
    context: CallbackContext,
    section_key: str,
    page: int = 0,
    from_callback: bool = False,
):
    user_id = update.effective_user.id
    can_manage = is_admin(user_id) or is_supervisor(user_id)
    clips = fetch_audio_clips(section_key)
    total = len(clips)
    safe_page = max(min(page, (total - 1) // AUDIO_PAGE_SIZE if total else 0), 0)
    AUDIO_USER_STATE[user_id] = {
        "section": section_key,
        "clips": clips,
        "page": safe_page,
    }

    logger.info(
        "📂 عرض قسم الصوتيات | user_id=%s | section=%s | total=%s | page=%s",
        user_id,
        section_key,
        total,
        safe_page,
    )

    header = f"{AUDIO_SECTIONS[section_key]['title']}\n\nعدد المقاطع المتوفرة: {total}"
    if total:
        header += "\n\n🎧 قائمة المقاطع المتاحة:"

    keyboard = _audio_section_inline_keyboard(section_key, clips, safe_page, can_manage)

    if from_callback and update.callback_query:
        try:
            update.callback_query.edit_message_text(header, reply_markup=keyboard)
            return
        except Exception:
            pass

    update.message.reply_text(header, reply_markup=keyboard)


def open_audio_section(update: Update, context: CallbackContext, section_key: str, page: int = 0):
    _send_audio_section_page(update, context, section_key, page)


def handle_audio_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    if not query:
        return

    data = query.data or ""
    user_id = query.from_user.id

    if data == "audio_back_sections":
        query.answer()
        AUDIO_USER_STATE.pop(user_id, None)
        query.message.reply_text(
            "اختر قسمًا من المكتبة الصوتية:",
            reply_markup=AUDIO_LIBRARY_KB,
        )
        return

    if data.startswith("audio_page:"):
        query.answer()
        try:
            _, section_key, page_str = data.split(":", 2)
            page = int(page_str)
        except ValueError:
            return
        _send_audio_section_page(update, context, section_key, page, from_callback=True)
        return

    if data.startswith("audio_play:"):
        query.answer()
        try:
            _, section_key, clip_id = data.split(":", 2)
        except ValueError:
            return

        state = AUDIO_USER_STATE.get(user_id, {})
        clips = state.get("clips", []) if state.get("section") == section_key else []
        if not clips:
            clips = fetch_audio_clips(section_key)

        clip = next((c for c in clips if str(c.get("message_id")) == clip_id), None)
        if not clip:
            return

        title = clip.get("title") or "مقطع صوتي"
        try:
            file_type = clip.get("file_type")
            file_id = clip.get("file_id")
            if file_type == "voice":
                context.bot.send_voice(update.effective_chat.id, file_id, caption=title)
            elif file_type == "document":
                context.bot.send_document(update.effective_chat.id, file_id, caption=title)
            else:
                context.bot.send_audio(update.effective_chat.id, file_id, caption=title)
        except Exception as e:
            logger.error(f"❌ خطأ في إرسال المقطع الصوتي: {e}")
            query.message.reply_text("تعذر إرسال المقطع الآن. حاول مرة أخرى لاحقًا.")
        return

    if data.startswith("audio_delete:"):
        query.answer()
        try:
            _, section_key, clip_id = data.split(":", 2)
        except ValueError:
            return

        if not (is_admin(user_id) or is_supervisor(user_id)):
            query.answer("غير مصرح بحذف المقاطع.", show_alert=True)
            return

        state = AUDIO_USER_STATE.get(user_id, {})
        clips = state.get("clips", []) if state.get("section") == section_key else []
        if not clips:
            clips = fetch_audio_clips(section_key)

        clip = next((c for c in clips if str(c.get("message_id")) == clip_id), None)
        if not clip:
            query.answer("المقطع غير موجود.", show_alert=True)
            return

        message_id = clip.get("message_id")
        delete_audio_clip_by_message_id(message_id)
        _attempt_delete_storage_message(context.bot, clip)

        AUDIO_USER_STATE[user_id] = {
            "section": section_key,
            "clips": [c for c in fetch_audio_clips(section_key)],
            "page": 0,
        }

        _send_audio_section_page(update, context, section_key, 0, from_callback=True)
        return

    query.answer()


def handle_clean_audio_library_command(update: Update, context: CallbackContext):
    user = update.effective_user
    if not user or not is_admin(user.id):
        update.message.reply_text("هذا الأمر مخصص للأدمن فقط.")
        return

    result = clean_audio_library_records()
    update.message.reply_text(
        (
            "🧹 تم تنظيف المكتبة الصوتية.\n"
            f"- السجلات المفحوصة (Firestore): {result['firestore_scanned']}\n"
            f"- السجلات المفحوصة (محلي): {result['local_scanned']}\n"
            f"- السجلات غير الصالحة المحذوفة: {result['deleted']}"
        )
    )


def _ensure_storage_channel_admin(bot):
    try:
        target = AUDIO_STORAGE_CHANNEL_ID
        if not target:
            return

        chat_ref = int(target) if str(target).lstrip("-").isdigit() else target
        member = bot.get_chat_member(chat_ref, bot.id)
        status = getattr(member, "status", "")
        is_admin = status in ("administrator", "creator")

        logger.info(
            "🔒 تحقق صلاحيات البوت في قناة التخزين | target=%s | status=%s | is_admin=%s",
            target,
            status,
            is_admin,
        )

        if not is_admin:
            logger.warning(
                "⚠️ البوت ليس مديرًا في قناة التخزين. قد تفشل معالجة المقاطع الصوتية."
            )
    except Exception as e:
        logger.warning("⚠️ تعذر التحقق من صلاحيات قناة التخزين: %s", e)


def error_handler(update: Update, context: CallbackContext):
    """Log unexpected errors to help diagnose callback issues."""

    logger.exception("Unhandled error: %s", context.error, exc_info=context.error)


def start_bot():
    """بدء البوت"""
    global IS_RUNNING, job_queue, dispatcher
    global data
    
    if not BOT_TOKEN:
        raise RuntimeError("❌ BOT_TOKEN غير موجود!")
    
    logger.info("🚀 بدء تهيئة البوت...")
    
    try:
        logger.info("🔄 جارٍ تحميل بيانات المستخدمين...")
        data = load_data()
        logger.info(f"✅ تم تحميل {len([k for k in data if k != GLOBAL_KEY])} مستخدم في الذاكرة")

        # تحميل المكتبة الصوتية من التخزين المحلي عند الحاجة
        _load_local_audio_library()

        # تمييز البيانات المحملة على أنها محدثة حديثًا لتجنب قراءات Firestore المكررة فور التشغيل
        preload_time = datetime.now(timezone.utc)
        for uid in data:
            if str(uid) == str(GLOBAL_KEY):
                continue
            ensure_water_defaults(data[uid])
            USER_CACHE_TIMESTAMPS[uid] = preload_time

        perform_initial_water_cleanup()

        # عدم ترحيل بيانات Firestore عند كل تشغيل لمنع الكتابة فوق البيانات الحالية
        if db is not None and not DATA_LOADED_FROM_FIRESTORE:
            logger.info("جاري ترحيل البيانات من التخزين المحلي إلى Firestore...")
            try:
                migrate_data_to_firestore()
            except Exception as e:
                logger.warning(f"⚠️ خطأ في الترحيل: {e}")

        # تنظيف مكتبة الصوتيات لضمان عدم تكرار نفس المقطع في أكثر من قسم
        try:
            reconcile_audio_library_uniqueness()
        except Exception as e:
            logger.warning(f"⚠️ تعذر تنظيف مكتبة الصوتيات: {e}")

        try:
            _ensure_storage_channel_admin(dispatcher.bot)
        except Exception as e:
            logger.warning("⚠️ تعذر التأكد من صلاحيات قناة التخزين: %s", e)

        logger.info("جاري تسجيل المعالجات...")
        dispatcher.add_handler(CommandHandler("start", start_command))
        dispatcher.add_handler(CommandHandler("help", help_command))
        dispatcher.add_handler(CommandHandler("clean_audio_library", handle_clean_audio_library_command))
        dispatcher.add_handler(CommandHandler("books_backfill", _run_books_backfill_for_admin))

        dispatcher.add_handler(CallbackQueryHandler(handle_support_open_callback, pattern=r"^SUPPORT:OPEN$"))
        dispatcher.add_handler(CallbackQueryHandler(handle_like_benefit_callback, pattern=r"^like_benefit_\d+$"))
        dispatcher.add_handler(CallbackQueryHandler(handle_edit_benefit_callback, pattern=r"^edit_benefit_\d+$"))
        dispatcher.add_handler(CallbackQueryHandler(handle_delete_benefit_callback, pattern=r"^delete_benefit_\d+$"))
        dispatcher.add_handler(CallbackQueryHandler(handle_admin_delete_benefit_callback, pattern=r"^admin_delete_benefit_\d+$"))
        dispatcher.add_handler(CallbackQueryHandler(handle_delete_benefit_confirm_callback, pattern=r"^confirm_delete_benefit_\d+$|^cancel_delete_benefit$|^confirm_admin_delete_benefit_\d+$|^cancel_admin_delete_benefit$"))
        dispatcher.add_handler(CallbackQueryHandler(handle_courses_callback, pattern=r"^COURSE:"))
        dispatcher.add_handler(CallbackQueryHandler(handle_courses_callback, pattern=r"^COURSES:"))
        dispatcher.add_handler(CallbackQueryHandler(handle_audio_callback, pattern=r"^audio_"))
        dispatcher.add_handler(
            CallbackQueryHandler(
                handle_books_callback, pattern=rf"^{BOOKS_CALLBACK_PREFIX}:"
            )
        )
        dispatcher.add_handler(CallbackQueryHandler(handle_support_open_callback, pattern=r"^support_open$"))
        dispatcher.add_error_handler(error_handler)

        audio_document_filter = (
            Filters.document.audio
            | Filters.document.mime_type("audio/mpeg")
            | Filters.document.mime_type("audio/mp4")
            | Filters.document.mime_type("audio/ogg")
            | Filters.document.mime_type("audio/opus")
            | Filters.document.mime_type("audio/x-m4a")
            | Filters.document.mime_type("application/octet-stream")
            | Filters.document.file_extension("mp3")
            | Filters.document.file_extension("wav")
            | Filters.document.file_extension("ogg")
            | Filters.document.file_extension("oga")
            | Filters.document.file_extension("opus")
            | Filters.document.file_extension("m4a")
            | Filters.document.file_extension("flac")
            | Filters.document.file_extension("aac")
        )

        lesson_audio_filter = (
            (Filters.voice | Filters.audio | Filters.document)
            & Filters.chat_type.private
            & Filters.user(WAITING_LESSON_AUDIO)
        )
        user_audio_filter = (Filters.audio | Filters.voice | audio_document_filter) & Filters.chat_type.private
        channel_audio_filter = Filters.chat_type.channel & (Filters.audio | Filters.voice | audio_document_filter)

        reply_support_filter = (
            Filters.reply
            & (
                Filters.text
                | Filters.photo
                | Filters.video
                | Filters.voice
                | Filters.audio
                | Filters.video_note
            )
            & ~Filters.chat_type.channel
        )

        def _in_presentation_mode(message) -> bool:
            user = getattr(message, "from_user", None)
            if not user:
                return False
            return user.id in WAITING_COURSE_PRESENTATION_MEDIA

        def _in_benefit_mode(message) -> bool:
            user = getattr(message, "from_user", None)
            if not user:
                return False
            return user.id in WAITING_COURSE_BENEFIT_MEDIA

        book_media_filter = (
            Filters.photo
            | Filters.document.mime_type("application/pdf")
            | Filters.document.file_extension("pdf")
        ) & Filters.chat_type.private
        support_photo_filter = (
            (
                Filters.photo
                | Filters.document.mime_type("image/jpeg")
                | Filters.document.mime_type("image/png")
                | Filters.document.mime_type("image/webp")
            )
            & Filters.chat_type.private
            & ~FuncMessageFilter(_in_presentation_mode)
            & ~FuncMessageFilter(_in_benefit_mode)
        )
        support_audio_filter = (
            (Filters.audio | Filters.voice)
            & Filters.chat_type.private
            & ~FuncMessageFilter(_in_presentation_mode)
            & ~FuncMessageFilter(_in_benefit_mode)
        )
        support_video_filter = (
            Filters.video
            & Filters.chat_type.private
            & ~FuncMessageFilter(_in_presentation_mode)
            & ~FuncMessageFilter(_in_benefit_mode)
        )
        support_video_note_filter = (
            Filters.video_note
            & Filters.chat_type.private
            & ~FuncMessageFilter(_in_presentation_mode)
            & ~FuncMessageFilter(_in_benefit_mode)
        )
        presentation_media_filter = (
            Filters.chat_type.private
            & FuncMessageFilter(_in_presentation_mode)
            & (
                (Filters.text & ~Filters.command)
                | Filters.voice
                | Filters.audio
                | Filters.photo
                | Filters.video_note
                | Filters.document
            )
        )

        benefit_media_filter = (
            Filters.chat_type.private
            & FuncMessageFilter(_in_benefit_mode)
            & (Filters.photo | (Filters.text & ~Filters.command))
        )

        dispatcher.add_handler(
            MessageHandler(
                Filters.update.channel_post & channel_audio_filter, handle_channel_post
            )
        )
        dispatcher.add_handler(
            MessageHandler(
                Filters.update.edited_channel_post & channel_audio_filter,
                handle_edited_channel_post,
            )
        )
        dispatcher.add_handler(
            MessageHandler(
                Filters.status_update & Filters.chat_type.channel,
                handle_deleted_channel_post,
            )
        )

        dispatcher.add_handler(
            MessageHandler(
                reply_support_filter,
                handle_support_admin_reply_any,
            )
        )

        dispatcher.add_handler(
            MessageHandler(
                presentation_media_filter,
                course_presentation_router,
            ),
            group=0,
        )

        dispatcher.add_handler(
            MessageHandler(
                benefit_media_filter,
                course_benefit_router,
            ),
            group=0,
        )

        dispatcher.add_handler(
            MessageHandler(
                support_photo_filter,
                handle_support_photo,
            ),
            group=0,
        )

        dispatcher.add_handler(
            MessageHandler(
                support_audio_filter,
                handle_support_audio,
            ),
            group=0,
        )
        dispatcher.add_handler(
            MessageHandler(
                support_video_filter,
                handle_support_video,
            ),
            group=0,
        )
        dispatcher.add_handler(
            MessageHandler(
                support_video_note_filter,
                handle_support_video_note,
            ),
            group=0,
        )

        dispatcher.add_handler(
            MessageHandler(
                book_media_filter,
                handle_book_media_message,
            ),
            group=1,
        )

        dispatcher.add_handler(
            MessageHandler(
                lesson_audio_filter,
                handle_audio_message,
            ),
            group=0,
        )

        dispatcher.add_handler(
            MessageHandler(
                user_audio_filter,
                handle_audio_message,
            ),
            group=1,
        )
        dispatcher.add_handler(
            MessageHandler(
                Filters.text
                & ~Filters.command
                & ~FuncMessageFilter(_in_presentation_mode)
                & ~FuncMessageFilter(_in_benefit_mode),
                books_search_text_router,
            ),
            group=0,
        )
        dispatcher.add_handler(
            MessageHandler(
                Filters.text
                & ~Filters.command
                & ~FuncMessageFilter(_in_presentation_mode)
                & ~FuncMessageFilter(_in_benefit_mode),
                handle_text,
            ),
            group=1,
        )
        
        logger.info("✅ تم تسجيل جميع المعالجات")
        
        logger.info("جاري تشغيل المهام اليومية...")
        
        try:
            job_queue.run_daily(
                check_and_award_medal,
                time=time(hour=0, minute=0, second=random.randint(0, 30), tzinfo=pytz.UTC),
                name="check_and_award_medal",
                job_kwargs={"misfire_grace_time": 300, "coalesce": True},
            )
        except Exception as e:
            logger.warning(f"⚠️ خطأ في جدولة الميدالية: {e}")
        
        REMINDER_HOURS_UTC = [7, 10, 13, 16, 19]
        refresh_water_jobs()
        
        try:
            first_run_delay = _seconds_until_next_minute() + random.uniform(0, 10)
            job_queue.run_repeating(
                motivation_job,
                interval=timedelta(minutes=1),
                first=first_run_delay,
                name="motivation_job_minutely",
                job_kwargs={"misfire_grace_time": 60, "coalesce": True},
            )
            logger.info(
                "✅ تم تفعيل فحص الجرعة التحفيزية كل دقيقة (أول تشغيل بعد %.1f ثانية)",
                first_run_delay,
            )
        except Exception as e:
            logger.error(f"Error scheduling motivation job: {e}")
        
        # جدولة التصفير اليومي عند 00:00 بتوقيت الجزائر
        algeria_tz = pytz.timezone('Africa/Algiers')
        try:
            job_queue.run_daily(
                daily_reset_all,
                time=time(hour=0, minute=0, tzinfo=algeria_tz),
                name="daily_reset_all",
            )
            logger.info("✅ تم جدولة التصفير اليومي عند 00:00 بتوقيت الجزائر")
        except Exception as e:
            logger.warning(f"⚠️ خطأ في جدولة التصفير اليومي: {e}")
        
        logger.info("✅ تم تشغيل المهام اليومية")
        
    except Exception as e:
        logger.error(f"❌ خطأ في البوت: {e}", exc_info=True)
        raise


# =================== قسم الدورات - Handlers الفعلية ===================

# ثوابت Firestore
COURSES_COLLECTION = "courses"
COURSE_LESSONS_COLLECTION = "course_lessons"
COURSE_QUIZZES_COLLECTION = "course_quizzes"
COURSE_SUBSCRIPTIONS_COLLECTION = "course_subscriptions"
COURSE_PRESENTATIONS_THREADS_COLLECTION = "course_presentations_threads"
COURSE_PRESENTATION_MESSAGES_COLLECTION = "course_presentation_messages"
COURSE_PRESENTATION_CONTEXT_TYPE = "course_presentation"
COURSE_BENEFITS_COLLECTION = "course_benefits"  # بيانات سابقة محفوظة للأرشفة فقط
COURSE_BENEFIT_CONTEXT_TYPE = "course_benefit"
COURSE_BENEFIT_THREADS_COLLECTION = "course_benefit_threads"
COURSE_BENEFIT_MESSAGES_COLLECTION = "course_benefit_messages"

COURSE_NAME_MIN_LENGTH = 3
COURSE_NAME_MAX_LENGTH = 60
COURSE_LEADERBOARD_PAGE_SIZE = 10

# =================== لوحات المفاتيح للدورات ===================

COURSES_USER_MENU_KB = InlineKeyboardMarkup([
    [InlineKeyboardButton("📚 الدورات المتاحة", callback_data="COURSES:available")],
    [InlineKeyboardButton("📒 دوراتي", callback_data="COURSES:my_courses")],
    [InlineKeyboardButton("🗂 أرشيف الدورات", callback_data="COURSES:archive")],
    [InlineKeyboardButton("📝 تعديل بياناتي", callback_data="COURSES:edit_profile")],
])

COURSES_ADMIN_MENU_KB = InlineKeyboardMarkup([
    [InlineKeyboardButton("➕ إنشاء دورة", callback_data="COURSES:create")],
    [InlineKeyboardButton("🧩 إدارة الدروس", callback_data="COURSES:manage_lessons")],
    [InlineKeyboardButton("📝 إدارة الاختبارات", callback_data="COURSES:manage_quizzes")],
    [InlineKeyboardButton("📊 إحصائيات الدورات", callback_data="COURSES:statistics")],
    [InlineKeyboardButton("🗂 أرشفة/إيقاف/تشغيل", callback_data="COURSES:archive_manage")],
    [InlineKeyboardButton("🗑 حذف نهائي للدورة", callback_data="COURSES:delete")],
    [InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:admin_back")],
])


def safe_edit_message_text(query, text, reply_markup=None):
    """تعديل الرسائل بأمان بدون كسر الواجهات."""
    try:
        query.edit_message_text(text=text, reply_markup=reply_markup, parse_mode="HTML")
    except Exception as e:
        error_str = str(e)
        if "Message is not modified" in error_str:
            logger.debug("[COURSES] تم تجاهل Message is not modified")
            return
        if "Inline keyboard expected" in error_str:
            logger.warning("[COURSES] Inline keyboard expected - إعادة بناء الكيبورد")
            try:
                query.answer("📌 حدث تحديث للواجهة. أعد المحاولة.", show_alert=True)
            except Exception:
                pass
            return

        logger.exception(f"[COURSES] خطأ في تعديل الرسالة: {error_str}")
        try:
            query.answer("❌ حدث خطأ. حاول مرة أخرى.", show_alert=True)
        except Exception:
            pass


def _course_document(course_id: str):
    doc = db.collection(COURSES_COLLECTION).document(course_id).get()
    return doc.to_dict() if doc.exists else None


def _subscription_document_id(user_id: int, course_id: str) -> str:
    return f"{course_id}_{user_id}"


def _ensure_subscription(user_id: int, course_id: str):
    sub_id = _subscription_document_id(user_id, course_id)
    sub_ref = db.collection(COURSE_SUBSCRIPTIONS_COLLECTION).document(sub_id)
    sub_doc = sub_ref.get()
    if not sub_doc.exists:
        return None, sub_ref
    return sub_doc.to_dict(), sub_ref


def _get_saved_course_full_name(user_id: int) -> str:
    record = get_user_record_by_id(user_id) or {}
    saved_name = (record.get("course_full_name") or "").strip()
    if saved_name:
        return saved_name

    if not firestore_available():
        return None

    try:
        docs = (
            db.collection(COURSE_SUBSCRIPTIONS_COLLECTION)
            .where("user_id", "==", user_id)
            .limit(1)
            .stream()
        )
        for doc in docs:
            name = (doc.to_dict() or {}).get("full_name")
            if name:
                return name
    except Exception as e:
        logger.debug(f"تعذر جلب اسم الشهادة المحفوظ: {e}")
    return None


def _user_attended_lesson(user_id: int, course_id: str, lesson_id: str) -> bool:
    if not firestore_available():
        return False
    sub_ref = db.collection(COURSE_SUBSCRIPTIONS_COLLECTION).document(
        _subscription_document_id(user_id, course_id)
    )
    sub_doc = sub_ref.get()
    if not sub_doc.exists:
        return False
    attended_lessons = sub_doc.to_dict().get("lessons_attended") or []
    return lesson_id in attended_lessons


def _cancel_presentation_media_timeout(user_id: int):
    job = PRESENTATION_MEDIA_TIMEOUTS.pop(user_id, None)
    if job:
        try:
            job.schedule_removal()
        except Exception:
            pass


def _presentation_media_timeout(context: CallbackContext):
    data = context.job.context or {}
    user_id = data.get("user_id")
    chat_id = data.get("chat_id")
    WAITING_COURSE_PRESENTATION_MEDIA.pop(user_id, None)
    PRESENTATION_MEDIA_TIMEOUTS.pop(user_id, None)
    if chat_id:
        try:
            context.bot.send_message(
                chat_id=chat_id,
                text="⏳ انتهت مهلة العَرْض، افتحه من جديد إذا احتجت.",
            )
        except Exception as e:
            logger.debug("[PRES] Failed to send media timeout notice: %s", e)


def _schedule_presentation_media_timeout(user_id: int, chat_id: int, thread_id: str):
    if not job_queue:
        return
    _cancel_presentation_media_timeout(user_id)
    job = job_queue.run_once(
        _presentation_media_timeout,
        when=timedelta(minutes=10),
        context={"user_id": user_id, "chat_id": chat_id, "thread_id": thread_id},
    )
    PRESENTATION_MEDIA_TIMEOUTS[user_id] = job


def _clear_presentation_states(user_id: int):
    WAITING_COURSE_PRESENTATION_MEDIA.pop(user_id, None)
    _cancel_presentation_media_timeout(user_id)

def _cancel_course_benefit_timeout(user_id: int):
    job = COURSE_BENEFIT_TIMEOUTS.pop(user_id, None)
    if job:
        try:
            job.schedule_removal()
        except Exception:
            pass


def _course_benefit_timeout(context: CallbackContext):
    data = context.job.context or {}
    user_id = data.get("user_id")
    chat_id = data.get("chat_id")
    thread_id = data.get("thread_id") or data.get("session_id")
    WAITING_COURSE_BENEFIT_MEDIA.pop(user_id, None)
    COURSE_BENEFIT_TIMEOUTS.pop(user_id, None)
    if thread_id:
        try:
            db.collection(COURSE_BENEFIT_THREADS_COLLECTION).document(thread_id).update(
                {"status": "closed", "last_message_at": firestore.SERVER_TIMESTAMP}
            )
        except Exception as e:
            logger.debug("[BENEFIT] Failed to mark thread timeout %s: %s", thread_id, e)
    if chat_id:
        try:
            context.bot.send_message(
                chat_id=chat_id,
                text="⏳ انتهت مهلة الفائدة، افتحها مجدداً إذا احتجت.",
            )
        except Exception as e:
            logger.debug("[BENEFIT] Failed to send timeout notice: %s", e)


def _schedule_course_benefit_timeout(user_id: int, chat_id: int, session_id: str):
    if not job_queue:
        return
    _cancel_course_benefit_timeout(user_id)
    job = job_queue.run_once(
        _course_benefit_timeout,
        when=timedelta(minutes=10),
        context={
            "user_id": user_id,
            "chat_id": chat_id,
            "session_id": session_id,
            "thread_id": session_id,
        },
    )
    COURSE_BENEFIT_TIMEOUTS[user_id] = job


def _clear_benefit_states(user_id: int):
    WAITING_COURSE_BENEFIT_MEDIA.pop(user_id, None)
    _cancel_course_benefit_timeout(user_id)

def _format_gender_label(gender: Optional[str]) -> str:
    if gender == "male":
        return "ذكر"
    if gender == "female":
        return "أنثى"
    return "غير محدد"


# =================== Handlers للمستخدمين العاديين ===================


def open_courses_menu(update: Update, context: CallbackContext):
    """فتح قائمة الدورات الرئيسية"""
    user_id = update.effective_user.id
    _clear_presentation_states(user_id)
    _clear_course_transient_messages(context, update.message.chat_id, user_id)
    msg = update.message

    msg.reply_text(
        "🎓 قسم الدورات\n\nاختر من الخيارات التالية:",
        reply_markup=COURSES_USER_MENU_KB,
    )
    # إعادة الكيبورد الرئيسي لمنع ظهور زر الرجوع للقائمة الرئيسية في قوائم الدورات
    try:
        kb_msg = msg.reply_text(
            " ",  # رسالة فارغة لإجبار تحديث الكيبورد فقط
            reply_markup=user_main_keyboard(user_id),
        )
        context.user_data["courses_keyboard_msg_id"] = kb_msg.message_id
    except Exception:
        logger.debug("[COURSES] تعذر تحديث كيبورد المستخدم للقائمة الرئيسية")


def open_courses_admin_menu(update: Update, context: CallbackContext):
    """فتح لوحة إدارة الدورات من لوحة التحكم."""
    user_id = update.effective_user.id
    _clear_presentation_states(user_id)
    _clear_course_transient_messages(context, update.message.chat_id, user_id)
    msg = update.message

    if not (is_admin(user_id) or is_supervisor(user_id)):
        msg.reply_text(
            "هذا القسم خاص بالإدارة فقط.",
            reply_markup=user_main_keyboard(user_id),
        )
        return

    msg.reply_text(
        "📋 لوحة إدارة الدورات\n\nاختر ما تريد القيام به:",
        reply_markup=COURSES_ADMIN_MENU_KB,
    )
    try:
        kb_msg = msg.reply_text(
            " ",
            reply_markup=admin_panel_keyboard_for(user_id),
        )
        context.user_data["courses_keyboard_msg_id"] = kb_msg.message_id
    except Exception:
        logger.debug("[COURSES] تعذر تحديث كيبورد لوحة التحكم للأدمن/المشرفة من الرسائل")


def _show_courses_admin_menu_from_callback(query: Update.callback_query, user_id: int):
    """عرض لوحة إدارة الدورات من زر الرجوع داخل الكولباك."""

    safe_edit_message_text(
        query,
        "📋 لوحة إدارة الدورات\n\nاختر ما تريد القيام به:",
        reply_markup=COURSES_ADMIN_MENU_KB,
    )

    # تحديث الكيبورد السفلي لضمان بقاء لوحة التحكم للأدمن/المشرفة
    try:
        query.bot.send_message(
            chat_id=query.message.chat_id,
            text=" ",
            reply_markup=admin_panel_keyboard_for(user_id),
        )
    except Exception:
        logger.debug("[COURSES] تعذر تحديث كيبورد لوحة التحكم للأدمن/المشرفة بعد الرجوع")


def show_available_courses(query: Update.callback_query, context: CallbackContext):
    if not firestore_available():
        safe_edit_message_text(
            query,
            "❌ خطأ في الاتصال بقاعدة البيانات.\n\nحاول لاحقاً.",
            reply_markup=COURSES_USER_MENU_KB,
        )
        return

    try:
        _clear_course_transient_messages(
            context, query.message.chat_id, query.from_user.id if query.from_user else None
        )
        try:
            context.bot.send_message(
                chat_id=query.message.chat_id,
                text=" ",
                reply_markup=ReplyKeyboardRemove(),
            )
        except Exception:
            logger.debug("[COURSES] تعذر تحديث كيبورد المستخدم للقائمة الرئيسية")
        else:
            context.user_data.pop("courses_keyboard_msg_id", None)

        courses_ref = db.collection(COURSES_COLLECTION)
        docs = courses_ref.where("status", "==", "active").stream()
        courses = []
        for doc in docs:
            data = doc.to_dict()
            data["id"] = doc.id
            courses.append(data)

        filtered_courses = []
        for course in courses:
            course_name = course.get("name", "دورة")
            if _is_back_placeholder_course(course_name):
                continue
            filtered_courses.append(course)

        if not filtered_courses:
            safe_edit_message_text(
                query,
                "📚 الدورات المتاحة\n\nلا توجد دورات متاحة حالياً.",
                reply_markup=COURSES_USER_MENU_KB,
            )
            return

        text = "📚 الدورات المتاحة:\n\n"
        keyboard = []
        for course in filtered_courses:
            course_name = course.get("name", "دورة")
            course_id = course.get("id")
            text += f"• {course_name}\n"
            keyboard.append(
                [
                    InlineKeyboardButton(
                        f"🔍 {course_name}", callback_data=f"COURSES:view_{course_id}"
                    )
                ]
            )

        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:back_user")])
        safe_edit_message_text(query, text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"خطأ في جلب الدورات المتاحة: {e}")
        safe_edit_message_text(
            query,
            "❌ حدث خطأ. حاول مرة أخرى.",
            reply_markup=COURSES_USER_MENU_KB,
        )


def show_my_courses(query: Update.callback_query, context: CallbackContext):
    user_id = query.from_user.id
    if not firestore_available():
        safe_edit_message_text(
            query,
            "❌ خطأ في الاتصال بقاعدة البيانات.",
            reply_markup=COURSES_USER_MENU_KB,
        )
        return

    try:
        _clear_course_transient_messages(context, query.message.chat_id, user_id)
        try:
            context.bot.send_message(
                chat_id=query.message.chat_id,
                text=" ",
                reply_markup=ReplyKeyboardRemove(),
            )
        except Exception:
            logger.debug("[COURSES] تعذر تحديث كيبورد المستخدم للقائمة الرئيسية")
        else:
            context.user_data.pop("courses_keyboard_msg_id", None)

        subs_ref = db.collection(COURSE_SUBSCRIPTIONS_COLLECTION)
        subs_docs = subs_ref.where("user_id", "==", user_id).stream()
        course_ids = []
        for doc in subs_docs:
            data = doc.to_dict()
            course_ids.append(data.get("course_id"))

        if not course_ids:
            safe_edit_message_text(
                query,
                "📒 دوراتي\n\nأنت لم تشترك في أي دورة حتى الآن.",
                reply_markup=COURSES_USER_MENU_KB,
            )
            return

        text = "📒 دوراتي:\n\n"
        keyboard = []
        for course_id in course_ids:
            course = _course_document(course_id)
            if not course:
                continue
            course_name = course.get("name", "دورة")
            if _is_back_placeholder_course(course_name):
                continue
            text += f"• {course_name}\n"
            keyboard.append(
                [
                    InlineKeyboardButton(
                        f"📖 {course_name}", callback_data=f"COURSES:view_{course_id}"
                    )
                ]
            )

        if not keyboard:
            safe_edit_message_text(
                query,
                "📒 دوراتي\n\nأنت لم تشترك في أي دورة صالحة للعرض حالياً.",
                reply_markup=COURSES_USER_MENU_KB,
            )
            return

        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:back_user")])
        safe_edit_message_text(query, text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"خطأ في جلب دورات المستخدم: {e}")
        safe_edit_message_text(
            query,
            "❌ حدث خطأ. حاول مرة أخرى.",
            reply_markup=COURSES_USER_MENU_KB,
        )


def show_archived_courses(query: Update.callback_query, context: CallbackContext):
    if not firestore_available():
        safe_edit_message_text(
            query,
            "❌ خطأ في الاتصال بقاعدة البيانات.",
            reply_markup=COURSES_USER_MENU_KB,
        )
        return

    try:
        courses_ref = db.collection(COURSES_COLLECTION)
        docs = courses_ref.where("status", "==", "inactive").stream()
        courses = []
        for doc in docs:
            data = doc.to_dict()
            data["id"] = doc.id
            courses.append(data)

        filtered_courses = []
        for course in courses:
            course_name = course.get("name", "دورة")
            if _is_back_placeholder_course(course_name):
                continue
            filtered_courses.append(course)

        if not filtered_courses:
            safe_edit_message_text(
                query,
                "🗂 أرشيف الدورات\n\nلا توجد دورات مؤرشفة.",
                reply_markup=COURSES_USER_MENU_KB,
            )
            return

        text = "🗂 أرشيف الدورات:\n\n"
        keyboard = []
        for course in filtered_courses:
            course_name = course.get("name", "دورة")
            course_id = course.get("id")
            text += f"• {course_name}\n"
            keyboard.append(
                [
                    InlineKeyboardButton(
                        f"📖 {course_name}", callback_data=f"COURSES:view_{course_id}"
                    )
                ]
            )

        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:back_user")])
        safe_edit_message_text(query, text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"خطأ في جلب الدورات المؤرشفة: {e}")
        safe_edit_message_text(
            query,
            "❌ حدث خطأ. حاول مرة أخرى.",
            reply_markup=COURSES_USER_MENU_KB,
        )


def start_profile_edit(query: Update.callback_query, context: CallbackContext):
    user_id = query.from_user.id
    if not firestore_available():
        safe_edit_message_text(
            query,
            "❌ لا يمكن تعديل البيانات الآن.",
            reply_markup=COURSES_USER_MENU_KB,
        )
        return

    _reset_course_subscription_flow(user_id)
    _reset_profile_edit_flow(user_id)

    record = get_user_record_by_id(user_id) or {}
    saved_name = _get_saved_course_full_name(user_id)
    age = record.get("age")
    country = record.get("country")

    PROFILE_EDIT_CONTEXT[user_id] = {
        "full_name": saved_name,
        "age": age,
        "country": country,
    }

    summary_lines = [
        "📝 تعديل بياناتي",
        f"الاسم الكامل: {saved_name or 'غير محدد'}",
        f"العمر: {age if age is not None else 'غير محدد'}",
        f"الدولة: {country or 'غير محددة'}",
        "",
        "أرسل الاسم الكامل الذي توده أن يظهر على الشهادة.",
    ]

    safe_edit_message_text(
        query,
        "\n".join(summary_lines),
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:back_user")]]
        ),
    )
    try:
        context.bot.send_message(
            chat_id=query.message.chat_id,
            text="ادخل اسمك الكامل الذي توده أن يظهر على الشهادة",
            reply_markup=ReplyKeyboardMarkup([[KeyboardButton(BTN_CANCEL)]], resize_keyboard=True),
        )
    except Exception as e:
        logger.debug(f"تعذر إرسال رسالة بدء تعديل البيانات: {e}")
    WAITING_PROFILE_EDIT_NAME.add(user_id)


def _course_details_text(course_id: str, course: Dict, subscribed: bool, subscription: Dict):
    desc = course.get("description") or "لا يوجد وصف متاح."
    status = course.get("status", "active")
    status_label = "✅ مفعلة" if status == "active" else "📁 مؤرشفة"
    points = subscription.get("points", 0) if subscription else 0
    lines = [
        f"📖 <b>{course.get('name', 'دورة')}</b>",
        f"الحالة: {status_label}",
        f"الوصف:\n{desc}",
    ]
    if subscribed:
        lines.append(f"⭐️ نقاطك في الدورة: {points}")
    return "\n\n".join(lines)


def show_course_details(
    query: Update.callback_query,
    context: CallbackContext,
    user_id: int,
    course_id: str,
):
    _clear_course_transient_messages(context, query.message.chat_id, user_id)
    course = _course_document(course_id)
    if not course:
        safe_edit_message_text(query, "❌ الدورة غير موجودة.", reply_markup=COURSES_USER_MENU_KB)
        return

    subscription, _ = _ensure_subscription(user_id, course_id)
    subscribed = subscription is not None
    keyboard = []

    if course.get("status", "active") == "active" and not subscribed:
        keyboard.append(
            [
                InlineKeyboardButton(
                    "📝 التسجيل في الدورة", callback_data=f"COURSES:subscribe_{course_id}"
                )
            ]
        )

    if subscribed:
        keyboard.extend(
            [
                [InlineKeyboardButton("📚 الدروس", callback_data=f"COURSES:user_lessons_{course_id}")],
                [InlineKeyboardButton("📝 الاختبارات", callback_data=f"COURSES:user_quizzes_{course_id}")],
                [InlineKeyboardButton("⭐️ نقاطي", callback_data=f"COURSES:user_points_{course_id}")],
            ]
        )

    back_target = context.user_data.get("courses_back_target", "COURSES:back_user")
    keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data=back_target)])

    safe_edit_message_text(
        query,
        _course_details_text(course_id, course, subscribed, subscription or {}),
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


def subscribe_to_course(query: Update.callback_query, context: CallbackContext, course_id: str):
    user = query.from_user
    if not firestore_available():
        safe_edit_message_text(
            query,
            "❌ لا يمكن التسجيل الآن. جرّب لاحقاً.",
            reply_markup=COURSES_USER_MENU_KB,
        )
        return

    course = _course_document(course_id)
    if not course or course.get("status", "active") != "active":
        safe_edit_message_text(query, "❌ هذه الدورة غير متاحة للتسجيل.", reply_markup=COURSES_USER_MENU_KB)
        return

    existing, sub_ref = _ensure_subscription(user.id, course_id)
    if existing:
        safe_edit_message_text(query, "✅ أنت مسجّل بالفعل في هذه الدورة.", reply_markup=COURSES_USER_MENU_KB)
        return

    _reset_course_subscription_flow(user.id)
    COURSE_SUBSCRIPTION_CONTEXT[user.id] = {
        "course_id": course_id,
        "course_name": course.get("name", "دورة"),
    }
    WAITING_COURSE_COUNTRY.add(user.id)

    try:
        safe_edit_message_text(
            query,
            "📝 لإتمام التسجيل يرجى إرسال بيانات بسيطة.\n\nأرسل اسم بلدك الآن.",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🔙 رجوع", callback_data=f"COURSES:view_{course_id}")],
                ]
            ),
        )
        context.bot.send_message(
            chat_id=query.message.chat_id,
            text="🌍 أرسل اسم بلدك للتسجيل في الدورة.",
            reply_markup=ReplyKeyboardRemove(),
        )
    except Exception as e:
        logger.error(f"خطأ في بدء جمع بيانات التسجيل للدورة: {e}")
        safe_edit_message_text(query, "❌ لم نتمكن من بدء التسجيل حالياً.", reply_markup=COURSES_USER_MENU_KB)
        _reset_course_subscription_flow(user.id)


def _finalize_course_subscription(user: User, context: CallbackContext):
    user_id = user.id
    ctx = COURSE_SUBSCRIPTION_CONTEXT.get(user_id, {})
    course_id = ctx.get("course_id")
    country = ctx.get("country")
    age = ctx.get("age")
    gender = ctx.get("gender")
    full_name_value = (ctx.get("full_name") or _get_saved_course_full_name(user_id) or "").strip()

    if not course_id:
        context.bot.send_message(
            chat_id=user_id,
            text="❌ لا توجد دورة قيد التسجيل حالياً.",
            reply_markup=user_main_keyboard(user_id),
        )
        _reset_course_subscription_flow(user_id)
        return

    course = _course_document(course_id)
    if not course or course.get("status", "active") != "active":
        context.bot.send_message(
            chat_id=user_id,
            text="❌ هذه الدورة لم تعد متاحة للتسجيل.",
            reply_markup=user_main_keyboard(user_id),
        )
        _reset_course_subscription_flow(user_id)
        return

    existing, sub_ref = _ensure_subscription(user_id, course_id)
    if existing:
        context.bot.send_message(
            chat_id=user_id,
            text="✅ أنت مسجل بالفعل في هذه الدورة.",
            reply_markup=user_main_keyboard(user_id),
        )
        _reset_course_subscription_flow(user_id)
        return

    if not country or age is None or not gender:
        context.bot.send_message(
            chat_id=user_id,
            text="⚠️ البيانات غير مكتملة. أعد المحاولة من جديد.",
            reply_markup=user_main_keyboard(user_id),
        )
        _reset_course_subscription_flow(user_id)
        return

    if not full_name_value:
        WAITING_COURSE_FULL_NAME.add(user_id)
        context.bot.send_message(
            chat_id=user_id,
            text="ادخل اسمك الكامل الذي توده أن يظهر على الشهادة",
            reply_markup=ReplyKeyboardMarkup([[KeyboardButton(BTN_CANCEL)]], resize_keyboard=True),
        )
        return

    try:
        sub_data = {
            "id": sub_ref.id,
            "course_id": course_id,
            "user_id": user_id,
            "username": user.username,
            "full_name": full_name_value,
            "points": 0,
            "joined_at": firestore.SERVER_TIMESTAMP,
            "country": country,
            "age": age,
            "gender": gender,
        }
        sub_ref.set(sub_data)
        update_user_record(
            user_id,
            country=country,
            age=age,
            gender=gender,
            course_full_name=full_name_value,
        )
        context.bot.send_message(
            chat_id=user_id,
            text="✅ تم تسجيلك في الدورة بنجاح!\nستصلك الدروس والاختبارات هنا.",
            reply_markup=user_main_keyboard(user_id),
        )

        notify_text = (
            "📥 تسجيل جديد في دورة\n"
            f"اسم الدورة: {course.get('name', 'دورة')}\n"
            f"المستخدم: {user.mention_html()} ({user.id})\n"
            f"الاسم الكامل: {full_name_value}\n"
            f"البلد: {country}\n"
            f"العمر: {age}\n"
            f"الجنس: {'ذكر' if gender == 'male' else 'أنثى'}"
        )
        for admin_id in [ADMIN_ID, SUPERVISOR_ID]:
            try:
                context.bot.send_message(admin_id, notify_text, parse_mode="HTML")
            except Exception as e:
                logger.warning(f"تعذر إرسال إشعار التسجيل إلى {admin_id}: {e}")
    except Exception as e:
        logger.error(f"خطأ في إتمام التسجيل بالدورة: {e}")
        context.bot.send_message(
            chat_id=user_id,
            text="❌ لم نتمكن من إتمام التسجيل حالياً. حاول لاحقاً.",
            reply_markup=user_main_keyboard(user_id),
        )
    finally:
        _reset_course_subscription_flow(user_id)


def _finalize_profile_edit(user_id: int, chat_id: int, context: CallbackContext):
    if not firestore_available():
        context.bot.send_message(
            chat_id=chat_id,
            text="❌ لا يمكن تعديل البيانات الآن.",
            reply_markup=user_main_keyboard(user_id),
        )
        _reset_profile_edit_flow(user_id)
        return

    ctx = PROFILE_EDIT_CONTEXT.get(user_id, {})
    full_name = (ctx.get("full_name") or "").strip()
    age = ctx.get("age")
    country = ctx.get("country")

    if not full_name:
        WAITING_PROFILE_EDIT_NAME.add(user_id)
        context.bot.send_message(
            chat_id=chat_id,
            text="⚠️ أدخل اسمك الكامل لاعتماده على الشهادة.",
            reply_markup=ReplyKeyboardMarkup([[KeyboardButton(BTN_CANCEL)]], resize_keyboard=True),
        )
        return

    try:
        update_user_record(
            user_id,
            course_full_name=full_name,
            age=age,
            country=country,
        )
        try:
            subs = db.collection(COURSE_SUBSCRIPTIONS_COLLECTION).where("user_id", "==", user_id).stream()
            batch = db.batch()
            count = 0
            for sub in subs:
                batch.update(
                    sub.reference,
                    {"full_name": full_name, "age": age, "country": country},
                )
                count += 1
                if count % 400 == 0:
                    batch.commit()
                    batch = db.batch()
            batch.commit()
        except Exception as e:
            logger.warning(f"تعذر تحديث بيانات الاشتراك للدورات: {e}")

        context.bot.send_message(
            chat_id=chat_id,
            text="✅ تم تحديث بياناتك بنجاح.",
            reply_markup=user_main_keyboard(user_id),
        )
    except Exception as e:
        logger.error(f"خطأ في حفظ بيانات الملف الشخصي: {e}")
        context.bot.send_message(
            chat_id=chat_id,
            text="❌ لم نتمكن من تحديث البيانات حالياً.",
            reply_markup=user_main_keyboard(user_id),
        )
    finally:
        _reset_profile_edit_flow(user_id)


def _clear_attendance_confirmation(context: CallbackContext, chat_id: int):
    msg_id = context.user_data.get("attendance_confirmation_msg_id")
    if not msg_id:
        return

    try:
        context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
    except Exception as e:
        logger.debug(f"تعذر حذف رسالة تأكيد الحضور: {e}")
    finally:
        context.user_data.pop("attendance_confirmation_msg_id", None)


def _clear_lesson_audio(context: CallbackContext, chat_id: int):
    msg_id = context.user_data.get("lesson_audio_msg_id")
    if not msg_id:
        return

    try:
        context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
    except Exception:
        pass
    finally:
        context.user_data.pop("lesson_audio_msg_id", None)


def _clear_course_transient_messages(
    context: CallbackContext, chat_id: int, user_id: Optional[int] = None
):
    _clear_lesson_audio(context, chat_id)
    _clear_attendance_confirmation(context, chat_id)
    if user_id is not None:
        _clear_presentation_states(user_id)
        _clear_benefit_states(user_id)
    kb_msg_id = context.user_data.pop("courses_keyboard_msg_id", None)
    if kb_msg_id:
        try:
            context.bot.delete_message(chat_id=chat_id, message_id=kb_msg_id)
        except Exception as e:
            logger.debug(f"تعذر حذف رسالة كيبورد الدورات: {e}")


def user_lessons_list(query: Update.callback_query, context: CallbackContext, course_id: str):
    _clear_course_transient_messages(context, query.message.chat_id, query.from_user.id)
    try:
        lessons_ref = db.collection(COURSE_LESSONS_COLLECTION)
        lessons = list(lessons_ref.where("course_id", "==", course_id).stream())

        if not lessons:
            safe_edit_message_text(
                query,
                "📚 لا توجد دروس مضافة بعد لهذه الدورة.",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "🔙 رجوع", callback_data=f"COURSES:back_course_{course_id}"
                            )
                        ],
                    ]
                ),
            )
            return

        keyboard = []
        for doc in lessons:
            lesson = doc.to_dict()
            keyboard.append(
                [
                    InlineKeyboardButton(
                        f"📖 {lesson.get('title', 'درس')}",
                        callback_data=f"COURSES:view_lesson_{doc.id}",
                    )
                ]
            )

        keyboard.append(
            [InlineKeyboardButton("🔙 رجوع", callback_data=f"COURSES:back_course_{course_id}")]
        )
        safe_edit_message_text(
            query,
            "📚 دروس الدورة:\nاختر درساً للعرض",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    except Exception as e:
        logger.error(f"خطأ في جلب دروس الدورة: {e}")
        safe_edit_message_text(query, "❌ تعذر تحميل الدروس حالياً.", reply_markup=COURSES_USER_MENU_KB)


def _lesson_view_keyboard(
    course_id: str,
    lesson_id: str,
    show_presentation: bool = False,
    presentation_thread_id: Optional[str] = None,
    benefit_thread_id: Optional[str] = None,
) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("🔙 رجوع", callback_data=f"COURSES:user_lessons_{course_id}")],
        [
            InlineKeyboardButton(
                "✅ تسجيل الحضور", callback_data=f"COURSES:attend_{lesson_id}"
            )
        ],
    ]
    if show_presentation and not presentation_thread_id:
        keyboard.append(
            [
                InlineKeyboardButton(
                    "🎙️ العَرْض",
                    callback_data=f"COURSE:PRES:OPEN:{course_id}:{lesson_id}",
                )
            ]
        )

    keyboard.append(
        [
            InlineKeyboardButton(
                "📸 الفائدة",
                callback_data=f"COURSE:BEN:OPEN:{course_id}:{lesson_id}",
            )
        ]
    )
    return InlineKeyboardMarkup(keyboard)


def user_view_lesson(query: Update.callback_query, context: CallbackContext, lesson_id: str, user_id: int):
    _clear_course_transient_messages(context, query.message.chat_id, user_id)
    doc = db.collection(COURSE_LESSONS_COLLECTION).document(lesson_id).get()
    if not doc.exists:
        safe_edit_message_text(query, "❌ الدرس غير موجود.", reply_markup=COURSES_USER_MENU_KB)
        return

    lesson = doc.to_dict()
    course_id = lesson.get("course_id")
    has_presentation = lesson.get("has_presentation", False)
    show_presentation = has_presentation
    presentation_thread_id = None
    benefit_thread_id = None
    if has_presentation:
        waiting_thread_id = WAITING_COURSE_PRESENTATION_MEDIA.get(user_id)
        if waiting_thread_id:
            if firestore_available():
                thread_doc = db.collection(COURSE_PRESENTATIONS_THREADS_COLLECTION).document(waiting_thread_id).get()
                if thread_doc.exists:
                    thread = thread_doc.to_dict() or {}
                    if (
                        thread.get("lesson_id") == lesson_id
                        and thread.get("status") == "open"
                    ):
                        presentation_thread_id = waiting_thread_id
            else:
                presentation_thread_id = waiting_thread_id

    benefit_ctx = WAITING_COURSE_BENEFIT_MEDIA.get(user_id)
    if benefit_ctx and benefit_ctx.get("lesson_id") == lesson_id:
        benefit_thread_id = benefit_ctx.get("thread_id") or benefit_ctx.get("session_id")

    view_keyboard = _lesson_view_keyboard(
        course_id,
        lesson_id,
        show_presentation,
        presentation_thread_id=presentation_thread_id,
        benefit_thread_id=benefit_thread_id,
    )

    content_type = lesson.get("content_type", "text")
    title = lesson.get("title", "درس")
    content = lesson.get("content", "")

    if content_type == "audio":
        file_id = lesson.get("audio_file_id")
        audio_kind = lesson.get("audio_kind")
        if not file_id:
            safe_edit_message_text(
                query,
                f"<b>{title}</b>\n\n⚠️ لا يوجد ملف صوتي مرفق لهذا الدرس.",
                reply_markup=view_keyboard,
            )
            return

        try:
            if audio_kind == "voice":
                audio_message = context.bot.send_voice(
                    chat_id=query.message.chat_id,
                    voice=file_id,
                    caption=title,
                )
            elif audio_kind == "document_audio":
                audio_message = context.bot.send_document(
                    chat_id=query.message.chat_id,
                    document=file_id,
                    caption=title,
                )
            else:
                audio_message = context.bot.send_audio(
                    chat_id=query.message.chat_id,
                    audio=file_id,
                    caption=title,
                )
            context.user_data["lesson_audio_msg_id"] = audio_message.message_id
            safe_edit_message_text(
                query,
                f"📖 {title}\nتم إرسال المقطع الصوتي أعلاه.",
                reply_markup=view_keyboard,
            )
        except Exception as e:
            logger.error(f"خطأ في إرسال الدرس الصوتي: {e}")
            try:
                if audio_kind == "document_audio":
                    doc_msg = context.bot.send_document(
                        chat_id=query.message.chat_id,
                        document=file_id,
                        caption=title,
                    )
                    context.user_data["lesson_audio_msg_id"] = doc_msg.message_id
                    safe_edit_message_text(
                        query,
                        f"📖 {title}\nتم إرسال المقطع الصوتي أعلاه.",
                        reply_markup=view_keyboard,
                    )
                    return
            except Exception:
                logger.debug("فشل إرسال الدرس كملف وثيقة بعد فشل الصوت.")

            safe_edit_message_text(
                query,
                f"<b>{title}</b>\n\nتعذر إرسال المقطع الصوتي. يرجى التأكد من صحة الملف الصوتي.",
                reply_markup=view_keyboard,
            )
        return

    document_id = lesson.get("document_file_id") or lesson.get("file_id")
    if content_type in {"document", "file"} and document_id:
        try:
            context.bot.send_document(
                chat_id=query.message.chat_id, document=document_id, caption=title
            )
            safe_edit_message_text(
                query,
                f"📖 {title}\nتم إرسال الملف أعلاه.",
                reply_markup=view_keyboard,
            )
        except Exception as e:
            logger.error(f"خطأ في إرسال ملف الدرس: {e}")
            safe_edit_message_text(
                query,
                f"<b>{title}</b>\n\nتعذر إرسال الملف المرفق لهذا الدرس.",
                reply_markup=view_keyboard,
            )
        return

    if content_type == "link" and content:
        content_display = f"<b>{title}</b>\n\n🔗 <a href='{content}'>فتح الرابط</a>"
    else:
        content_display = f"<b>{title}</b>\n\n{content}"

    safe_edit_message_text(
        query,
        content_display,
        reply_markup=view_keyboard,
    )


def register_lesson_attendance(
    query: Update.callback_query, context: CallbackContext, user_id: int, lesson_id: str
):
    lesson_id = str(lesson_id)
    if not firestore_available():
        safe_edit_message_text(
            query,
            "❌ لا يمكن تسجيل الحضور حالياً. حاول لاحقاً.",
            reply_markup=COURSES_USER_MENU_KB,
        )
        return

    lesson_doc = db.collection(COURSE_LESSONS_COLLECTION).document(lesson_id).get()
    if not lesson_doc.exists:
        safe_edit_message_text(query, "❌ الدرس غير موجود.", reply_markup=COURSES_USER_MENU_KB)
        return

    lesson = lesson_doc.to_dict()
    course_id = lesson.get("course_id")
    logger.info(
        "🟢 ATTEND_START | user_id=%s | course_id=%s | lesson_id=%s",
        user_id,
        course_id,
        lesson_id,
    )
    sub_id = _subscription_document_id(user_id, course_id)
    sub_ref = db.collection(COURSE_SUBSCRIPTIONS_COLLECTION).document(sub_id)
    logger.info("📄 SUB_DOC_REF | path=%s", sub_ref.path)
    sub_doc = sub_ref.get()
    logger.info("📄 SUB_DOC_EXISTS=%s", sub_doc.exists)
    if sub_doc.exists:
        logger.info("📄 SUB_DATA_KEYS=%s", list((sub_doc.to_dict() or {}).keys()))

    if not sub_doc.exists:
        safe_edit_message_text(
            query,
            "❌ يجب التسجيل في الدورة أولاً لتسجيل الحضور.",
            reply_markup=COURSES_USER_MENU_KB,
        )
        return

    subscription = sub_doc.to_dict() or {}
    attended_lessons = subscription.get("lessons_attended") or []
    logger.info(
        "🧾 ATTEND_STATE | lesson_id=%s | attended_type=%s | attended_len=%s | attended_sample=%s",
        lesson_id,
        type(attended_lessons).__name__,
        len(attended_lessons),
        attended_lessons[:5] if isinstance(attended_lessons, list) else str(attended_lessons)[:200],
    )
    _clear_attendance_confirmation(context, query.message.chat_id)
    if lesson_id in attended_lessons:
        logger.info("🟡 ATTEND_ALREADY | user_id=%s | lesson_id=%s", user_id, lesson_id)
        query.answer("✅ تم تسجيل حضورك مسبقًا.", show_alert=True)
        try:
            confirmation_message = query.message.reply_text("✅ تم تسجيل حضورك مسبقًا.")
            context.user_data["attendance_confirmation_msg_id"] = (
                confirmation_message.message_id
            )
        except Exception:
            pass
        return

    try:
        current_points = int(subscription.get("points", 0))
        new_points = current_points + 1

        logger.info("✏️ ATTEND_UPDATE_TRY | lesson_id=%s", lesson_id)
        sub_ref.update(
            {
                "lessons_attended": firestore.ArrayUnion([lesson_id]),
                "points": firestore.Increment(1),
                "updated_at": firestore.SERVER_TIMESTAMP,
            }
        )
        fresh = sub_ref.get().to_dict() or {}
        logger.info(
            "✅ ATTEND_UPDATE_OK | points=%s | lessons_attended_len=%s",
            fresh.get("points"),
            len(fresh.get("lessons_attended") or []),
        )
        confirmation_text = "✅ تم تسجيل حضورك بنجاح."
        if lesson.get("has_presentation"):
            confirmation_text += "\n🎙️ يمكنك الآن فتح العَرْض لهذا الدرس."
        query.answer(confirmation_text, show_alert=True)
        try:
            confirmation_message = query.message.reply_text("✅ تم تسجيل حضورك بنجاح.")
            context.user_data["attendance_confirmation_msg_id"] = (
                confirmation_message.message_id
            )
        except Exception:
            pass
    except Exception as e:
        logger.error("❌ ATTEND_UPDATE_FAIL", exc_info=True)
        query.answer("❌ تعذر تسجيل الحضور حالياً.", show_alert=True)


def _build_presentation_header(thread: Dict, thread_id: str) -> str:
    username = thread.get("user_username")
    username_part = f" @{username}" if username else ""
    return (
        "🎓 عَرْض دورة\n"
        f"👤 المتعلم: {thread.get('user_name', 'متعلم')}{username_part}\n"
        f"🧕 الجنس: {_format_gender_label(thread.get('user_gender'))}\n"
        f"📚 الدورة: {thread.get('course_title', 'دورة')}\n"
        f"📘 الدرس: {thread.get('lesson_title', 'درس')}\n"
        f"🆔 Thread: {thread_id}"
    )

def _extract_presentation_payload(message) -> Optional[Dict]:
    if not message:
        return None
    if message.text:
        return {"type": "text", "text": message.text}
    if message.voice:
        return {
            "type": "voice",
            "file_id": message.voice.file_id,
            "duration": getattr(message.voice, "duration", None),
            "caption": message.caption,
        }
    if message.audio:
        return {
            "type": "audio",
            "file_id": message.audio.file_id,
            "duration": getattr(message.audio, "duration", None),
            "caption": message.caption,
        }
    if message.photo:
        return {
            "type": "photo",
            "file_id": message.photo[-1].file_id,
            "caption": message.caption,
        }
    if message.video_note:
        return {
            "type": "video_note",
            "file_id": message.video_note.file_id,
            "duration": getattr(message.video_note, "duration", None),
            "caption": message.caption,
        }
    if message.document:
        return {
            "type": "document",
            "file_id": message.document.file_id,
            "caption": message.caption,
        }
    return None


def _store_presentation_message(
    thread_id: str,
    sender_type: str,
    payload: Dict,
    telegram_message_id: int,
    sender_id: Optional[int] = None,
):
    message_payload = {
        "thread_id": thread_id,
        "sender_type": sender_type,
        "sender_id": sender_id,
        "message_type": payload.get("type"),
        "text": payload.get("text"),
        "file_id": payload.get("file_id"),
        "duration": payload.get("duration"),
        "caption": payload.get("caption"),
        "telegram_message_id": telegram_message_id,
        "context_type": COURSE_PRESENTATION_CONTEXT_TYPE,
        "created_at": firestore.SERVER_TIMESTAMP,
    }
    try:
        db.collection(COURSE_PRESENTATION_MESSAGES_COLLECTION).add(message_payload)
        db.collection(COURSE_PRESENTATIONS_THREADS_COLLECTION).document(thread_id).update(
            {"last_message_at": firestore.SERVER_TIMESTAMP}
        )
    except Exception as e:
        logger.error(f"Error storing presentation message: {e}")


def _build_benefit_header(thread: Dict, thread_id: str) -> str:
    username = thread.get("user_username")
    username_part = f" @{username}" if username else ""
    section_line = (
        f"📘 باب المقرر: {thread.get('curriculum_section')}\n"
        if thread.get("curriculum_section")
        else ""
    )
    return (
        "📸 فائدة درس\n"
        f"👤 المتعلم: {thread.get('user_name', 'متعلم')}{username_part}\n"
        f"🧕 الجنس: {_format_gender_label(thread.get('user_gender'))}\n"
        f"📚 الدورة: {thread.get('course_title', 'دورة')}\n"
        f"📖 الدرس: {thread.get('lesson_title', 'درس')}\n"
        f"🆔 Thread: {thread_id}\n"
        f"{section_line}"
    ).rstrip()


def _store_course_benefit(context: Dict, message_id: int, incoming_payload: Dict):
    doc_payload = {
        "session_id": context.get("session_id"),
        "thread_id": context.get("thread_id"),
        "user_id": context.get("user_id"),
        "user_name": context.get("user_name"),
        "user_username": context.get("user_username"),
        "user_gender": context.get("user_gender"),
        "course_id": context.get("course_id"),
        "course_title": context.get("course_title"),
        "lesson_id": context.get("lesson_id"),
        "lesson_title": context.get("lesson_title"),
        "curriculum_section": context.get("curriculum_section"),
        "message_type": incoming_payload.get("type"),
        "file_id": incoming_payload.get("file_id"),
        "text": incoming_payload.get("text"),
        "caption": incoming_payload.get("caption"),
        "telegram_message_id": message_id,
        "context_type": COURSE_BENEFIT_CONTEXT_TYPE,
        "created_at": firestore.SERVER_TIMESTAMP,
    }
    try:
        db.collection(COURSE_BENEFITS_COLLECTION).add(doc_payload)
    except Exception as e:
        logger.error(f"Error storing course benefit: {e}")


def _store_benefit_message(
    thread_id: str,
    sender_type: str,
    payload: Dict,
    telegram_message_id: int,
    sender_id: Optional[int] = None,
):
    message_payload = {
        "thread_id": thread_id,
        "sender_type": sender_type,
        "sender_id": sender_id,
        "message_type": payload.get("type"),
        "text": payload.get("text"),
        "file_id": payload.get("file_id"),
        "caption": payload.get("caption"),
        "telegram_message_id": telegram_message_id,
        "context_type": COURSE_BENEFIT_CONTEXT_TYPE,
        "created_at": firestore.SERVER_TIMESTAMP,
    }
    try:
        db.collection(COURSE_BENEFIT_MESSAGES_COLLECTION).add(message_payload)
        db.collection(COURSE_BENEFIT_THREADS_COLLECTION).document(thread_id).update(
            {"last_message_at": firestore.SERVER_TIMESTAMP}
        )
    except Exception as e:
        logger.error(f"Error storing benefit message: {e}")


def _extract_benefit_payload(message) -> Optional[Dict]:
    if getattr(message, "text", None):
        return {"type": "text", "text": message.text}
    if message.photo:
        return {
            "type": "photo",
            "file_id": message.photo[-1].file_id,
            "caption": message.caption,
            "text": message.caption,
        }
    return None


def handle_course_presentation_open(
    query: Update.callback_query,
    context: CallbackContext,
    user_id: int,
    course_id: str,
    lesson_id: str,
):
    if not firestore_available():
        safe_edit_message_text(
            query,
            "❌ لا يمكن فتح العَرْض حالياً. حاول لاحقاً.",
            reply_markup=_lesson_view_keyboard(course_id, lesson_id),
        )
        return

    lesson_doc = db.collection(COURSE_LESSONS_COLLECTION).document(lesson_id).get()
    if not lesson_doc.exists:
        safe_edit_message_text(query, "❌ الدرس غير موجود.", reply_markup=COURSES_USER_MENU_KB)
        return

    lesson = lesson_doc.to_dict() or {}
    if not lesson.get("has_presentation"):
        safe_edit_message_text(
            query,
            "❌ لا يوجد عَرْض مفعّل لهذا الدرس.",
            reply_markup=_lesson_view_keyboard(course_id, lesson_id),
        )
        return

    thread_doc = None
    thread_data = None
    try:
        existing_threads = list(
            db.collection(COURSE_PRESENTATIONS_THREADS_COLLECTION)
            .where("user_id", "==", user_id)
            .where("lesson_id", "==", lesson_id)
            .where("status", "==", "open")
            .stream()
        )
        if existing_threads:
            thread_doc = existing_threads[0]
    except Exception as e:
        logger.error(f"Error checking existing presentation threads: {e}")

    course = _course_document(course_id) or {}
    record = get_user_record_by_id(user_id) or {}
    _clear_benefit_states(user_id)

    if thread_doc:
        thread_id = thread_doc.id
        thread_data = thread_doc.to_dict() or {}
        if query.message and not thread_data.get("user_chat_id"):
            try:
                thread_doc.reference.update({"user_chat_id": query.message.chat_id})
                thread_data["user_chat_id"] = query.message.chat_id
            except Exception as e:
                logger.warning("⚠️ تعذر تحديث user_chat_id لجلسة العرض: %s", e)
    else:
        payload = {
            "user_id": user_id,
            "user_name": query.from_user.full_name,
            "user_username": query.from_user.username,
            "user_gender": record.get("gender"),
            "course_id": course_id,
            "course_title": course.get("name", "دورة"),
            "lesson_id": lesson_id,
            "lesson_title": lesson.get("title", "درس"),
            "supervisor_id": None if record.get("gender") == "male" else SUPERVISOR_ID,
            "status": "open",
            "created_at": firestore.SERVER_TIMESTAMP,
            "last_message_at": firestore.SERVER_TIMESTAMP,
            "admin_mirror_enabled": True,
            "context_type": COURSE_PRESENTATION_CONTEXT_TYPE,
            "user_chat_id": query.message.chat_id if query.message else user_id,
        }
        try:
            doc_ref = db.collection(COURSE_PRESENTATIONS_THREADS_COLLECTION).document()
            doc_ref.set(payload)
            thread_id = doc_ref.id
            thread_data = payload
        except Exception as e:
            logger.error(f"Error creating presentation thread: {e}")
            safe_edit_message_text(
                query,
                "❌ تعذر فتح العَرْض حالياً.",
                reply_markup=_lesson_view_keyboard(course_id, lesson_id),
            )
            return

    WAITING_COURSE_PRESENTATION_MEDIA[user_id] = thread_id
    if query.message:
        _schedule_presentation_media_timeout(user_id, query.message.chat_id, thread_id)
    target_label = "للأدمن" if (record.get("gender") == "male") else "للمشرفة"
    try:
        context.bot.send_message(
            chat_id=user_id,
            text=(
                "✅ تم فتح وضع العرض. أرسل تسميعك هنا.\n"
                "اضغط (خروج من العرض) لإنهاء الجلسة."
            ),
            reply_markup=PRESENTATION_SESSION_KB,
        )
    except Exception as e:
        logger.debug("[PRES] Failed to send chat-style open message: %s", e)
    safe_edit_message_text(
        query,
        f"تم فتح العَرْض {target_label}\nأرسل/أرسلي تسميعك الآن",
        reply_markup=_lesson_view_keyboard(
            course_id,
            lesson_id,
            show_presentation=True,
            presentation_thread_id=thread_id,
        ),
    )


def handle_course_presentation_close(
    query: Optional[Update.callback_query],
    context: CallbackContext,
    thread_id: str,
    *,
    user_id: Optional[int] = None,
    user_chat_id: Optional[int] = None,
):
    """إنهاء جلسة العرض وإرجاع المستخدم مباشرةً لقائمة الدروس كخيار UX معتمد."""

    resolved_user_id = user_id or (query.from_user.id if query else None)
    chat_id = user_chat_id or (
        query.message.chat_id if query and query.message else resolved_user_id
    )
    if not resolved_user_id:
        return

    if not firestore_available():
        if query:
            query.answer("❌ لا يمكن إنهاء العَرْض حالياً.", show_alert=True)
        elif chat_id:
            context.bot.send_message(
                chat_id=chat_id,
                text="❌ لا يمكن إنهاء العَرْض حالياً. حاول لاحقاً.",
            )
        return

    thread_ref = db.collection(COURSE_PRESENTATIONS_THREADS_COLLECTION).document(thread_id)
    thread_doc = thread_ref.get()
    if not thread_doc.exists:
        WAITING_COURSE_PRESENTATION_MEDIA.pop(resolved_user_id, None)
        if query:
            query.answer("⚠️ هذه الجلسة غير متاحة الآن.", show_alert=True)
        elif chat_id:
            context.bot.send_message(chat_id=chat_id, text="⚠️ هذه الجلسة غير متاحة الآن.")
        return

    thread = thread_doc.to_dict() or {}
    if thread.get("user_id") != resolved_user_id:
        if query:
            query.answer("❌ هذا الزر خاص بالطالبة صاحبة العرض.", show_alert=True)
        return

    _cancel_presentation_media_timeout(resolved_user_id)
    WAITING_COURSE_PRESENTATION_MEDIA.pop(resolved_user_id, None)
    try:
        thread_ref.update(
            {"status": "closed", "last_message_at": firestore.SERVER_TIMESTAMP}
        )
    except Exception as e:
        logger.warning(f"⚠️ تعذر تحديث حالة العَرْض عند الإنهاء: {e}")

    course_id = thread.get("course_id")
    lesson_id = thread.get("lesson_id")
    # الإغلاق يعيد المستخدم مباشرةً لقائمة دروس الدورة لضمان وضوح المسار.
    if query:
        safe_edit_message_text(
            query,
            "✅ تم إغلاق العرض.",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "📚 الرجوع لقائمة الدروس",
                            callback_data=f"COURSES:user_lessons_{course_id}",
                        )
                    ]
                ]
            ),
        )

    if chat_id:
        context.bot.send_message(
            chat_id=chat_id,
            text="✅ تم الخروج.",
            reply_markup=user_main_keyboard(resolved_user_id),
        )


def handle_course_presentation_user_media(update: Update, context: CallbackContext):
    user = update.effective_user
    if not user or user.id not in WAITING_COURSE_PRESENTATION_MEDIA:
        return

    thread_id = WAITING_COURSE_PRESENTATION_MEDIA.get(user.id)
    if not firestore_available():
        update.message.reply_text(
            "❌ لا يمكن إرسال العرض حالياً. حاول لاحقاً.",
            reply_markup=PRESENTATION_SESSION_KB,
        )
        return

    message_text = update.message.text if update.message else None
    if message_text:
        normalized_text = message_text.strip()
        if normalized_text in SESSION_EXIT_PRESENTATION_TEXTS:
            handle_course_presentation_close(
                None,
                context,
                thread_id,
                user_id=user.id,
                user_chat_id=update.message.chat_id,
            )
            raise DispatcherHandlerStop()
        if normalized_text in MAIN_MENU_BUTTON_TEXTS:
            update.message.reply_text(
                "⚠️ أنت الآن داخل وضع العرض/الفائدة. اضغط خروج لإنهاء الجلسة.",
                reply_markup=PRESENTATION_SESSION_KB,
            )
            return

    payload = _extract_presentation_payload(update.message)
    if not payload:
        update.message.reply_text(
            "⚠️ يمكن إرسال نص، صوت، صورة، ملف أو فيديو دائري فقط داخل العَرْض.",
            reply_markup=PRESENTATION_SESSION_KB,
        )
        return

    thread_doc = db.collection(COURSE_PRESENTATIONS_THREADS_COLLECTION).document(thread_id).get()
    if not thread_doc.exists:
        update.message.reply_text(
            "❌ لم يتم العثور على جلسة العَرْض. افتحها مجدداً من الدرس.",
            reply_markup=PRESENTATION_SESSION_KB,
        )
        WAITING_COURSE_PRESENTATION_MEDIA.pop(user.id, None)
        return

    thread = thread_doc.to_dict() or {}
    header = _build_presentation_header(thread, thread_id)
    _cancel_presentation_media_timeout(user.id)

    _store_presentation_message(
        thread_id, "user", payload, update.message.message_id, sender_id=user.id
    )

    user_gender = thread.get("user_gender")
    send_to_supervisor = user_gender != "male" and bool(SUPERVISOR_ID)
    admin_target = ADMIN_ID
    mirror_to_admin = admin_target and (
        user_gender == "male" or thread.get("admin_mirror_enabled", True)
    )

    def _send_presentation_to_staff(target_id: int):
        sent_header = None
        sent_payload = None
        caption = payload.get("caption")
        msg_type = payload.get("type")
        try:
            sent_header = context.bot.send_message(chat_id=target_id, text=header)
            if msg_type == "text":
                sent_payload = context.bot.send_message(
                    chat_id=target_id, text=payload.get("text", "")
                )
            elif msg_type == "voice":
                sent_payload = context.bot.send_voice(
                    chat_id=target_id,
                    voice=payload.get("file_id"),
                    caption=caption,
                )
            elif msg_type == "audio":
                sent_payload = context.bot.send_audio(
                    chat_id=target_id,
                    audio=payload.get("file_id"),
                    caption=caption,
                )
            elif msg_type == "photo":
                sent_payload = context.bot.send_photo(
                    chat_id=target_id,
                    photo=payload.get("file_id"),
                    caption=caption,
                )
            elif msg_type == "video_note":
                sent_payload = context.bot.send_video_note(
                    chat_id=target_id, video_note=payload.get("file_id")
                )
            elif msg_type == "document":
                sent_payload = context.bot.send_document(
                    chat_id=target_id,
                    document=payload.get("file_id"),
                    caption=caption,
                )
        except Exception as e:
            logger.error(f"Error sending presentation to staff {target_id}: {e}")
            return

        # ✅ لازم نخزن على الهيدر + على المحتوى (لأن المشرفة ممكن تعمل Reply على أي واحد)
        def _bridge_store(m):
            if not m:
                return
            STAFF_REPLY_BRIDGE[(target_id, m.message_id)] = {
                "kind": "presentation",
                "thread_id": thread_id,
                "user_chat_id": thread.get("user_chat_id") or user.id,
                "user_id": thread.get("user_id") or user.id,
                "course_id": thread.get("course_id"),
                "lesson_id": thread.get("lesson_id"),
                "user_gender": thread.get("user_gender"),
            }

        _bridge_store(sent_header)
        _bridge_store(sent_payload)

    if send_to_supervisor:
        _send_presentation_to_staff(SUPERVISOR_ID)

    if mirror_to_admin:
        _send_presentation_to_staff(admin_target)

    target_label = "الأدمن" if user_gender == "male" else "المشرفة"
    update.message.reply_text(
        f"✅ تم إرسال عرضك إلى {target_label}. يمكنك متابعة الإرسال هنا.",
        reply_markup=PRESENTATION_SESSION_KB,
    )
    _schedule_presentation_media_timeout(user.id, update.message.chat_id, thread_id)


def course_presentation_router(update: Update, context: CallbackContext):
    user = update.effective_user
    if not user:
        return

    user_id = user.id
    if user_id in WAITING_COURSE_PRESENTATION_MEDIA:
        handle_course_presentation_user_media(update, context)


def handle_course_benefit_open(
    query: Update.callback_query,
    context: CallbackContext,
    user_id: int,
    course_id: str,
    lesson_id: str,
):
    if not firestore_available():
        safe_edit_message_text(
            query,
            "❌ لا يمكن فتح الفائدة حالياً.",
            reply_markup=_lesson_view_keyboard(course_id, lesson_id),
        )
        return

    lesson_doc = db.collection(COURSE_LESSONS_COLLECTION).document(lesson_id).get()
    if not lesson_doc.exists:
        safe_edit_message_text(query, "❌ الدرس غير موجود.", reply_markup=COURSES_USER_MENU_KB)
        return

    lesson = lesson_doc.to_dict() or {}
    course = _course_document(course_id) or {}

    record = get_user_record_by_id(user_id) or {}
    _clear_presentation_states(user_id)
    _clear_benefit_states(user_id)

    callback_message = query.message
    callback_chat_id = callback_message.chat_id if callback_message else None

    thread_doc = None
    try:
        # 🔎 Firestore composite index required: user_id + lesson_id + status on
        # course_benefit_threads to keep this open-thread lookup efficient and
        # avoid the fallback scan below.
        existing_threads = list(
            db.collection(COURSE_BENEFIT_THREADS_COLLECTION)
            .where("user_id", "==", user_id)
            .where("lesson_id", "==", lesson_id)
            .where("status", "==", "open")
            .stream()
        )
        if existing_threads:
            thread_doc = existing_threads[0]
    except FailedPrecondition as e:
        logger.error(
            "[BENEFIT] Composite index missing for open-thread lookup: %s", e
        )
        try:
            fallback_stream = db.collection(COURSE_BENEFIT_THREADS_COLLECTION).where(
                "user_id", "==", user_id
            ).stream()
            existing_threads = []
            for doc in fallback_stream:
                doc_data = doc.to_dict() or {}
                if (
                    doc_data.get("lesson_id") == lesson_id
                    and doc_data.get("status") == "open"
                ):
                    existing_threads.append(doc)
            if existing_threads:
                thread_doc = existing_threads[0]
        except Exception as fallback_error:
            logger.error(
                "[BENEFIT] Fallback open-thread lookup failed: %s", fallback_error
            )
    except Exception as e:
        logger.error(f"Error checking existing benefit threads: {e}")

    if thread_doc:
        thread_id = thread_doc.id
        thread_data = thread_doc.to_dict() or {}
        if callback_chat_id and not thread_data.get("user_chat_id"):
            try:
                thread_doc.reference.update({"user_chat_id": callback_chat_id})
            except Exception as e:
                logger.debug("[BENEFIT] Failed to update user_chat_id: %s", e)
    else:
        thread_payload = {
            "user_id": user_id,
            "user_name": query.from_user.full_name,
            "user_username": query.from_user.username,
            "user_gender": record.get("gender"),
            "course_id": course_id,
            "course_title": course.get("name"),
            "lesson_id": lesson_id,
            "lesson_title": lesson.get("title"),
            "curriculum_section": lesson.get("curriculum_section"),
            "status": "open",
            "created_at": firestore.SERVER_TIMESTAMP,
            "last_message_at": firestore.SERVER_TIMESTAMP,
            "context_type": COURSE_BENEFIT_CONTEXT_TYPE,
            "user_chat_id": callback_chat_id if callback_chat_id is not None else user_id,
        }
        try:
            doc_ref = db.collection(COURSE_BENEFIT_THREADS_COLLECTION).document()
            doc_ref.set(thread_payload)
            thread_id = doc_ref.id
            thread_data = thread_payload
        except Exception as e:
            logger.error(f"Error creating benefit thread: {e}")
            safe_edit_message_text(
                query,
                "❌ لا يمكن فتح الفائدة حالياً.",
                reply_markup=_lesson_view_keyboard(course_id, lesson_id),
            )
            return

    WAITING_COURSE_BENEFIT_MEDIA[user_id] = {
        "session_id": thread_id,
        "thread_id": thread_id,
        "user_id": user_id,
        "user_name": thread_data.get("user_name"),
        "user_username": thread_data.get("user_username"),
        "user_gender": thread_data.get("user_gender"),
        "course_id": course_id,
        "course_title": thread_data.get("course_title"),
        "lesson_id": lesson_id,
        "lesson_title": thread_data.get("lesson_title"),
        "curriculum_section": thread_data.get("curriculum_section"),
    }

    if callback_chat_id is not None:
        _schedule_course_benefit_timeout(user_id, callback_chat_id, thread_id)
    safe_edit_message_text(
        query,
        "📸 أرسِل صورة أو نص الفائدة الآن.",
        reply_markup=_lesson_view_keyboard(
            course_id,
            lesson_id,
            show_presentation=lesson.get("has_presentation", False),
            presentation_thread_id=None,
            benefit_thread_id=thread_id,
        ),
    )
    try:
        context.bot.send_message(
            chat_id=user_id,
            text=(
                "✅ تم فتح وضع الفائدة. أرسل/ي الصورة أو النص هنا.\n"
                "اضغط (خروج من الفائدة) لإنهاء الجلسة."
            ),
            reply_markup=BENEFIT_SESSION_KB,
        )
    except Exception:
        pass


def handle_course_benefit_close(
    query: Optional[Update.callback_query],
    context: CallbackContext,
    session_id: str,
    *,
    user_id: Optional[int] = None,
    user_chat_id: Optional[int] = None,
):
    resolved_user_id = user_id or (query.from_user.id if query else None)
    thread_id = session_id
    chat_id = user_chat_id or (
        query.message.chat_id if query and query.message else resolved_user_id
    )
    if not resolved_user_id:
        return

    active = WAITING_COURSE_BENEFIT_MEDIA.get(resolved_user_id)
    if not active or active.get("session_id") != session_id:
        if query:
            query.answer("⚠️ لا توجد فائدة مفتوحة الآن.", show_alert=True)
        elif chat_id:
            context.bot.send_message(chat_id=chat_id, text="⚠️ لا توجد فائدة مفتوحة الآن.")
        return
    try:
        db.collection(COURSE_BENEFIT_THREADS_COLLECTION).document(thread_id).update(
            {"status": "closed", "last_message_at": firestore.SERVER_TIMESTAMP}
        )
    except Exception as e:
        logger.debug("[BENEFIT] Failed to close thread %s: %s", session_id, e)
    _clear_benefit_states(resolved_user_id)
    if query:
        query.answer("✅ تم إغلاق وضع الفائدة.", show_alert=True)
    course_id = active.get("course_id")
    if query:
        safe_edit_message_text(
            query,
            "✅ تم إغلاق وضع الفائدة.",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "📚 الرجوع لقائمة الدروس",
                            callback_data=f"COURSES:user_lessons_{course_id}",
                        )
                    ]
                ]
            ),
        )

    if chat_id:
        context.bot.send_message(
            chat_id=chat_id,
            text="✅ تم الخروج.",
            reply_markup=user_main_keyboard(resolved_user_id),
        )


def handle_course_benefit_user_message(update: Update, context: CallbackContext):
    user = update.effective_user
    if not user or user.id not in WAITING_COURSE_BENEFIT_MEDIA:
        return

    ctx = WAITING_COURSE_BENEFIT_MEDIA.get(user.id) or {}
    thread_id = ctx.get("thread_id") or ctx.get("session_id")
    if not thread_id:
        _clear_benefit_states(user.id)
        return

    message_text = update.message.text if update.message else None
    if message_text:
        normalized_text = message_text.strip()
        if normalized_text in SESSION_EXIT_BENEFIT_TEXTS:
            handle_course_benefit_close(
                None,
                context,
                thread_id,
                user_id=user.id,
                user_chat_id=update.message.chat_id,
            )
            raise DispatcherHandlerStop()
        if normalized_text in MAIN_MENU_BUTTON_TEXTS:
            update.message.reply_text(
                "⚠️ أنت الآن داخل وضع العرض/الفائدة. اضغط خروج لإنهاء الجلسة.",
                reply_markup=BENEFIT_SESSION_KB,
            )
            return

    payload = _extract_benefit_payload(update.message)
    if not payload:
        update.message.reply_text(
            "⚠️ الفائدة تقبل الصور أو النص فقط.",
            reply_markup=BENEFIT_SESSION_KB,
        )
        return

    thread_doc = db.collection(COURSE_BENEFIT_THREADS_COLLECTION).document(thread_id).get()
    if not thread_doc.exists:
        update.message.reply_text(
            "❌ جلسة الفائدة غير متاحة. افتحها من الدرس مرة أخرى.",
            reply_markup=BENEFIT_SESSION_KB,
        )
        _clear_benefit_states(user.id)
        return

    thread_data = thread_doc.to_dict() or {}

    _cancel_course_benefit_timeout(user.id)

    _store_course_benefit(ctx, update.message.message_id, payload)
    _store_benefit_message(thread_id, "user", payload, update.message.message_id, sender_id=user.id)

    user_gender = ctx.get("user_gender")
    header = _build_benefit_header(thread_data, thread_id)

    def _send_benefit_to_staff(target_id: int):
        sent_header = None
        sent_payload = None
        try:
            sent_header = context.bot.send_message(chat_id=target_id, text=header)
            if payload.get("type") == "photo":
                sent_payload = context.bot.send_photo(
                    chat_id=target_id,
                    photo=payload.get("file_id"),
                    caption=payload.get("caption") or payload.get("text"),
                )
            elif payload.get("type") == "text":
                sent_payload = context.bot.send_message(
                    chat_id=target_id, text=payload.get("text", "")
                )
        except Exception as e:
            logger.error(f"Error sending benefit to staff {target_id}: {e}")
            return

        # ✅ نفس الشيء: نخزن للهيدر + للرسالة نفسها
        def _bridge_store(m):
            if not m:
                return
            STAFF_REPLY_BRIDGE[(target_id, m.message_id)] = {
                "kind": "benefit",
                "thread_id": thread_id,
                "user_chat_id": thread_data.get("user_chat_id")
                or ctx.get("user_id"),
                "user_id": thread_data.get("user_id") or ctx.get("user_id"),
                "course_id": ctx.get("course_id"),
                "lesson_id": ctx.get("lesson_id"),
                "user_gender": thread_data.get("user_gender") or ctx.get("user_gender"),
            }

        _bridge_store(sent_header)
        _bridge_store(sent_payload)

    send_to_supervisor = user_gender != "male" and SUPERVISOR_ID
    if send_to_supervisor:
        _send_benefit_to_staff(SUPERVISOR_ID)

    if ADMIN_ID:
        _send_benefit_to_staff(ADMIN_ID)

    try:
        sub, sub_ref = _ensure_subscription(user.id, ctx.get("course_id"))
        if sub is not None:
            sub_ref.set(
                {
                    "last_benefit": {
                        "lesson_id": ctx.get("lesson_id"),
                        "curriculum_section": ctx.get("curriculum_section"),
                        "updated_at": firestore.SERVER_TIMESTAMP,
                    },
                },
                merge=True,
            )
    except Exception as e:
        logger.error(f"Error updating benefit metadata: {e}")

    update.message.reply_text(
        "✅ تم استلام الفائدة. يمكنك إرسال فائدة أخرى أو الخروج.",
        reply_markup=BENEFIT_SESSION_KB,
    )

    _schedule_course_benefit_timeout(user.id, update.message.chat_id, thread_id)


def course_benefit_router(update: Update, context: CallbackContext):
    user = update.effective_user
    if not user:
        return

    if user.id in WAITING_COURSE_BENEFIT_MEDIA:
        handle_course_benefit_user_message(update, context)


def user_quizzes_list(query: Update.callback_query, context: CallbackContext, course_id: str):
    _clear_course_transient_messages(context, query.message.chat_id, query.from_user.id)
    try:
        quizzes_ref = db.collection(COURSE_QUIZZES_COLLECTION)
        quizzes = list(quizzes_ref.where("course_id", "==", course_id).stream())

        if not quizzes:
            safe_edit_message_text(
                query,
                "📝 لا توجد اختبارات متاحة حالياً لهذه الدورة.",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "🔙 رجوع", callback_data=f"COURSES:back_course_{course_id}"
                            )
                        ]
                    ]
                ),
            )
            return

        keyboard = []
        for doc in quizzes:
            quiz = doc.to_dict()
            keyboard.append(
                [
                    InlineKeyboardButton(
                        f"📝 {quiz.get('title', 'اختبار')}",
                        callback_data=f"COURSES:start_quiz_{doc.id}",
                    )
                ]
            )

        keyboard.append(
            [InlineKeyboardButton("🔙 رجوع", callback_data=f"COURSES:back_course_{course_id}")]
        )
        safe_edit_message_text(
            query,
            "📝 اختبارات الدورة:\nاختر اختباراً للإجابة عنه.",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    except Exception as e:
        logger.error(f"خطأ في جلب الاختبارات: {e}")
        safe_edit_message_text(query, "❌ تعذر تحميل الاختبارات حالياً.", reply_markup=COURSES_USER_MENU_KB)


def user_points(query: Update.callback_query, user_id: int, course_id: str):
    subscription, _ = _ensure_subscription(user_id, course_id)
    if not subscription:
        safe_edit_message_text(query, "❌ لست مشتركاً في هذه الدورة.", reply_markup=COURSES_USER_MENU_KB)
        return

    points = subscription.get("points", 0)
    completed = len(subscription.get("completed_quizzes", []))
    lessons_count = len(subscription.get("lessons_attended", []))
    text = (
        f"⭐️ نقاطك في الدورة: {points}"
        f"\n📚 حضور الدروس: {lessons_count}"
        f"\n📝 اختبارات مكتملة: {completed}"
    )
    safe_edit_message_text(
        query,
        text,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "🔙 رجوع", callback_data=f"COURSES:back_course_{course_id}"
                    )
                ]
            ]
        ),
    )


def start_quiz_flow(query: Update.callback_query, user_id: int, quiz_id: str):
    doc = db.collection(COURSE_QUIZZES_COLLECTION).document(quiz_id).get()
    if not doc.exists:
        safe_edit_message_text(query, "❌ الاختبار غير موجود.", reply_markup=COURSES_USER_MENU_KB)
        return

    quiz = doc.to_dict()
    course_id = quiz.get("course_id")
    subscription, sub_ref = _ensure_subscription(user_id, course_id)
    if not subscription:
        safe_edit_message_text(query, "❌ يجب التسجيل في الدورة أولاً.", reply_markup=COURSES_USER_MENU_KB)
        return

    if quiz_id in (subscription or {}).get("completed_quizzes", []):
        safe_edit_message_text(
            query,
            "✅ تم حل هذا الاختبار مسبقاً.",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🔙 رجوع", callback_data=f"COURSES:back_course_{course_id}"
                        )
                    ]
                ]
            ),
        )
        return

    options = quiz.get("options") or []
    if not options:
        safe_edit_message_text(
            query,
            "❌ هذا الاختبار غير مكتمل حالياً.",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🔙 رجوع", callback_data=f"COURSES:back_course_{course_id}"
                        )
                    ]
                ]
            ),
        )
        return

    keyboard = []
    for idx, option in enumerate(options):
        keyboard.append(
            [
                InlineKeyboardButton(
                    option.get("text", f"اختيار {idx+1}"),
                    callback_data=f"COURSES:quiz_answer_{quiz_id}_{idx}",
                )
            ]
        )
    keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data=f"COURSES:user_quizzes_{course_id}")])

    safe_edit_message_text(
        query,
        f"📝 {quiz.get('title', 'اختبار')}\n\n{quiz.get('question', '')}\n\nاختر الإجابة المناسبة:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


def handle_quiz_answer_selection(query: Update.callback_query, user_id: int, quiz_id: str, option_idx: str):
    doc = db.collection(COURSE_QUIZZES_COLLECTION).document(quiz_id).get()
    if not doc.exists:
        safe_edit_message_text(query, "❌ الاختبار غير موجود.", reply_markup=COURSES_USER_MENU_KB)
        return

    quiz = doc.to_dict()
    course_id = quiz.get("course_id")
    subscription, sub_ref = _ensure_subscription(user_id, course_id)
    if not sub_ref:
        safe_edit_message_text(query, "❌ يجب التسجيل في الدورة أولاً.", reply_markup=COURSES_USER_MENU_KB)
        return

    if quiz_id in (subscription or {}).get("completed_quizzes", []):
        safe_edit_message_text(
            query,
            "✅ تم تسجيل إجابتك سابقاً.",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🔙 رجوع", callback_data=f"COURSES:back_course_{course_id}"
                        )
                    ]
                ]
            ),
        )
        return

    try:
        idx = int(option_idx)
    except Exception:
        query.answer("خيار غير صالح", show_alert=True)
        return

    options = quiz.get("options") or []
    if idx < 0 or idx >= len(options):
        query.answer("خيار غير صالح", show_alert=True)
        return

    option = options[idx]
    points = int(option.get("points", 0))
    try:
        sub_ref.update(
            {
                "points": firestore.Increment(points),
                "completed_quizzes": firestore.ArrayUnion([quiz_id]),
            }
        )
        safe_edit_message_text(
            query,
            f"✅ تم تسجيل إجابتك. (+{points} نقاط)",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🔙 رجوع", callback_data=f"COURSES:back_course_{course_id}"
                        )
                    ]
                ]
            ),
        )
    except Exception as e:
        logger.error(f"خطأ في تحديث نقاط الاختبار: {e}")
        safe_edit_message_text(query, "⚠️ تعذر حفظ النتيجة حالياً.", reply_markup=COURSES_USER_MENU_KB)


def _complete_quiz_answer(user_id: int, answer_text: str, update: Update, context: CallbackContext):
    state = ACTIVE_QUIZ_STATE.get(user_id)
    if not state:
        WAITING_QUIZ_ANSWER.discard(user_id)
        return False

    correct_answer = state.get("answer", "").strip().lower()
    user_answer = answer_text.strip().lower()
    course_id = state.get("course_id")
    sub_ref = state.get("subscription_ref")

    if not sub_ref:
        WAITING_QUIZ_ANSWER.discard(user_id)
        ACTIVE_QUIZ_STATE.pop(user_id, None)
        return False

    if user_answer == correct_answer:
        try:
            sub_ref.update(
                {
                    "points": firestore.Increment(state.get("points", 0)),
                    "completed_quizzes": firestore.ArrayUnion([state.get("quiz_id")]),
                }
            )
            update.message.reply_text(
                "✅ إجابة صحيحة! تمت إضافة نقاط الاختبار إلى رصيدك.",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "🔙 رجوع",
                                callback_data=f"COURSES:back_course_{course_id}",
                            )
                        ]
                    ]
                ),
            )
        except Exception as e:
            logger.error(f"خطأ في تحديث نقاط الاختبار: {e}")
            update.message.reply_text("⚠️ تعذر حفظ النتيجة حالياً. حاول لاحقاً.")
    else:
        update.message.reply_text(
            "❌ إجابة غير صحيحة. يمكنك المحاولة مرة أخرى من قائمة الاختبارات.",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🔙 رجوع", callback_data=f"COURSES:back_course_{course_id}"
                        )
                    ]
                ]
            ),
        )

    WAITING_QUIZ_ANSWER.discard(user_id)
    ACTIVE_QUIZ_STATE.pop(user_id, None)
    return True


# =================== Handlers للأدمن/المشرفة ===================


def admin_create_course(query: Update.callback_query, context: CallbackContext):
    user_id = query.from_user.id
    if not (is_admin(user_id) or is_supervisor(user_id)):
        safe_edit_message_text(query, "❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return

    _reset_course_creation(user_id)
    WAITING_NEW_COURSE.add(user_id)
    COURSE_CREATION_CONTEXT[user_id] = {}
    safe_edit_message_text(
        query,
        "➕ إنشاء دورة جديدة\n\nأدخل اسم الدورة",
        reply_markup=_course_creation_keyboard(),
    )


def admin_manage_lessons(query: Update.callback_query, context: CallbackContext):
    user_id = query.from_user.id
    if not (is_admin(user_id) or is_supervisor(user_id)):
        safe_edit_message_text(query, "❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return

    if not firestore_available():
        safe_edit_message_text(query, "❌ خطأ في الاتصال بقاعدة البيانات.", reply_markup=COURSES_ADMIN_MENU_KB)
        return

    try:
        courses = [
            {**doc.to_dict(), "id": doc.id}
            for doc in db.collection(COURSES_COLLECTION).stream()
        ]

        if not courses:
            safe_edit_message_text(
                query,
                "🧩 إدارة الدروس\n\nلا توجد دورات لإضافة دروس إليها.",
                reply_markup=COURSES_ADMIN_MENU_KB,
            )
            return

        filtered_courses = [c for c in courses if not _is_back_placeholder_course(c.get("name"))]
        if not filtered_courses:
            safe_edit_message_text(
                query,
                "🧩 إدارة الدروس\n\nلا توجد دورات صالحة لإدارة الدروس حاليًا.",
                reply_markup=COURSES_ADMIN_MENU_KB,
            )
            return

        keyboard = [
            [
                InlineKeyboardButton(
                    f"📖 {c.get('name', 'دورة')}", callback_data=f"COURSES:lessons_{c.get('id')}"
                )
            ]
            for c in filtered_courses
        ]
        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:admin_back")])
        safe_edit_message_text(
            query,
            "🧩 اختر دورة لإدارة دروسها:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    except Exception as e:
        logger.error(f"خطأ في إدارة الدروس: {e}")
        safe_edit_message_text(query, "❌ حدث خطأ. حاول مرة أخرى.", reply_markup=COURSES_ADMIN_MENU_KB)


def _admin_show_lessons_panel(query: Update.callback_query, course_id: str):
    course = _course_document(course_id)
    if not course:
        safe_edit_message_text(query, "❌ الدورة غير موجودة.", reply_markup=COURSES_ADMIN_MENU_KB)
        return

    lessons = list(db.collection(COURSE_LESSONS_COLLECTION).where("course_id", "==", course_id).stream())
    keyboard = [
        [InlineKeyboardButton("➕ إضافة درس", callback_data=f"COURSES:add_lesson_{course_id}")]
    ]
    for doc in lessons:
        lesson = doc.to_dict()
        keyboard.append(
            [InlineKeyboardButton(f"📖 {lesson.get('title', 'درس')}", callback_data=f"COURSES:view_lesson_{doc.id}")]
        )
        keyboard.append(
            [
                InlineKeyboardButton("✏️ تعديل", callback_data=f"COURSES:lesson_edit_{doc.id}"),
                InlineKeyboardButton("🗑 حذف", callback_data=f"COURSES:lesson_delete_{doc.id}"),
            ]
        )

    keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:manage_lessons")])
    safe_edit_message_text(
        query,
        f"📖 إدارة الدروس للدورة: {course.get('name', 'دورة')}\nاختر إجراءً.",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


def _admin_open_lesson_edit_menu(query: Update.callback_query, lesson_id: str):
    user_id = query.from_user.id
    if not (is_admin(user_id) or is_supervisor(user_id)):
        safe_edit_message_text(query, "❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return

    lesson_doc = db.collection(COURSE_LESSONS_COLLECTION).document(lesson_id).get()
    if not lesson_doc.exists:
        safe_edit_message_text(query, "❌ الدرس غير موجود.", reply_markup=COURSES_ADMIN_MENU_KB)
        return

    lesson = lesson_doc.to_dict()
    course_id = lesson.get("course_id")
    has_presentation = lesson.get("has_presentation", False)
    pres_label = "مفعّل" if has_presentation else "غير مفعّل"
    curriculum_section = lesson.get("curriculum_section") or "غير مفعّل"
    keyboard = [
        [InlineKeyboardButton("✏️ تعديل العنوان", callback_data=f"COURSES:lesson_edit_title_{lesson_id}")],
        [InlineKeyboardButton("📝 تعديل المحتوى", callback_data=f"COURSES:lesson_edit_content_{lesson_id}")],
        [
            InlineKeyboardButton(
                f"🎙️ العرض ({pres_label})",
                callback_data=f"COURSES:lesson_toggle_pres_{lesson_id}",
            )
        ],
        [
            InlineKeyboardButton(
                f"📘 باب المقرر ({curriculum_section})",
                callback_data=f"COURSES:lesson_toggle_curriculum_{lesson_id}",
            )
        ],
        [InlineKeyboardButton("🔙 رجوع", callback_data=f"COURSES:lessons_{course_id}")],
    ]
    safe_edit_message_text(
        query,
        f"🔧 إدارة الدرس: {lesson.get('title', 'درس')}",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


def _admin_request_lesson_title_edit(query: Update.callback_query, lesson_id: str):
    user_id = query.from_user.id
    if not (is_admin(user_id) or is_supervisor(user_id)):
        safe_edit_message_text(query, "❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return

    lesson_doc = db.collection(COURSE_LESSONS_COLLECTION).document(lesson_id).get()
    if not lesson_doc.exists:
        safe_edit_message_text(query, "❌ الدرس غير موجود.", reply_markup=COURSES_ADMIN_MENU_KB)
        return

    lesson = lesson_doc.to_dict()
    course_id = lesson.get("course_id")
    _reset_lesson_creation(user_id)
    LESSON_CREATION_CONTEXT[user_id] = {
        "course_id": course_id,
        "lesson_id": lesson_id,
        "edit_action": "edit_title",
    }
    WAITING_LESSON_TITLE.add(user_id)
    safe_edit_message_text(
        query,
        "✏️ أرسل العنوان الجديد للدرس.",
        reply_markup=_lessons_back_keyboard(course_id),
    )


def _admin_request_lesson_content_edit(query: Update.callback_query, lesson_id: str):
    user_id = query.from_user.id
    if not (is_admin(user_id) or is_supervisor(user_id)):
        safe_edit_message_text(query, "❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return

    lesson_doc = db.collection(COURSE_LESSONS_COLLECTION).document(lesson_id).get()
    if not lesson_doc.exists:
        safe_edit_message_text(query, "❌ الدرس غير موجود.", reply_markup=COURSES_ADMIN_MENU_KB)
        return

    lesson = lesson_doc.to_dict()
    course_id = lesson.get("course_id")
    _reset_lesson_creation(user_id)
    LESSON_CREATION_CONTEXT[user_id] = {
        "course_id": course_id,
        "lesson_id": lesson_id,
        "edit_action": "edit_content",
        "title": lesson.get("title", "درس"),
    }
    lesson_type_kb = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📝 نص", callback_data=f"COURSES:lesson_type_text_{course_id}")],
            [InlineKeyboardButton("🔊 ملف صوتي", callback_data=f"COURSES:lesson_type_audio_{course_id}")],
            [InlineKeyboardButton("🔗 رابط", callback_data=f"COURSES:lesson_type_link_{course_id}")],
            [InlineKeyboardButton("🔙 رجوع", callback_data=f"COURSES:lessons_{course_id}")],
        ]
    )
    safe_edit_message_text(
        query,
        "اختر نوع المحتوى الجديد ثم أرسله.",
        reply_markup=lesson_type_kb,
    )


def _admin_toggle_curriculum_section(query: Update.callback_query, lesson_id: str):
    user_id = query.from_user.id
    if not (is_admin(user_id) or is_supervisor(user_id)):
        safe_edit_message_text(query, "❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return

    lesson_doc = db.collection(COURSE_LESSONS_COLLECTION).document(lesson_id).get()
    if not lesson_doc.exists:
        safe_edit_message_text(query, "❌ الدرس غير موجود.", reply_markup=COURSES_ADMIN_MENU_KB)
        return

    lesson = lesson_doc.to_dict()
    course_id = lesson.get("course_id")
    current_section = (lesson.get("curriculum_section") or "").strip()

    if current_section:
        try:
            lesson_doc.reference.update(
                {"curriculum_section": firestore.DELETE_FIELD, "updated_at": firestore.SERVER_TIMESTAMP}
            )
            safe_edit_message_text(
                query,
                "✅ تم تعطيل باب المقرر لهذا الدرس.",
                reply_markup=_lessons_back_keyboard(course_id),
            )
        except Exception as e:
            logger.error(f"خطأ في تعطيل باب المقرر: {e}")
            safe_edit_message_text(query, "❌ تعذر التعطيل حالياً.", reply_markup=_lessons_back_keyboard(course_id))
        return

    _reset_lesson_creation(user_id)
    LESSON_CREATION_CONTEXT[user_id] = {
        "course_id": course_id,
        "lesson_id": lesson_id,
        "edit_action": "edit_curriculum_section",
    }
    WAITING_LESSON_CURRICULUM_NAME.add(user_id)
    safe_edit_message_text(
        query,
        "✏️ أرسل اسم باب المقرر لهذا الدرس.",
        reply_markup=_lessons_back_keyboard(course_id),
    )


def _admin_toggle_lesson_presentation(query: Update.callback_query, lesson_id: str):
    user_id = query.from_user.id
    if not (is_admin(user_id) or is_supervisor(user_id)):
        safe_edit_message_text(query, "❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return

    lesson_doc = db.collection(COURSE_LESSONS_COLLECTION).document(lesson_id).get()
    if not lesson_doc.exists:
        safe_edit_message_text(query, "❌ الدرس غير موجود.", reply_markup=COURSES_ADMIN_MENU_KB)
        return

    lesson = lesson_doc.to_dict() or {}
    current = bool(lesson.get("has_presentation"))
    try:
        db.collection(COURSE_LESSONS_COLLECTION).document(lesson_id).update(
            {"has_presentation": not current, "updated_at": firestore.SERVER_TIMESTAMP}
        )
        _admin_open_lesson_edit_menu(query, lesson_id)
    except Exception as e:
        logger.error(f"خطأ في تبديل حالة العرض: {e}")
        safe_edit_message_text(query, "❌ تعذر تحديث حالة العرض.", reply_markup=COURSES_ADMIN_MENU_KB)


def _admin_confirm_delete_lesson(query: Update.callback_query, lesson_id: str):
    user_id = query.from_user.id
    if not (is_admin(user_id) or is_supervisor(user_id)):
        safe_edit_message_text(query, "❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return

    lesson_doc = db.collection(COURSE_LESSONS_COLLECTION).document(lesson_id).get()
    if not lesson_doc.exists:
        safe_edit_message_text(query, "❌ الدرس غير موجود.", reply_markup=COURSES_ADMIN_MENU_KB)
        return

    lesson = lesson_doc.to_dict()
    course_id = lesson.get("course_id")
    keyboard = [
        [InlineKeyboardButton("✅ تأكيد الحذف", callback_data=f"COURSES:lesson_delete_confirm_{lesson_id}")],
        [InlineKeyboardButton("🔙 رجوع", callback_data=f"COURSES:lessons_{course_id}")],
    ]
    safe_edit_message_text(
        query,
        f"🗑 هل تريد حذف الدرس «{lesson.get('title', 'درس')}»؟",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


def _admin_delete_lesson(query: Update.callback_query, lesson_id: str):
    user_id = query.from_user.id
    if not (is_admin(user_id) or is_supervisor(user_id)):
        safe_edit_message_text(query, "❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return

    lesson_doc = db.collection(COURSE_LESSONS_COLLECTION).document(lesson_id).get()
    if not lesson_doc.exists:
        safe_edit_message_text(query, "❌ الدرس غير موجود.", reply_markup=COURSES_ADMIN_MENU_KB)
        return

    course_id = lesson_doc.to_dict().get("course_id")
    try:
        db.collection(COURSE_LESSONS_COLLECTION).document(lesson_id).delete()
        _admin_show_lessons_panel(query, course_id)
    except Exception as e:
        logger.error(f"خطأ في حذف الدرس: {e}")
        safe_edit_message_text(query, "❌ تعذر حذف الدرس حالياً.", reply_markup=_lessons_back_keyboard(course_id))


def admin_manage_quizzes(query: Update.callback_query, context: CallbackContext):
    user_id = query.from_user.id
    if not (is_admin(user_id) or is_supervisor(user_id)):
        safe_edit_message_text(query, "❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return

    if not firestore_available():
        safe_edit_message_text(query, "❌ خطأ في الاتصال بقاعدة البيانات.", reply_markup=COURSES_ADMIN_MENU_KB)
        return

    try:
        courses = [
            {**doc.to_dict(), "id": doc.id}
            for doc in db.collection(COURSES_COLLECTION).stream()
        ]

        if not courses:
            safe_edit_message_text(
                query,
                "📝 إدارة الاختبارات\n\nلا توجد دورات لإضافة اختبارات إليها.",
                reply_markup=COURSES_ADMIN_MENU_KB,
            )
            return

        filtered_courses = [c for c in courses if not _is_back_placeholder_course(c.get("name"))]
        if not filtered_courses:
            safe_edit_message_text(
                query,
                "📝 إدارة الاختبارات\n\nلا توجد دورات صالحة لإضافة اختبارات إليها.",
                reply_markup=COURSES_ADMIN_MENU_KB,
            )
            return

        keyboard = [
            [
                InlineKeyboardButton(
                    f"📝 {c.get('name', 'دورة')}", callback_data=f"COURSES:quizzes_{c.get('id')}"
                )
            ]
            for c in filtered_courses
        ]
        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:admin_back")])
        safe_edit_message_text(
            query,
            "📝 اختر دورة لإدارة اختباراتها:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    except Exception as e:
        logger.error(f"خطأ في إدارة الاختبارات: {e}")
        safe_edit_message_text(query, "❌ حدث خطأ. حاول مرة أخرى.", reply_markup=COURSES_ADMIN_MENU_KB)


def _admin_show_quizzes_panel(query: Update.callback_query, course_id: str):
    course = _course_document(course_id)
    if not course:
        safe_edit_message_text(query, "❌ الدورة غير موجودة.", reply_markup=COURSES_ADMIN_MENU_KB)
        return

    quizzes = list(db.collection(COURSE_QUIZZES_COLLECTION).where("course_id", "==", course_id).stream())
    keyboard = [
        [InlineKeyboardButton("➕ إضافة اختبار", callback_data=f"COURSES:add_quiz_{course_id}")]
    ]
    for doc in quizzes:
        quiz = doc.to_dict()
        keyboard.append(
            [InlineKeyboardButton(f"📝 {quiz.get('title', 'اختبار')}", callback_data=f"COURSES:start_quiz_{doc.id}")]
        )
        keyboard.append(
            [
                InlineKeyboardButton("✏️ تعديل", callback_data=f"COURSES:quiz_edit_{doc.id}"),
                InlineKeyboardButton("🗑 حذف", callback_data=f"COURSES:quiz_delete_{doc.id}"),
            ]
        )

    keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:manage_quizzes")])
    safe_edit_message_text(
        query,
        f"📝 إدارة الاختبارات للدورة: {course.get('name', 'دورة')}\nاختر إجراءً.",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


def _admin_start_quiz_edit(query: Update.callback_query, quiz_id: str):
    user_id = query.from_user.id
    if not (is_admin(user_id) or is_supervisor(user_id)):
        safe_edit_message_text(query, "❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return

    quiz_doc = db.collection(COURSE_QUIZZES_COLLECTION).document(quiz_id).get()
    if not quiz_doc.exists:
        safe_edit_message_text(query, "❌ الاختبار غير موجود.", reply_markup=COURSES_ADMIN_MENU_KB)
        return

    quiz = quiz_doc.to_dict()
    course_id = quiz.get("course_id")
    _reset_quiz_creation(user_id)
    QUIZ_CREATION_CONTEXT[user_id] = {
        "course_id": course_id,
        "quiz_id": quiz_id,
        "mode": "edit",
    }
    WAITING_NEW_QUIZ.add(user_id)
    WAITING_QUIZ_TITLE.add(user_id)
    safe_edit_message_text(
        query,
        "✏️ أرسل عنوان الاختبار الجديد.",
        reply_markup=_quizzes_back_keyboard(course_id),
    )


def _admin_confirm_delete_quiz(query: Update.callback_query, quiz_id: str):
    user_id = query.from_user.id
    if not (is_admin(user_id) or is_supervisor(user_id)):
        safe_edit_message_text(query, "❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return

    quiz_doc = db.collection(COURSE_QUIZZES_COLLECTION).document(quiz_id).get()
    if not quiz_doc.exists:
        safe_edit_message_text(query, "❌ الاختبار غير موجود.", reply_markup=COURSES_ADMIN_MENU_KB)
        return

    quiz = quiz_doc.to_dict()
    course_id = quiz.get("course_id")
    keyboard = [
        [InlineKeyboardButton("✅ تأكيد الحذف", callback_data=f"COURSES:quiz_delete_confirm_{quiz_id}")],
        [InlineKeyboardButton("🔙 رجوع", callback_data=f"COURSES:quizzes_{course_id}")],
    ]
    safe_edit_message_text(
        query,
        f"🗑 هل تريد حذف الاختبار «{quiz.get('title', 'اختبار')}»؟",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


def _admin_delete_quiz(query: Update.callback_query, quiz_id: str):
    user_id = query.from_user.id
    if not (is_admin(user_id) or is_supervisor(user_id)):
        safe_edit_message_text(query, "❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return

    quiz_doc = db.collection(COURSE_QUIZZES_COLLECTION).document(quiz_id).get()
    if not quiz_doc.exists:
        safe_edit_message_text(query, "❌ الاختبار غير موجود.", reply_markup=COURSES_ADMIN_MENU_KB)
        return

    course_id = quiz_doc.to_dict().get("course_id")
    try:
        db.collection(COURSE_QUIZZES_COLLECTION).document(quiz_id).delete()
        _admin_show_quizzes_panel(query, course_id)
    except Exception as e:
        logger.error(f"خطأ في حذف الاختبار: {e}")
        safe_edit_message_text(query, "❌ تعذر حذف الاختبار حالياً.", reply_markup=_quizzes_back_keyboard(course_id))


def admin_statistics(query: Update.callback_query, context: CallbackContext):
    user_id = query.from_user.id
    if not (is_admin(user_id) or is_supervisor(user_id)):
        safe_edit_message_text(query, "❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return

    if not firestore_available():
        safe_edit_message_text(query, "❌ خطأ في الاتصال بقاعدة البيانات.", reply_markup=COURSES_ADMIN_MENU_KB)
        return

    try:
        courses = [
            {**doc.to_dict(), "id": doc.id}
            for doc in db.collection(COURSES_COLLECTION).stream()
        ]
        if not courses:
            safe_edit_message_text(query, "لا توجد دورات حالياً.", reply_markup=COURSES_ADMIN_MENU_KB)
            return

        filtered_courses = [c for c in courses if not _is_back_placeholder_course(c.get("name"))]
        if not filtered_courses:
            safe_edit_message_text(query, "لا توجد دورات صالحة حالياً.", reply_markup=COURSES_ADMIN_MENU_KB)
            return

        keyboard = [
            [InlineKeyboardButton(course.get("name", "دورة"), callback_data=f"COURSES:stats_course_{course.get('id')}")]
            for course in filtered_courses
        ]
        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:admin_back")])

        safe_edit_message_text(
            query,
            "📊 إحصائيات الدورات\nاختر دورة لعرض تفاصيل المشاركين.",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    except Exception as e:
        logger.error(f"خطأ في جلب الإحصائيات: {e}")
        safe_edit_message_text(query, "❌ حدث خطأ. حاول مرة أخرى.", reply_markup=COURSES_ADMIN_MENU_KB)


def admin_archive_manage(query: Update.callback_query, context: CallbackContext):
    user_id = query.from_user.id
    if not (is_admin(user_id) or is_supervisor(user_id)):
        safe_edit_message_text(query, "❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return

    if not firestore_available():
        safe_edit_message_text(query, "❌ خطأ في الاتصال بقاعدة البيانات.", reply_markup=COURSES_ADMIN_MENU_KB)
        return

    try:
        courses = [
            {**doc.to_dict(), "id": doc.id}
            for doc in db.collection(COURSES_COLLECTION).stream()
        ]
        if not courses:
            safe_edit_message_text(
                query,
                "🗂 أرشفة/إيقاف/تشغيل\n\nلا توجد دورات.",
                reply_markup=COURSES_ADMIN_MENU_KB,
            )
            return

        filtered_courses = [c for c in courses if not _is_back_placeholder_course(c.get("name"))]
        if not filtered_courses:
            safe_edit_message_text(
                query,
                "🗂 أرشفة/إيقاف/تشغيل\n\nلا توجد دورات صالحة للتعديل.",
                reply_markup=COURSES_ADMIN_MENU_KB,
            )
            return

        keyboard = []
        text = "🗂 اختر دورة لتغيير حالتها:\n\n"
        for course in filtered_courses:
            status = course.get("status", "active")
            status_emoji = "✅" if status == "active" else "❌"
            keyboard.append(
                [
                    InlineKeyboardButton(
                        f"{status_emoji} {course.get('name', 'دورة')}",
                        callback_data=f"COURSES:toggle_{course.get('id')}",
                    )
                ]
            )

        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:admin_back")])
        safe_edit_message_text(query, text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"خطأ في إدارة الأرشفة: {e}")
        safe_edit_message_text(query, "❌ حدث خطأ. حاول مرة أخرى.", reply_markup=COURSES_ADMIN_MENU_KB)


def admin_statistics_course(query: Update.callback_query, course_id: str):
    try:
        subs = list(
            db.collection(COURSE_SUBSCRIPTIONS_COLLECTION)
            .where("course_id", "==", course_id)
            .stream()
        )
        course = _course_document(course_id)
        if not subs:
            safe_edit_message_text(
                query,
                "لا يوجد مشاركون في هذه الدورة.",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:statistics")],
                        [InlineKeyboardButton("⬅️ لوحة الإدارة", callback_data="COURSES:admin_back")],
                    ]
                ),
            )
            return

        keyboard = [
            [
                InlineKeyboardButton(
                    "🏆 ترتيب الدورة",
                    callback_data=f"COURSES:leaderboard_{course_id}_1",
                )
            ]
        ]
        for sub in subs:
            data = sub.to_dict()
            user_name = data.get("full_name") or data.get("username") or str(data.get("user_id"))
            points = data.get("points", 0)
            button_label = f"{user_name} | نقاط: {points}"
            keyboard.append(
                [
                    InlineKeyboardButton(
                        button_label,
                        callback_data=f"COURSES:stats_user_{course_id}_{data.get('user_id')}",
                    )
                ]
            )

        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:statistics")])
        keyboard.append([InlineKeyboardButton("⬅️ لوحة الإدارة", callback_data="COURSES:admin_back")])
        safe_edit_message_text(
            query,
            f"📊 مشاركو دورة {course.get('name', 'دورة')}",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    except Exception as e:
        logger.error(f"خطأ في إحصائيات الدورة: {e}")
        safe_edit_message_text(query, "❌ حدث خطأ في جلب البيانات.", reply_markup=COURSES_ADMIN_MENU_KB)


def admin_statistics_user(query: Update.callback_query, course_id: str, target_user_id: str):
    try:
        sub_id = _subscription_document_id(int(target_user_id), course_id)
        doc = db.collection(COURSE_SUBSCRIPTIONS_COLLECTION).document(sub_id).get()
        if not doc.exists:
            safe_edit_message_text(query, "المستخدم غير مشترك في هذه الدورة.", reply_markup=COURSES_ADMIN_MENU_KB)
            return

        data = doc.to_dict()
        lessons_count = len(data.get("lessons_attended", []))
        quizzes_count = len(data.get("completed_quizzes", []))
        points = data.get("points", 0)
        user_record = get_user_record_by_id(int(target_user_id)) or {}
        name = data.get("full_name") or user_record.get("course_full_name") or data.get("username") or target_user_id
        age = data.get("age") or user_record.get("age")
        country = data.get("country") or user_record.get("country") or "غير محدد"
        gender_val = data.get("gender") or user_record.get("gender")
        gender_label = "ذكر" if gender_val == "male" else "أنثى" if gender_val == "female" else "غير محدد"
        username = data.get("username") or user_record.get("username")

        username_line = f"اسم المستخدم: @{username}" if username else None
        lines = [
            "📌 بيانات الطالب",
            f"الاسم الكامل: {name}",
            f"المعرف: {target_user_id}",
            username_line,
            f"العمر: {age if age is not None else 'غير محدد'}",
            f"الدولة: {country}",
            f"الجنس: {gender_label}",
            "",
            "📊 التقدم",
            f"حضور الدروس: {lessons_count}",
            f"الاختبارات: {quizzes_count}",
            f"مجموع النقاط: {points}",
        ]

        text = "\n".join([ln for ln in lines if ln is not None])

        keyboard = [
            [InlineKeyboardButton("🔙 رجوع", callback_data=f"COURSES:stats_course_{course_id}")],
            [InlineKeyboardButton("⬅️ لوحة الإدارة", callback_data="COURSES:admin_back")],
        ]
        safe_edit_message_text(query, text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"خطأ في إحصائيات المستخدم: {e}")
        safe_edit_message_text(query, "❌ حدث خطأ في جلب بيانات المستخدم.", reply_markup=COURSES_ADMIN_MENU_KB)


def admin_course_leaderboard(query: Update.callback_query, course_id: str, page: int = 1):
    """عرض ترتيب المشاركين في دورة معينة مع دعم الصفحات."""

    try:
        course = _course_document(course_id)
        if not course:
            safe_edit_message_text(query, "❌ الدورة غير موجودة.", reply_markup=COURSES_ADMIN_MENU_KB)
            return

        subs = [
            doc.to_dict()
            for doc in db.collection(COURSE_SUBSCRIPTIONS_COLLECTION)
            .where("course_id", "==", course_id)
            .stream()
        ]

        if not subs:
            safe_edit_message_text(
                query,
                "لا يوجد مشاركون في هذه الدورة.",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:statistics")],
                        [InlineKeyboardButton("⬅️ لوحة الإدارة", callback_data="COURSES:admin_back")],
                    ]
                ),
            )
            return

        sorted_subs = sorted(
            subs,
            key=lambda item: (
                -(item.get("points", 0) or 0),
                item.get("user_id", 0),
            ),
        )

        total_entries = len(sorted_subs)
        total_pages = (total_entries + COURSE_LEADERBOARD_PAGE_SIZE - 1) // COURSE_LEADERBOARD_PAGE_SIZE
        current_page = max(1, min(page, total_pages))
        start_index = (current_page - 1) * COURSE_LEADERBOARD_PAGE_SIZE
        end_index = start_index + COURSE_LEADERBOARD_PAGE_SIZE
        page_items = sorted_subs[start_index:end_index]

        lines = [f"🏆 ترتيب دورة {course.get('name', 'دورة')}", ""]
        for rank, item in enumerate(page_items, start=start_index + 1):
            name = item.get("full_name") or item.get("username") or str(item.get("user_id"))
            points = item.get("points", 0)
            lines.append(f"{rank}. {name} — {points} نقطة")

        lines.append("")
        lines.append(f"صفحة {current_page}/{total_pages}")

        nav_buttons = []
        if current_page > 1:
            nav_buttons.append(
                InlineKeyboardButton(
                    "⬅️ السابق",
                    callback_data=f"COURSES:leaderboard_{course_id}_{current_page - 1}",
                )
            )
        if current_page < total_pages:
            nav_buttons.append(
                InlineKeyboardButton(
                    "➡️ التالي",
                    callback_data=f"COURSES:leaderboard_{course_id}_{current_page + 1}",
                )
            )

        keyboard = []
        if nav_buttons:
            keyboard.append(nav_buttons)

        keyboard.append(
            [InlineKeyboardButton("🔙 رجوع", callback_data=f"COURSES:stats_course_{course_id}")]
        )
        keyboard.append([InlineKeyboardButton("⬅️ لوحة الإدارة", callback_data="COURSES:admin_back")])

        safe_edit_message_text(
            query,
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    except Exception as e:
        logger.error(f"خطأ في ترتيب الدورة: {e}")
        safe_edit_message_text(query, "❌ حدث خطأ في جلب الترتيب.", reply_markup=COURSES_ADMIN_MENU_KB)


def admin_delete_course(query: Update.callback_query, context: CallbackContext):
    user_id = query.from_user.id
    if not (is_admin(user_id) or is_supervisor(user_id)):
        safe_edit_message_text(query, "❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return

    if not firestore_available():
        safe_edit_message_text(query, "❌ خطأ في الاتصال بقاعدة البيانات.", reply_markup=COURSES_ADMIN_MENU_KB)
        return

    try:
        courses = [
            {**doc.to_dict(), "id": doc.id}
            for doc in db.collection(COURSES_COLLECTION).stream()
        ]
        if not courses:
            safe_edit_message_text(
                query,
                "🗑 حذف دورة\n\nلا توجد دورات.",
                reply_markup=COURSES_ADMIN_MENU_KB,
            )
            return

        text = "🗑 اختر دورة للحذف النهائي:\n\n⚠️ تحذير: هذا الإجراء لا يمكن التراجع عنه\n\n"
        filtered_courses = [c for c in courses if not _is_back_placeholder_course(c.get("name"))]
        if not filtered_courses:
            safe_edit_message_text(
                query,
                "🗑 حذف دورة\n\nلا توجد دورات صالحة للحذف.",
                reply_markup=COURSES_ADMIN_MENU_KB,
            )
            return

        keyboard = []
        for course in filtered_courses:
            course_name = course.get("name", "دورة")
            course_id = course.get("id")
            keyboard.append(
                [
                    InlineKeyboardButton(
                        f"🗑 {course_name}",
                        callback_data=f"COURSES:confirm_delete_{course_id}",
                    )
                ]
            )

        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:admin_back")])
        safe_edit_message_text(
            query,
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    except Exception as e:
        logger.error(f"خطأ في حذف الدورة: {e}")
        safe_edit_message_text(query, "❌ حدث خطأ. حاول مرة أخرى.", reply_markup=COURSES_ADMIN_MENU_KB)


# =================== معالج Callback الرئيسي ===================


def handle_courses_callback(update: Update, context: CallbackContext):
    """معالج جميع callbacks الدورات"""
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data or ""

    try:
        query.answer()

        if (
            data.startswith("COURSES:")
            and not data.startswith("COURSES:subscribe_")
            and (
                user_id in WAITING_COURSE_COUNTRY
                or user_id in WAITING_COURSE_FULL_NAME
                or user_id in WAITING_COURSE_AGE
                or user_id in WAITING_COURSE_GENDER
            )
        ):
            _reset_course_subscription_flow(user_id)

        if (
            data.startswith("COURSES:")
            and data != "COURSES:edit_profile"
            and (
                user_id in WAITING_PROFILE_EDIT_NAME
                or user_id in WAITING_PROFILE_EDIT_AGE
                or user_id in WAITING_PROFILE_EDIT_COUNTRY
            )
        ):
            _reset_profile_edit_flow(user_id)

        if (
            user_id in WAITING_NEW_COURSE
            and not data.startswith("COURSES:create")
        ):
            _reset_course_creation(user_id)

        if data == "COURSES:available":
            show_available_courses(query, context)
        elif data == "COURSES:my_courses":
            context.user_data["courses_back_target"] = "COURSES:my_courses"
            show_my_courses(query, context)
        elif data == "COURSES:archive":
            show_archived_courses(query, context)
        elif data == "COURSES:edit_profile":
            start_profile_edit(query, context)
        elif data == "COURSES:back_user":
            safe_edit_message_text(
                query,
                "🎓 قسم الدورات\n\nاختر من الخيارات التالية:",
                reply_markup=COURSES_USER_MENU_KB,
            )

        elif data == "COURSES:create":
            admin_create_course(query, context)
        elif data == "COURSES:create_cancel":
            _reset_course_creation(user_id)
            safe_edit_message_text(
                query, "تم الإلغاء بنجاح", reply_markup=COURSES_ADMIN_MENU_KB
            )
        elif data == "COURSES:manage_lessons":
            admin_manage_lessons(query, context)
        elif data == "COURSES:manage_quizzes":
            admin_manage_quizzes(query, context)
        elif data == "COURSES:statistics":
            admin_statistics(query, context)
        elif data.startswith("COURSES:stats_course_"):
            course_id = data.replace("COURSES:stats_course_", "")
            admin_statistics_course(query, course_id)
        elif data.startswith("COURSES:stats_user_"):
            payload = data.replace("COURSES:stats_user_", "")
            if "_" in payload:
                course_id, target_user = payload.rsplit("_", 1)
                admin_statistics_user(query, course_id, target_user)
        elif data.startswith("COURSES:leaderboard_"):
            payload = data.replace("COURSES:leaderboard_", "")
            if "_" in payload:
                course_id, page_str = payload.rsplit("_", 1)
                try:
                    page = int(page_str)
                except ValueError:
                    page = 1
            else:
                course_id = payload
                page = 1
            admin_course_leaderboard(query, course_id, page)
        elif data == "COURSES:archive_manage":
            admin_archive_manage(query, context)
        elif data == "COURSES:delete":
            admin_delete_course(query, context)
        elif data == "COURSES:admin_back":
            _reset_course_creation(user_id)
            _reset_lesson_creation(user_id)
            _reset_quiz_creation(user_id)
            _show_courses_admin_menu_from_callback(query, user_id)

        elif data.startswith("COURSES:back_course_"):
            course_id = data.replace("COURSES:back_course_", "")
            _clear_course_transient_messages(context, query.message.chat_id, user_id)
            show_course_details(query, context, user_id, course_id)
        elif data.startswith("COURSES:subscribe_"):
            course_id = data.replace("COURSES:subscribe_", "")
            subscribe_to_course(query, context, course_id)
        elif data.startswith("COURSES:user_lessons_"):
            course_id = data.replace("COURSES:user_lessons_", "")
            user_lessons_list(query, context, course_id)
        elif data.startswith("COURSES:user_quizzes_"):
            course_id = data.replace("COURSES:user_quizzes_", "")
            user_quizzes_list(query, context, course_id)
        elif data.startswith("COURSES:user_points_"):
            course_id = data.replace("COURSES:user_points_", "")
            user_points(query, user_id, course_id)
        elif data.startswith("COURSES:view_lesson_"):
            lesson_id = data.replace("COURSES:view_lesson_", "")
            user_view_lesson(query, context, lesson_id, user_id)
        elif data.startswith("COURSES:attend_"):
            lesson_id = data.replace("COURSES:attend_", "")
            logger.info("✅ ATTEND_CALLBACK_HIT | data=%s | user_id=%s", data, user_id)
            register_lesson_attendance(query, context, user_id, lesson_id)
        elif data.startswith("COURSE:BEN:OPEN:"):
            parts = data.split(":", 4)
            if len(parts) == 5:
                _, _, _, course_id, lesson_id = parts
                handle_course_benefit_open(query, context, user_id, course_id, lesson_id)
        elif data.startswith("COURSE:BEN:CLOSE:"):
            session_id = data.replace("COURSE:BEN:CLOSE:", "")
            handle_course_benefit_close(query, context, session_id)
        elif data.startswith("COURSE:PRES:OPEN:"):
            parts = data.split(":", 4)
            if len(parts) == 5:
                _, _, _, course_id, lesson_id = parts
                handle_course_presentation_open(query, context, user_id, course_id, lesson_id)
        elif data.startswith("COURSE:PRES:CLOSE:"):
            thread_id = data.replace("COURSE:PRES:CLOSE:", "")
            handle_course_presentation_close(query, context, thread_id)

        elif data.startswith("COURSES:view_"):
            course_id = data.replace("COURSES:view_", "")
            show_course_details(query, context, user_id, course_id)
        elif data.startswith("COURSES:start_quiz_"):
            quiz_id = data.replace("COURSES:start_quiz_", "")
            start_quiz_flow(query, user_id, quiz_id)

        elif data.startswith("COURSES:lessons_"):
            _reset_lesson_creation(user_id)
            course_id = data.replace("COURSES:lessons_", "")
            _admin_show_lessons_panel(query, course_id)
        elif data.startswith("COURSES:add_lesson_"):
            course_id = data.replace("COURSES:add_lesson_", "")
            WAITING_NEW_LESSON.add(user_id)
            WAITING_LESSON_TITLE.add(user_id)
            LESSON_CREATION_CONTEXT[user_id] = {"course_id": course_id, "edit_action": "create"}
            safe_edit_message_text(
                query,
                "✏️ أرسل عنوان الدرس أولاً.",
                reply_markup=_lessons_back_keyboard(course_id),
            )
        elif data.startswith("COURSES:lesson_edit_title_"):
            lesson_id = data.replace("COURSES:lesson_edit_title_", "")
            _admin_request_lesson_title_edit(query, lesson_id)
        elif data.startswith("COURSES:lesson_edit_content_"):
            lesson_id = data.replace("COURSES:lesson_edit_content_", "")
            _admin_request_lesson_content_edit(query, lesson_id)
        elif data.startswith("COURSES:lesson_edit_"):
            lesson_id = data.replace("COURSES:lesson_edit_", "")
            _admin_open_lesson_edit_menu(query, lesson_id)
        elif data.startswith("COURSES:lesson_toggle_pres_"):
            lesson_id = data.replace("COURSES:lesson_toggle_pres_", "")
            _admin_toggle_lesson_presentation(query, lesson_id)
        elif data.startswith("COURSES:lesson_toggle_curriculum_"):
            lesson_id = data.replace("COURSES:lesson_toggle_curriculum_", "")
            _admin_toggle_curriculum_section(query, lesson_id)
        elif data.startswith("COURSES:lesson_delete_confirm_"):
            lesson_id = data.replace("COURSES:lesson_delete_confirm_", "")
            _admin_delete_lesson(query, lesson_id)
        elif data.startswith("COURSES:lesson_delete_"):
            lesson_id = data.replace("COURSES:lesson_delete_", "")
            _admin_confirm_delete_lesson(query, lesson_id)
        elif data.startswith("COURSES:lesson_type_"):
            parts = data.replace("COURSES:lesson_type_", "").split("_", 1)
            if len(parts) == 2:
                content_type, course_id = parts
                LESSON_CREATION_CONTEXT.setdefault(user_id, {})["course_id"] = course_id
                LESSON_CREATION_CONTEXT[user_id]["content_type"] = content_type
                if content_type == "audio":
                    WAITING_LESSON_AUDIO.add(user_id)
                    WAITING_LESSON_CONTENT.discard(user_id)
                    safe_edit_message_text(
                        query,
                        "🔊 أرسل الملف الصوتي الآن (من الهاتف أو إعادة توجيه من القناة).",
                        reply_markup=_lessons_back_keyboard(course_id),
                    )
                else:
                    WAITING_LESSON_CONTENT.add(user_id)
                    WAITING_LESSON_AUDIO.discard(user_id)
                    prompt = "📝 أرسل نص الدرس." if content_type == "text" else "🔗 أرسل الرابط الخاص بالدرس."
                    safe_edit_message_text(
                        query,
                        prompt,
                        reply_markup=_lessons_back_keyboard(course_id),
                    )
        elif data.startswith("COURSES:quizzes_"):
            _reset_quiz_creation(user_id)
            course_id = data.replace("COURSES:quizzes_", "")
            _admin_show_quizzes_panel(query, course_id)
        elif data.startswith("COURSES:add_quiz_"):
            course_id = data.replace("COURSES:add_quiz_", "")
            WAITING_NEW_QUIZ.add(user_id)
            WAITING_QUIZ_TITLE.add(user_id)
            QUIZ_CREATION_CONTEXT[user_id] = {"course_id": course_id}
            safe_edit_message_text(
                query,
                "✏️ أرسل عنوان الاختبار.",
                reply_markup=_quizzes_back_keyboard(course_id),
            )
        elif data.startswith("COURSES:quiz_edit_"):
            quiz_id = data.replace("COURSES:quiz_edit_", "")
            _admin_start_quiz_edit(query, quiz_id)
        elif data.startswith("COURSES:quiz_delete_confirm_"):
            quiz_id = data.replace("COURSES:quiz_delete_confirm_", "")
            _admin_delete_quiz(query, quiz_id)
        elif data.startswith("COURSES:quiz_delete_"):
            quiz_id = data.replace("COURSES:quiz_delete_", "")
            _admin_confirm_delete_quiz(query, quiz_id)
        elif data.startswith("COURSES:quiz_more_"):
            course_id = data.replace("COURSES:quiz_more_", "")
            WAITING_QUIZ_ANSWER_TEXT.add(user_id)
            safe_edit_message_text(
                query,
                "أرسل الإجابة التالية.",
                reply_markup=_quizzes_back_keyboard(course_id),
            )
        elif data.startswith("COURSES:quiz_finish_"):
            course_id = data.replace("COURSES:quiz_finish_", "")
            _finalize_quiz_creation_from_callback(user_id, query)
        elif data.startswith("COURSES:quiz_answer_"):
            parts = data.replace("COURSES:quiz_answer_", "").split("_", 1)
            if len(parts) == 2:
                quiz_id, option_idx = parts
                handle_quiz_answer_selection(query, user_id, quiz_id, option_idx)
        elif data.startswith("COURSES:toggle_"):
            course_id = data.replace("COURSES:toggle_", "")
            doc = db.collection(COURSES_COLLECTION).document(course_id).get()
            if doc.exists:
                course = doc.to_dict()
                new_status = "inactive" if course.get("status") == "active" else "active"
                db.collection(COURSES_COLLECTION).document(course_id).update({"status": new_status})
                safe_edit_message_text(
                    query,
                    f"✅ تم تحديث حالة الدورة إلى: {'مفعلة' if new_status == 'active' else 'معطلة'}",
                    reply_markup=COURSES_ADMIN_MENU_KB,
                )
            else:
                safe_edit_message_text(query, "❌ الدورة غير موجودة.", reply_markup=COURSES_ADMIN_MENU_KB)

        elif data.startswith("COURSES:confirm_delete_"):
            course_id = data.replace("COURSES:confirm_delete_", "")
            try:
                subs = (
                    db.collection(COURSE_SUBSCRIPTIONS_COLLECTION)
                    .where("course_id", "==", course_id)
                    .stream()
                )
                batch = db.batch()
                count = 0
                for sub in subs:
                    batch.delete(sub.reference)
                    count += 1
                    if count % 400 == 0:
                        batch.commit()
                        batch = db.batch()
                batch.commit()
            except Exception as e:
                logger.error(f"خطأ في حذف اشتراكات الدورة: {e}")
            db.collection(COURSES_COLLECTION).document(course_id).delete()
            safe_edit_message_text(query, "✅ تم حذف الدورة بنجاح", reply_markup=COURSES_ADMIN_MENU_KB)

    except Exception as e:
        logger.error(f"خطأ في معالجة callback الدورات: {e}")
        safe_edit_message_text(query, "❌ حدث خطأ. حاول مرة أخرى.")

# =================== نهاية قسم الدورات ===================


if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info("🚀 بدء سُقيا الكوثر")
    logger.info("=" * 50)
    
    # تهيئة Firebase/Firestore مرة واحدة
    initialize_firebase()
    
    # تهيئة Updater و Dispatcher و job_queue مرة واحدة
    try:
        updater = Updater(BOT_TOKEN, use_context=True, request_kwargs=REQUEST_KWARGS)
        dispatcher = updater.dispatcher
        job_queue = updater.job_queue
    except Exception as e:
        logger.error(f"❌ خطأ في تهيئة Updater: {e}", exc_info=True)
        exit(1)
        
    try:
        if WEBHOOK_URL:
            # وضع Webhook
            logger.info("🌐 تشغيل البوت في وضع Webhook...")

            # تهيئة البوت (تسجيل handlers والمهام اليومية)
            start_bot()

            # JobQueue لا يعمل تلقائيًا في وضع Webhook المخصّص
            try:
                if job_queue:
                    job_queue.start()
                    logger.info("✅ تم تشغيل JobQueue في وضع Webhook")
            except Exception as e:
                logger.error(f"❌ خطأ في تشغيل JobQueue: {e}", exc_info=True)

            # إعداد Webhook
            updater.bot.set_webhook(
                WEBHOOK_URL + BOT_TOKEN,
                max_connections=WEBHOOK_MAX_CONNECTIONS,
                timeout=WEBHOOK_TIMEOUT,
                allowed_updates=ALLOWED_UPDATES,
            )
            logger.info(f"✅ تم إعداد Webhook على {WEBHOOK_URL + BOT_TOKEN} بعدد اتصالات {WEBHOOK_MAX_CONNECTIONS}")
            
            # تشغيل Flask (Blocking)
            run_flask()
            
        else:
            # وضع Polling
            logger.info("🔄 تشغيل البوت في وضع Polling...")
            
            # حذف الويب هوك القديم في وضع Polling فقط
            try:
                updater.bot.delete_webhook(drop_pending_updates=True)
                logger.info("✅ تم حذف الويب هوك القديم")
            except Exception as e:
                logger.warning(f"⚠️ خطأ في حذف الويب هوك: {e}")
            
            # تهيئة البوت
            start_bot()

            # بدء Polling
            updater.start_polling(allowed_updates=ALLOWED_UPDATES)
            logger.info("✅ تم بدء Polling بنجاح")
            updater.idle()
            
    except KeyboardInterrupt:
        logger.info("⏹️ إيقاف البوت...")
        if updater:
            updater.stop()
    except Exception as e:
        logger.error(f"❌ خطأ نهائي: {e}", exc_info=True)
