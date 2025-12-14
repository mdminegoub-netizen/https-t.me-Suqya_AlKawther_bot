import os
import sys
import json
import logging
import re
import random
from datetime import datetime, timezone, time, timedelta
from threading import Thread
from typing import List, Dict

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
)

import firebase_admin
from firebase_admin import credentials, firestore

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
SUPERVISOR_ID = 1745150161  # المشرفة

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
LETTERS_COLLECTION = "letters"
GLOBAL_CONFIG_COLLECTION = "global_config"
# Collections جديدة للمجتمع والمنافسات
COMMUNITY_BENEFITS_COLLECTION = "community_benefits"
COMPETITION_POINTS_COLLECTION = "competition_points"
COMMUNITY_MEDALS_COLLECTION = "community_medals"
AUDIO_LIBRARY_COLLECTION = "audio_library"


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
            "letters_to_self": []
        }
    ensure_medal_defaults(data[uid])
    return data[uid]

# دالة المساعدة للرسائل (محلية)
def save_letter_local(user_id: int, letter_data: Dict) -> str:
    """نسخة محلية من save_letter"""
    record = get_user_record_local_by_id(user_id)
    letters = record.get("letters_to_self", [])
    
    letter_data["id"] = f"letter_{len(letters)}"
    letters.append(letter_data)
    
    update_user_record_local(user_id, letters_to_self=letters)
    return letter_data["id"]

def get_user_letters_local(user_id: int) -> List[Dict]:
    """نسخة محلية من get_user_letters"""
    record = get_user_record_local_by_id(user_id)
    return record.get("letters_to_self", [])

def update_letter_local(letter_id: str, letter_data: Dict):
    """نسخة محلية من update_letter"""
    try:
        idx = int(letter_id.split("_")[1])
        user_id = int(letter_id.split("_")[0])
        record = get_user_record_local_by_id(user_id)
        letters = record.get("letters_to_self", [])
        
        if 0 <= idx < len(letters):
            letters[idx].update(letter_data)
            update_user_record_local(user_id, letters_to_self=letters)
    except:
        pass


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
            
            # تحويل letters_to_self إلى تنسيق Firestore
            letters = user_data.get("letters_to_self", [])
            if letters and isinstance(letters, list) and len(letters) > 0:
                # حفظ كل رسالة كوثيقة منفصلة
                for letter in letters:
                    if isinstance(letter, dict) and letter.get("content"):
                        save_letter(user_id, letter)
                
                # إزالة الرسائل من بيانات المستخدم
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
            "today_date": None,
            "today_cups": 0,
            "quran_pages_goal": None,
            "quran_pages_today": 0,
            "quran_today_date": None,
            "tasbih_total": 0,
            "adhkar_count": 0,
            "heart_memos": [],
            "letters_to_self": [],
            "points": 0,
            "level": 0,
            "medals": [],
            "best_rank": None,
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
            "age": None,
            "weight": None,
            "water_liters": None,
            "cups_goal": None,
            "reminders_on": False,
            "today_date": None,
            "today_cups": 0,
            "quran_pages_goal": None,
            "quran_pages_today": 0,
            "quran_today_date": None,
            "tasbih_total": 0,
            "adhkar_count": 0,
            "heart_memos": [],
            "letters_to_self": [],
            "points": 0,
            "level": 0,
            "medals": [],
            "best_rank": None,
            "daily_full_streak": 0,
            "last_full_day": None,
            "daily_full_count": 0,
            "motivation_on": True,
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
    return [int(uid) for uid, rec in data.items() 
            if uid != "GLOBAL_KEY" and not rec.get("is_banned", False)]

def get_banned_user_ids_local() -> List[int]:
    """نسخة محلية من get_banned_user_ids"""
    return [int(uid) for uid, rec in data.items() 
            if uid != "GLOBAL_KEY" and rec.get("is_banned", False)]

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


def get_active_user_ids_local() -> List[int]:
    """نسخة محلية من get_active_user_ids"""
    return [int(uid) for uid, rec in data.items() 
            if uid != "GLOBAL_KEY" and not rec.get("is_banned", False)]

def get_banned_user_ids_local() -> List[int]:
    """نسخة محلية من get_banned_user_ids"""
    return [int(uid) for uid, rec in data.items() 
            if uid != "GLOBAL_KEY" and rec.get("is_banned", False)]

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


def get_banned_user_ids_local() -> List[int]:
    """نسخة محلية من get_banned_user_ids"""
    return [int(uid) for uid, rec in data.items() 
            if uid != "GLOBAL_KEY" and rec.get("is_banned", False)]

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
            "letters_to_self": []
        }
    return data[uid]

# دالة المساعدة للرسائل (محلية)
def save_letter_local(user_id: int, letter_data: Dict) -> str:
    """نسخة محلية من save_letter"""
    record = get_user_record_local_by_id(user_id)
    letters = record.get("letters_to_self", [])
    
    letter_data["id"] = f"letter_{len(letters)}"
    letters.append(letter_data)
    
    update_user_record_local(user_id, letters_to_self=letters)
    return letter_data["id"]

def get_user_letters_local(user_id: int) -> List[Dict]:
    """نسخة محلية من get_user_letters"""
    record = get_user_record_local_by_id(user_id)
    return record.get("letters_to_self", [])

def update_letter_local(letter_id: str, letter_data: Dict):
    """نسخة محلية من update_letter"""
    try:
        idx = int(letter_id.split("_")[1])
        user_id = int(letter_id.split("_")[0])
        record = get_user_record_local_by_id(user_id)
        letters = record.get("letters_to_self", [])
        
        if 0 <= idx < len(letters):
            letters[idx].update(letter_data)
            update_user_record_local(user_id, letters_to_self=letters)
    except:
        pass


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
            
            # تحويل letters_to_self إلى تنسيق Firestore
            letters = user_data.get("letters_to_self", [])
            if letters and isinstance(letters, list) and len(letters) > 0:
                # حفظ كل رسالة كوثيقة منفصلة
                for letter in letters:
                    if isinstance(letter, dict) and letter.get("content"):
                        save_letter(user_id, letter)
                
                # إزالة الرسائل من بيانات المستخدم
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
            "today_date": None,
            "today_cups": 0,
            "quran_pages_goal": None,
            "quran_pages_today": 0,
            "quran_today_date": None,
            "tasbih_total": 0,
            "adhkar_count": 0,
            "heart_memos": [],
            "letters_to_self": [],
            "points": 0,
            "level": 0,
            "medals": [],
            "best_rank": None,
            "daily_full_streak": 0,
            "last_full_day": None,
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
            "age": None,
            "weight": None,
            "water_liters": None,
            "cups_goal": None,
            "reminders_on": False,
            "today_date": None,
            "today_cups": 0,
            "quran_pages_goal": None,
            "quran_pages_today": 0,
            "quran_today_date": None,
            "tasbih_total": 0,
            "adhkar_count": 0,
            "heart_memos": [],
            "letters_to_self": [],
            "points": 0,
            "level": 0,
            "medals": [],
            "best_rank": None,
            "daily_full_streak": 0,
            "last_full_day": None,
            "motivation_on": True,
            "is_new_user": False
        }
        
        for field, default_value in default_fields.items():
            if field not in record:
                record[field] = default_value
    
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
    return [int(uid) for uid, rec in data.items() 
            if uid != "GLOBAL_KEY" and not rec.get("is_banned", False)]

def get_banned_user_ids_local() -> List[int]:
    """نسخة محلية من get_banned_user_ids"""
    return [int(uid) for uid, rec in data.items() 
            if uid != "GLOBAL_KEY" and rec.get("is_banned", False)]

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


def get_active_user_ids_local() -> List[int]:
    """نسخة محلية من get_active_user_ids"""
    return [int(uid) for uid, rec in data.items() 
            if uid != "GLOBAL_KEY" and not rec.get("is_banned", False)]

def get_banned_user_ids_local() -> List[int]:
    """نسخة محلية من get_banned_user_ids"""
    return [int(uid) for uid, rec in data.items() 
            if uid != "GLOBAL_KEY" and rec.get("is_banned", False)]

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


def get_banned_user_ids_local() -> List[int]:
    """نسخة محلية من get_banned_user_ids"""
    return [int(uid) for uid, rec in data.items() 
            if uid != "GLOBAL_KEY" and rec.get("is_banned", False)]

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
            # تحميل المذكرات والرسائل من Subcollections إذا كانت غير موجودة في السجل
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
                if not record.get("letters_to_self"):
                    letters_list = []
                    for letter_doc in doc_ref.collection("letters").stream():
                        letter_data = letter_doc.to_dict()
                        if letter_data:
                            letters_list.append(letter_data)
                    if letters_list:
                        letters_list.sort(
                            key=lambda l: l.get("created_at") or l.get("reminder_date") or ""
                        )
                        record["letters_to_self"] = letters_list
            except Exception as e:
                logger.warning(f"⚠️ تعذر تحميل المذكرات/الرسائل الفرعية للمستخدم {user_id}: {e}")

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
                "age": None,
                "weight": None,
                "water_liters": None,
                "cups_goal": None,
                "reminders_on": False,
                "today_date": None,
                "today_cups": 0,
                "quran_pages_goal": None,
                "quran_pages_today": 0,
                "quran_today_date": None,
                "tasbih_total": 0,
                "adhkar_count": 0,
                "heart_memos": [],
                "letters_to_self": [],
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

# أذكار النوم
SLEEP_ADHKAR_STATE = {}  # user_id -> current_index

# إدارة الجرعة التحفيزية (من لوحة التحكم)
WAITING_MOTIVATION_ADD = set()
WAITING_MOTIVATION_DELETE = set()
WAITING_MOTIVATION_TIMES = set()

# مكتبة الصوتيات
LOCAL_AUDIO_LIBRARY: List[Dict] = []
AUDIO_USER_STATE: Dict[int, Dict] = {}

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
BTN_ADHKAR_MAIN = "أذكاري 🤲"
BTN_QURAN_MAIN = "وردي القرآني 📖"
BTN_TASBIH_MAIN = "السبحة 📿"
BTN_MEMOS_MAIN = "مذكّرات قلبي 🩵"
BTN_WATER_MAIN = "منبّه الماء 💧"
BTN_STATS = "احصائياتي 📊"
BTN_STATS_ONLY = "📊 إحصائياتي"
BTN_MEDALS_ONLY = "🏅 ميدالياتي"
BTN_STATS_BACK_MAIN = "↩️ رجوع للقائمة الرئيسية"
BTN_MEDALS = "ميدالياتي 🏵️"
BTN_LETTER_MAIN = "رسالة إلى نفسي 💌"

BTN_SUPPORT = "تواصل مع الدعم ✉️"
BTN_NOTIFICATIONS_MAIN = "الاشعارات 🔔"
# =================== أزرار قسم الدورات ===================
BTN_COURSES_SECTION = "قسم الدورات 📚"
BTN_MANAGE_COURSES = "إدارة الدورات 📋"
BTN_AUDIO_LIBRARY = "مكتبة صوتية 🎧"

BTN_CANCEL = "إلغاء ❌"
BTN_BACK_MAIN = "رجوع للقائمة الرئيسية ⬅️"

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

# المنافسات و المجتمع
BTN_COMP_MAIN = "المنافسات و المجتمع 🏅"
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

# ===== تعديل القوائم الرئيسية حسب طلبك =====

MAIN_KEYBOARD_USER = ReplyKeyboardMarkup(
    [
        # السطر الأول: وردي القرآني بجانب أذكاري
        [KeyboardButton(BTN_ADHKAR_MAIN), KeyboardButton(BTN_QURAN_MAIN)],
        # السطر الثاني: منبه الماء بجانب السبحة
        [KeyboardButton(BTN_TASBIH_MAIN), KeyboardButton(BTN_WATER_MAIN)],
        # السطر الثالث: رسالة إلى نفسي بجانب مذكرات قلبي
        [KeyboardButton(BTN_MEMOS_MAIN), KeyboardButton(BTN_LETTER_MAIN)],
        # السطر الرابع: مكتبة الصوتيات بجانب احصائياتي
        [KeyboardButton(BTN_STATS), KeyboardButton(BTN_AUDIO_LIBRARY)],
        # السطر الخامس: مجتمع الفوائد والنصائح بجانب المنافسات والمجتمع
        [KeyboardButton(BTN_COMP_MAIN), KeyboardButton(BTN_BENEFITS_MAIN)],
        # السطر السادس: التواصل مع الدعم على اليسار، الاشعارات على اليمين
        [KeyboardButton(BTN_NOTIFICATIONS_MAIN), KeyboardButton(BTN_SUPPORT)],
        # السطر السابع: قسم الدورات
        [KeyboardButton(BTN_COURSES_SECTION)],
    ],
    resize_keyboard=True,
)

MAIN_KEYBOARD_ADMIN = ReplyKeyboardMarkup(
    [
        # السطر الأول: وردي القرآني بجانب أذكاري
        [KeyboardButton(BTN_ADHKAR_MAIN), KeyboardButton(BTN_QURAN_MAIN)],
        # السطر الثاني: منبه الماء بجانب السبحة
        [KeyboardButton(BTN_TASBIH_MAIN), KeyboardButton(BTN_WATER_MAIN)],
        # السطر الثالث: رسالة إلى نفسي بجانب مذكرات قلبي
        [KeyboardButton(BTN_MEMOS_MAIN), KeyboardButton(BTN_LETTER_MAIN)],
        # السطر الرابع: مكتبة الصوتيات بجانب احصائياتي
        [KeyboardButton(BTN_STATS), KeyboardButton(BTN_AUDIO_LIBRARY)],
        # السطر الخامس: مجتمع الفوائد والنصائح بجانب المنافسات والمجتمع
        [KeyboardButton(BTN_COMP_MAIN), KeyboardButton(BTN_BENEFITS_MAIN)],
        # السطر السادس: التواصل مع الدعم على اليسار، الاشعارات على اليمين
        [KeyboardButton(BTN_NOTIFICATIONS_MAIN), KeyboardButton(BTN_SUPPORT)],
        # السطر السابع: لوحة التحكم (فقط للمدير)
        [KeyboardButton(BTN_ADMIN_PANEL)],
    ],
    resize_keyboard=True,
)

MAIN_KEYBOARD_SUPERVISOR = ReplyKeyboardMarkup(
    [
        # السطر الأول: وردي القرآني بجانب أذكاري
        [KeyboardButton(BTN_ADHKAR_MAIN), KeyboardButton(BTN_QURAN_MAIN)],
        # السطر الثاني: منبه الماء بجانب السبحة
        [KeyboardButton(BTN_TASBIH_MAIN), KeyboardButton(BTN_WATER_MAIN)],
        # السطر الثالث: رسالة إلى نفسي بجانب مذكرات قلبي
        [KeyboardButton(BTN_MEMOS_MAIN), KeyboardButton(BTN_LETTER_MAIN)],
        # السطر الرابع: مكتبة الصوتيات بجانب احصائياتي
        [KeyboardButton(BTN_STATS), KeyboardButton(BTN_AUDIO_LIBRARY)],
        # السطر الخامس: مجتمع الفوائد والنصائح بجانب المنافسات والمجتمع
        [KeyboardButton(BTN_COMP_MAIN), KeyboardButton(BTN_BENEFITS_MAIN)],
        # السطر السادس: التواصل مع الدعم على اليسار، الاشعارات على اليمين
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
BTN_SLEEP_ADHKAR_BACK = "⬅️ رجوع للقائمة الرئيسية"

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
    reminders_on = bool(record.get("reminders_on"))
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
POINTS_PER_LETTER = 5


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


def save_letter(user_id: int, letter_data: Dict):
    """حفظ رسالة إلى نفسي في Firestore"""
    user_id_str = str(user_id)
    
    if not firestore_available():
        logger.warning("Firestore غير متوفر")
        return
    
    try:
        # إضافة معلومات إضافية
        letter_data["user_id"] = user_id
        if "created_at" not in letter_data:
            letter_data["created_at"] = datetime.now(timezone.utc).isoformat()
        
        # حفظ الرسالة في subcollection
        db.collection(USERS_COLLECTION).document(user_id_str).collection("letters").add(letter_data)
        logger.info(f"✅ تم حفظ رسالة إلى نفسي للمستخدم {user_id} في Firestore")
        
    except Exception as e:
        logger.error(f"❌ خطأ في حفظ الرسالة للمستخدم {user_id}: {e}")


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


def save_letter(user_id: int, letter_data: Dict):
    """حفظ رسالة إلى نفسي في Firestore"""
    user_id_str = str(user_id)
    
    if not firestore_available():
        logger.warning("Firestore غير متوفر")
        return
    
    try:
        # إضافة معلومات إضافية
        letter_data["user_id"] = user_id
        if "created_at" not in letter_data:
            letter_data["created_at"] = datetime.now(timezone.utc).isoformat()
        
        # حفظ الرسالة في subcollection
        db.collection(USERS_COLLECTION).document(user_id_str).collection("letters").add(letter_data)
        logger.info(f"✅ تم حفظ رسالة إلى نفسي للمستخدم {user_id} في Firestore")
        
    except Exception as e:
        logger.error(f"❌ خطأ في حفظ الرسالة للمستخدم {user_id}: {e}")


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


def save_letter(user_id: int, letter_data: Dict):
    """حفظ رسالة إلى نفسي في Firestore"""
    user_id_str = str(user_id)
    
    if not firestore_available():
        logger.warning("Firestore غير متوفر")
        return
    
    try:
        # إضافة معلومات إضافية
        letter_data["user_id"] = user_id
        if "created_at" not in letter_data:
            letter_data["created_at"] = datetime.now(timezone.utc).isoformat()
        
        # حفظ الرسالة في subcollection
        db.collection(USERS_COLLECTION).document(user_id_str).collection("letters").add(letter_data)
        logger.info(f"✅ تم حفظ رسالة إلى نفسي للمستخدم {user_id} في Firestore")
        
    except Exception as e:
        logger.error(f"❌ خطأ في حفظ الرسالة للمستخدم {user_id}: {e}")


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
    WAITING_LETTER_MENU.discard(user_id)
    WAITING_LETTER_ADD.discard(user_id)
    WAITING_LETTER_ADD_CONTENT.discard(user_id)
    WAITING_LETTER_REMINDER_OPTION.discard(user_id)
    WAITING_LETTER_CUSTOM_DATE.discard(user_id)
    WAITING_LETTER_DELETE_SELECT.discard(user_id)
    WAITING_SUPPORT_GENDER.discard(user_id)
    WAITING_SUPPORT.discard(user_id)
    WAITING_BROADCAST.discard(user_id)
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
    # تحديث Firestore مباشرة
    update_user_record(user_id, letters_to_self=letters)

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
        update_user_record(user_id, letters_to_self=letters)

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
                # تم حفظ البيانات في Firestore عبر update_user_record

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
    
    # حفظ في Firestore
    update_user_record(user.id, letters_to_self=record["letters_to_self"])
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
    new_total = before + cups

    update_user_record(user.id, today_cups=new_total)
    record["today_cups"] = new_total

    add_points(user.id, cups * POINTS_PER_WATER_CUP, context, reason="إضافة أكواب ماء")

    cups_goal = record.get("cups_goal")
    if cups_goal and before < cups_goal <= new_total:
        add_points(user.id, POINTS_WATER_DAILY_BONUS, context, reason="إكمال هدف الماء اليومي")

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
    
    # حفظ في Firestore
    update_user_record(user.id, reminders_on=record["reminders_on"])
    save_data()

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
    record["reminders_on"] = False
    
    # حفظ في Firestore
    update_user_record(user.id, reminders_on=record["reminders_on"])
    save_data()

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
    increment_adhkar_count(user_id, 1)

    if current_index >= len(SLEEP_ADHKAR_ITEMS) - 1:
        SLEEP_ADHKAR_STATE.pop(user_id, None)
        update.message.reply_text(
            "اكتملت أذكار النوم. تصبح على خير ✨",
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

    tasbih_total = record.get("tasbih_total", 0)
    adhkar_count = record.get("adhkar_count", 0)

    memos_count = len(record.get("heart_memos", []))
    letters_count = len(record.get("letters_to_self", []))

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
    text_lines.append(f"- مجموع التسبيحات المسجّلة عبر السبحة: {tasbih_total} تسبيحة.")
    text_lines.append(f"- عدد مذكّرات قلبك المسجّلة: {memos_count} مذكرة.")
    text_lines.append(f"- عدد رسائلك إلى نفسك: {letters_count} رسالة.")

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
    water_status = "مفعّل ✅" if record.get("reminders_on") else "متوقف ⛔️"

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
        if not rec.get("reminders_on"):
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
        if not rec.get("reminders_on"):
            continue
        hours.update(_normalize_hours(rec.get("water_reminder_hours"), REMINDER_HOURS_UTC))

    return sorted(hours) or REMINDER_HOURS_UTC


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
            update_user_record(user.id, gender="male")
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
            update_user_record(user.id, gender="female")
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
        SLEEP_ADHKAR_STATE.pop(user_id, None)
        AUDIO_USER_STATE.pop(user_id, None)
        
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

    if text == BTN_MEMOS_MAIN:
        open_memos_menu(update, context)
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

    if text == BTN_LETTER_MAIN:
        open_letters_menu(update, context)
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
        open_courses_menu(update, context)
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

    if text == BTN_BENEFIT_TOP100:
        handle_top100_benefits(update, context)
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
    msg.reply_text(
        "تنبيه: رسالتك الآن لا تصل للدعم بشكل مباشر.\n"
        "لو حاب ترسل رسالة للدعم:\n"
        "1️⃣ اضغط على زر «تواصل مع الدعم ✉️»\n"
        "2️⃣ أو اضغط على الرسالة التي وصلتك من البوت، ثم اختر Reply / الرد، واكتب رسالتك.",
        reply_markup=main_kb,
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

def _normalize_hashtag(tag: str) -> str:
    return (tag or "").strip().lower().rstrip(".,،؛؛")


def extract_hashtags_from_message(message) -> List[str]:
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

    return [_normalize_hashtag(tag) for tag in hashtags]


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
    return caption.strip() or "مقطع صوتي"


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
        if mime_type.startswith("audio/") or file_name.endswith((".mp3", ".wav", ".m4a", ".ogg")):
            file_id = doc.file_id
            file_unique_id = getattr(doc, "file_unique_id", None)
            file_type = "document"

    return file_id, file_type, file_unique_id


def _is_audio_storage_channel(message) -> bool:
    try:
        return AUDIO_STORAGE_CHANNEL_ID and str(message.chat.id) == AUDIO_STORAGE_CHANNEL_ID
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
            return
        except Exception as e:
            logger.error(f"❌ خطأ في حفظ المقطع الصوتي: {e}")

    # fallback محلي
    global LOCAL_AUDIO_LIBRARY
    LOCAL_AUDIO_LIBRARY.append(record)


def fetch_audio_clips(section_key: str) -> List[Dict]:
    clips: List[Dict] = []

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
                clips.append(clip_data)
        except Exception as e:
            logger.error(f"❌ خطأ في قراءة مكتبة الصوتيات: {e}")
    else:
        clips.extend([c for c in LOCAL_AUDIO_LIBRARY if c.get("section") == section_key])

    clips.sort(
        key=lambda c: (
            c.get("created_at") or "",
            c.get("message_id") or 0,
        ),
        reverse=True,
    )
    return clips


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
    if not message or not _is_audio_storage_channel(message):
        return

    if getattr(message, "is_automatic_forward", False) or message.forward_from_chat:
        return

    hashtags = extract_hashtags_from_message(message)
    section_key = _match_audio_section(hashtags)

    file_id, file_type, file_unique_id = _extract_audio_file(message)

    if not section_key or not file_id:
        delete_audio_clip_by_message_id(message.message_id)
        logger.info(
            "📥 تم إزالة المقطع لعدم وجود هاشتاق مطابق أو ملف صوتي | chat_id=%s | msg_id=%s | hashtags=%s",
            message.chat.id,
            message.message_id,
            hashtags,
        )
        return

    logger.info(
        "🎧 %s قناة التخزين | chat_id=%s | msg_id=%s | file_type=%s | hashtags=%s",
        "تعديل" if is_edit else "رسالة",
        message.chat.id,
        message.message_id,
        file_type or "unknown",
        hashtags,
    )

    record = {
        "section": section_key,
        "title": _audio_title_from_message(message),
        "file_id": file_id,
        "file_type": file_type,
        "file_unique_id": file_unique_id,
        "message_id": message.message_id,
        "created_at": (message.date or datetime.now(timezone.utc)).isoformat(),
    }
    save_audio_clip_record(record)


def _audio_section_inline_keyboard(section_key: str, clips: List[Dict], page: int) -> InlineKeyboardMarkup:
    start = max(page, 0) * AUDIO_PAGE_SIZE
    end = start + AUDIO_PAGE_SIZE
    sliced = clips[start:end]

    rows: List[List[InlineKeyboardButton]] = []
    for clip in sliced:
        title = clip.get("title") or "مقطع صوتي"
        rows.append(
            [
                InlineKeyboardButton(
                    f"🔹 {title}",
                    callback_data=f"audio_play:{section_key}:{clip.get('message_id')}",
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
    clips = fetch_audio_clips(section_key)
    total = len(clips)
    safe_page = max(min(page, (total - 1) // AUDIO_PAGE_SIZE if total else 0), 0)
    AUDIO_USER_STATE[user_id] = {
        "section": section_key,
        "clips": clips,
        "page": safe_page,
    }

    header = f"{AUDIO_SECTIONS[section_key]['title']}\n\nعدد المقاطع المتوفرة: {total}"
    if total:
        header += "\n\n🎧 قائمة المقاطع المتاحة:"

    keyboard = _audio_section_inline_keyboard(section_key, clips, safe_page)

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

    query.answer()

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

        # تمييز البيانات المحملة على أنها محدثة حديثًا لتجنب قراءات Firestore المكررة فور التشغيل
        preload_time = datetime.now(timezone.utc)
        for uid in data:
            if uid != GLOBAL_KEY:
                USER_CACHE_TIMESTAMPS[uid] = preload_time

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

        logger.info("جاري تسجيل المعالجات...")
        dispatcher.add_handler(CommandHandler("start", start_command))
        dispatcher.add_handler(CommandHandler("help", help_command))
        
        dispatcher.add_handler(CallbackQueryHandler(handle_like_benefit_callback, pattern=r"^like_benefit_\d+$"))
        dispatcher.add_handler(CallbackQueryHandler(handle_edit_benefit_callback, pattern=r"^edit_benefit_\d+$"))
        dispatcher.add_handler(CallbackQueryHandler(handle_delete_benefit_callback, pattern=r"^delete_benefit_\d+$"))
        dispatcher.add_handler(CallbackQueryHandler(handle_admin_delete_benefit_callback, pattern=r"^admin_delete_benefit_\d+$"))
        dispatcher.add_handler(CallbackQueryHandler(handle_delete_benefit_confirm_callback, pattern=r"^confirm_delete_benefit_\d+$|^cancel_delete_benefit$|^confirm_admin_delete_benefit_\d+$|^cancel_admin_delete_benefit$"))
        dispatcher.add_handler(CallbackQueryHandler(handle_courses_callback, pattern=r"^COURSES:"))
        dispatcher.add_handler(CallbackQueryHandler(handle_audio_callback, pattern=r"^audio_"))

        dispatcher.add_handler(MessageHandler(Filters.update.channel_post, handle_channel_post))
        dispatcher.add_handler(MessageHandler(Filters.update.edited_channel_post, handle_edited_channel_post))
        dispatcher.add_handler(MessageHandler(Filters.status_update & Filters.chat_type.channel, handle_deleted_channel_post))
        dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_text))
        
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
        for h in _all_water_hours():
            try:
                job_queue.run_daily(
                    water_reminder_job,
                    time=time(hour=h, minute=0, second=random.randint(0, 45), tzinfo=pytz.UTC),
                    name=f"water_reminder_{h}",
                    context=h,
                    job_kwargs={"misfire_grace_time": 300, "coalesce": True},
                )
            except Exception as e:
                logger.warning(f"⚠️ خطأ في جدولة التذكير: {e}")
        
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

# =================== لوحات المفاتيح للدورات ===================

COURSES_USER_MENU_KB = InlineKeyboardMarkup([
    [InlineKeyboardButton("📚 الدورات المتاحة", callback_data="COURSES:available")],
    [InlineKeyboardButton("📒 دوراتي", callback_data="COURSES:my_courses")],
    [InlineKeyboardButton("🗂 أرشيف الدورات", callback_data="COURSES:archive")],
    [InlineKeyboardButton("🔙 رجوع للقائمة الرئيسية", callback_data="COURSES:back_main")],
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

# =================== Handlers للمستخدمين العاديين ===================

def open_courses_menu(update: Update, context: CallbackContext):
    """فتح قائمة الدورات الرئيسية"""
    user_id = update.effective_user.id
    msg = update.message
    
    # فصل الصلاحيات: أدمن/مشرفة فقط للإدارة
    if is_admin(user_id) or is_supervisor(user_id):
        msg.reply_text(
            "📋 لوحة إدارة الدورات\n\nاختر ما تريد القيام به:",
            reply_markup=COURSES_ADMIN_MENU_KB,
        )
    else:
        # المستخدمون العاديون فقط
        msg.reply_text(
            "🎓 قسم الدورات\n\nاختر من الخيارات التالية:",
            reply_markup=COURSES_USER_MENU_KB,
        )

def show_available_courses(query: Update.callback_query, context: CallbackContext):
    """عرض الدورات المتاحة"""
    user_id = query.from_user.id
    
    if not firestore_available():
        query.edit_message_text(
            "❌ خطأ في الاتصال بقاعدة البيانات.\n\nحاول لاحقاً.",
            reply_markup=COURSES_USER_MENU_KB
        )
        return
    
    try:
        # جلب الدورات النشطة من Firestore
        courses_ref = db.collection(COURSES_COLLECTION)
        docs = courses_ref.where("status", "==", "active").stream()
        
        courses = []
        for doc in docs:
            data = doc.to_dict()
            data["id"] = doc.id
            courses.append(data)
        
        if not courses:
            query.edit_message_text(
                "📚 الدورات المتاحة\n\nلا توجد دورات متاحة حالياً.",
                reply_markup=COURSES_USER_MENU_KB
            )
            return
        
        # عرض الدورات
        text = "📚 الدورات المتاحة:\n\n"
        keyboard = []
        
        for course in courses:
            course_name = course.get("name", "دورة")
            course_id = course.get("id")
            text += f"• {course_name}\n"
            keyboard.append([InlineKeyboardButton(f"🔍 {course_name}", callback_data=f"COURSES:view_{course_id}")])
        
        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:back_user")])
        
        query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    except Exception as e:
        logger.error(f"خطأ في جلب الدورات المتاحة: {e}")
        query.edit_message_text(
            "❌ حدث خطأ. حاول مرة أخرى.",
            reply_markup=COURSES_USER_MENU_KB
        )

def show_my_courses(query: Update.callback_query, context: CallbackContext):
    """عرض دورات المستخدم"""
    user_id = query.from_user.id
    
    if not firestore_available():
        query.edit_message_text(
            "❌ خطأ في الاتصال بقاعدة البيانات.",
            reply_markup=COURSES_USER_MENU_KB
        )
        return
    
    try:
        # جلب الدورات المشترك بها المستخدم
        subs_ref = db.collection(COURSE_SUBSCRIPTIONS_COLLECTION)
        subs_docs = subs_ref.where("user_id", "==", user_id).stream()
        
        course_ids = []
        for doc in subs_docs:
            data = doc.to_dict()
            course_ids.append(data.get("course_id"))
        
        if not course_ids:
            query.edit_message_text(
                "📒 دوراتي\n\nأنت لم تشترك في أي دورة حتى الآن.",
                reply_markup=COURSES_USER_MENU_KB
            )
            return
        
        # جلب بيانات الدورات
        text = "📒 دوراتي:\n\n"
        keyboard = []
        
        for course_id in course_ids:
            doc = db.collection(COURSES_COLLECTION).document(course_id).get()
            if doc.exists:
                course = doc.to_dict()
                course_name = course.get("name", "دورة")
                text += f"• {course_name}\n"
                keyboard.append([InlineKeyboardButton(f"📖 {course_name}", callback_data=f"COURSES:view_{course_id}")])
        
        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:back_user")])
        
        query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    except Exception as e:
        logger.error(f"خطأ في جلب دورات المستخدم: {e}")
        query.edit_message_text(
            "❌ حدث خطأ. حاول مرة أخرى.",
            reply_markup=COURSES_USER_MENU_KB
        )

def show_archived_courses(query: Update.callback_query, context: CallbackContext):
    """عرض الدورات المؤرشفة"""
    user_id = query.from_user.id
    
    if not firestore_available():
        query.edit_message_text(
            "❌ خطأ في الاتصال بقاعدة البيانات.",
            reply_markup=COURSES_USER_MENU_KB
        )
        return
    
    try:
        # جلب الدورات المؤرشفة
        courses_ref = db.collection(COURSES_COLLECTION)
        docs = courses_ref.where("status", "==", "inactive").stream()
        
        courses = []
        for doc in docs:
            data = doc.to_dict()
            data["id"] = doc.id
            courses.append(data)
        
        if not courses:
            query.edit_message_text(
                "🗂 أرشيف الدورات\n\nلا توجد دورات مؤرشفة.",
                reply_markup=COURSES_USER_MENU_KB
            )
            return
        
        # عرض الدورات المؤرشفة
        text = "🗂 أرشيف الدورات:\n\n"
        keyboard = []
        
        for course in courses:
            course_name = course.get("name", "دورة")
            course_id = course.get("id")
            text += f"• {course_name}\n"
            keyboard.append([InlineKeyboardButton(f"📖 {course_name}", callback_data=f"COURSES:view_{course_id}")])
        
        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:back_user")])
        
        query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    except Exception as e:
        logger.error(f"خطأ في جلب الدورات المؤرشفة: {e}")
        query.edit_message_text(
            "❌ حدث خطأ. حاول مرة أخرى.",
            reply_markup=COURSES_USER_MENU_KB
        )

# =================== Handlers للأدمن والمشرفة ===================

def admin_create_course(query: Update.callback_query, context: CallbackContext):
    """إنشاء دورة جديدة"""
    user_id = query.from_user.id
    
    # التحقق من الصلاحيات
    if not (is_admin(user_id) or is_supervisor(user_id)):
        query.edit_message_text("❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return
    
    query.edit_message_text(
        "➕ إنشاء دورة جديدة\n\n"
        "قريباً: سيتم إضافة نموذج الإنشاء\n"
        "الخطوات:\n"
        "1. اسم الدورة\n"
        "2. الوصف\n"
        "3. المستوى\n"
        "4. عدد الدروس",
        reply_markup=COURSES_ADMIN_MENU_KB
    )

def admin_manage_lessons(query: Update.callback_query, context: CallbackContext):
    """إدارة الدروس"""
    user_id = query.from_user.id
    
    if not (is_admin(user_id) or is_supervisor(user_id)):
        query.edit_message_text("❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return
    
    if not firestore_available():
        query.edit_message_text(
            "❌ خطأ في الاتصال بقاعدة البيانات.",
            reply_markup=COURSES_ADMIN_MENU_KB
        )
        return
    
    try:
        # جلب الدورات
        courses_ref = db.collection(COURSES_COLLECTION)
        docs = courses_ref.stream()
        
        courses = []
        for doc in docs:
            data = doc.to_dict()
            data["id"] = doc.id
            courses.append(data)
        
        if not courses:
            query.edit_message_text(
                "🧩 إدارة الدروس\n\nلا توجد دورات لإضافة دروس إليها.",
                reply_markup=COURSES_ADMIN_MENU_KB
            )
            return
        
        # عرض الدورات
        text = "🧩 اختر دورة لإدارة دروسها:\n\n"
        keyboard = []
        
        for course in courses:
            course_name = course.get("name", "دورة")
            course_id = course.get("id")
            keyboard.append([InlineKeyboardButton(f"📖 {course_name}", callback_data=f"COURSES:lessons_{course_id}")])
        
        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:admin_back")])
        
        query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    except Exception as e:
        logger.error(f"خطأ في إدارة الدروس: {e}")
        query.edit_message_text(
            "❌ حدث خطأ. حاول مرة أخرى.",
            reply_markup=COURSES_ADMIN_MENU_KB
        )

def admin_manage_quizzes(query: Update.callback_query, context: CallbackContext):
    """إدارة الاختبارات"""
    user_id = query.from_user.id
    
    if not (is_admin(user_id) or is_supervisor(user_id)):
        query.edit_message_text("❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return
    
    if not firestore_available():
        query.edit_message_text(
            "❌ خطأ في الاتصال بقاعدة البيانات.",
            reply_markup=COURSES_ADMIN_MENU_KB
        )
        return
    
    try:
        # جلب الدورات
        courses_ref = db.collection(COURSES_COLLECTION)
        docs = courses_ref.stream()
        
        courses = []
        for doc in docs:
            data = doc.to_dict()
            data["id"] = doc.id
            courses.append(data)
        
        if not courses:
            query.edit_message_text(
                "📝 إدارة الاختبارات\n\nلا توجد دورات لإضافة اختبارات إليها.",
                reply_markup=COURSES_ADMIN_MENU_KB
            )
            return
        
        # عرض الدورات
        text = "📝 اختر دورة لإدارة اختباراتها:\n\n"
        keyboard = []
        
        for course in courses:
            course_name = course.get("name", "دورة")
            course_id = course.get("id")
            keyboard.append([InlineKeyboardButton(f"📝 {course_name}", callback_data=f"COURSES:quizzes_{course_id}")])
        
        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:admin_back")])
        
        query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    except Exception as e:
        logger.error(f"خطأ في إدارة الاختبارات: {e}")
        query.edit_message_text(
            "❌ حدث خطأ. حاول مرة أخرى.",
            reply_markup=COURSES_ADMIN_MENU_KB
        )

def admin_statistics(query: Update.callback_query, context: CallbackContext):
    """عرض إحصائيات الدورات"""
    user_id = query.from_user.id
    
    if not (is_admin(user_id) or is_supervisor(user_id)):
        query.edit_message_text("❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return
    
    if not firestore_available():
        query.edit_message_text(
            "❌ خطأ في الاتصال بقاعدة البيانات.",
            reply_markup=COURSES_ADMIN_MENU_KB
        )
        return
    
    try:
        # جلب إحصائيات الدورات
        courses_ref = db.collection(COURSES_COLLECTION)
        courses_docs = courses_ref.stream()
        
        stats_text = "📊 إحصائيات الدورات:\n\n"
        total_courses = 0
        total_subscribers = 0
        
        for doc in courses_docs:
            total_courses += 1
            course = doc.to_dict()
            course_name = course.get("name", "دورة")
            course_id = doc.id
            
            # عد المشتركين
            subs_ref = db.collection(COURSE_SUBSCRIPTIONS_COLLECTION)
            subs_count = len(list(subs_ref.where("course_id", "==", course_id).stream()))
            total_subscribers += subs_count
            
            stats_text += f"📚 {course_name}: {subs_count} مشترك\n"
        
        stats_text += f"\n📊 الإجمالي:\n"
        stats_text += f"• عدد الدورات: {total_courses}\n"
        stats_text += f"• عدد المشتركين: {total_subscribers}\n"
        
        query.edit_message_text(
            stats_text,
            reply_markup=COURSES_ADMIN_MENU_KB
        )
    
    except Exception as e:
        logger.error(f"خطأ في جلب الإحصائيات: {e}")
        query.edit_message_text(
            "❌ حدث خطأ. حاول مرة أخرى.",
            reply_markup=COURSES_ADMIN_MENU_KB
        )

def admin_archive_manage(query: Update.callback_query, context: CallbackContext):
    """إدارة أرشفة الدورات"""
    user_id = query.from_user.id
    
    if not (is_admin(user_id) or is_supervisor(user_id)):
        query.edit_message_text("❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return
    
    if not firestore_available():
        query.edit_message_text(
            "❌ خطأ في الاتصال بقاعدة البيانات.",
            reply_markup=COURSES_ADMIN_MENU_KB
        )
        return
    
    try:
        # جلب جميع الدورات
        courses_ref = db.collection(COURSES_COLLECTION)
        docs = courses_ref.stream()
        
        courses = []
        for doc in docs:
            data = doc.to_dict()
            data["id"] = doc.id
            courses.append(data)
        
        if not courses:
            query.edit_message_text(
                "🗂 أرشفة/إيقاف/تشغيل\n\nلا توجد دورات.",
                reply_markup=COURSES_ADMIN_MENU_KB
            )
            return
        
        # عرض الدورات
        text = "🗂 اختر دورة لتغيير حالتها:\n\n"
        keyboard = []
        
        for course in courses:
            course_name = course.get("name", "دورة")
            course_id = course.get("id")
            status = course.get("status", "active")
            status_emoji = "✅" if status == "active" else "❌"
            
            keyboard.append([InlineKeyboardButton(f"{status_emoji} {course_name}", callback_data=f"COURSES:toggle_{course_id}")])
        
        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:admin_back")])
        
        query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    except Exception as e:
        logger.error(f"خطأ في إدارة الأرشفة: {e}")
        query.edit_message_text(
            "❌ حدث خطأ. حاول مرة أخرى.",
            reply_markup=COURSES_ADMIN_MENU_KB
        )

def admin_delete_course(query: Update.callback_query, context: CallbackContext):
    """حذف دورة"""
    user_id = query.from_user.id
    
    if not (is_admin(user_id) or is_supervisor(user_id)):
        query.edit_message_text("❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return
    
    if not firestore_available():
        query.edit_message_text(
            "❌ خطأ في الاتصال بقاعدة البيانات.",
            reply_markup=COURSES_ADMIN_MENU_KB
        )
        return
    
    try:
        # جلب جميع الدورات
        courses_ref = db.collection(COURSES_COLLECTION)
        docs = courses_ref.stream()
        
        courses = []
        for doc in docs:
            data = doc.to_dict()
            data["id"] = doc.id
            courses.append(data)
        
        if not courses:
            query.edit_message_text(
                "🗑 حذف دورة\n\nلا توجد دورات.",
                reply_markup=COURSES_ADMIN_MENU_KB
            )
            return
        
        # عرض الدورات
        text = "🗑 اختر دورة للحذف النهائي:\n\n⚠️ تحذير: هذا الإجراء لا يمكن التراجع عنه\n\n"
        keyboard = []
        
        for course in courses:
            course_name = course.get("name", "دورة")
            course_id = course.get("id")
            keyboard.append([InlineKeyboardButton(f"🗑 {course_name}", callback_data=f"COURSES:confirm_delete_{course_id}")])
        
        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:admin_back")])
        
        query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    except Exception as e:
        logger.error(f"خطأ في حذف الدورة: {e}")
        query.edit_message_text(
            "❌ حدث خطأ. حاول مرة أخرى.",
            reply_markup=COURSES_ADMIN_MENU_KB
        )

# =================== معالج Callback الرئيسي ===================

def handle_courses_callback(update: Update, context: CallbackContext):
    """معالج جميع callbacks الدورات"""
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data
    
    try:
        query.answer()
        
        # معالجات المستخدمين العاديين
        if data == "COURSES:available":
            show_available_courses(query, context)
        
        elif data == "COURSES:my_courses":
            show_my_courses(query, context)
        
        elif data == "COURSES:archive":
            show_archived_courses(query, context)
        
        elif data == "COURSES:back_user":
            query.edit_message_text(
                "🎓 قسم الدورات\n\nاختر من الخيارات التالية:",
                reply_markup=COURSES_USER_MENU_KB
            )
        
        elif data == "COURSES:back_main":
            main_kb = user_main_keyboard(user_id)
            query.edit_message_text(
                "عدنا إلى القائمة الرئيسية",
                reply_markup=main_kb
            )
        
        # معالجات الأدمن والمشرفة
        elif data == "COURSES:create":
            admin_create_course(query, context)
        
        elif data == "COURSES:manage_lessons":
            admin_manage_lessons(query, context)
        
        elif data == "COURSES:manage_quizzes":
            admin_manage_quizzes(query, context)
        
        elif data == "COURSES:statistics":
            admin_statistics(query, context)
        
        elif data == "COURSES:archive_manage":
            admin_archive_manage(query, context)
        
        elif data == "COURSES:delete":
            admin_delete_course(query, context)
        
        elif data == "COURSES:admin_back":
            admin_kb = admin_panel_keyboard_for(user_id)
            query.edit_message_text(
                "عدنا إلى لوحة التحكم",
                reply_markup=admin_kb
            )
        
        # معالجات إضافية
        elif data.startswith("COURSES:view_"):
            course_id = data.replace("COURSES:view_", "")
            # سيتم إضافة عرض تفاصيل الدورة لاحقاً
            query.edit_message_text(
                "📖 تفاصيل الدورة\n\nقريباً: سيتم عرض التفاصيل هنا",
                reply_markup=COURSES_USER_MENU_KB
            )
        
        elif data.startswith("COURSES:lessons_"):
            course_id = data.replace("COURSES:lessons_", "")
            query.edit_message_text(
                "📖 إدارة الدروس\n\nقريباً: سيتم عرض الدروس هنا",
                reply_markup=COURSES_ADMIN_MENU_KB
            )
        
        elif data.startswith("COURSES:quizzes_"):
            course_id = data.replace("COURSES:quizzes_", "")
            query.edit_message_text(
                "📝 إدارة الاختبارات\n\nقريباً: سيتم عرض الاختبارات هنا",
                reply_markup=COURSES_ADMIN_MENU_KB
            )
        
        elif data.startswith("COURSES:toggle_"):
            course_id = data.replace("COURSES:toggle_", "")
            # تبديل حالة الدورة
            doc = db.collection(COURSES_COLLECTION).document(course_id).get()
            if doc.exists:
                course = doc.to_dict()
                new_status = "inactive" if course.get("status") == "active" else "active"
                db.collection(COURSES_COLLECTION).document(course_id).update({"status": new_status})
                query.edit_message_text(
                    f"✅ تم تحديث حالة الدورة إلى: {'مفعلة' if new_status == 'active' else 'معطلة'}",
                    reply_markup=COURSES_ADMIN_MENU_KB
                )
            else:
                query.edit_message_text("❌ الدورة غير موجودة.", reply_markup=COURSES_ADMIN_MENU_KB)
        
        elif data.startswith("COURSES:confirm_delete_"):
            course_id = data.replace("COURSES:confirm_delete_", "")
            # حذف الدورة
            db.collection(COURSES_COLLECTION).document(course_id).delete()
            query.edit_message_text(
                "✅ تم حذف الدورة بنجاح",
                reply_markup=COURSES_ADMIN_MENU_KB
            )
    
    except Exception as e:
        logger.error(f"خطأ في معالجة callback الدورات: {e}")
        query.edit_message_text("❌ حدث خطأ. حاول مرة أخرى.")

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

# =================== قسم الدورات - Handlers الفعلية ===================

# ثوابت Firestore
COURSES_COLLECTION = "courses"
COURSE_LESSONS_COLLECTION = "course_lessons"
COURSE_QUIZZES_COLLECTION = "course_quizzes"
COURSE_SUBSCRIPTIONS_COLLECTION = "course_subscriptions"

# =================== لوحات المفاتيح للدورات ===================

COURSES_USER_MENU_KB = InlineKeyboardMarkup([
    [InlineKeyboardButton("📚 الدورات المتاحة", callback_data="COURSES:available")],
    [InlineKeyboardButton("📒 دوراتي", callback_data="COURSES:my_courses")],
    [InlineKeyboardButton("🗂 أرشيف الدورات", callback_data="COURSES:archive")],
    [InlineKeyboardButton("🔙 رجوع للقائمة الرئيسية", callback_data="COURSES:back_main")],
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

# =================== Handlers للمستخدمين العاديين ===================

def open_courses_menu(update: Update, context: CallbackContext):
    """فتح قائمة الدورات الرئيسية"""
    user_id = update.effective_user.id
    msg = update.message
    
    # فصل الصلاحيات: أدمن/مشرفة فقط للإدارة
    if is_admin(user_id) or is_supervisor(user_id):
        msg.reply_text(
            "📋 لوحة إدارة الدورات\n\nاختر ما تريد القيام به:",
            reply_markup=COURSES_ADMIN_MENU_KB,
        )
    else:
        # المستخدمون العاديون فقط
        msg.reply_text(
            "🎓 قسم الدورات\n\nاختر من الخيارات التالية:",
            reply_markup=COURSES_USER_MENU_KB,
        )

def show_available_courses(query: Update.callback_query, context: CallbackContext):
    """عرض الدورات المتاحة"""
    user_id = query.from_user.id
    
    if not firestore_available():
        query.edit_message_text(
            "❌ خطأ في الاتصال بقاعدة البيانات.\n\nحاول لاحقاً.",
            reply_markup=COURSES_USER_MENU_KB
        )
        return
    
    try:
        # جلب الدورات النشطة من Firestore
        courses_ref = db.collection(COURSES_COLLECTION)
        docs = courses_ref.where("status", "==", "active").stream()
        
        courses = []
        for doc in docs:
            data = doc.to_dict()
            data["id"] = doc.id
            courses.append(data)
        
        if not courses:
            query.edit_message_text(
                "📚 الدورات المتاحة\n\nلا توجد دورات متاحة حالياً.",
                reply_markup=COURSES_USER_MENU_KB
            )
            return
        
        # عرض الدورات
        text = "📚 الدورات المتاحة:\n\n"
        keyboard = []
        
        for course in courses:
            course_name = course.get("name", "دورة")
            course_id = course.get("id")
            text += f"• {course_name}\n"
            keyboard.append([InlineKeyboardButton(f"🔍 {course_name}", callback_data=f"COURSES:view_{course_id}")])
        
        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:back_user")])
        
        query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    except Exception as e:
        logger.error(f"خطأ في جلب الدورات المتاحة: {e}")
        query.edit_message_text(
            "❌ حدث خطأ. حاول مرة أخرى.",
            reply_markup=COURSES_USER_MENU_KB
        )

def show_my_courses(query: Update.callback_query, context: CallbackContext):
    """عرض دورات المستخدم"""
    user_id = query.from_user.id
    
    if not firestore_available():
        query.edit_message_text(
            "❌ خطأ في الاتصال بقاعدة البيانات.",
            reply_markup=COURSES_USER_MENU_KB
        )
        return
    
    try:
        # جلب الدورات المشترك بها المستخدم
        subs_ref = db.collection(COURSE_SUBSCRIPTIONS_COLLECTION)
        subs_docs = subs_ref.where("user_id", "==", user_id).stream()
        
        course_ids = []
        for doc in subs_docs:
            data = doc.to_dict()
            course_ids.append(data.get("course_id"))
        
        if not course_ids:
            query.edit_message_text(
                "📒 دوراتي\n\nأنت لم تشترك في أي دورة حتى الآن.",
                reply_markup=COURSES_USER_MENU_KB
            )
            return
        
        # جلب بيانات الدورات
        text = "📒 دوراتي:\n\n"
        keyboard = []
        
        for course_id in course_ids:
            doc = db.collection(COURSES_COLLECTION).document(course_id).get()
            if doc.exists:
                course = doc.to_dict()
                course_name = course.get("name", "دورة")
                text += f"• {course_name}\n"
                keyboard.append([InlineKeyboardButton(f"📖 {course_name}", callback_data=f"COURSES:view_{course_id}")])
        
        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:back_user")])
        
        query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    except Exception as e:
        logger.error(f"خطأ في جلب دورات المستخدم: {e}")
        query.edit_message_text(
            "❌ حدث خطأ. حاول مرة أخرى.",
            reply_markup=COURSES_USER_MENU_KB
        )

def show_archived_courses(query: Update.callback_query, context: CallbackContext):
    """عرض الدورات المؤرشفة"""
    user_id = query.from_user.id
    
    if not firestore_available():
        query.edit_message_text(
            "❌ خطأ في الاتصال بقاعدة البيانات.",
            reply_markup=COURSES_USER_MENU_KB
        )
        return
    
    try:
        # جلب الدورات المؤرشفة
        courses_ref = db.collection(COURSES_COLLECTION)
        docs = courses_ref.where("status", "==", "inactive").stream()
        
        courses = []
        for doc in docs:
            data = doc.to_dict()
            data["id"] = doc.id
            courses.append(data)
        
        if not courses:
            query.edit_message_text(
                "🗂 أرشيف الدورات\n\nلا توجد دورات مؤرشفة.",
                reply_markup=COURSES_USER_MENU_KB
            )
            return
        
        # عرض الدورات المؤرشفة
        text = "🗂 أرشيف الدورات:\n\n"
        keyboard = []
        
        for course in courses:
            course_name = course.get("name", "دورة")
            course_id = course.get("id")
            text += f"• {course_name}\n"
            keyboard.append([InlineKeyboardButton(f"📖 {course_name}", callback_data=f"COURSES:view_{course_id}")])
        
        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:back_user")])
        
        query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    except Exception as e:
        logger.error(f"خطأ في جلب الدورات المؤرشفة: {e}")
        query.edit_message_text(
            "❌ حدث خطأ. حاول مرة أخرى.",
            reply_markup=COURSES_USER_MENU_KB
        )

# =================== Handlers للأدمن والمشرفة ===================

def admin_create_course(query: Update.callback_query, context: CallbackContext):
    """إنشاء دورة جديدة"""
    user_id = query.from_user.id
    
    # التحقق من الصلاحيات
    if not (is_admin(user_id) or is_supervisor(user_id)):
        query.edit_message_text("❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return
    
    query.edit_message_text(
        "➕ إنشاء دورة جديدة\n\n"
        "قريباً: سيتم إضافة نموذج الإنشاء\n"
        "الخطوات:\n"
        "1. اسم الدورة\n"
        "2. الوصف\n"
        "3. المستوى\n"
        "4. عدد الدروس",
        reply_markup=COURSES_ADMIN_MENU_KB
    )

def admin_manage_lessons(query: Update.callback_query, context: CallbackContext):
    """إدارة الدروس"""
    user_id = query.from_user.id
    
    if not (is_admin(user_id) or is_supervisor(user_id)):
        query.edit_message_text("❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return
    
    if not firestore_available():
        query.edit_message_text(
            "❌ خطأ في الاتصال بقاعدة البيانات.",
            reply_markup=COURSES_ADMIN_MENU_KB
        )
        return
    
    try:
        # جلب الدورات
        courses_ref = db.collection(COURSES_COLLECTION)
        docs = courses_ref.stream()
        
        courses = []
        for doc in docs:
            data = doc.to_dict()
            data["id"] = doc.id
            courses.append(data)
        
        if not courses:
            query.edit_message_text(
                "🧩 إدارة الدروس\n\nلا توجد دورات لإضافة دروس إليها.",
                reply_markup=COURSES_ADMIN_MENU_KB
            )
            return
        
        # عرض الدورات
        text = "🧩 اختر دورة لإدارة دروسها:\n\n"
        keyboard = []
        
        for course in courses:
            course_name = course.get("name", "دورة")
            course_id = course.get("id")
            keyboard.append([InlineKeyboardButton(f"📖 {course_name}", callback_data=f"COURSES:lessons_{course_id}")])
        
        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:admin_back")])
        
        query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    except Exception as e:
        logger.error(f"خطأ في إدارة الدروس: {e}")
        query.edit_message_text(
            "❌ حدث خطأ. حاول مرة أخرى.",
            reply_markup=COURSES_ADMIN_MENU_KB
        )

def admin_manage_quizzes(query: Update.callback_query, context: CallbackContext):
    """إدارة الاختبارات"""
    user_id = query.from_user.id
    
    if not (is_admin(user_id) or is_supervisor(user_id)):
        query.edit_message_text("❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return
    
    if not firestore_available():
        query.edit_message_text(
            "❌ خطأ في الاتصال بقاعدة البيانات.",
            reply_markup=COURSES_ADMIN_MENU_KB
        )
        return
    
    try:
        # جلب الدورات
        courses_ref = db.collection(COURSES_COLLECTION)
        docs = courses_ref.stream()
        
        courses = []
        for doc in docs:
            data = doc.to_dict()
            data["id"] = doc.id
            courses.append(data)
        
        if not courses:
            query.edit_message_text(
                "📝 إدارة الاختبارات\n\nلا توجد دورات لإضافة اختبارات إليها.",
                reply_markup=COURSES_ADMIN_MENU_KB
            )
            return
        
        # عرض الدورات
        text = "📝 اختر دورة لإدارة اختباراتها:\n\n"
        keyboard = []
        
        for course in courses:
            course_name = course.get("name", "دورة")
            course_id = course.get("id")
            keyboard.append([InlineKeyboardButton(f"📝 {course_name}", callback_data=f"COURSES:quizzes_{course_id}")])
        
        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:admin_back")])
        
        query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    except Exception as e:
        logger.error(f"خطأ في إدارة الاختبارات: {e}")
        query.edit_message_text(
            "❌ حدث خطأ. حاول مرة أخرى.",
            reply_markup=COURSES_ADMIN_MENU_KB
        )

def admin_statistics(query: Update.callback_query, context: CallbackContext):
    """عرض إحصائيات الدورات"""
    user_id = query.from_user.id
    
    if not (is_admin(user_id) or is_supervisor(user_id)):
        query.edit_message_text("❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return
    
    if not firestore_available():
        query.edit_message_text(
            "❌ خطأ في الاتصال بقاعدة البيانات.",
            reply_markup=COURSES_ADMIN_MENU_KB
        )
        return
    
    try:
        # جلب إحصائيات الدورات
        courses_ref = db.collection(COURSES_COLLECTION)
        courses_docs = courses_ref.stream()
        
        stats_text = "📊 إحصائيات الدورات:\n\n"
        total_courses = 0
        total_subscribers = 0
        
        for doc in courses_docs:
            total_courses += 1
            course = doc.to_dict()
            course_name = course.get("name", "دورة")
            course_id = doc.id
            
            # عد المشتركين
            subs_ref = db.collection(COURSE_SUBSCRIPTIONS_COLLECTION)
            subs_count = len(list(subs_ref.where("course_id", "==", course_id).stream()))
            total_subscribers += subs_count
            
            stats_text += f"📚 {course_name}: {subs_count} مشترك\n"
        
        stats_text += f"\n📊 الإجمالي:\n"
        stats_text += f"• عدد الدورات: {total_courses}\n"
        stats_text += f"• عدد المشتركين: {total_subscribers}\n"
        
        query.edit_message_text(
            stats_text,
            reply_markup=COURSES_ADMIN_MENU_KB
        )
    
    except Exception as e:
        logger.error(f"خطأ في جلب الإحصائيات: {e}")
        query.edit_message_text(
            "❌ حدث خطأ. حاول مرة أخرى.",
            reply_markup=COURSES_ADMIN_MENU_KB
        )

def admin_archive_manage(query: Update.callback_query, context: CallbackContext):
    """إدارة أرشفة الدورات"""
    user_id = query.from_user.id
    
    if not (is_admin(user_id) or is_supervisor(user_id)):
        query.edit_message_text("❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return
    
    if not firestore_available():
        query.edit_message_text(
            "❌ خطأ في الاتصال بقاعدة البيانات.",
            reply_markup=COURSES_ADMIN_MENU_KB
        )
        return
    
    try:
        # جلب جميع الدورات
        courses_ref = db.collection(COURSES_COLLECTION)
        docs = courses_ref.stream()
        
        courses = []
        for doc in docs:
            data = doc.to_dict()
            data["id"] = doc.id
            courses.append(data)
        
        if not courses:
            query.edit_message_text(
                "🗂 أرشفة/إيقاف/تشغيل\n\nلا توجد دورات.",
                reply_markup=COURSES_ADMIN_MENU_KB
            )
            return
        
        # عرض الدورات
        text = "🗂 اختر دورة لتغيير حالتها:\n\n"
        keyboard = []
        
        for course in courses:
            course_name = course.get("name", "دورة")
            course_id = course.get("id")
            status = course.get("status", "active")
            status_emoji = "✅" if status == "active" else "❌"
            
            keyboard.append([InlineKeyboardButton(f"{status_emoji} {course_name}", callback_data=f"COURSES:toggle_{course_id}")])
        
        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:admin_back")])
        
        query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    except Exception as e:
        logger.error(f"خطأ في إدارة الأرشفة: {e}")
        query.edit_message_text(
            "❌ حدث خطأ. حاول مرة أخرى.",
            reply_markup=COURSES_ADMIN_MENU_KB
        )

def admin_delete_course(query: Update.callback_query, context: CallbackContext):
    """حذف دورة"""
    user_id = query.from_user.id
    
    if not (is_admin(user_id) or is_supervisor(user_id)):
        query.edit_message_text("❌ ليس لديك صلاحية للقيام بهذا الإجراء.")
        return
    
    if not firestore_available():
        query.edit_message_text(
            "❌ خطأ في الاتصال بقاعدة البيانات.",
            reply_markup=COURSES_ADMIN_MENU_KB
        )
        return
    
    try:
        # جلب جميع الدورات
        courses_ref = db.collection(COURSES_COLLECTION)
        docs = courses_ref.stream()
        
        courses = []
        for doc in docs:
            data = doc.to_dict()
            data["id"] = doc.id
            courses.append(data)
        
        if not courses:
            query.edit_message_text(
                "🗑 حذف دورة\n\nلا توجد دورات.",
                reply_markup=COURSES_ADMIN_MENU_KB
            )
            return
        
        # عرض الدورات
        text = "🗑 اختر دورة للحذف النهائي:\n\n⚠️ تحذير: هذا الإجراء لا يمكن التراجع عنه\n\n"
        keyboard = []
        
        for course in courses:
            course_name = course.get("name", "دورة")
            course_id = course.get("id")
            keyboard.append([InlineKeyboardButton(f"🗑 {course_name}", callback_data=f"COURSES:confirm_delete_{course_id}")])
        
        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="COURSES:admin_back")])
        
        query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    except Exception as e:
        logger.error(f"خطأ في حذف الدورة: {e}")
        query.edit_message_text(
            "❌ حدث خطأ. حاول مرة أخرى.",
            reply_markup=COURSES_ADMIN_MENU_KB
        )

# =================== معالج Callback الرئيسي ===================

def handle_courses_callback(update: Update, context: CallbackContext):
    """معالج جميع callbacks الدورات"""
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data
    
    try:
        query.answer()
        
        # معالجات المستخدمين العاديين
        if data == "COURSES:available":
            show_available_courses(query, context)
        
        elif data == "COURSES:my_courses":
            show_my_courses(query, context)
        
        elif data == "COURSES:archive":
            show_archived_courses(query, context)
        
        elif data == "COURSES:back_user":
            query.edit_message_text(
                "🎓 قسم الدورات\n\nاختر من الخيارات التالية:",
                reply_markup=COURSES_USER_MENU_KB
            )
        
        elif data == "COURSES:back_main":
            main_kb = user_main_keyboard(user_id)
            query.edit_message_text(
                "عدنا إلى القائمة الرئيسية",
                reply_markup=main_kb
            )
        
        # معالجات الأدمن والمشرفة
        elif data == "COURSES:create":
            admin_create_course(query, context)
        
        elif data == "COURSES:manage_lessons":
            admin_manage_lessons(query, context)
        
        elif data == "COURSES:manage_quizzes":
            admin_manage_quizzes(query, context)
        
        elif data == "COURSES:statistics":
            admin_statistics(query, context)
        
        elif data == "COURSES:archive_manage":
            admin_archive_manage(query, context)
        
        elif data == "COURSES:delete":
            admin_delete_course(query, context)
        
        elif data == "COURSES:admin_back":
            admin_kb = admin_panel_keyboard_for(user_id)
            query.edit_message_text(
                "عدنا إلى لوحة التحكم",
                reply_markup=admin_kb
            )
        
        # معالجات إضافية
        elif data.startswith("COURSES:view_"):
            course_id = data.replace("COURSES:view_", "")
            # سيتم إضافة عرض تفاصيل الدورة لاحقاً
            query.edit_message_text(
                "📖 تفاصيل الدورة\n\nقريباً: سيتم عرض التفاصيل هنا",
                reply_markup=COURSES_USER_MENU_KB
            )
        
        elif data.startswith("COURSES:lessons_"):
            course_id = data.replace("COURSES:lessons_", "")
            query.edit_message_text(
                "📖 إدارة الدروس\n\nقريباً: سيتم عرض الدروس هنا",
                reply_markup=COURSES_ADMIN_MENU_KB
            )
        
        elif data.startswith("COURSES:quizzes_"):
            course_id = data.replace("COURSES:quizzes_", "")
            query.edit_message_text(
                "📝 إدارة الاختبارات\n\nقريباً: سيتم عرض الاختبارات هنا",
                reply_markup=COURSES_ADMIN_MENU_KB
            )
        
        elif data.startswith("COURSES:toggle_"):
            course_id = data.replace("COURSES:toggle_", "")
            # تبديل حالة الدورة
            doc = db.collection(COURSES_COLLECTION).document(course_id).get()
            if doc.exists:
                course = doc.to_dict()
                new_status = "inactive" if course.get("status") == "active" else "active"
                db.collection(COURSES_COLLECTION).document(course_id).update({"status": new_status})
                query.edit_message_text(
                    f"✅ تم تحديث حالة الدورة إلى: {'مفعلة' if new_status == 'active' else 'معطلة'}",
                    reply_markup=COURSES_ADMIN_MENU_KB
                )
            else:
                query.edit_message_text("❌ الدورة غير موجودة.", reply_markup=COURSES_ADMIN_MENU_KB)
        
        elif data.startswith("COURSES:confirm_delete_"):
            course_id = data.replace("COURSES:confirm_delete_", "")
            # حذف الدورة
            db.collection(COURSES_COLLECTION).document(course_id).delete()
            query.edit_message_text(
                "✅ تم حذف الدورة بنجاح",
                reply_markup=COURSES_ADMIN_MENU_KB
            )
    
    except Exception as e:
        logger.error(f"خطأ في معالجة callback الدورات: {e}")
        query.edit_message_text("❌ حدث خطأ. حاول مرة أخرى.")

# =================== نهاية قسم الدورات ===================


if __name__ == "__main__":
    main()
