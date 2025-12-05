import os
import json
import logging
import re
import random
from datetime import datetime, timezone
from threading import Thread
from typing import List, Dict, Any, Optional

from flask import Flask   # ⬅️ أضيفي هذا السطر

app = Flask(__name__)    # ⬅️ وهذا السطر بعده مباشرة

import pytz
from flask import Flask
from telegram import (
    Update,
    User,
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

# =================== إضافة مكتبة Firebase ===================
import firebase_admin
from firebase_admin import credentials, firestore

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

# =================== تهيئة Firebase ===================

def initialize_firebase():
    """تهيئة اتصال Firebase من ملف الخدمة في Render"""
    try:
        # البحث عن ملف خدمة Firebase في المسار المحدد
        secrets_path = "/etc/secrets"
        firebase_files = []
        
        if os.path.exists(secrets_path):
            for file in os.listdir(secrets_path):
                if file.startswith("soqya-") and file.endswith(".json"):
                    firebase_files.append(os.path.join(secrets_path, file))
        
        if firebase_files:
            # استخدام أول ملف يطابق النمط
            cred_path = firebase_files[0]
            logger.info(f"تم العثور على ملف Firebase: {cred_path}")
            
            # التحقق من أن التطبيق لم يتم تهيئته مسبقاً
            if not firebase_admin._apps:
                cred = credentials.Certificate(cred_path)
                firebase_admin.initialize_app(cred)
                logger.info("✅ تم تهيئة Firebase بنجاح")
            else:
                logger.info("✅ Firebase مفعل بالفعل")
        else:
            logger.warning("❌ لم يتم العثور على ملف Firebase. سيتم استخدام التخزين المحلي")
            
    except Exception as e:
        logger.error(f"❌ خطأ في تهيئة Firebase: {e}")
        logger.warning("سيتم استخدام التخزين المحلي كبديل")

# استدعاء التهيئة
initialize_firebase()

# إنشاء عميل Firestore
try:
    db = firestore.client()
    logger.info("✅ تم الاتصال بـ Firestore بنجاح")
except Exception as e:
    logger.error(f"❌ خطأ في الاتصال بـ Firestore: {e}")
    db = None

# =================== دوال Firebase المساعدة ===================

def firestore_available():
    """التحقق مما إذا كان Firestore متاحاً"""
    return db is not None

# المجموعات (Collections) في Firestore
USERS_COLLECTION = "users"
WATER_LOGS_COLLECTION = "water_logs"
TIPS_COLLECTION = "tips"
NOTES_COLLECTION = "notes"
LETTERS_COLLECTION = "letters"
GLOBAL_CONFIG_COLLECTION = "global_config"

# =================== دوال القراءة والكتابة لـ Firestore ===================

def get_user_record(user: User) -> Dict[str, Any]:
    """
    الحصول على سجل المستخدم من Firestore أو إنشاءه إذا لم يكن موجوداً
    """
    user_id = str(user.id)
    
    if firestore_available():
        try:
            doc_ref = db.collection(USERS_COLLECTION).document(user_id)
            doc = doc_ref.get()
            
            now_iso = datetime.now(timezone.utc).isoformat()
            
            if doc.exists:
                # تحديث بيانات المستخدم الحالية
                data = doc.to_dict()
                data["first_name"] = user.first_name
                data["username"] = user.username
                data["last_active"] = now_iso
                
                # ضمان وجود جميع الحقول
                default_fields = {
                    "user_id": user.id,
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
                    if field not in data:
                        data[field] = default_value
                
                # تحديث الميداليات القديمة
                if "medals" in data and data["medals"]:
                    medals = data["medals"]
                    new_medals = []
                    for m in medals:
                        if m == "ميدالية الاستمرار 💫":
                            new_medals.append("ميدالية الاستمرار 🎓")
                        elif m == "ميدالية بطل سُقيا الكوثر 👑":
                            new_medals.append("ميدالية بطل سُقيا الكوثر 🏆")
                        else:
                            new_medals.append(m)
                    data["medals"] = new_medals
                
                doc_ref.set(data)
                return data
            else:
                # إنشاء مستخدم جديد
                new_user = {
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
                doc_ref.set(new_user)
                return new_user
                
        except Exception as e:
            logger.error(f"خطأ في get_user_record من Firestore: {e}")
    
    # إذا كان Firestore غير متوفر، استخدام التخزين المحلي
    return get_user_record_local(user)

def update_user_record(user_id: int, **kwargs):
    """تحديث سجل المستخدم في Firestore"""
    uid = str(user_id)
    
    if firestore_available():
        try:
            doc_ref = db.collection(USERS_COLLECTION).document(uid)
            kwargs["last_active"] = datetime.now(timezone.utc).isoformat()
            doc_ref.update(kwargs)
        except Exception as e:
            logger.error(f"خطأ في update_user_record من Firestore: {e}")
    else:
        # استخدام التخزين المحلي
        update_user_record_local(user_id, **kwargs)

def get_all_user_ids() -> List[int]:
    """الحصول على جميع معرفات المستخدمين"""
    if firestore_available():
        try:
            users_ref = db.collection(USERS_COLLECTION)
            docs = users_ref.stream()
            return [int(doc.id) for doc in docs if doc.id != GLOBAL_CONFIG_COLLECTION]
        except Exception as e:
            logger.error(f"خطأ في get_all_user_ids من Firestore: {e}")
            return []
    else:
        return get_all_user_ids_local()

def get_active_user_ids() -> List[int]:
    """الحصول على معرفات المستخدمين النشطين (غير المحظورين)"""
    if firestore_available():
        try:
            users_ref = db.collection(USERS_COLLECTION)
            query = users_ref.where("is_banned", "==", False)
            docs = query.stream()
            return [int(doc.id) for doc in docs]
        except Exception as e:
            logger.error(f"خطأ في get_active_user_ids من Firestore: {e}")
            return []
    else:
        return get_active_user_ids_local()

def get_banned_user_ids() -> List[int]:
    """الحصول على معرفات المستخدمين المحظورين"""
    if firestore_available():
        try:
            users_ref = db.collection(USERS_COLLECTION)
            query = users_ref.where("is_banned", "==", True)
            docs = query.stream()
            return [int(doc.id) for doc in docs]
        except Exception as e:
            logger.error(f"خطأ في get_banned_user_ids من Firestore: {e}")
            return []
    else:
        return get_banned_user_ids_local()

def get_users_sorted_by_points() -> List[Dict]:
    """الحصول على المستخدمين مرتبين حسب النقاط"""
    if firestore_available():
        try:
            users_ref = db.collection(USERS_COLLECTION)
            query = users_ref.order_by("points", direction=firestore.Query.DESCENDING)
            docs = query.stream()
            return [doc.to_dict() for doc in docs]
        except Exception as e:
            logger.error(f"خطأ في get_users_sorted_by_points من Firestore: {e}")
            return []
    else:
        return get_users_sorted_by_points_local()

def save_water_log(user_id: int, cups: int, date: str = None):
    """حفظ سجل شرب الماء"""
    if firestore_available():
        try:
            if date is None:
                date = datetime.now(timezone.utc).date().isoformat()
            
            log_data = {
                "user_id": user_id,
                "cups": cups,
                "date": date,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            db.collection(WATER_LOGS_COLLECTION).add(log_data)
        except Exception as e:
            logger.error(f"خطأ في save_water_log من Firestore: {e}")

def get_today_water_logs(user_id: int) -> List[Dict]:
    """الحصول على سجلات الماء لليوم"""
    if firestore_available():
        try:
            today = datetime.now(timezone.utc).date().isoformat()
            logs_ref = db.collection(WATER_LOGS_COLLECTION)
            query = logs_ref.where("user_id", "==", user_id).where("date", "==", today)
            docs = query.stream()
            return [doc.to_dict() for doc in docs]
        except Exception as e:
            logger.error(f"خطأ في get_today_water_logs من Firestore: {e}")
            return []
    return []

# =================== إدارة الفوائد والنصائح ===================

def get_benefits() -> List[Dict]:
    """الحصول على جميع الفوائد والنصائح"""
    if firestore_available():
        try:
            tips_ref = db.collection(TIPS_COLLECTION)
            docs = tips_ref.order_by("date", direction=firestore.Query.DESCENDING).stream()
            return [doc.to_dict() for doc in docs]
        except Exception as e:
            logger.error(f"خطأ في get_benefits من Firestore: {e}")
            return []
    else:
        return get_benefits_local()

def save_benefit(benefit_data: Dict) -> str:
    """حفظ فائدة جديدة"""
    if firestore_available():
        try:
            # إضافة المعرف تلقائياً
            if "id" not in benefit_data:
                benefit_data["id"] = get_next_benefit_id()
            
            if "date" not in benefit_data:
                benefit_data["date"] = datetime.now(timezone.utc).isoformat()
            
            doc_ref = db.collection(TIPS_COLLECTION).document(str(benefit_data["id"]))
            doc_ref.set(benefit_data)
            return str(benefit_data["id"])
        except Exception as e:
            logger.error(f"خطأ في save_benefit من Firestore: {e}")
            return ""
    else:
        return save_benefit_local(benefit_data)

def update_benefit(benefit_id: int, benefit_data: Dict):
    """تحديث فائدة موجودة"""
    if firestore_available():
        try:
            doc_ref = db.collection(TIPS_COLLECTION).document(str(benefit_id))
            doc_ref.update(benefit_data)
        except Exception as e:
            logger.error(f"خطأ في update_benefit من Firestore: {e}")
    else:
        update_benefit_local(benefit_id, benefit_data)

def delete_benefit(benefit_id: int):
    """حذف فائدة"""
    if firestore_available():
        try:
            doc_ref = db.collection(TIPS_COLLECTION).document(str(benefit_id))
            doc_ref.delete()
        except Exception as e:
            logger.error(f"خطأ في delete_benefit من Firestore: {e}")
    else:
        delete_benefit_local(benefit_id)

def get_next_benefit_id() -> int:
    """الحصول على المعرف التالي للفائدة"""
    if firestore_available():
        try:
            tips_ref = db.collection(TIPS_COLLECTION)
            # الحصول على آخر فائدة مرتبة حسب المعرف
            query = tips_ref.order_by("id", direction=firestore.Query.DESCENDING).limit(1)
            docs = query.stream()
            
            for doc in docs:
                data = doc.to_dict()
                return data.get("id", 0) + 1
            
            return 1
        except Exception as e:
            logger.error(f"خطأ في get_next_benefit_id من Firestore: {e}")
            return 1
    else:
        return get_next_benefit_id_local()

# =================== إدارة المذكرات ===================

def save_note(user_id: int, note_text: str) -> str:
    """حفظ مذكرة جديدة"""
    if firestore_available():
        try:
            note_data = {
                "user_id": user_id,
                "text": note_text,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
            
            doc_ref = db.collection(NOTES_COLLECTION).document()
            note_id = doc_ref.id
            note_data["id"] = note_id
            doc_ref.set(note_data)
            return note_id
        except Exception as e:
            logger.error(f"خطأ في save_note من Firestore: {e}")
            return ""
    else:
        return save_note_local(user_id, note_text)

def get_user_notes(user_id: int) -> List[Dict]:
    """الحصول على مذكرات المستخدم"""
    if firestore_available():
        try:
            notes_ref = db.collection(NOTES_COLLECTION)
            query = notes_ref.where("user_id", "==", user_id).order_by("created_at", direction=firestore.Query.DESCENDING)
            docs = query.stream()
            return [doc.to_dict() for doc in docs]
        except Exception as e:
            logger.error(f"خطأ في get_user_notes من Firestore: {e}")
            return []
    else:
        return get_user_notes_local(user_id)

def update_note(note_id: str, new_text: str):
    """تحديث مذكرة"""
    if firestore_available():
        try:
            doc_ref = db.collection(NOTES_COLLECTION).document(note_id)
            doc_ref.update({
                "text": new_text,
                "updated_at": datetime.now(timezone.utc).isoformat()
            })
        except Exception as e:
            logger.error(f"خطأ في update_note من Firestore: {e}")
    else:
        update_note_local(note_id, new_text)

def delete_note(note_id: str):
    """حذف مذكرة"""
    if firestore_available():
        try:
            doc_ref = db.collection(NOTES_COLLECTION).document(note_id)
            doc_ref.delete()
        except Exception as e:
            logger.error(f"خطأ في delete_note من Firestore: {e}")
    else:
        delete_note_local(note_id)

# =================== إدارة الرسائل للنفس ===================

def save_letter(user_id: int, letter_data: Dict) -> str:
    """حفظ رسالة جديدة للنفس"""
    if firestore_available():
        try:
            letter_data["user_id"] = user_id
            
            if "created_at" not in letter_data:
                letter_data["created_at"] = datetime.now(timezone.utc).isoformat()
            
            if "sent" not in letter_data:
                letter_data["sent"] = False
            
            doc_ref = db.collection(LETTERS_COLLECTION).document()
            letter_id = doc_ref.id
            letter_data["id"] = letter_id
            doc_ref.set(letter_data)
            return letter_id
        except Exception as e:
            logger.error(f"خطأ في save_letter من Firestore: {e}")
            return ""
    else:
        return save_letter_local(user_id, letter_data)

def get_user_letters(user_id: int) -> List[Dict]:
    """الحصول على رسائل المستخدم للنفس"""
    if firestore_available():
        try:
            letters_ref = db.collection(LETTERS_COLLECTION)
            query = letters_ref.where("user_id", "==", user_id).order_by("created_at", direction=firestore.Query.DESCENDING)
            docs = query.stream()
            return [doc.to_dict() for doc in docs]
        except Exception as e:
            logger.error(f"خطأ في get_user_letters من Firestore: {e}")
            return []
    else:
        return get_user_letters_local(user_id)

def update_letter(letter_id: str, letter_data: Dict):
    """تحديث رسالة"""
    if firestore_available():
        try:
            doc_ref = db.collection(LETTERS_COLLECTION).document(letter_id)
            doc_ref.update(letter_data)
        except Exception as e:
            logger.error(f"خطأ في update_letter من Firestore: {e}")
    else:
        update_letter_local(letter_id, letter_data)

def delete_letter(letter_id: str):
    """حذف رسالة"""
    if firestore_available():
        try:
            doc_ref = db.collection(LETTERS_COLLECTION).document(letter_id)
            doc_ref.delete()
        except Exception as e:
            logger.error(f"خطأ في delete_letter من Firestore: {e}")
    else:
        delete_letter_local(letter_id)

# =================== الإعدادات العامة ===================

def get_global_config() -> Dict:
    """الحصول على الإعدادات العامة"""
    if firestore_available():
        try:
            doc_ref = db.collection(GLOBAL_CONFIG_COLLECTION).document("config")
            doc = doc_ref.get()
            
            if doc.exists:
                return doc.to_dict()
            else:
                # إنشاء الإعدادات الافتراضية
                default_config = {
                    "motivation_hours": [6, 9, 12, 15, 18, 21],
                    "motivation_messages": [
                        "🍃 تذكّر: قليلٌ دائم خيرٌ من كثير منقطع، خطوة اليوم تقرّبك من نسختك الأفضل 🤍",
                        "💧 جرعة ماء + آية من القرآن + ذكر بسيط = راحة قلب يوم كامل بإذن الله.",
                        "🤍 مهما كان يومك مزدحمًا، قلبك يستحق لحظات هدوء مع ذكر الله.",
                        "📖 لو شعرت بثقل، افتح المصحف صفحة واحدة فقط… ستشعر أن همّك خفّ ولو قليلًا.",
                        "💫 لا تستصغر كوب ماء تشربه بنية حفظ الصحة، ولا صفحة قرآن تقرؤها بنية القرب من الله.",
                        "🕊 قل: الحمد لله الآن… أحيانًا شكرٌ صادق يغيّر مزاج يومك كله.",
                        "🌿 استعن بالله ولا تعجز، كل محاولة للالتزام خير، حتى لو تعثّرت بعدها.",
                    ],
                    "benefits": []
                }
                doc_ref.set(default_config)
                return default_config
                
        except Exception as e:
            logger.error(f"خطأ في get_global_config من Firestore: {e}")
            return get_global_config_local()
    else:
        return get_global_config_local()

def update_global_config(config_data: Dict):
    """تحديث الإعدادات العامة"""
    if firestore_available():
        try:
            doc_ref = db.collection(GLOBAL_CONFIG_COLLECTION).document("config")
            doc_ref.update(config_data)
        except Exception as e:
            logger.error(f"خطأ في update_global_config من Firestore: {e}")
    else:
        update_global_config_local(config_data)

# =================== دوال التخزين المحلي (للبديل) ===================

# هذه الدوال تستخدم فقط إذا كان Firestore غير متوفر
data = {}

def load_data_local():
    """تحميل البيانات من الملف المحلي"""
    global data
    if not os.path.exists(DATA_FILE):
        data = {}
        return data
    
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Error loading local data: {e}")
        data = {}
    
    return data

def save_data_local():
    """حفظ البيانات للملف المحلي"""
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Error saving local data: {e}")

# تحميل البيانات المحلية
if not firestore_available():
    data = load_data_local()

# تعريف الدوال المحلية
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

def delete_benefit_local(benefit_id: int):
    """نسخة محلية من delete_benefit"""
    config = get_global_config_local()
    benefits = config.get("benefits", [])
    
    config["benefits"] = [b for b in benefits if b.get("id") != benefit_id]
    update_global_config_local(config)

def get_next_benefit_id_local() -> int:
    """نسخة محلية من get_next_benefit_id"""
    config = get_global_config_local()
    benefits = config.get("benefits", [])
    
    if not benefits:
        return 1
    
    max_id = max(b.get("id", 0) for b in benefits)
    return max_id + 1

# دالة المساعدة للمذكرات (محلية)
def save_note_local(user_id: int, note_text: str) -> str:
    """نسخة محلية من save_note"""
    record = get_user_record_local_by_id(user_id)
    memos = record.get("heart_memos", [])
    memos.append(note_text)
    update_user_record_local(user_id, heart_memos=memos)
    return f"note_{len(memos)-1}"

def get_user_notes_local(user_id: int) -> List[Dict]:
    """نسخة محلية من get_user_notes"""
    record = get_user_record_local_by_id(user_id)
    memos = record.get("heart_memos", [])
    return [{"id": f"note_{i}", "text": memo, "user_id": user_id} for i, memo in enumerate(memos)]

def update_note_local(note_id: str, new_text: str):
    """نسخة محلية من update_note"""
    try:
        idx = int(note_id.split("_")[1])
        user_id = int(note_id.split("_")[0])
        record = get_user_record_local_by_id(user_id)
        memos = record.get("heart_memos", [])
        
        if 0 <= idx < len(memos):
            memos[idx] = new_text
            update_user_record_local(user_id, heart_memos=memos)
    except:
        pass

def delete_note_local(note_id: str):
    """نسخة محلية من delete_note"""
    try:
        idx = int(note_id.split("_")[1])
        user_id = int(note_id.split("_")[0])
        record = get_user_record_local_by_id(user_id)
        memos = record.get("heart_memos", [])
        
        if 0 <= idx < len(memos):
            memos.pop(idx)
            update_user_record_local(user_id, heart_memes=memos)
    except:
        pass

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

def delete_letter_local(letter_id: str):
    """نسخة محلية من delete_letter"""
    try:
        idx = int(letter_id.split("_")[1])
        user_id = int(letter_id.split("_")[0])
        record = get_user_record_local_by_id(user_id)
        letters = record.get("letters_to_self", [])
        
        if 0 <= idx < len(letters):
            letters.pop(idx)
            update_user_record_local(user_id, letters_to_self=letters)
    except:
        pass

# دالة المساعدة للإعدادات العامة (محلية)
def get_global_config_local() -> Dict:
    """نسخة محلية من get_global_config"""
    if "GLOBAL_KEY" not in data:
        data["GLOBAL_KEY"] = {
            "motivation_hours": [6, 9, 12, 15, 18, 21],
            "motivation_messages": [
                "🍃 تذكّر: قليلٌ دائم خيرٌ من كثير منقطع، خطوة اليوم تقرّبك من نسختك الأفضل 🤍",
                "💧 جرعة ماء + آية من القرآن + ذكر بسيط = راحة قلب يوم كامل بإذن الله.",
                "🤍 مهما كان يومك مزدحمًا، قلبك يستحق لحظات هدوء مع ذكر الله.",
                "📖 لو شعرت بثقل، افتح المصحف صفحة واحدة فقط… ستشعر أن همّك خفّ ولو قليلًا.",
                "💫 لا تستصغر كوب ماء تشربه بنية حفظ الصحة، ولا صفحة قرآن تقرؤها بنية القرب من الله.",
                "🕊 قل: الحمد لله الآن… أحيانًا شكرٌ صادق يغيّر مزاج يومك كله.",
                "🌿 استعن بالله ولا تعجز، كل محاولة للالتزام خير، حتى لو تعثّرت بعدها.",
            ],
            "benefits": []
        }
        save_data_local()
    
    return data["GLOBAL_KEY"]

def update_global_config_local(config_data: Dict):
    """نسخة محلية من update_global_config"""
    data["GLOBAL_KEY"] = config_data
    save_data_local()

# =================== استبدال الاستدعاءات في الكود ===================

# تحديث متغيرات الإعدادات العامة لاستخدام Firestore
_global_cfg = get_global_config()
MOTIVATION_HOURS_UTC = _global_cfg.get("motivation_hours", [6, 9, 12, 15, 18, 21])
MOTIVATION_MESSAGES = _global_cfg.get("motivation_messages", [
    "🍃 تذكّر: قليلٌ دائم خيرٌ من كثير منقطع، خطوة اليوم تقرّبك من نسختك الأفضل 🤍",
    "💧 جرعة ماء + آية من القرآن + ذكر بسيط = راحة قلب يوم كامل بإذن الله.",
    "🤍 مهما كان يومك مزدحمًا، قلبك يستحق لحظات هدوء مع ذكر الله.",
    "📖 لو شعرت بثقل، افتح المصحف صفحة واحدة فقط… ستشعر أن همّك خفّ ولو قليلًا.",
    "💫 لا تستصغر كوب ماء تشربه بنية حفظ الصحة، ولا صفحة قرآن تقرؤها بنية القرب من الله.",
    "🕊 قل: الحمد لله الآن… أحيانًا شكرٌ صادق يغيّر مزاج يومك كله.",
    "🌿 استعن بالله ولا تعجز، كل محاولة للالتزام خير، حتى لو تعثّرت بعدها.",
])

# تحديث الدوال التي تستخدم الإعدادات العامة
def save_benefits(benefits_list):
    """يحفظ قائمة الفوائد المحدثة في الإعدادات العامة"""
    cfg = get_global_config()
    cfg["benefits"] = benefits_list
    update_global_config(cfg)

# =================== تحديث دوال إدارة المذكرات ===================

def handle_memo_add_input(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    text = (update.message.text or "").strip()

    if text == BTN_CANCEL:
        WAITING_MEMO_ADD.discard(user_id)
        open_memos_menu(update, context)
        return

    # حفظ المذكرة في Firestore
    note_id = save_note(user_id, text)
    
    if note_id:
        WAITING_MEMO_ADD.discard(user_id)
        update.message.reply_text(
            "تم حفظ مذكّرتك في قلب البوت 🤍.",
            reply_markup=build_memos_menu_kb(is_admin(user_id)),
        )
        open_memos_menu(update, context)
    else:
        update.message.reply_text(
            "حدث خطأ في حفظ المذكرة. يرجى المحاولة مرة أخرى.",
            reply_markup=build_memos_menu_kb(is_admin(user_id)),
        )

def format_memos_list(memos):
    """تنسيق قائمة المذكرات"""
    if not memos:
        return "لا توجد مذكّرات بعد."
    
    formatted = []
    for idx, memo in enumerate(memos, start=1):
        if isinstance(memo, dict):
            text = memo.get("text", "")
        else:
            text = memo
        
        if len(text) > 50:
            text = text[:50] + "..."
        
        formatted.append(f"{idx}. {text}")
    
    return "\n\n".join(formatted)

def get_user_memos(user_id: int) -> List:
    """الحصول على مذكرات المستخدم"""
    if firestore_available():
        notes = get_user_notes(user_id)
        return [note.get("text", "") for note in notes]
    else:
        record = get_user_record_local_by_id(user_id)
        return record.get("heart_memos", [])

def open_memos_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    
    # الحصول على المذكرات من Firestore
    memos = get_user_memos(user_id)
    
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

def handle_memo_edit_index_input(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    text = (update.message.text or "").strip()

    if text == BTN_CANCEL:
        WAITING_MEMO_EDIT_SELECT.discard(user_id)
        open_memos_menu(update, context)
        return

    try:
        idx = int(text) - 1
        memos = get_user_memos(user_id)
        
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
    text = (update.message.text or "").strip()

    if text == BTN_CANCEL:
        WAITING_MEMO_EDIT_TEXT.discard(user_id)
        MEMO_EDIT_INDEX.pop(user_id, None)
        open_memos_menu(update, context)
        return

    idx = MEMO_EDIT_INDEX.get(user_id)
    if idx is None:
        WAITING_MEMO_EDIT_TEXT.discard(user_id)
        update.message.reply_text(
            "حدث خطأ بسيط في اختيار المذكرة، جرّب من جديد من «مذكّرات قلبي 🩵».",
            reply_markup=user_main_keyboard(user_id),
        )
        return

    # تحديث المذكرة في Firestore
    if firestore_available():
        notes = get_user_notes(user_id)
        if 0 <= idx < len(notes):
            note_id = notes[idx].get("id")
            if note_id:
                update_note(note_id, text)
    else:
        memos = get_user_memos(user_id)
        if 0 <= idx < len(memos):
            memos[idx] = text
            record = get_user_record_local_by_id(user_id)
            update_user_record_local(user_id, heart_memos=memos)

    WAITING_MEMO_EDIT_TEXT.discard(user_id)
    MEMO_EDIT_INDEX.pop(user_id, None)

    update.message.reply_text(
        "تم تعديل المذكرة بنجاح ✅.",
        reply_markup=build_memos_menu_kb(is_admin(user_id)),
    )
    open_memos_menu(update, context)

def handle_memo_delete_index_input(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    text = (update.message.text or "").strip()

    if text == BTN_CANCEL:
        WAITING_MEMO_DELETE_SELECT.discard(user_id)
        open_memos_menu(update, context)
        return

    try:
        idx = int(text) - 1
        memos = get_user_memos(user_id)
        
        if idx < 0 or idx >= len(memos):
            raise ValueError()
            
    except ValueError:
        update.message.reply_text(
            "رجاءً أرسل رقم صحيح من القائمة الموجودة أمامك، أو اضغط «إلغاء ❌».",
            reply_markup=CANCEL_KB,
        )
        return

    # حذف المذكرة من Firestore
    if firestore_available():
        notes = get_user_notes(user_id)
        if 0 <= idx < len(notes):
            note_id = notes[idx].get("id")
            if note_id:
                delete_note(note_id)
    else:
        memos = get_user_memos(user_id)
        if 0 <= idx < len(memos):
            deleted = memos.pop(idx)
            update_user_record_local(user_id, heart_memos=memos)

    WAITING_MEMO_DELETE_SELECT.discard(user_id)

    update.message.reply_text(
        "🗑 تم حذف المذكرة بنجاح.",
        reply_markup=build_memos_menu_kb(is_admin(user_id)),
    )
    open_memos_menu(update, context)

# =================== تحديث دوال إدارة الرسائل ===================

def format_letters_list(letters: List[Dict]) -> str:
    """تنسيق قائمة الرسائل"""
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

def handle_letter_add_content(update: Update, context: CallbackContext):
    user = update.effective_user
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

    # حفظ الرسالة في Firestore
    new_letter = {
        "content": LETTER_CURRENT_DATA[user_id]["content"],
        "created_at": now.isoformat(),
        "reminder_date": reminder_date.isoformat() if reminder_date else None,
        "sent": False
    }
    
    letter_id = save_letter(user_id, new_letter)
    
    if letter_id:
        # إضافة نقاط
        add_points(user_id, POINTS_PER_LETTER, context, "كتابة رسالة إلى النفس")

        # جدولة التذكير إذا كان هناك تاريخ
        if reminder_date and context.job_queue:
            try:
                context.job_queue.run_once(
                    send_letter_reminder,
                    when=reminder_date,
                    context={
                        "user_id": user_id,
                        "letter_content": new_letter["content"],
                        "letter_id": letter_id
                    },
                    name=f"letter_reminder_{user_id}_{letter_id}"
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
    else:
        update.message.reply_text(
            "حدث خطأ في حفظ الرسالة. يرجى المحاولة مرة أخرى.",
            reply_markup=build_letters_menu_kb(is_admin(user_id)),
        )

def open_letters_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    
    # الحصول على الرسائل من Firestore
    letters = get_user_letters(user_id)

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

def handle_letter_view(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    
    letters = get_user_letters(user_id)

    if not letters:
        update.message.reply_text(
            "لا توجد رسائل بعد.\n"
            "يمكنك كتابة رسالة جديدة من زر «✍️ كتابة رسالة جديدة».",
            reply_markup=build_letters_menu_kb(is_admin(user_id)),
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
        reply_markup=build_letters_menu_kb(is_admin(user_id)),
    )

def handle_letter_delete_input(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    text = (update.message.text or "").strip()

    if text == BTN_CANCEL:
        WAITING_LETTER_DELETE_SELECT.discard(user_id)
        open_letters_menu(update, context)
        return

    try:
        idx = int(text) - 1
        letters = get_user_letters(user_id)
        
        if idx < 0 or idx >= len(letters):
            raise ValueError()
            
    except ValueError:
        update.message.reply_text(
            "رجاءً أرسل رقم صحيح من القائمة، أو اضغط «إلغاء ❌».",
            reply_markup=CANCEL_KB,
        )
        return

    # حذف الرسالة من Firestore
    letter_id = letters[idx].get("id")
    if letter_id:
        delete_letter(letter_id)

    WAITING_LETTER_DELETE_SELECT.discard(user_id)

    content_preview = letters[idx].get("content", "")[:50]
    update.message.reply_text(
        f"🗑 تم حذف الرسالة:\n\n{content_preview}...",
        reply_markup=build_letters_menu_kb(is_admin(user_id)),
    )
    open_letters_menu(update, context)

# =================== تحديث دوال إدارة الفوائد ===================

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

    WAITING_BENEFIT_TEXT.discard(user_id)

    # 1. تخزين الفائدة في Firestore
    benefit_id = get_next_benefit_id()
    now_iso = datetime.now(timezone.utc).isoformat()
    
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

    save_benefit(new_benefit)

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
    user_id = user.id
    
    benefits = get_benefits()
    
    if not benefits:
        update.message.reply_text(
            "لا توجد فوائد أو نصائح مضافة حتى الآن. كن أول من يشارك! 💡",
            reply_markup=BENEFITS_MENU_KB,
        )
        return

    # عرض آخر 5 فوائد
    latest_benefits = benefits[:5]  # get_benefits ترجع بالفعل مصنفة حسب التاريخ
    
    is_privileged = is_admin(user_id) or is_supervisor(user_id)
    
    update.message.reply_text(
        "📖 آخر 5 فوائد ونصائح مضافة:",
        reply_markup=BENEFITS_MENU_KB,
    )
    
    for benefit in latest_benefits:
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
        
        like_button_text = f"👍 أعجبني ({benefit['likes_count']})"
        
        if user_id in benefit.get("liked_by", []):
            like_button_text = f"✅ أعجبتني ({benefit['likes_count']})"
        
        keyboard_row = [
            InlineKeyboardButton(
                like_button_text, 
                callback_data=f"like_benefit_{benefit['id']}"
            )
        ]
        
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
        
    update.message.reply_text(
        "انتهى عرض آخر الفوائد.",
        reply_markup=BENEFITS_MENU_KB,
    )

def handle_my_benefits(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    
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
        
    if benefit.get("user_id") != user_id:
        query.answer("لا تملك صلاحية تعديل هذه الفائدة.")
        return

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

def handle_edit_benefit_text(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    
    if user_id not in WAITING_BENEFIT_EDIT_TEXT:
        return

    text = update.message.text.strip()
    
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
    
    if benefit_id:
        benefits = get_benefits()
        benefit = next((b for b in benefits if b.get("id") == benefit_id and b.get("user_id") == user_id), None)
        
        if benefit:
            benefit["text"] = text
            update_benefit(benefit_id, {"text": text})
            
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
def water_reminder_job(context: CallbackContext):
    """
    وظيفة مجدولة لتذكير المستخدمين بشرب الماء.
    """
    try:
        # نستخدم نفس data المستعملة في باقي الكود
        global data

        for uid_str, record in data.items():
            # تخطي المحظورين
            if record.get("is_banned", False):
                continue

            # إذا حاب لاحقاً تضيف خيار تعطيل تذكير الماء، خله هنا
            # مثلاً: if not record.get("water_reminders_enabled", True): continue

            try:
                chat_id = int(uid_str)
            except (TypeError, ValueError):
                continue

            # إرسال رسالة التذكير 💧
            context.bot.send_message(
                chat_id=chat_id,
                text="🚰 تذكير لطيف: اشرب قليلاً من الماء الآن 🌿",
            )

    except Exception as e:
        logger.error(f"خطأ في مهمة تذكير الماء: {e}")    

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
                        text=(
                            "تهانينا! 🎉\n"
                            f"لقد حصلت على وسام جديد: *{MEDAL_TEXT}*\n"
                            "أحد فوائدك وصل إلى قائمة أفضل 10 فوائد. استمر في المشاركة! 🤍"
                        ),
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
    
    is_privileged = is_admin(user_id) or is_supervisor(user_id)
    
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
    
    if not is_admin_delete and not is_owner:
        query.answer("لا تملك صلاحية حذف هذه الفائدة.")
        query.edit_message_text(
            text="⚠️ حدث خطأ: لا تملك صلاحية حذف هذه الفائدة.",
            reply_markup=None,
        )
        WAITING_BENEFIT_DELETE_CONFIRM.discard(user_id)
        BENEFIT_EDIT_ID.pop(user_id, None)
        return
        
    if is_admin_delete and not is_privileged:
        query.answer("لا تملك صلاحية حذف فوائد الآخرين.")
        query.edit_message_text(
            text="⚠️ حدث خطأ: لا تملك صلاحية حذف فوائد الآخرين.",
            reply_markup=None,
        )
        WAITING_BENEFIT_DELETE_CONFIRM.discard(user_id)
        BENEFIT_EDIT_ID.pop(user_id, None)
        return

    delete_benefit(benefit_id)
    
    query.answer("✅ تم حذف الفائدة بنجاح.")
    query.edit_message_text(
        text=f"✅ تم حذف الفائدة رقم {benefit_id} بنجاح.",
        reply_markup=None,
    )
    
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
        benefit = next((b for b in benefits if b.get("id") == benefit_id), None)
        
        if benefit is None:
            query.answer("هذه الفائدة لم تعد موجودة.")
            return

        liked_by = benefit.get("liked_by", [])
        
        if user_id in liked_by:
            query.answer("لقد أعجبت بهذه الفائدة مسبقًا.")
            return
            
        if user_id == benefit["user_id"]:
            query.answer("لا يمكنك الإعجاب بفائدتك الخاصة.")
            return
        
        # إضافة الإعجاب
        liked_by.append(user_id)
        benefit["likes_count"] = benefit.get("likes_count", 0) + 1
        benefit["liked_by"] = liked_by
        
        # منح نقطة لصاحب الفائدة
        owner_id = benefit["user_id"]
        add_points(owner_id, 1)
        
        # حفظ التغييرات
        update_benefit(benefit_id, {
            "likes_count": benefit["likes_count"],
            "liked_by": liked_by
        })
        
        # تحديث زر الإعجاب
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
        
        check_and_award_medal(context)

# =================== خادم ويب بسيط لـ Render ===================

app = Flask(__name__)

@app.route("/")
def index():
    return "Suqya Al-Kawther bot is running ✅"

def run_flask():
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)

# =================== بقية الكود بدون تغيير ===================

# [أدخل هنا بقية الكود كما هو بدون تغيير من السطر 111 إلى نهاية الملف]
# بما في ذلك جميع تعريفات المتغيرات، الأزرار، الدوال، والأوامر
# يجب أن تبقى كما هي تماماً لأننا غيرنا فقط دوال التخزين

# =================== سكربت ترحيل البيانات ===================

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
        if user_id_str == "GLOBAL_KEY":
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
            "motivation_hours": global_config.get("motivation_hours", [6, 9, 12, 15, 18, 21]),
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

def handle_text(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    msg = update.message
    text = (msg.text or "").strip()

    record = get_user_record(user)

    # منع المحظور من استخدام البوت
    if record.get("is_banned", False):
        return

    # الوضع الافتراضي: القائمة الرئيسية
    main_kb = user_main_keyboard(user_id)

    # التعامل مع كل حالات الانتظار
    if user_id in WAITING_QURAN_GOAL:
        return handle_quran_goal_input(update, context)

    if user_id in WAITING_QURAN_ADD_PAGES:
        return handle_quran_add_pages_input(update, context)

    if user_id in WAITING_TASBIH:
        update.message.reply_text("استخدم زر «تسبيحة ✅» في الأسفل.")
        return

    if user_id in WAITING_MEMO_ADD:
        return handle_memo_add_input(update, context)

    if user_id in WAITING_MEMO_EDIT_SELECT:
        return handle_memo_edit_index_input(update, context)

    if user_id in WAITING_MEMO_EDIT_TEXT:
        return handle_memo_edit_text_input(update, context)

    if user_id in WAITING_MEMO_DELETE_SELECT:
        return handle_memo_delete_index_input(update, context)

    if user_id in WAITING_SUPPORT:
        forward_support_to_admin(user, text, context)
        WAITING_SUPPORT.discard(user_id)
        update.message.reply_text("📨 تم إرسال رسالتك للدعم.")
        return

    if user_id in WAITING_BENEFIT_ADD_TEXT:
        return handle_add_benefit_text(update, context)

    if user_id in WAITING_BENEFIT_EDIT_TEXT:
        return handle_edit_benefit_text(update, context)

    if user_id in WAITING_BENEFIT_DELETE_CONFIRM:
        return handle_delete_benefit_callback(update, context)

    # آخر خيار: رد قياسي
    update.message.reply_text(
        "👇 اختر من القائمة الرئيسية:",
        reply_markup=main_kb,
    )

# =================== تشغيل البوت ===================

def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN غير موجود في متغيرات البيئة!")

    # تشغيل ترحيل البيانات مرة واحدة عند البدء
    if firestore_available():
        migrate_data_to_firestore()
    
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
        # أوقات تذكير الماء بتوقيت UTC
    REMINDER_HOURS_UTC = [7, 10, 13, 16, 19]

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

from telegram import ReplyKeyboardMarkup  # تأكدي إنها موجودة فوق في الاستيراد مرة وحدة فقط

def user_main_keyboard(user_id: int):
    """
    كيبورد القائمة الرئيسية للمستخدم
    """
    keyboard = [
        ["✋ أذكاري", "📖 وردي القرآني"],
        ["💧 منبه الماء", "🌙 السبحة"],
        ["💙 مذكّرات قلبي", "📩 رسالة إلى نفسي"],
        ["📊 إحصائياتي", "🏅 المنافسات و المجتمع"],
        ["💡 مجتمع الفوائد و النصائح"],
        ["🔔 الاشعارات", "✉️ تواصل مع الدعم"],
        ["⚙️ لوحة التحكم"],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def start_command(update: Update, context: CallbackContext):
    user = update.effective_user
    get_user_record(user)

    update.message.reply_text(
        "مرحبًا بك في بوت سُقيا الكوثر 🤍\n"
        "أنا هنا لأرافقك في رحلة الإهتمام بالماء والقرآن والقلب.\n"
        "اختر ما يناسبك من القائمة 👇",
        reply_markup=user_main_keyboard(user.id),
    )


def help_command(update: Update, context: CallbackContext):
    user = update.effective_user
    update.message.reply_text(
        "💡 مساعدة البوت:\n"
        "• راقب استهلاك الماء\n"
        "• سجل ورد القرآن\n"
        "• استخدم السبحة\n"
        "• اكتب مذكراتك\n"
        "• شارك نصائحك\n\n"
        "ابدأ الآن من القائمة الرئيسية 👇",
        reply_markup=user_main_keyboard(user.id),
    )
def main():
    if not BOT_TOKEN:
        raise RuntimeError("❌ BOT_TOKEN غير مهيأ في المتغيرات البيئية")

    logger.info("🚀 البوت بدأ العمل!")

    from telegram.ext import Updater
    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    # نحذف أي Webhook قديم قبل ما نبدأ polling
    try:
        updater.bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        logger.error(f"⚠️ خطأ أثناء حذف الويب هوك: {e}")

def main():
    if not BOT_TOKEN:
        raise RuntimeError("❌ BOT_TOKEN غير مضبوط!")

    from telegram.ext import Updater
    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    logger.info("🚀 البوت بدأ العمل!")

    # نحذف الويب هوك القديم إن وُجد
    try:
        updater.bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        logger.error(f"⚠️ خطأ أثناء حذف الويب هوك: {e}")

    # نبدأ استقبال الرسائل
    updater.start_polling()
    updater.idle()


if __name__ == "__main__":
    from threading import Thread

    bot_thread = Thread(target=main)
    bot_thread.start()

    # تشغيل Flask حتى يبقى السيرفر على Render شغال
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
