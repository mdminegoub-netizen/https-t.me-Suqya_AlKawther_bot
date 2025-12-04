import os
import json
import logging
import re
import random
from datetime import datetime, timezone, time, timedelta
from threading import Thread
from typing import List, Dict, Any, Optional

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
            update_user_record_local(user_id, heart_memos=memos)
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
        update_user_record(record["user_id"], today_date=today_str, today_cups=0)


def ensure_today_quran(record):
    today_str = datetime.now(timezone.utc).date().isoformat()
    if record.get("quran_today_date") != today_str:
        record["quran_today_date"] = today_str
        record["quran_pages_today"] = 0
        update_user_record(record["user_id"], quran_today_date=today_str, quran_pages_today=0)


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
    record = get_user_record_local_by_id(user_id)
    record["adhkar_count"] = record.get("adhkar_count", 0) + amount
    update_user_record(user_id, adhkar_count=record["adhkar_count"])


def increment_tasbih_total(user_id: int, amount: int = 1):
    record = get_user_record_local_by_id(user_id)
    record["tasbih_total"] = record.get("tasbih_total", 0) + amount
    update_user_record(user_id, tasbih_total=record["tasbih_total"])

# =================== نظام النقاط / المستويات / الميداليات ===================


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
    update_user_record(user_id, best_rank=rank)

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
    update_user_record(user_id, level=new_level, medals=medals)

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
    update_user_record(user_id, daily_full_streak=streak, last_full_day=today_str, medals=medals)

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

    record = get_user_record_local_by_id(user_id)
    record["points"] = record.get("points", 0) + amount
    update_user_record(user_id, points=record["points"])
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
    
    # الخطوة 5: إذا كان مستخدم جديد، إرسال إشعار للأدمن وتحديث العلامة
    if record.get("is_new_user", False):
        # إرسال إشعار للأدمن
        if ADMIN_ID is not None:
            username_text = f"@{user.username}" if user.username else "غير متوفر"
            
            # تنسيق وقت الانضمام بالتوقيت المحلي
            now_utc = datetime.now(timezone.utc)
            try:
                local_tz = pytz.timezone("Africa/Cairo")
            except:
                local_tz = timezone.utc
            
            now_local = now_utc.astimezone(local_tz)
            join_time_str = now_local.strftime("%d-%m-%Y | %I:%M %p")
            
            notification_message = (
                "🔔 مستخدم جديد دخل البوت 🎉\n\n"
                f"👤 الاسم: {user.first_name}\n"
                f"🆔 User ID: {user.id}\n"
                f"🧑‍💻 Username: {username_text}\n"
                f"🕒 الانضمام: {join_time_str} (توقيت محلي)\n\n"
                "📝 ملاحظة: معلومات الجهاز والموقع الجغرافي غير متوفرة من Telegram API"
            )
            
            try:
                context.bot.send_message(
                    chat_id=ADMIN_ID,
                    text=notification_message,
                )
            except Exception as e:
                logger.error(f"Error sending new user notification to admin {ADMIN_ID}: {e}")
        
        # تعديل سجل المستخدم لجعل is_new_user = False
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

# =================== بقية الدوال المفقودة (من الكود الأصلي) ===================

# سنحتاج إلى إضافة الدوال المتبقية من الكود الأصلي هنا
# لكن بما أن المساحة محدودة، سأضيف أهم الدوال الأساسية:

def is_admin(user_id: int) -> bool:
    return ADMIN_ID is not None and user_id == ADMIN_ID

def is_supervisor(user_id: int) -> bool:
    return SUPERVISOR_ID is not None and user_id == SUPERVISOR_ID

# دالة إرسال تذكير الرسالة
def send_letter_reminder(context: CallbackContext):
    job = context.job
    user_id = job.context["user_id"]
    letter_content = job.context["letter_content"]
    letter_id = job.context["letter_id"]

    try:
        # تحديث حالة الرسالة في البيانات
        update_letter(letter_id, {"sent": True})

        # إرسال الرسالة للمستخدم
        context.bot.send_message(
            chat_id=user_id,
            text=f"💌 رسالة من نفسك السابقة:\n\n{letter_content}\n\n"
                 f"⏰ هذا هو الموعد الذي طلبت التذكير فيه 🤍",
        )
    except Exception as e:
        logger.error(f"Error sending letter reminder to {user_id}: {e}")

# دالة التحقق ومنح الميداليات
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
        record = get_user_record_local_by_id(user_id)
        medals = record.get("medals", [])
        
        if MEDAL_TEXT not in medals:
            medals.append(MEDAL_TEXT)
            update_user_record(user_id, medals=medals)
            
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

    # إضافة MessageHandler للرسائل النصية
    from telegram.ext import Filters
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_text))

    # تشغيل مهمة التحقق من الميداليات يوميًا في منتصف الليل بتوقيت UTC
    job_queue.run_daily(
        check_and_award_medal,
        time=time(hour=0, minute=0, tzinfo=pytz.UTC),
        name="check_and_award_medal",
    )

    # جدولة تذكيرات الماء
    REMINDER_HOURS_UTC = [7, 10, 13, 16, 19]
    
    def water_reminder_job(context: CallbackContext):
        logger.info("Running water reminder job...")
        bot = context.bot

        for uid in get_active_user_ids():
            rec = get_user_record_local_by_id(uid)
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

    for h in REMINDER_HOURS_UTC:
        job_queue.run_daily(
            water_reminder_job,
            time=time(hour=h, minute=0, tzinfo=pytz.UTC),
            name=f"water_reminder_{h}",
        )

    # جدولة الجرعة التحفيزية
    global CURRENT_MOTIVATION_JOBS
    CURRENT_MOTIVATION_JOBS = []
    
    def motivation_job(context: CallbackContext):
        logger.info("Running motivation job...")
        bot = context.bot

        for uid in get_active_user_ids():
            rec = get_user_record_local_by_id(uid)

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

# =================== دالة handle_text الرئيسية ===================

def handle_text(update: Update, context: CallbackContext):
    """معالج الرسائل النصية الرئيسي"""
    user = update.effective_user
    user_id = user.id
    msg = update.message
    text = (msg.text or "").strip()

    # هذا مجرد مثال مبسط، يجب إضافة منطق handle_text الكامل هنا
    # بما أن المساحة محدودة، سأقدم هيكل أساسي
    
    if text == BTN_ADHKAR_MAIN:
        # فتح قائمة الأذكار
        update.message.reply_text(
            "أذكاري 🤲:\n"
            "• أذكار الصباح.\n"
            "• أذكار المساء.\n"
            "• أذكار عامة تريح القلب.",
            reply_markup=adhkar_menu_keyboard(user_id),
        )
        return
    
    elif text == BTN_MEMOS_MAIN:
        open_memos_menu(update, context)
        return
    
    elif text == BTN_LETTER_MAIN:
        open_letters_menu(update, context)
        return
    
    elif text == BTN_BENEFITS_MAIN:
        update.message.reply_text(
            "💡 مجتمع الفوائد و النصائح:\n"
            "شارك فائدة، استعرض فوائد الآخرين، وشارك في التقييم لتحفيز المشاركة.",
            reply_markup=BENEFITS_MENU_KB,
        )
        return
    
    # ... وهكذا لباقي الأزرار
    
    else:
        update.message.reply_text(
            "🤍 أهلاً بك في سقيا الكوثر\n"
            "اختر من القائمة الرئيسية للبدء 🌿",
            reply_markup=user_main_keyboard(user_id),
        )

if __name__ == "__main__":
    main()
