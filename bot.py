import os
import json
import logging
import re
import random
from datetime import datetime, timezone, time, timedelta
from threading import Thread
from typing import List, Dict, Optional

import pytz
import firebase_admin
from firebase_admin import credentials, firestore
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

# =================== إعدادات أساسية ===================

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATA_FILE = "suqya_users.json"

# معرف الأدمن (أنت)
ADMIN_ID = 931350292  # غيّره لو احتجت مستقبلاً

# معرف المشرفة (الأخوات)
SUPERVISOR_ID = 1745150161  # المشرفة

# مسار ملف Firebase Service Account
FIREBASE_CRED_PATH = "/etc/secrets/soqya-firebase-adminsdk.json"

# ملف اللوج
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# =================== تهيئة Firebase ===================

def init_firebase():
    """تهيئة Firebase Admin SDK"""
    try:
        # البحث عن ملف Service Account
        cred_paths = [
            FIREBASE_CRED_PATH,
            "./soqya-firebase-adminsdk.json",
            os.path.join(os.path.dirname(__file__), "soqya-firebase-adminsdk.json")
        ]
        
        cred = None
        for path in cred_paths:
            if os.path.exists(path):
                cred = credentials.Certificate(path)
                logger.info(f"✅ تم العثور على ملف Firebase Service Account في: {path}")
                break
        
        if cred is None:
            logger.error("❌ لم يتم العثور على ملف Firebase Service Account في أي من المسارات المتاحة")
            raise FileNotFoundError("Firebase Service Account file not found")
        
        # تهيئة Firebase App إذا لم تكن مهيئة مسبقاً
        if not firebase_admin._apps:
            firebase_admin.initialize_app(cred)
        
        return firestore.client()
    
    except Exception as e:
        logger.error(f"❌ خطأ في تهيئة Firebase: {e}")
        raise

# تهيئة Firestore Client
try:
    db = init_firebase()
    logger.info("✅ تم تهيئة Firebase Firestore بنجاح")
    USE_FIREBASE = True
except Exception as e:
    logger.error(f"❌ فشل تهيئة Firebase، سيتم استخدام التخزين المحلي: {e}")
    USE_FIREBASE = False
    db = None

# =================== مجمعات Firestore ===================

USERS_COLLECTION = "users"
WATER_LOGS_COLLECTION = "water_logs"
TIPS_COLLECTION = "tips"
NOTES_COLLECTION = "notes"
LETTERS_COLLECTION = "letters"
GLOBAL_CONFIG_COLLECTION = "global_config"
POINTS_HISTORY_COLLECTION = "points_history"

# =================== دوال Firebase Helper ===================

def get_user_doc(user_id: int):
    """يرجع وثيقة المستخدم من Firestore"""
    if not USE_FIREBASE or db is None:
        return None
    return db.collection(USERS_COLLECTION).document(str(user_id))

def get_user_record(user_id: int) -> Optional[Dict]:
    """يرجع بيانات المستخدم من Firestore"""
    if not USE_FIREBASE or db is None:
        return None
    
    doc = get_user_doc(user_id)
    doc_snapshot = doc.get()
    
    if doc_snapshot.exists:
        data = doc_snapshot.to_dict()
        data['id'] = user_id
        return data
    return None

def save_user_record(user_id: int, data: Dict):
    """يحفظ بيانات المستخدم في Firestore"""
    if not USE_FIREBASE or db is None:
        return False
    
    try:
        doc = get_user_doc(user_id)
        # إضافة timestamp للتحديث
        data['updated_at'] = firestore.SERVER_TIMESTAMP
        doc.set(data, merge=True)
        return True
    except Exception as e:
        logger.error(f"❌ خطأ في حفظ بيانات المستخدم {user_id}: {e}")
        return False

def update_user_record(user_id: int, updates: Dict):
    """يحدث بيانات المستخدم في Firestore"""
    if not USE_FIREBASE or db is None:
        return False
    
    try:
        doc = get_user_doc(user_id)
        updates['updated_at'] = firestore.SERVER_TIMESTAMP
        doc.update(updates)
        return True
    except Exception as e:
        logger.error(f"❌ خطأ في تحديث بيانات المستخدم {user_id}: {e}")
        return False

def add_water_log(user_id: int, cups: int, date_str: str = None):
    """يسجل كوب ماء في Firestore"""
    if not USE_FIREBASE or db is None:
        return False
    
    try:
        if date_str is None:
            date_str = datetime.now(timezone.utc).date().isoformat()
        
        log_data = {
            'user_id': user_id,
            'cups': cups,
            'date': date_str,
            'timestamp': firestore.SERVER_TIMESTAMP
        }
        
        db.collection(WATER_LOGS_COLLECTION).add(log_data)
        return True
    except Exception as e:
        logger.error(f"❌ خطأ في تسجيل كوب ماء للمستخدم {user_id}: {e}")
        return False

def get_today_water_logs(user_id: int) -> List[Dict]:
    """يرجع سجل الماء لليوم"""
    if not USE_FIREBASE or db is None:
        return []
    
    try:
        today_str = datetime.now(timezone.utc).date().isoformat()
        logs = db.collection(WATER_LOGS_COLLECTION) \
                .where('user_id', '==', user_id) \
                .where('date', '==', today_str) \
                .stream()
        
        return [log.to_dict() for log in logs]
    except Exception as e:
        logger.error(f"❌ خطأ في جلب سجل الماء للمستخدم {user_id}: {e}")
        return []

def get_global_config():
    """يرجع الإعدادات العامة من Firestore"""
    if not USE_FIREBASE or db is None:
        return {}
    
    try:
        doc = db.collection(GLOBAL_CONFIG_COLLECTION).document('bot_config')
        doc_snapshot = doc.get()
        
        if doc_snapshot.exists:
            return doc_snapshot.to_dict()
        else:
            # إنشاء إعدادات افتراضية
            default_config = {
                'motivation_hours': [6, 9, 12, 15, 18, 21],
                'motivation_messages': [
                    "🍃 تذكّر: قليلٌ دائم خيرٌ من كثير منقطع، خطوة اليوم تقرّبك من نسختك الأفضل 🤍",
                    "💧 جرعة ماء + آية من القرآن + ذكر بسيط = راحة قلب يوم كامل بإذن الله.",
                    "🤍 مهما كان يومك مزدحمًا، قلبك يستحق لحظات هدوء مع ذكر الله.",
                    "📖 لو شعرت بثقل، افتح المصحف صفحة واحدة فقط… ستشعر أن همّك خفّ ولو قليلًا.",
                    "💫 لا تستصغر كوب ماء تشربه بنية حفظ الصحة، ولا صفحة قرآن تقرؤها بنية القرب من الله.",
                    "🕊 قل: الحمد لله الآن… أحيانًا شكرٌ صادق يغيّر مزاج يومك كله.",
                    "🌿 استعن بالله ولا تعجز، كل محاولة للالتزام خير، حتى لو تعثّرت بعدها.",
                ],
                'benefits': [],
                'created_at': firestore.SERVER_TIMESTAMP
            }
            doc.set(default_config)
            return default_config
    except Exception as e:
        logger.error(f"❌ خطأ في جلب الإعدادات العامة: {e}")
        return {}

def save_global_config(config: Dict):
    """يحفظ الإعدادات العامة في Firestore"""
    if not USE_FIREBASE or db is None:
        return False
    
    try:
        doc = db.collection(GLOBAL_CONFIG_COLLECTION).document('bot_config')
        config['updated_at'] = firestore.SERVER_TIMESTAMP
        doc.set(config, merge=True)
        return True
    except Exception as e:
        logger.error(f"❌ خطأ في حفظ الإعدادات العامة: {e}")
        return False

def add_tip(tip_data: Dict):
    """يضيف فائدة/نصيحة جديدة"""
    if not USE_FIREBASE or db is None:
        return None
    
    try:
        tip_ref = db.collection(TIPS_COLLECTION).add(tip_data)
        return tip_ref[1].id  # إرجاع Document ID
    except Exception as e:
        logger.error(f"❌ خطأ في إضافة فائدة: {e}")
        return None

def get_all_tips() -> List[Dict]:
    """يرجع جميع الفوائد"""
    if not USE_FIREBASE or db is None:
        return []
    
    try:
        tips = db.collection(TIPS_COLLECTION).stream()
        result = []
        for tip in tips:
            data = tip.to_dict()
            data['id'] = tip.id
            result.append(data)
        return result
    except Exception as e:
        logger.error(f"❌ خطأ في جلب الفوائد: {e}")
        return []

def get_tip_by_id(tip_id: str) -> Optional[Dict]:
    """يرجع فائدة حسب الـ ID"""
    if not USE_FIREBASE or db is None:
        return None
    
    try:
        doc = db.collection(TIPS_COLLECTION).document(tip_id).get()
        if doc.exists:
            data = doc.to_dict()
            data['id'] = tip_id
            return data
        return None
    except Exception as e:
        logger.error(f"❌ خطأ في جلب الفائدة {tip_id}: {e}")
        return None

def update_tip(tip_id: str, updates: Dict):
    """يحدث فائدة"""
    if not USE_FIREBASE or db is None:
        return False
    
    try:
        updates['updated_at'] = firestore.SERVER_TIMESTAMP
        db.collection(TIPS_COLLECTION).document(tip_id).update(updates)
        return True
    except Exception as e:
        logger.error(f"❌ خطأ في تحديث الفائدة {tip_id}: {e}")
        return False

def delete_tip(tip_id: str):
    """يحذف فائدة"""
    if not USE_FIREBASE or db is None:
        return False
    
    try:
        db.collection(TIPS_COLLECTION).document(tip_id).delete()
        return True
    except Exception as e:
        logger.error(f"❌ خطأ في حذف الفائدة {tip_id}: {e}")
        return False

def add_note(user_id: int, note_data: Dict):
    """يضيف مذكرة قلبية"""
    if not USE_FIREBASE or db is None:
        return None
    
    try:
        note_ref = db.collection(NOTES_COLLECTION).add({
            'user_id': user_id,
            'content': note_data['content'],
            'created_at': firestore.SERVER_TIMESTAMP,
            'updated_at': firestore.SERVER_TIMESTAMP
        })
        return note_ref[1].id
    except Exception as e:
        logger.error(f"❌ خطأ في إضافة مذكرة للمستخدم {user_id}: {e}")
        return None

def get_user_notes(user_id: int) -> List[Dict]:
    """يرجع مذكرات المستخدم"""
    if not USE_FIREBASE or db is None:
        return []
    
    try:
        notes = db.collection(NOTES_COLLECTION) \
                 .where('user_id', '==', user_id) \
                 .stream()
        
        result = []
        for note in notes:
            data = note.to_dict()
            data['id'] = note.id
            result.append(data)
        return result
    except Exception as e:
        logger.error(f"❌ خطأ في جلب مذكرات المستخدم {user_id}: {e}")
        return []

def update_note(note_id: str, content: str):
    """يحدث مذكرة"""
    if not USE_FIREBASE or db is None:
        return False
    
    try:
        db.collection(NOTES_COLLECTION).document(note_id).update({
            'content': content,
            'updated_at': firestore.SERVER_TIMESTAMP
        })
        return True
    except Exception as e:
        logger.error(f"❌ خطأ في تحديث المذكرة {note_id}: {e}")
        return False

def delete_note(note_id: str):
    """يحذف مذكرة"""
    if not USE_FIREBASE or db is None:
        return False
    
    try:
        db.collection(NOTES_COLLECTION).document(note_id).delete()
        return True
    except Exception as e:
        logger.error(f"❌ خطأ في حذف المذكرة {note_id}: {e}")
        return False

def add_letter(user_id: int, letter_data: Dict):
    """يضيف رسالة إلى النفس"""
    if not USE_FIREBASE or db is None:
        return None
    
    try:
        letter_ref = db.collection(LETTERS_COLLECTION).add({
            'user_id': user_id,
            'content': letter_data['content'],
            'reminder_date': letter_data.get('reminder_date'),
            'created_at': firestore.SERVER_TIMESTAMP,
            'sent': False
        })
        return letter_ref[1].id
    except Exception as e:
        logger.error(f"❌ خطأ في إضافة رسالة للمستخدم {user_id}: {e}")
        return None

def get_user_letters(user_id: int) -> List[Dict]:
    """يرجع رسائل المستخدم"""
    if not USE_FIREBASE or db is None:
        return []
    
    try:
        letters = db.collection(LETTERS_COLLECTION) \
                   .where('user_id', '==', user_id) \
                   .stream()
        
        result = []
        for letter in letters:
            data = letter.to_dict()
            data['id'] = letter.id
            result.append(data)
        return result
    except Exception as e:
        logger.error(f"❌ خطأ في جلب رسائل المستخدم {user_id}: {e}")
        return []

def update_letter(letter_id: str, updates: Dict):
    """يحدث رسالة"""
    if not USE_FIREBASE or db is None:
        return False
    
    try:
        updates['updated_at'] = firestore.SERVER_TIMESTAMP
        db.collection(LETTERS_COLLECTION).document(letter_id).update(updates)
        return True
    except Exception as e:
        logger.error(f"❌ خطأ في تحديث الرسالة {letter_id}: {e}")
        return False

def delete_letter(letter_id: str):
    """يحذف رسالة"""
    if not USE_FIREBASE or db is None:
        return False
    
    try:
        db.collection(LETTERS_COLLECTION).document(letter_id).delete()
        return True
    except Exception as e:
        logger.error(f"❌ خطأ في حذف الرسالة {letter_id}: {e}")
        return False

def add_points_history(user_id: int, points: int, reason: str, source: str = ""):
    """يسجل تاريخ النقاط"""
    if not USE_FIREBASE or db is None:
        return False
    
    try:
        history_data = {
            'user_id': user_id,
            'points': points,
            'reason': reason,
            'source': source,
            'timestamp': firestore.SERVER_TIMESTAMP
        }
        db.collection(POINTS_HISTORY_COLLECTION).add(history_data)
        return True
    except Exception as e:
        logger.error(f"❌ خطأ في تسجيل تاريخ النقاط للمستخدم {user_id}: {e}")
        return False

def get_all_users() -> List[Dict]:
    """يرجع جميع المستخدمين"""
    if not USE_FIREBASE or db is None:
        return []
    
    try:
        users = db.collection(USERS_COLLECTION).stream()
        result = []
        for user in users:
            data = user.to_dict()
            data['id'] = int(user.id)
            result.append(data)
        return result
    except Exception as e:
        logger.error(f"❌ خطأ في جلب جميع المستخدمين: {e}")
        return []

def get_active_users() -> List[Dict]:
    """يرجع المستخدمين النشطين (غير المحظورين)"""
    if not USE_FIREBASE or db is None:
        return []
    
    try:
        users = db.collection(USERS_COLLECTION) \
                 .where('is_banned', '==', False) \
                 .stream()
        
        result = []
        for user in users:
            data = user.to_dict()
            data['id'] = int(user.id)
            result.append(data)
        return result
    except Exception as e:
        logger.error(f"❌ خطأ في جلب المستخدمين النشطين: {e}")
        return []

def get_banned_users() -> List[Dict]:
    """يرجع المستخدمين المحظورين"""
    if not USE_FIREBASE or db is None:
        return []
    
    try:
        users = db.collection(USERS_COLLECTION) \
                 .where('is_banned', '==', True) \
                 .stream()
        
        result = []
        for user in users:
            data = user.to_dict()
            data['id'] = int(user.id)
            result.append(data)
        return result
    except Exception as e:
        logger.error(f"❌ خطأ في جلب المستخدمين المحظورين: {e}")
        return []

# =================== خادم ويب بسيط لـ Render ===================

app = Flask(__name__)

@app.route("/")
def index():
    firebase_status = "✅ متصل" if USE_FIREBASE else "❌ غير متصل"
    return f"Suqya Al-Kawther bot is running ✅<br>Firebase Status: {firebase_status}"


def run_flask():
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)

# =================== تخزين البيانات (النسخ الاحتياطي المحلي) ===================

def load_data():
    """تحميل البيانات من ملف JSON (للتوافق مع الكود القديم)"""
    if not os.path.exists(DATA_FILE):
        return {}
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return {}

def save_data():
    """حفظ البيانات إلى ملف JSON (للتوافق مع الكود القديم)"""
    if not USE_FIREBASE:
        # إذا لم يكن Firebase متوفراً، نستخدم التخزين المحلي
        try:
            with open(DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Error saving data: {e}")

# بيانات مؤقتة للتوافق مع الكود القديم
data = load_data()

# =================== إعدادات افتراضية للجرعة التحفيزية (على مستوى البوت) ===================

DEFAULT_MOTIVATION_HOURS_UTC = [6, 9, 12, 15, 18, 21]

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

MOTIVATION_HOURS_UTC = []
MOTIVATION_MESSAGES = []

CURRENT_MOTIVATION_JOBS = []

def get_global_config_firebase():
    """يرجع الإعدادات العامة من Firebase"""
    config = get_global_config()
    
    # إذا كانت الإعدادات فارغة، نستخدم الإعدادات الافتراضية
    if not config or not config.get('motivation_hours'):
        config = {
            'motivation_hours': DEFAULT_MOTIVATION_HOURS_UTC.copy(),
            'motivation_messages': DEFAULT_MOTIVATION_MESSAGES.copy(),
            'benefits': [],
            'created_at': firestore.SERVER_TIMESTAMP if USE_FIREBASE else None
        }
        save_global_config(config)
    
    return config

def get_motivation_hours():
    """يرجع ساعات الجرعة التحفيزية"""
    config = get_global_config_firebase()
    return config.get('motivation_hours', DEFAULT_MOTIVATION_HOURS_UTC)

def get_motivation_messages():
    """يرجع رسائل الجرعة التحفيزية"""
    config = get_global_config_firebase()
    return config.get('motivation_messages', DEFAULT_MOTIVATION_MESSAGES)

def get_benefits_firebase():
    """يرجع الفوائد من Firebase"""
    if USE_FIREBASE:
        return get_all_tips()
    
    # إذا لم يكن Firebase متوفراً، نستخدم البيانات المحلية
    config = get_global_config_firebase()
    return config.get('benefits', [])

def save_benefits_firebase(benefits_list):
    """يحفظ الفوائد في Firebase"""
    if USE_FIREBASE:
        # في Firebase، الفوائد مخزنة في collection منفصل
        # لذا لا نحتاج هذه الدالة هنا
        pass
    else:
        config = get_global_config_firebase()
        config['benefits'] = benefits_list
        save_global_config(config)

# =================== سجلات المستخدمين مع Firebase ===================

def get_next_benefit_id():
    """يرجع المعرف الفريد التالي للفائدة."""
    benefits = get_benefits_firebase()
    if not benefits:
        return 1
    
    if USE_FIREBASE:
        # في Firebase، الـ ID يتم إنشاؤه تلقائياً
        return None
    
    # البحث عن أكبر ID موجود
    max_id = max(b.get("id", 0) for b in benefits)
    return max_id + 1

def get_user_record_modern(user):
    """
    ينشئ أو يرجع سجل المستخدم من Firebase
    """
    user_id = user.id
    now_iso = datetime.now(timezone.utc).isoformat()
    
    if USE_FIREBASE:
        # جلب بيانات المستخدم من Firebase
        user_data = get_user_record(user_id)
        
        if user_data is None:
            # إنشاء مستخدم جديد في Firebase
            user_data = {
                "user_id": user_id,
                "first_name": user.first_name,
                "username": user.username,
                "created_at": firestore.SERVER_TIMESTAMP,
                "last_active": firestore.SERVER_TIMESTAMP,
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
                
                "points": 0,
                "level": 0,
                "medals": [],
                "best_rank": None,
                
                "daily_full_streak": 0,
                "last_full_day": None,
                
                "motivation_on": True,
            }
            
            # حفظ المستخدم الجديد
            save_user_record(user_id, user_data)
            return user_data
        else:
            # تحديث آخر نشاط
            update_user_record(user_id, {
                "first_name": user.first_name,
                "username": user.username,
                "last_active": firestore.SERVER_TIMESTAMP
            })
            
            # ضمان وجود جميع الحقول
            defaults = {
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
                "points": 0,
                "level": 0,
                "medals": [],
                "best_rank": None,
                "daily_full_streak": 0,
                "last_full_day": None,
                "motivation_on": True,
                "is_new_user": False
            }
            
            # التحقق من وجود الحقول وتحديثها إذا لزم الأمر
            needs_update = False
            for key, default_value in defaults.items():
                if key not in user_data:
                    user_data[key] = default_value
                    needs_update = True
            
            if needs_update:
                save_user_record(user_id, user_data)
            
            return user_data
    else:
        # استخدام التخزين المحلي إذا لم يكن Firebase متوفراً
        return get_user_record_legacy(user)

def get_user_record_legacy(user):
    """
    نسخة قديمة للتوافق مع JSON (للتخزين المحلي)
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
        defaults = {
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
            "points": 0,
            "level": 0,
            "medals": [],
            "best_rank": None,
            "daily_full_streak": 0,
            "last_full_day": None,
            "motivation_on": True,
            "is_new_user": False
        }
        
        for key, default_value in defaults.items():
            record.setdefault(key, default_value)

        # تحديث أسماء الميداليات القديمة
        medals = record.get("medals", [])
        if medals:
            new_medals = []
            for m in medals:
                if m == "ميدالية الاستمرار 💫":
                    new_medals.append("ميدالية الاستمرار 🎓")
                elif m == "ميدالية بطل سُقيا الكوثر 👑":
                    new_medals.append("ميدالية بطل سُقيا الكوثر 🏆")
                else:
                    new_medals.append(m)
            record["medals"] = new_medals

    save_data()
    return data[user_id]

# استخدام الدالة المناسبة حسب نوع التخزين
def get_user_record(user):
    if USE_FIREBASE:
        return get_user_record_modern(user)
    else:
        return get_user_record_legacy(user)

def update_user_record_modern(user_id: int, **kwargs):
    """يحدث سجل المستخدم في Firebase"""
    if USE_FIREBASE:
        update_user_record(user_id, kwargs)
    else:
        # استخدام التخزين المحلي
        uid = str(user_id)
        if uid not in data:
            return
        data[uid].update(kwargs)
        data[uid]["last_active"] = datetime.now(timezone.utc).isoformat()
        save_data()

def add_points_modern(user_id: int, points: int, context: CallbackContext = None, reason: str = ""):
    """يضيف نقاطًا للمستخدم في Firebase"""
    if points <= 0:
        return
    
    if USE_FIREBASE:
        user_data = get_user_record(user_id)
        if not user_data:
            return
        
        current_points = user_data.get("points", 0)
        new_points = current_points + points
        
        # تحديث النقاط
        update_user_record(user_id, {"points": new_points})
        
        # تسجيل في تاريخ النقاط
        add_points_history(user_id, points, reason, "bot")
        
        # تحديث المستوى والميداليات
        update_level_and_medals(user_id, user_data, context)
        
        # التحقق من تحسن الترتيب
        check_rank_improvement(user_id, user_data, context)
        
        # التحقق من النشاط اليومي
        check_daily_full_activity(user_id, user_data, context)
    else:
        # استخدام التخزين المحلي
        uid = str(user_id)
        if uid not in data or uid == GLOBAL_KEY:
            return
        
        record = data[uid]
        current_points = record.get("points", 0)
        record["points"] = current_points + points
        save_data()

# استبدال الدالة القديمة بالجديدة
add_points = add_points_modern

def get_all_user_ids_modern():
    """يرجع جميع معرفات المستخدمين"""
    if USE_FIREBASE:
        users = get_all_users()
        return [user['id'] for user in users]
    else:
        return [int(uid) for uid in data.keys() if uid != GLOBAL_KEY]

def get_active_user_ids_modern():
    """يرجع قائمة المستخدمين النشطين (غير المحظورين)"""
    if USE_FIREBASE:
        users = get_active_users()
        return [user['id'] for user in users]
    else:
        return [int(uid) for uid, rec in data.items() 
                if uid != GLOBAL_KEY and not rec.get("is_banned", False)]

def get_banned_user_ids_modern():
    """يرجع قائمة المستخدمين المحظورين"""
    if USE_FIREBASE:
        users = get_banned_users()
        return [user['id'] for user in users]
    else:
        return [int(uid) for uid, rec in data.items() 
                if uid != GLOBAL_KEY and rec.get("is_banned", False)]

# استبدال الدوال القديمة بالجديدة
get_all_user_ids = get_all_user_ids_modern
get_active_user_ids = get_active_user_ids_modern
get_banned_user_ids = get_banned_user_ids_modern

def is_admin(user_id: int) -> bool:
    return ADMIN_ID is not None and user_id == ADMIN_ID

def is_supervisor(user_id: int) -> bool:
    return SUPERVISOR_ID is not None and user_id == SUPERVISOR_ID

def user_main_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    """يرجع لوحة المفاتيح الرئيسية المناسبة للمستخدم"""
    if is_admin(user_id):
        return MAIN_KEYBOARD_ADMIN
    elif is_supervisor(user_id):
        return MAIN_KEYBOARD_SUPERVISOR
    else:
        return MAIN_KEYBOARD_USER

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
        [KeyboardButton(BTN_ADHKAR_MAIN), KeyboardButton(BTN_QURAN_MAIN)],
        [KeyboardButton(BTN_TASBIH_MAIN), KeyboardButton(BTN_WATER_MAIN)],
        [KeyboardButton(BTN_MEMOS_MAIN), KeyboardButton(BTN_LETTER_MAIN)],
        [KeyboardButton(BTN_STATS), KeyboardButton(BTN_COMP_MAIN)],
        [KeyboardButton(BTN_BENEFITS_MAIN)],
        [KeyboardButton(BTN_NOTIFICATIONS_MAIN), KeyboardButton(BTN_SUPPORT)],
    ],
    resize_keyboard=True,
)

MAIN_KEYBOARD_ADMIN = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_ADHKAR_MAIN), KeyboardButton(BTN_QURAN_MAIN)],
        [KeyboardButton(BTN_TASBIH_MAIN), KeyboardButton(BTN_WATER_MAIN)],
        [KeyboardButton(BTN_MEMOS_MAIN), KeyboardButton(BTN_LETTER_MAIN)],
        [KeyboardButton(BTN_STATS), KeyboardButton(BTN_COMP_MAIN)],
        [KeyboardButton(BTN_BENEFITS_MAIN)],
        [KeyboardButton(BTN_NOTIFICATIONS_MAIN), KeyboardButton(BTN_SUPPORT)],
        [KeyboardButton(BTN_ADMIN_PANEL)],
    ],
    resize_keyboard=True,
)

MAIN_KEYBOARD_SUPERVISOR = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_ADHKAR_MAIN), KeyboardButton(BTN_QURAN_MAIN)],
        [KeyboardButton(BTN_TASBIH_MAIN), KeyboardButton(BTN_WATER_MAIN)],
        [KeyboardButton(BTN_MEMOS_MAIN), KeyboardButton(BTN_LETTER_MAIN)],
        [KeyboardButton(BTN_STATS), KeyboardButton(BTN_COMP_MAIN)],
        [KeyboardButton(BTN_BENEFITS_MAIN)],
        [KeyboardButton(BTN_NOTIFICATIONS_MAIN), KeyboardButton(BTN_SUPPORT)],
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
        if USE_FIREBASE:
            update_user_record(record.get("user_id"), {
                "today_date": today_str,
                "today_cups": 0
            })
        else:
            save_data()


def ensure_today_quran(record):
    today_str = datetime.now(timezone.utc).date().isoformat()
    if record.get("quran_today_date") != today_str:
        record["quran_today_date"] = today_str
        record["quran_pages_today"] = 0
        if USE_FIREBASE:
            update_user_record(record.get("user_id"), {
                "quran_today_date": today_str,
                "quran_pages_today": 0
            })
        else:
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
    if USE_FIREBASE:
        user_data = get_user_record(user_id)
        if user_data:
            current_count = user_data.get("adhkar_count", 0)
            update_user_record(user_id, {"adhkar_count": current_count + amount})
    else:
        uid = str(user_id)
        if uid not in data:
            return
        record = data[uid]
        record["adhkar_count"] = record.get("adhkar_count", 0) + amount
        save_data()


def increment_tasbih_total(user_id: int, amount: int = 1):
    if USE_FIREBASE:
        user_data = get_user_record(user_id)
        if user_data:
            current_total = user_data.get("tasbih_total", 0)
            update_user_record(user_id, {"tasbih_total": current_total + amount})
    else:
        uid = str(user_id)
        if uid not in data:
            return
        record = data[uid]
        record["tasbih_total"] = record.get("tasbih_total", 0) + amount
        save_data()

# =================== نظام النقاط / المستويات / الميداليات ===================

def get_users_sorted_by_points():
    if USE_FIREBASE:
        users = get_all_users()
        # فلترة المستخدمين المحظورين
        users = [user for user in users if not user.get("is_banned", False)]
        return sorted(users, key=lambda r: r.get("points", 0), reverse=True)
    else:
        return sorted(
            [r for k, r in data.items() if k != GLOBAL_KEY],
            key=lambda r: r.get("points", 0),
            reverse=True,
        )

def check_rank_improvement(user_id: int, record: dict, context: CallbackContext = None):
    sorted_users = get_users_sorted_by_points()
    rank = None
    for idx, rec in enumerate(sorted_users, start=1):
        if rec.get("user_id") == user_id or rec.get("id") == user_id:
            rank = idx
            break

    if rank is None:
        return

    best_rank = record.get("best_rank")
    if best_rank is not None and rank >= best_rank:
        return

    record["best_rank"] = rank
    
    if USE_FIREBASE:
        update_user_record(user_id, {"best_rank": rank})
    else:
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
    
    if USE_FIREBASE:
        update_user_record(user_id, {
            "level": new_level,
            "medals": medals
        })
    else:
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
    
    if USE_FIREBASE:
        update_user_record(user_id, {
            "daily_full_streak": streak,
            "last_full_day": today_str,
            "medals": medals
        })
    else:
        save_data()

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

# =================== دوال الفوائد والنصائح مع Firebase ===================

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

    # إعداد بيانات الفائدة
    tip_data = {
        'text': text,
        'user_id': user_id,
        'first_name': user.first_name if user.first_name else "مستخدم مجهول",
        'username': user.username if user.username else None,
        'created_at': firestore.SERVER_TIMESTAMP if USE_FIREBASE else datetime.now(timezone.utc).isoformat(),
        'likes_count': 0,
        'liked_by': [],
    }

    if USE_FIREBASE:
        # إضافة الفائدة إلى Firebase
        tip_id = add_tip(tip_data)
        
        if tip_id:
            # منح النقاط
            add_points(user_id, 2, context, "إضافة فائدة/نصيحة")
            
            update.message.reply_text(
                "✅ تم إضافة فائدتك بنجاح! شكرًا لمشاركتك.\n"
                f"لقد حصلت على 2 نقطة مكافأة.",
                reply_markup=BENEFITS_MENU_KB,
            )
        else:
            update.message.reply_text(
                "❌ حدث خطأ في إضافة الفائدة. حاول مرة أخرى.",
                reply_markup=BENEFITS_MENU_KB,
            )
    else:
        # استخدام التخزين المحلي
        benefit_id = get_next_benefit_id()
        if benefit_id:
            tip_data['id'] = benefit_id
            benefits = get_benefits_firebase()
            benefits.append(tip_data)
            save_benefits_firebase(benefits)
            
            # منح النقاط
            add_points(user_id, 2, context, "إضافة فائدة/نصيحة")
            
            update.message.reply_text(
                "✅ تم إضافة فائدتك بنجاح! شكرًا لمشاركتك.\n"
                f"لقد حصلت على 2 نقطة مكافأة.",
                reply_markup=BENEFITS_MENU_KB,
            )


def handle_view_benefits(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    if record.get("is_banned", False):
        return

    if USE_FIREBASE:
        benefits = get_all_tips()
    else:
        benefits = get_benefits_firebase()
    
    if not benefits:
        update.message.reply_text(
            "لا توجد فوائد أو نصائح مضافة حتى الآن. كن أول من يشارك! 💡",
            reply_markup=BENEFITS_MENU_KB,
        )
        return

    # عرض آخر 5 فوائد
    if USE_FIREBASE:
        latest_benefits = sorted(benefits, key=lambda b: b.get("created_at", ""), reverse=True)[:5]
    else:
        latest_benefits = sorted(benefits, key=lambda b: b.get("date", ""), reverse=True)[:5]
    
    # التحقق من صلاحيات المدير/المشرف
    is_privileged = is_admin(user.id) or is_supervisor(user.id)
    
    update.message.reply_text(
        "📖 آخر 5 فوائد ونصائح مضافة:",
        reply_markup=BENEFITS_MENU_KB,
    )
    
    for benefit in latest_benefits:
        # تنسيق التاريخ
        try:
            if USE_FIREBASE:
                if 'created_at' in benefit:
                    dt = benefit['created_at']
                    if hasattr(dt, 'strftime'):
                        date_str = dt.strftime("%Y-%m-%d")
                    else:
                        date_str = str(dt)
                else:
                    date_str = "تاريخ غير معروف"
            else:
                dt = datetime.fromisoformat(benefit["date"].replace('Z', '+00:00'))
                date_str = dt.strftime("%Y-%m-%d")
        except:
            date_str = "تاريخ غير معروف"
            
        text_benefit = (
            f"• *{benefit['text']}*\n"
            f"  - من: {benefit['first_name']} | الإعجابات: {benefit.get('likes_count', 0)} 👍\n"
            f"  - تاريخ الإضافة: {date_str}\n"
        )
        
        # إضافة زر الإعجاب
        like_button_text = f"👍 أعجبني ({benefit.get('likes_count', 0)})"
        
        # التحقق مما إذا كان المستخدم قد أعجب بالفعل
        if user.id in benefit.get("liked_by", []):
            like_button_text = f"✅ أعجبتني ({benefit.get('likes_count', 0)})"
        
        # استخدام InlineKeyboardCallbackData للإعجاب
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
        
    # إرسال رسالة ختامية ولوحة المفاتيح الرئيسية للقسم
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

    if USE_FIREBASE:
        benefits = get_all_tips()
        user_benefits = [b for b in benefits if b.get("user_id") == user_id]
    else:
        benefits = get_benefits_firebase()
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
            if USE_FIREBASE:
                if 'created_at' in benefit:
                    dt = benefit['created_at']
                    if hasattr(dt, 'strftime'):
                        date_str = dt.strftime("%Y-%m-%d")
                    else:
                        date_str = str(dt)
                else:
                    date_str = "تاريخ غير معروف"
            else:
                dt = datetime.fromisoformat(benefit["date"].replace('Z', '+00:00'))
                date_str = dt.strftime("%Y-%m-%d")
        except:
            date_str = "تاريخ غير معروف"
            
        text_benefit = (
            f"• *{benefit['text']}*\n"
            f"  - الإعجابات: {benefit.get('likes_count', 0)} 👍\n"
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
        benefit_id = query.data.split("_")[-1]
    except ValueError:
        query.answer("خطأ في تحديد الفائدة.")
        return

    if USE_FIREBASE:
        benefit = get_tip_by_id(benefit_id)
    else:
        benefits = get_benefits_firebase()
        benefit = next((b for b in benefits if str(b.get("id")) == benefit_id), None)
    
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
        text=f"✏️ أرسل النص الجديد للفائدة الآن.\n"
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
    
    if USE_FIREBASE:
        success = update_tip(benefit_id, {"text": text})
        
        if success:
            WAITING_BENEFIT_EDIT_TEXT.discard(user_id)
            BENEFIT_EDIT_ID.pop(user_id, None)
            
            update.message.reply_text(
                "✅ تم تعديل الفائدة بنجاح.",
                reply_markup=BENEFITS_MENU_KB,
            )
        else:
            update.message.reply_text(
                "❌ حدث خطأ في تعديل الفائدة. حاول مرة أخرى.",
                reply_markup=BENEFITS_MENU_KB,
            )
    else:
        benefits = get_benefits_firebase()
        
        for i, b in enumerate(benefits):
            if str(b.get("id")) == str(benefit_id) and b.get("user_id") == user_id:
                benefits[i]["text"] = text
                save_benefits_firebase(benefits)
                
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
        benefit_id = query.data.split("_")[-1]
    except ValueError:
        query.answer("خطأ في تحديد الفائدة.")
        return

    if USE_FIREBASE:
        benefit = get_tip_by_id(benefit_id)
    else:
        benefits = get_benefits_firebase()
        benefit = next((b for b in benefits if str(b.get("id")) == benefit_id and b.get("user_id") == user_id), None)
    
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
        text=f"⚠️ هل أنت متأكد من حذف الفائدة؟\n"
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
        benefit_id = query.data.split("_")[-1]
    except ValueError:
        query.answer("خطأ في تحديد الفائدة.")
        return

    if USE_FIREBASE:
        benefit = get_tip_by_id(benefit_id)
        
        if benefit is None:
            query.answer("هذه الفائدة غير موجودة.")
            query.edit_message_text(
                text="⚠️ حدث خطأ: هذه الفائدة غير موجودة.",
                reply_markup=None,
            )
            WAITING_BENEFIT_DELETE_CONFIRM.discard(user_id)
            BENEFIT_EDIT_ID.pop(user_id, None)
            return

        is_owner = benefit.get("user_id") == user_id
        is_privileged = is_admin(user_id) or is_supervisor(user_id)
        
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

        # حذف الفائدة من Firebase
        success = delete_tip(benefit_id)
        
        if success:
            query.answer("✅ تم حذف الفائدة بنجاح.")
            query.edit_message_text(
                text=f"✅ تم حذف الفائدة بنجاح.",
                reply_markup=None,
            )
            
            # إرسال رسالة لصاحب الفائدة إذا كان الحذف إشرافيًا
            if is_admin_delete and benefit.get("user_id") != user_id:
                try:
                    context.bot.send_message(
                        chat_id=benefit.get("user_id"),
                        text=f"⚠️ تنبيه: تم حذف فائدتك بواسطة المشرف/المدير.\n"
                             f"النص المحذوف: *{benefit['text']}*\n"
                             f"يرجى مراجعة سياسات المجتمع.",
                        parse_mode="Markdown",
                    )
                except Exception as e:
                    logger.error(f"Error sending deletion message to benefit owner: {e}")
        else:
            query.answer("⚠️ حدث خطأ في حذف الفائدة.")
            query.edit_message_text(
                text="⚠️ حدث خطأ في حذف الفائدة.",
                reply_markup=None,
            )
    else:
        benefits = get_benefits_firebase()
        
        # البحث عن الفائدة
        benefit_to_delete = None
        for i, b in enumerate(benefits):
            if str(b.get("id")) == benefit_id:
                benefit_to_delete = b
                break
        
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
        
        if not is_owner:
            query.answer("لا تملك صلاحية حذف هذه الفائدة.")
            query.edit_message_text(
                text="⚠️ حدث خطأ: لا تملك صلاحية حذف هذه الفائدة.",
                reply_markup=None,
            )
            WAITING_BENEFIT_DELETE_CONFIRM.discard(user_id)
            BENEFIT_EDIT_ID.pop(user_id, None)
            return

        # حذف الفائدة
        benefits = [b for b in benefits if str(b.get("id")) != benefit_id]
        save_benefits_firebase(benefits)
        
        query.answer("✅ تم حذف الفائدة بنجاح.")
        query.edit_message_text(
            text=f"✅ تم حذف الفائدة بنجاح.",
            reply_markup=None,
        )

    WAITING_BENEFIT_DELETE_CONFIRM.discard(user_id)
    BENEFIT_EDIT_ID.pop(user_id, None)


def handle_top10_benefits(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    if record.get("is_banned", False):
        return

    if USE_FIREBASE:
        benefits = get_all_tips()
    else:
        benefits = get_benefits_firebase()
    
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
        text += f"   - من: {benefit['first_name']} | الإعجابات: {benefit.get('likes_count', 0)} 👍\n\n"
        
    update.message.reply_text(
        text=text,
        reply_markup=BENEFITS_MENU_KB,
        parse_mode="Markdown",
    )


def check_and_award_medal(context: CallbackContext):
    """
    دالة تفحص أفضل 10 فوائد وتمنح الوسام لصاحبها إذا لم يكن لديه.
    """
    if USE_FIREBASE:
        benefits = get_all_tips()
    else:
        benefits = get_benefits_firebase()
    
    if not benefits:
        return

    # ترتيب الفوائد حسب عدد الإعجابات تنازليًا
    sorted_benefits = sorted(benefits, key=lambda b: b.get("likes_count", 0), reverse=True)
    
    top_10_user_ids = set()
    for benefit in sorted_benefits[:10]:
        top_10_user_ids.add(benefit["user_id"])
        
    MEDAL_TEXT = "وسام صاحب فائدة من العشرة الأوائل 💡🏅"
    
    for user_id in top_10_user_ids:
        user_data = get_user_record(user_id)
        if user_data:
            medals = user_data.get("medals", [])
            
            if MEDAL_TEXT not in medals:
                medals.append(MEDAL_TEXT)
                user_data["medals"] = medals
                
                if USE_FIREBASE:
                    update_user_record(user_id, {"medals": medals})
                else:
                    save_data()
                
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


def handle_admin_delete_benefit_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    
    # التحقق من الصلاحية
    if not (is_admin(user_id) or is_supervisor(user_id)):
        query.answer("لا تملك صلاحية حذف فوائد الآخرين.")
        return

    try:
        benefit_id = query.data.split("_")[-1]
    except ValueError:
        query.answer("خطأ في تحديد الفائدة.")
        return

    if USE_FIREBASE:
        benefit = get_tip_by_id(benefit_id)
    else:
        benefits = get_benefits_firebase()
        benefit = next((b for b in benefits if str(b.get("id")) == benefit_id), None)
    
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
        text=f"⚠️ هل أنت متأكد من حذف الفائدة للمستخدم {benefit['first_name']}؟\n"
             f"النص: *{benefit['text']}*",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown",
    )


def handle_like_benefit_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    
    if query.data.startswith("like_benefit_"):
        try:
            benefit_id = query.data.split("_")[-1]
        except ValueError:
            query.answer("خطأ في تحديد الفائدة.")
            return

        if USE_FIREBASE:
            benefit = get_tip_by_id(benefit_id)
        else:
            benefits = get_benefits_firebase()
            benefit = next((b for b in benefits if str(b.get("id")) == benefit_id), None)
        
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
        
        # إضافة الإعجاب
        liked_by.append(user_id)
        new_likes_count = benefit.get("likes_count", 0) + 1
        
        if USE_FIREBASE:
            # تحديث الفائدة في Firebase
            update_tip(benefit_id, {
                "likes_count": new_likes_count,
                "liked_by": liked_by
            })
            
            # منح نقطة لصاحب الفائدة
            owner_id = benefit["user_id"]
            add_points(owner_id, 1, context, "إعجاب بفائدتك")
        else:
            # تحديث الفائدة في التخزين المحلي
            benefits = get_benefits_firebase()
            for i, b in enumerate(benefits):
                if str(b.get("id")) == benefit_id:
                    benefits[i]["likes_count"] = new_likes_count
                    benefits[i]["liked_by"] = liked_by
                    break
            save_benefits_firebase(benefits)
        
        # تحديث زر الإعجاب
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
        
        # فحص ومنح الوسام
        check_and_award_medal(context)

# =================== دوال مذكرات القلب مع Firebase ===================

def format_memos_list_firebase(memos):
    if not memos:
        return "لا توجد مذكّرات بعد."
    
    result = []
    for idx, memo in enumerate(memos, start=1):
        content = memo.get('content', '')
        created_at = memo.get('created_at', '')
        
        # تنسيق التاريخ
        date_str = "تاريخ غير معروف"
        if created_at:
            try:
                if hasattr(created_at, 'strftime'):
                    date_str = created_at.strftime("%Y-%m-%d")
                else:
                    date_str = str(created_at)[:10]
            except:
                pass
        
        result.append(f"{idx}. {content[:50]}{'...' if len(content) > 50 else ''} ({date_str})")
    
    return "\n\n".join(result)


def open_memos_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id

    WAITING_MEMO_MENU.add(user_id)
    WAITING_MEMO_ADD.discard(user_id)
    WAITING_MEMO_EDIT_SELECT.discard(user_id)
    WAITING_MEMO_EDIT_TEXT.discard(user_id)
    WAITING_MEMO_DELETE_SELECT.discard(user_id)
    MEMO_EDIT_INDEX.pop(user_id, None)

    if USE_FIREBASE:
        memos = get_user_notes(user_id)
        memos_text = format_memos_list_firebase(memos)
    else:
        record = get_user_record(user)
        memos = record.get("heart_memos", [])
        memos_text = "\n\n".join(f"{idx+1}. {m}" for idx, m in enumerate(memos)) if memos else "لا توجد مذكّرات بعد."
    
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
    user_id = user.id
    text = (update.message.text or "").strip()

    if text == BTN_CANCEL:
        WAITING_MEMO_ADD.discard(user_id)
        open_memos_menu(update, context)
        return

    if USE_FIREBASE:
        # إضافة المذكرة إلى Firebase
        note_data = {
            'content': text,
        }
        note_id = add_note(user_id, note_data)
        
        if note_id:
            WAITING_MEMO_ADD.discard(user_id)
            update.message.reply_text(
                "تم حفظ مذكّرتك في قلب البوت 🤍.",
                reply_markup=build_memos_menu_kb(is_admin(user_id)),
            )
        else:
            update.message.reply_text(
                "❌ حدث خطأ في حفظ المذكرة. حاول مرة أخرى.",
                reply_markup=build_memos_menu_kb(is_admin(user_id)),
            )
    else:
        # استخدام التخزين المحلي
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
    
    open_memos_menu(update, context)


def handle_memo_edit_select(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id

    if USE_FIREBASE:
        memos = get_user_notes(user_id)
    else:
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

    if USE_FIREBASE:
        memos_text = format_memos_list_firebase(memos)
    else:
        memos_text = "\n\n".join(f"{idx+1}. {m}" for idx, m in enumerate(memos))
    
    update.message.reply_text(
        f"✏️ اختر رقم المذكرة التي تريد تعديلها:\n\n{memos_text}\n\n"
        "أرسل الرقم الآن، أو اضغط «إلغاء ❌».",
        reply_markup=CANCEL_KB,
    )


def handle_memo_edit_index_input(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    
    if USE_FIREBASE:
        memos = get_user_notes(user_id)
    else:
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

    if USE_FIREBASE:
        memo_content = memos[idx].get('content', '')
    else:
        memo_content = memos[idx]
    
    update.message.reply_text(
        f"✏️ أرسل النص الجديد للمذكرة رقم {idx+1}:\n\nالنص الحالي: {memo_content}",
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
    
    if USE_FIREBASE:
        memos = get_user_notes(user_id)
        if idx is None or idx < 0 or idx >= len(memos):
            WAITING_MEMO_EDIT_TEXT.discard(user_id)
            MEMO_EDIT_INDEX.pop(user_id, None)
            update.message.reply_text(
                "حدث خطأ بسيط في اختيار المذكرة، جرّب من جديد من «مذكّرات قلبي 🩵».",
                reply_markup=user_main_keyboard(user_id),
            )
            return

        # تحديث المذكرة في Firebase
        note_id = memos[idx]['id']
        success = update_note(note_id, text)
        
        if success:
            WAITING_MEMO_EDIT_TEXT.discard(user_id)
            MEMO_EDIT_INDEX.pop(user_id, None)
            update.message.reply_text(
                "تم تعديل المذكرة بنجاح ✅.",
                reply_markup=build_memos_menu_kb(is_admin(user_id)),
            )
        else:
            WAITING_MEMO_EDIT_TEXT.discard(user_id)
            MEMO_EDIT_INDEX.pop(user_id, None)
            update.message.reply_text(
                "❌ حدث خطأ في تعديل المذكرة. حاول مرة أخرى.",
                reply_markup=build_memos_menu_kb(is_admin(user_id)),
            )
    else:
        record = get_user_record(user)
        memos = record.get("heart_memos", [])
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
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id

    if USE_FIREBASE:
        memos = get_user_notes(user_id)
    else:
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

    if USE_FIREBASE:
        memos_text = format_memos_list_firebase(memos)
    else:
        memos_text = "\n\n".join(f"{idx+1}. {m}" for idx, m in enumerate(memos))
    
    update.message.reply_text(
        f"🗑 اختر رقم المذكرة التي تريد حذفها:\n\n{memos_text}\n\n"
        "أرسل الرقم الآن، أو اضغط «إلغاء ❌».",
        reply_markup=CANCEL_KB,
    )


def handle_memo_delete_index_input(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    
    if USE_FIREBASE:
        memos = get_user_notes(user_id)
    else:
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

    if USE_FIREBASE:
        # حذف المذكرة من Firebase
        note_id = memos[idx]['id']
        success = delete_note(note_id)
        
        if success:
            WAITING_MEMO_DELETE_SELECT.discard(user_id)
            deleted_content = memos[idx].get('content', '')[:50]
            update.message.reply_text(
                f"🗑 تم حذف المذكرة:\n\n{deleted_content}...",
                reply_markup=build_memos_menu_kb(is_admin(user_id)),
            )
        else:
            WAITING_MEMO_DELETE_SELECT.discard(user_id)
            update.message.reply_text(
                "❌ حدث خطأ في حذف المذكرة. حاول مرة أخرى.",
                reply_markup=build_memos_menu_kb(is_admin(user_id)),
            )
    else:
        deleted = memos.pop(idx)
        record["heart_memos"] = memos
        save_data()

        WAITING_MEMO_DELETE_SELECT.discard(user_id)
        update.message.reply_text(
            f"🗑 تم حذف المذكرة:\n\n{deleted}",
            reply_markup=build_memos_menu_kb(is_admin(user_id)),
        )
    
    open_memos_menu(update, context)

# =================== دوال رسائل إلى النفس مع Firebase ===================

def format_letters_list_firebase(letters: List[Dict]) -> str:
    if not letters:
        return "لا توجد رسائل بعد."
    
    lines = []
    for idx, letter in enumerate(letters, start=1):
        content_preview = letter.get("content", "")[:30]
        reminder_date = letter.get("reminder_date")
        sent = letter.get("sent", False)
        
        if sent:
            status = "✅ تم إرسالها"
        elif reminder_date:
            try:
                if hasattr(reminder_date, 'strftime'):
                    reminder_dt = reminder_date
                else:
                    reminder_dt = datetime.fromisoformat(str(reminder_date).replace('Z', '+00:00'))
                
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


def open_letters_menu(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
    user_id = user.id

    WAITING_LETTER_MENU.add(user_id)
    WAITING_LETTER_ADD.discard(user_id)
    WAITING_LETTER_ADD_CONTENT.discard(user_id)
    WAITING_LETTER_REMINDER_OPTION.discard(user_id)
    WAITING_LETTER_CUSTOM_DATE.discard(user_id)
    WAITING_LETTER_DELETE_SELECT.discard(user_id)
    LETTER_CURRENT_DATA.pop(user_id, None)

    if USE_FIREBASE:
        letters = get_user_letters(user_id)
        letters_text = format_letters_list_firebase(letters)
    else:
        record = get_user_record(user)
        letters = record.get("letters_to_self", [])
        letters_text = format_letters_list(letters)
    
    kb = build_letters_menu_kb(is_admin(user_id))

    update.message.reply_text(
        f"💌 رسالة إلى نفسي:\n\n{letters_text}\n\n"
        "يمكنك كتابة رسالة إلى نفسك المستقبلية مع تذكير بعد أسبوع، شهر، أو تاريخ مخصص.\n"
        "سأرسل لك الرسالة عندما يحين الموعد المحدد 🤍",
        reply_markup=kb,
    )


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

    # حفظ الرسالة
    if USE_FIREBASE:
        letter_data = {
            "content": LETTER_CURRENT_DATA[user_id]["content"],
            "reminder_date": reminder_date.isoformat() if reminder_date else None,
            "sent": False
        }
        
        letter_id = add_letter(user_id, letter_data)
        
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
                            "letter_content": letter_data["content"],
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
            WAITING_LETTER_REMINDER_OPTION.discard(user_id)
            LETTER_CURRENT_DATA.pop(user_id, None)
            update.message.reply_text(
                "❌ حدث خطأ في حفظ الرسالة. حاول مرة أخرى.",
                reply_markup=build_letters_menu_kb(is_admin(user_id)),
            )
    else:
        # استخدام التخزين المحلي
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


def send_letter_reminder(context: CallbackContext):
    job = context.job
    user_id = job.context["user_id"]
    letter_content = job.context["letter_content"]
    letter_id = job.context.get("letter_id")
    letter_index = job.context.get("letter_index")

    try:
        if USE_FIREBASE and letter_id:
            # تحديث حالة الرسالة في Firebase
            update_letter(letter_id, {"sent": True})
        elif not USE_FIREBASE:
            # تحديث حالة الرسالة في التخزين المحلي
            uid = str(user_id)
            if uid in data:
                record = data[uid]
                letters = record.get("letters_to_self", [])
                if letter_index < len(letters):
                    letters[letter_index]["sent"] = True
                    save_data()

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
    user_id = user.id
    
    if USE_FIREBASE:
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
            created_at = letter.get("created_at")
            reminder_date = letter.get("reminder_date")
            sent = letter.get("sent", False)

            # تنسيق تاريخ الإنشاء
            created_str = "تاريخ غير معروف"
            if created_at:
                try:
                    if hasattr(created_at, 'strftime'):
                        created_str = created_at.strftime("%Y-%m-%d")
                    else:
                        created_str = str(created_at)[:10]
                except:
                    pass

            if reminder_date:
                try:
                    if hasattr(reminder_date, 'strftime'):
                        reminder_dt = reminder_date
                    else:
                        reminder_dt = datetime.fromisoformat(str(reminder_date).replace('Z', '+00:00'))
                    
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
    else:
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
        reply_markup=build_letters_menu_kb(is_admin(user_id)),
    )

# =================== الجزء المتبقي من الدوال الأساسية ===================

# ملاحظة: سأستمر في الملف الأصلي لكن سأعدل الدوال لتعمل مع Firebase
# بما أن الملف طويل جداً، سأركز على التعديلات الأساسية فقط

# =================== منبّه الماء مع Firebase ===================

def handle_log_cup(update: Update, context: CallbackContext):
    user = update.effective_user
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return
    
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
    
    # تحديث عدد الأكواب
    if USE_FIREBASE:
        update_user_record(user.id, {"today_cups": new_cups})
        # تسجيل في سجل الماء
        add_water_log(user.id, 1)
    else:
        record["today_cups"] = new_cups

    add_points(user.id, POINTS_PER_WATER_CUP, context)

    cups_goal = record.get("cups_goal")
    if cups_goal and before < cups_goal <= new_cups:
        add_points(user.id, POINTS_WATER_DAILY_BONUS, context)

    if not USE_FIREBASE:
        save_data()

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
    
    text = (update.message.text or "").strip()

    if not record.get("cups_goal"):
        update.message.reply_text(
            "قبل استخدام هذه الميزة، احسب احتياجك من الماء أولًا من خلال:\n"
            "«إعدادات الماء ⚙️» → «حساب احتياج الماء 🧮».",
            reply_markup=water_menu_keyboard(user_id),
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
            reply_markup=water_menu_keyboard(user_id),
        )
        return

    ensure_today_water(record)
    before = record.get("today_cups", 0)
    new_cups = before + cups
    
    # تحديث عدد الأكواب
    if USE_FIREBASE:
        update_user_record(user_id, {"today_cups": new_cups})
        # تسجيل في سجل الماء
        add_water_log(user_id, cups)
    else:
        record["today_cups"] = new_cups

    add_points(user_id, cups * POINTS_PER_WATER_CUP, context)

    cups_goal = record.get("cups_goal")
    if cups_goal and before < cups_goal <= new_cups:
        add_points(user_id, POINTS_WATER_DAILY_BONUS, context)

    if not USE_FIREBASE:
        save_data()

    check_daily_full_activity(user_id, record, context)

    status_text = format_water_status_text(record)
    update.message.reply_text(
        f"🥤 تم إضافة {cups} كوب إلى عدّادك اليوم.\n\n{status_text}",
        reply_markup=water_menu_keyboard(user_id),
    )


# =================== ورد القرآن مع Firebase ===================

def handle_quran_add_pages_input(update: Update, context: CallbackContext):
    user = update.effective_user
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
    new_pages = before + pages
    
    # تحديث عدد الصفحات
    if USE_FIREBASE:
        update_user_record(user_id, {"quran_pages_today": new_pages})
    else:
        record["quran_pages_today"] = new_pages

    add_points(user_id, pages * POINTS_PER_QURAN_PAGE, context)

    goal = record.get("quran_pages_goal")
    if goal and before < goal <= new_pages:
        add_points(user_id, POINTS_QURAN_DAILY_BONUS, context)

    if not USE_FIREBASE:
        save_data()

    check_daily_full_activity(user_id, record, context)

    WAITING_QURAN_ADD_PAGES.discard(user_id)

    status_text = format_quran_status_text(record)
    update.message.reply_text(
        f"تم إضافة {pages} صفحة إلى وردك اليوم.\n\n{status_text}",
        reply_markup=quran_menu_keyboard(user_id),
    )


# =================== الاحصائيات مع Firebase ===================

def handle_stats(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    record = get_user_record(user)
    
    # التحقق إذا كان المستخدم محظورًا
    if record.get("is_banned", False):
        return

    ensure_today_water(record)
    ensure_today_quran(record)

    cups_goal = record.get("cups_goal")
    today_cups = record.get("today_cups", 0)

    q_goal = record.get("quran_pages_goal")
    q_today = record.get("quran_pages_today", 0)

    tasbih_total = record.get("tasbih_total", 0)
    adhkar_count = record.get("adhkar_count", 0)

    if USE_FIREBASE:
        memos_count = len(get_user_notes(user_id))
        letters_count = len(get_user_letters(user_id))
    else:
        memos_count = len(record.get("heart_memos", []))
        letters_count = len(record.get("letters_to_self", []))

    points = record.get("points", 0)
    level = record.get("level", 0)
    medals = record.get("medals", [])

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
    if medals:
        text_lines.append("- ميدالياتك: " + "، ".join(medals))

    update.message.reply_text(
        "\n".join(text_lines),
        reply_markup=user_main_keyboard(user_id),
    )


# =================== نظام الحظر مع Firebase ===================

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
    
    if USE_FIREBASE:
        target_data = get_user_record(target_id)
        if not target_data:
            WAITING_BAN_REASON.discard(user_id)
            BAN_TARGET_ID.pop(user_id, None)
            update.message.reply_text(
                "❌ المستخدم غير موجود!",
                reply_markup=admin_panel_keyboard_for(user_id),
            )
            return

        # تطبيق الحظر في Firebase
        ban_data = {
            "is_banned": True,
            "banned_by": user_id,
            "banned_at": firestore.SERVER_TIMESTAMP,
            "ban_reason": text
        }
        
        success = update_user_record(target_id, ban_data)
        
        if success:
            target_name = target_data.get("first_name", "مستخدم") or "مستخدم"
            
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
                    admin_name = get_user_record(user_id).get("first_name", "المشرفة") or "المشرفة"
                    context.bot.send_message(
                        chat_id=ADMIN_ID,
                        text=f"⚠️ تم حظر مستخدم بواسطة المشرفة:\n\n"
                             f"المستخدم: {target_name} (ID: {target_id})\n"
                             f"السبب: {text}\n"
                             f"بواسطة: {admin_name}"
                    )
                except Exception as e:
                    logger.error(f"Error notifying admin about ban: {e}")

            WAITING_BAN_REASON.discard(user_id)
            BAN_TARGET_ID.pop(user_id, None)

            update.message.reply_text(
                f"✅ تم حظر المستخدم: {target_name} (ID: {target_id}) بنجاح.\n"
                f"السبب: {text}",
                reply_markup=admin_panel_keyboard_for(user_id),
            )
        else:
            WAITING_BAN_REASON.discard(user_id)
            BAN_TARGET_ID.pop(user_id, None)
            update.message.reply_text(
                "❌ حدث خطأ في حظر المستخدم. حاول مرة أخرى.",
                reply_markup=admin_panel_keyboard_for(user_id),
            )
    else:
        # استخدام التخزين المحلي
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


# =================== نظام الدعم ولوحة التحكم مع Firebase ===================

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

    if USE_FIREBASE:
        users = get_all_users()
        lines = []
        for user_data in users[:200]:  # عرض أول 200 مستخدم فقط
            name = user_data.get("first_name") or "بدون اسم"
            username = user_data.get("username")
            is_banned = user_data.get("is_banned", False)
            status = "🚫" if is_banned else "✅"
            
            line = f"{status} {name} | ID: {user_data['id']}"
            if username:
                line += f" | @{username}"
            
            if is_banned:
                line += " (محظور)"
            
            lines.append(line)
    else:
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


# =================== المنافسات و المجتمع مع Firebase ===================

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
        if rec.get("user_id") == user_id or rec.get("id") == user_id:
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


# =================== الجرعة التحفيزية مع Firebase ===================

def get_motivation_settings():
    """يرجع إعدادات الجرعة التحفيزية من Firebase"""
    if USE_FIREBASE:
        config = get_global_config()
        return {
            'hours': config.get('motivation_hours', DEFAULT_MOTIVATION_HOURS_UTC),
            'messages': config.get('motivation_messages', DEFAULT_MOTIVATION_MESSAGES)
        }
    else:
        return {
            'hours': MOTIVATION_HOURS_UTC,
            'messages': MOTIVATION_MESSAGES
        }


def update_motivation_settings(hours=None, messages=None):
    """يحدث إعدادات الجرعة التحفيزية في Firebase"""
    if USE_FIREBASE:
        config = get_global_config()
        if hours is not None:
            config['motivation_hours'] = hours
        if messages is not None:
            config['motivation_messages'] = messages
        
        save_global_config(config)
        return True
    else:
        global MOTIVATION_HOURS_UTC, MOTIVATION_MESSAGES
        if hours is not None:
            MOTIVATION_HOURS_UTC = hours
        if messages is not None:
            MOTIVATION_MESSAGES = messages
        
        # حفظ في التخزين المحلي
        cfg = get_global_config()
        cfg["motivation_hours"] = MOTIVATION_HOURS_UTC
        cfg["motivation_messages"] = MOTIVATION_MESSAGES
        save_data()
        return True


# =================== بدء تشغيل البوت ===================

def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN غير موجود في متغيرات البيئة!")

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

    # تذكيرات الماء
    REMINDER_HOURS_UTC = [7, 10, 13, 16, 19]
    for h in REMINDER_HOURS_UTC:
        job_queue.run_daily(
            water_reminder_job,
            time=time(hour=h, minute=0, tzinfo=pytz.UTC),
            name=f"water_reminder_{h}",
        )

    # الجرعة التحفيزية
    global CURRENT_MOTIVATION_JOBS
    CURRENT_MOTIVATION_JOBS = []
    
    motivation_settings = get_motivation_settings()
    motivation_hours = motivation_settings['hours']
    
    for h in motivation_hours:
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
    logger.info(f"Firebase Status: {'✅ Connected' if USE_FIREBASE else '❌ Not Connected'}")
    updater.start_polling()
    updater.idle()
# ================ كود الترحيل المدمج ================

def simple_migrate():
    """يرحّل البيانات ببساطة"""
    print("🔍 فحص البيانات للترحيل...")
    
    # 1. فحص اتصال Firebase
    if not USE_FIREBASE:
        print("❌ Firebase غير متصل، لا يمكن الترحيل")
        return
    
    # 2. فحص وجود بيانات قديمة
    if not os.path.exists("suqya_users.json"):
        print("✅ لا توجد بيانات قديمة")
        return
    
    # 3. ترحيل البيانات
    try:
        import json
        from datetime import datetime
        
        print("📖 قراءة البيانات القديمة...")
        with open("suqya_users.json", "r", encoding="utf-8") as f:
            old_data = json.load(f)
        
        migrated = 0
        for user_id_str, user_data in old_data.items():
            if user_id_str == "_global_config":
                continue
            
            try:
                user_id = int(user_id_str)
                
                # تحويل التواريخ
                for date_field in ["created_at", "last_active", "banned_at"]:
                    if user_data.get(date_field):
                        try:
                            dt = datetime.fromisoformat(user_data[date_field].replace('Z', '+00:00'))
                            user_data[date_field] = dt
                        except:
                            pass
                
                # إضافة user_id
                user_data["user_id"] = user_id
                
                # الحفظ في Firebase
                save_user_record(user_id, user_data)
                migrated += 1
                
                if migrated % 10 == 0:
                    print(f"✅ تم ترحيل {migrated} مستخدم...")
                    
            except Exception as e:
                print(f"⚠️ خطأ في ترحيل المستخدم {user_id_str}: {e}")
        
        print(f"🎉 تم الانتهاء! تم ترحيل {migrated} مستخدم")
        
        # نسخة احتياطية
        import shutil
        shutil.copy("suqya_users.json", "suqya_users.json.backup")
        print("📦 تم عمل نسخة احتياطية")
        
    except Exception as e:
        print(f"❌ خطأ عام في الترحيل: {e}")

# تشغيل الترحيل
simple_migrate()

if __name__ == "__main__":
    main()
