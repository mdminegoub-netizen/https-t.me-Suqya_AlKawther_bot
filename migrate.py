#!/usr/bin/env python3
"""
سكربت لترحيل البيانات من JSON المحلي إلى Firebase Firestore
يتم تشغيله مرة واحدة فقط بعد تثبيت Firebase
"""

import os
import json
import logging
import firebase_admin
from firebase_admin import credentials, firestore

# إعدادات
DATA_FILE = "suqya_users.json"
SECRETS_PATH = "/etc/secrets"

# إعداد التسجيل
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def initialize_firebase():
    """تهيئة اتصال Firebase"""
    try:
        firebase_files = []
        
        if os.path.exists(SECRETS_PATH):
            for file in os.listdir(SECRETS_PATH):
                if file.startswith("soqya-") and file.endswith(".json"):
                    firebase_files.append(os.path.join(SECRETS_PATH, file))
        
        if firebase_files:
            cred_path = firebase_files[0]
            logger.info(f"تم العثور على ملف Firebase: {cred_path}")
            
            if not firebase_admin._apps:
                cred = credentials.Certificate(cred_path)
                firebase_admin.initialize_app(cred)
                logger.info("✅ تم تهيئة Firebase بنجاح")
            else:
                logger.info("✅ Firebase مفعل بالفعل")
                
            return firestore.client()
        else:
            logger.error("❌ لم يتم العثور على ملف Firebase")
            return None
            
    except Exception as e:
        logger.error(f"❌ خطأ في تهيئة Firebase: {e}")
        return None

def load_local_data():
    """تحميل البيانات المحلية"""
    if not os.path.exists(DATA_FILE):
        logger.error(f"❌ ملف البيانات {DATA_FILE} غير موجود")
        return {}
    
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"❌ خطأ في تحميل البيانات: {e}")
        return {}

def migrate_users(db, data):
    """ترحيل بيانات المستخدمين"""
    users_ref = db.collection("users")
    migrated = 0
    
    for user_id_str, user_data in data.items():
        if user_id_str == "GLOBAL_KEY":
            continue
            
        try:
            # تحقق من صحة البيانات
            if not isinstance(user_data, dict):
                continue
                
            # إضافة معرف المستخدم إذا لم يكن موجوداً
            user_data["user_id"] = int(user_id_str)
            
            # معالجة المذكرات
            heart_memos = user_data.get("heart_memos", [])
            if heart_memos and isinstance(heart_memos, list):
                notes_ref = db.collection("notes")
                for memo_text in heart_memos:
                    if memo_text and memo_text.strip():
                        note_data = {
                            "user_id": int(user_id_str),
                            "text": memo_text.strip(),
                            "created_at": user_data.get("created_at", ""),
                            "updated_at": user_data.get("last_active", "")
                        }
                        notes_ref.add(note_data)
                
                # إزالة المذكرات من بيانات المستخدم
                user_data.pop("heart_memos", None)
            
            # معالجة الرسائل
            letters = user_data.get("letters_to_self", [])
            if letters and isinstance(letters, list):
                letters_ref = db.collection("letters")
                for letter in letters:
                    if isinstance(letter, dict) and letter.get("content"):
                        letter["user_id"] = int(user_id_str)
                        letters_ref.add(letter)
                
                # إزالة الرسائل من بيانات المستخدم
                user_data.pop("letters_to_self", None)
            
            # حفظ بيانات المستخدم
            users_ref.document(user_id_str).set(user_data)
            migrated += 1
            
            if migrated % 10 == 0:
                logger.info(f"تم ترحيل {migrated} مستخدم...")
                
        except Exception as e:
            logger.error(f"خطأ في ترحيل المستخدم {user_id_str}: {e}")
    
    return migrated

def migrate_benefits(db, data):
    """ترحيل الفوائد والنصائح"""
    if "GLOBAL_KEY" not in data:
        return 0
    
    global_config = data["GLOBAL_KEY"]
    benefits = global_config.get("benefits", [])
    
    if not benefits:
        return 0
    
    tips_ref = db.collection("tips")
    migrated = 0
    
    for benefit in benefits:
        try:
            if isinstance(benefit, dict) and benefit.get("text"):
                tips_ref.add(benefit)
                migrated += 1
        except Exception as e:
            logger.error(f"خطأ في ترحيل الفائدة: {e}")
    
    return migrated

def migrate_global_config(db, data):
    """ترحيل الإعدادات العامة"""
    if "GLOBAL_KEY" not in data:
        return
    
    global_config = data["GLOBAL_KEY"]
    
    config_data = {
        "motivation_hours": global_config.get("motivation_hours", [6, 9, 12, 15, 18, 21]),
        "motivation_messages": global_config.get("motivation_messages", []),
        "benefits": []  # الفوائد محفوظة منفصلة الآن
    }
    
    try:
        db.collection("global_config").document("config").set(config_data)
        logger.info("✅ تم ترحيل الإعدادات العامة")
    except Exception as e:
        logger.error(f"خطأ في ترحيل الإعدادات العامة: {e}")

def create_backup(data):
    """إنشاء نسخة احتياطية من البيانات"""
    try:
        backup_file = f"{DATA_FILE}.backup"
        with open(backup_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"✅ تم إنشاء نسخة احتياطية في {backup_file}")
    except Exception as e:
        logger.error(f"خطأ في إنشاء النسخة الاحتياطية: {e}")

def main():
    """الدالة الرئيسية للترحيل"""
    logger.info("🚀 بدء عملية ترحيل البيانات إلى Firebase Firestore...")
    
    # تهيئة Firebase
    db = initialize_firebase()
    if not db:
        return
    
    # تحميل البيانات المحلية
    data = load_local_data()
    if not data:
        return
    
    # إنشاء نسخة احتياطية
    create_backup(data)
    
    # ترحيل البيانات
    logger.info("📤 ترحيل بيانات المستخدمين...")
    users_migrated = migrate_users(db, data)
    
    logger.info("📤 ترحيل الفوائد والنصائح...")
    benefits_migrated = migrate_benefits(db, data)
    
    logger.info("📤 ترحيل الإعدادات العامة...")
    migrate_global_config(db, data)
    
    # النتيجة النهائية
    logger.info("=" * 50)
    logger.info("✅ عملية الترحيل اكتملت بنجاح!")
    logger.info(f"📊 تم ترحيل {users_migrated} مستخدم")
    logger.info(f"📊 تم ترحيل {benefits_migrated} فائدة/نصيحة")
    logger.info("=" * 50)
    logger.info("⚠️ يمكنك الآن حذف ملف suqya_users.json بعد التأكد من عمل البوت")
    logger.info("ℹ️ تم حفظ نسخة احتياطية في suqya_users.json.backup")

if __name__ == "__main__":
    main()
