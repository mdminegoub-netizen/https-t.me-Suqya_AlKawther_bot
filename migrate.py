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
from datetime import datetime, timezone
from typing import Any, Optional

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


# ================================================
#  ترحيل / استكمال حقول الكتب الناقصة
# ================================================

def _normalize_bool(value: Any, default: bool) -> bool:
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


def _normalize_timestamp(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if hasattr(value, "to_datetime"):
        try:
            dt = value.to_datetime()
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    if hasattr(value, "timestamp") and not isinstance(value, (int, float)):
        try:
            ts_val = value.timestamp()
            return datetime.fromtimestamp(ts_val, tz=timezone.utc)
        except Exception:
            pass
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value, tz=timezone.utc)
        except Exception:
            return None
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None
    return None


def backfill_books_defaults(db) -> Optional[int]:
    """ملء الحقول الناقصة أو الخاطئة للكتب القديمة."""
    try:
        books_ref = db.collection("books")
        docs = books_ref.stream()
    except Exception as e:
        logger.error(f"❌ تعذر قراءة كتب Firestore: {e}")
        return None

    updated = 0
    total = 0
    for doc in docs:
        total += 1
        data = doc.to_dict() or {}
        updates = {}

        current_is_deleted = data.get("is_deleted")
        normalized_deleted = _normalize_bool(current_is_deleted, False)
        if current_is_deleted != normalized_deleted or not isinstance(current_is_deleted, bool):
            updates["is_deleted"] = normalized_deleted

        current_is_active = data.get("is_active")
        normalized_active = _normalize_bool(current_is_active, True)
        if current_is_active != normalized_active or not isinstance(current_is_active, bool):
            updates["is_active"] = normalized_active

        current_created = data.get("created_at")
        normalized_created = _normalize_timestamp(current_created)
        fallback_created = _normalize_timestamp(data.get("updated_at")) or firestore.SERVER_TIMESTAMP
        needs_created = current_created in (None, "") or normalized_created is None
        needs_created = needs_created or not isinstance(current_created, datetime) or (
            isinstance(current_created, datetime) and current_created.tzinfo is None
        )
        if needs_created:
            updates["created_at"] = fallback_created if normalized_created is None else normalized_created

        if updates:
            updates["updated_at"] = firestore.SERVER_TIMESTAMP
            try:
                books_ref.document(doc.id).update(updates)
                updated += 1
                logger.info("✅ تم تحديث كتاب %s بالحقول الافتراضية", doc.id)
            except Exception as e:
                logger.error("❌ خطأ في تحديث كتاب %s: %s", doc.id, e)

    logger.info("📚 فحص %s كتاب، تم تحديث %s منها", total, updated)
    return updated

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

    backfill_only = os.getenv("BACKFILL_BOOKS", "").strip().lower() in {"1", "true", "yes"}
    if backfill_only:
        logger.info("🛠 تشغيل مهمة استكمال حقول الكتب الناقصة فقط (BACKFILL_BOOKS=1)")
        backfill_books_defaults(db)
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
