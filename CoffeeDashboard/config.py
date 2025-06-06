import os
import sys

try:
    from dotenv import load_dotenv
    load_dotenv()  # Load biến môi trường từ file .env
except ImportError:
    # Fallback nếu không có dotenv
    def load_dotenv():
        pass  # Không làm gì cả

class Config:
    def __init__(self):
        load_dotenv()
        
    # --- Database Configuration ---
    def get_mongo_uri(self):
        """Lấy MongoDB URI từ biến môi trường"""
        return os.getenv("MONGO_URI")                     
    
    def get_mongo_database(self):
        """Lấy tên database MongoDB"""
        return os.getenv("MONGO_DATABASE", "db_kf")
    
    def get_mongo_collection(self):
        """Lấy tên collection MongoDB"""
        return os.getenv("MONGO_COLLECTION", "kf_new")
    
    def get_mongo_connection_timeout(self):
        """Thời gian timeout kết nối MongoDB (ms)"""
        return int(os.getenv("MONGO_CONNECTION_TIMEOUT", 5000))  # 5 giây mặc định
    
    def get_mongo_server_selection_timeout(self):
        """Thời gian timeout chọn server MongoDB (ms)"""
        return int(os.getenv("MONGO_SERVER_SELECTION_TIMEOUT", 10000))  # 10 giây mặc định
    
    # --- Application Configuration ---
    def get_app_title(self):
        """Lấy tiêu đề ứng dụng"""
        return os.getenv("APP_TITLE", "Coffee Dashboard")
    
    def is_debug_mode(self):
        """Kiểm tra chế độ debug"""
        return os.getenv("DEBUG", "False").lower() in ("true", "1", "t")
    
    def get_environment(self):
        """Lấy môi trường hoạt động"""
        return os.getenv("ENVIRONMENT", "development")
    
    def get_cache_ttl(self):
        """Lấy thời gian sống cache (giây)"""
        return int(os.getenv("CACHE_TTL", 3600))  # 1 giờ mặc định
    
    def get_secret_key(self):
        """Lấy secret key cho ứng dụng"""
        return os.getenv("SECRET_KEY", "default-secret-key-please-change-me")
    
    def get_log_level(self):
        """Lấy mức độ logging"""
        return os.getenv("LOG_LEVEL", "INFO")

def validate_config():
    """Validate required configuration"""
    config = Config()
    required_vars = [
        "MONGO_URI",
        "MONGO_DATABASE",
        "MONGO_COLLECTION"
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Tạo instance config toàn cục
config = Config()
