import os
from dotenv import load_dotenv
from typing import Optional, Union

# Load environment variables from .env file
load_dotenv()

class Config:
    """
    Configuration class to manage all environment variables with proper type conversion and error handling
    """
    
    @staticmethod
    def get_mongo_uri() -> str:
        """Get MongoDB connection URI"""
        uri = os.getenv("MONGO_URI")
        if not uri:
            raise ValueError("MongoDB URI not found in environment variables")
        return uri
    
    @staticmethod
    def get_mongo_database() -> str:
        """Get MongoDB database name"""
        db = os.getenv("MONGO_DATABASE")
        if not db:
            raise ValueError("MongoDB database name not found in environment variables")
        return db
    
    @staticmethod
    def get_mongo_collection() -> str:
        """Get MongoDB collection name"""
        collection = os.getenv("MONGO_COLLECTION")
        if not collection:
            raise ValueError("MongoDB collection name not found in environment variables")
        return collection
    
    @staticmethod
    def get_mongo_connection_timeout() -> int:
        """Get MongoDB connection timeout in milliseconds"""
        return int(os.getenv("MONGO_CONNECTION_TIMEOUT", "30000"))
    
    @staticmethod
    def get_mongo_server_selection_timeout() -> int:
        """Get MongoDB server selection timeout in milliseconds"""
        return int(os.getenv("MONGO_SERVER_SELECTION_TIMEOUT", "30000"))
    
    @staticmethod
    def get_mongo_socket_timeout() -> int:
        """Get MongoDB socket timeout in milliseconds"""
        return int(os.getenv("MONGO_SOCKET_TIMEOUT", "20000"))
    
    @staticmethod
    def get_mongo_max_pool_size() -> int:
        """Get MongoDB connection pool max size"""
        return int(os.getenv("MONGO_MAX_POOL_SIZE", "10"))
    
    @staticmethod
    def get_mongo_retry_writes() -> bool:
        """Get MongoDB retry writes setting"""
        return os.getenv("MONGO_RETRY_WRITES", "true").lower() == "true"
    
    @staticmethod
    def get_app_title() -> str:
        """Get application title"""
        return os.getenv("APP_TITLE", "Coffee Dashboard")
    
    @staticmethod
    def is_debug_mode() -> bool:
        """Check if debug mode is enabled"""
        return os.getenv("DEBUG", "false").lower() == "true"
    
    @staticmethod
    def get_environment() -> str:
        """Get current environment (development/production)"""
        return os.getenv("ENVIRONMENT", "development")
    
    @staticmethod
    def get_cache_ttl() -> int:
        """Get cache TTL in seconds"""
        return int(os.getenv("CACHE_TTL", "3600"))
    
    @staticmethod
    def get_secret_key() -> str:
        """Get application secret key"""
        key = os.getenv("SECRET_KEY")
        if not key:
            raise ValueError("Secret key not found in environment variables")
        return key
    
    @staticmethod
    def get_log_level() -> str:
        """Get logging level"""
        return os.getenv("LOG_LEVEL", "INFO").upper()
    
    @staticmethod
    def get_streamlit_port() -> int:
        """Get Streamlit server port"""
        return int(os.getenv("STREAMLIT_SERVER_PORT", "8501"))
    
    @staticmethod
    def get_streamlit_address() -> str:
        """Get Streamlit server address"""
        return os.getenv("STREAMLIT_SERVER_ADDRESS", "0.0.0.0")

# Singleton configuration instance
config = Config()