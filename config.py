import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    bot_token: str = os.getenv("BOT_TOKEN", "")
    mysql_host: str = os.getenv("MYSQL_HOST", "mysql")
    mysql_port: int = int(os.getenv("MYSQL_PORT", "3306"))
    mysql_db: str = os.getenv("MYSQL_DB", "chel3d_db")
    mysql_user: str = os.getenv("MYSQL_USER", "chel3d_user")
    mysql_password: str = os.getenv("MYSQL_PASSWORD", "")
    orders_chat_id: str = os.getenv("ORDERS_CHAT_ID", "")
    manager_username: str = os.getenv("MANAGER_USERNAME", "")
    placeholder_photo_path: str = os.getenv("PLACEHOLDER_PHOTO_PATH", "assets/placeholder.png")
    internal_api_key: str = os.getenv("INTERNAL_API_KEY", "")
    internal_api_host: str = os.getenv("INTERNAL_API_HOST", "0.0.0.0")
    internal_api_port: int = int(os.getenv("INTERNAL_API_PORT", "8081"))
    admin_panel_password: str = os.getenv("ADMIN_PANEL_PASSWORD", "admin123")
    secret_key: str = os.getenv("SECRET_KEY", "change-me")


settings = Settings()
