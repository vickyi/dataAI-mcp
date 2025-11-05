# db_config.py
import os
from typing import Dict, Optional

class DatabaseConfig:
    """数据库配置管理类"""

    def __init__(self):
        """初始化数据库配置"""
        self.configs = {
            'bigdata_db': {
                'host': os.getenv('BIGDATA_DB_HOST', 'localhost'),
                'port': int(os.getenv('BIGDATA_DB_PORT', 3306)),
                'user': os.getenv('BIGDATA_DB_USER', 'root'),
                'password': os.getenv('BIGDATA_DB_PASSWORD', ''),
                'database': os.getenv('BIGDATA_DB_NAME', 'bigdata_db')
            },
            'user_profile_db': {
                'host': os.getenv('USER_PROFILE_DB_HOST', 'localhost'),
                'port': int(os.getenv('USER_PROFILE_DB_PORT', 3306)),
                'user': os.getenv('USER_PROFILE_DB_USER', 'root'),
                'password': os.getenv('USER_PROFILE_DB_PASSWORD', ''),
                'database': os.getenv('USER_PROFILE_DB_NAME', 'user_profile_db')
            }
        }

    def get_config(self, db_name: str) -> Optional[Dict]:
        """获取指定数据库的配置

        Args:
            db_name: 数据库名称 ('bigdata_db' 或 'user_profile_db')

        Returns:
            数据库配置字典或None（如果配置不存在）
        """
        return self.configs.get(db_name)

    def get_connection_string(self, db_name: str) -> Optional[str]:
        """获取数据库连接字符串

        Args:
            db_name: 数据库名称

        Returns:
            数据库连接字符串或None（如果配置不存在）
        """
        config = self.get_config(db_name)
        if not config:
            return None

        return f"mysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"

# 全局配置实例
db_config = DatabaseConfig()