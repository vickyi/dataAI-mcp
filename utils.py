# utils.py
import os
from dotenv import load_dotenv
from typing import Optional

def setup_environment(env_file: str = ".env") -> bool:
    """
    设置环境变量 - 通用工具函数
    """
    try:
        if not os.path.exists(env_file):
            print(f"⚠️  警告: {env_file} 文件不存在")
            return False

        load_dotenv(env_file)
        print(f"✅ 环境变量已从 {env_file} 加载")
        return True

    except Exception as e:
        print(f"❌ 环境设置失败: {e}")
        return False

def get_env(key: str, default: Optional[str] = None) -> str:
    """获取环境变量的便捷函数"""
    value = os.getenv(key, default)
    if value is None:
        raise ValueError(f"环境变量 {key} 未设置")
    return value