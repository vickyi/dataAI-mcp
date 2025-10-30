# config.py
import os
from dotenv import load_dotenv
from typing import Optional

def setup_environment(env_file: str = ".env") -> bool:
    """
    从.env文件加载环境变量

    Args:
        env_file: .env文件路径

    Returns:
        bool: 是否成功加载
    """
    try:
        # 加载.env文件
        if not os.path.exists(env_file):
            print(f"⚠️  警告: {env_file} 文件不存在")
            return False

        load_dotenv(env_file)
        print(f"✅ 已从 {env_file} 加载环境变量")

        # 验证必要的环境变量
        required_vars = ['DEEPSEEK_API_KEY']
        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            print(f"❌ 缺少必要的环境变量: {', '.join(missing_vars)}")
            return False

        print("✅ 所有必要的环境变量已配置")
        return True

    except Exception as e:
        print(f"❌ 加载环境变量失败: {e}")
        return False

def get_env_variable(key: str, default: Optional[str] = None) -> str:
    """
    获取环境变量，如果不存在则返回默认值或抛出异常

    Args:
        key: 环境变量名
        default: 默认值

    Returns:
        str: 环境变量值
    """
    value = os.getenv(key, default)
    if value is None:
        raise ValueError(f"环境变量 {key} 未设置")
    return value

# 提供便捷的配置访问
class Config:
    """配置类"""

    @property
    def deepseek_api_key(self) -> str:
        return get_env_variable('DEEPSEEK_API_KEY')

    @property
    def mcp_server_path(self) -> str:
        return get_env_variable('MCP_SERVER_PATH', './sql-linter-mcp-server')

    @property
    def log_level(self) -> str:
        return get_env_variable('LOG_LEVEL', 'INFO')

# 创建全局配置实例
config = Config()