# test_metadata_collector.py
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.metadata_collector import metadata_collector

def test_metadata_collector():
    """测试元数据采集器"""
    print("测试元数据采集器...")

    # 测试同步元数据
    try:
        metadata_collector.sync_metadata()
        print("元数据同步测试通过")
    except Exception as e:
        print(f"元数据同步测试失败: {e}")

    print("测试完成")

if __name__ == "__main__":
    test_metadata_collector()