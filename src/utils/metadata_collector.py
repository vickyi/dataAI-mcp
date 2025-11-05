# metadata_collector.py
import pymysql
import sqlite3
import os
from typing import Dict, List, Tuple
from .db_config import db_config

class MetadataCollector:
    """元数据采集器"""

    def __init__(self, sqlite_db_path: str = "metadata.db"):
        """初始化元数据采集器

        Args:
            sqlite_db_path: 本地SQLite数据库路径
        """
        self.sqlite_db_path = sqlite_db_path
        self._init_sqlite_db()

    def _init_sqlite_db(self):
        """初始化SQLite数据库表结构"""
        conn = sqlite3.connect(self.sqlite_db_path)
        cursor = conn.cursor()

        # 创建表元数据表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tables_meta (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_schema TEXT NOT NULL,
                table_name TEXT NOT NULL,
                table_comment TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(table_schema, table_name)
            )
        ''')

        # 创建字段元数据表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS columns_meta (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_schema TEXT NOT NULL,
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                data_type TEXT,
                is_nullable TEXT,
                column_comment TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(table_schema, table_name, column_name)
            )
        ''')

        # 创建表血缘关系表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS table_lineage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_table TEXT NOT NULL,
                target_table TEXT NOT NULL,
                source_column TEXT,
                target_column TEXT,
                transform_rule TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # 创建业务术语表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS business_terms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                term_name TEXT NOT NULL UNIQUE,
                term_definition TEXT,
                related_tables TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        conn.commit()
        conn.close()

    def connect_to_mysql(self, db_name: str):
        """连接到MySQL数据库

        Args:
            db_name: 数据库名称 ('bigdata_db' 或 'user_profile_db')

        Returns:
            MySQL连接对象

        Raises:
            ValueError: 当数据库配置不存在时
            Exception: 当连接失败时
        """
        config = db_config.get_config(db_name)
        if not config:
            raise ValueError(f"数据库配置 {db_name} 不存在")

        try:
            connection = pymysql.connect(
                host=config['host'],
                port=config['port'],
                user=config['user'],
                password=config['password'],
                database=config['database'],
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            return connection
        except Exception as e:
            raise Exception(f"连接到 {db_name} 失败: {str(e)}")

    def collect_table_metadata(self, db_name: str) -> List[Dict]:
        """从MySQL收集表元数据

        Args:
            db_name: 数据库名称

        Returns:
            表元数据列表
        """
        connection = self.connect_to_mysql(db_name)
        try:
            with connection.cursor() as cursor:
                # 收集表结构和注释信息
                cursor.execute("""
                    SELECT
                        table_schema,
                        table_name,
                        column_name,
                        data_type,
                        is_nullable,
                        column_comment
                    FROM information_schema.columns
                    WHERE table_schema IN ('ods', 'dw', 'dim', 'dws', 'app')
                    ORDER BY table_schema, table_name, ordinal_position
                """)
                columns_data = cursor.fetchall()

                # 收集表注释信息
                cursor.execute("""
                    SELECT
                        table_schema,
                        table_name,
                        table_comment
                    FROM information_schema.tables
                    WHERE table_schema IN ('ods', 'dw', 'dim', 'dws', 'app')
                """)
                tables_data = cursor.fetchall()

                return {
                    'columns': columns_data,
                    'tables': tables_data
                }
        finally:
            connection.close()

    def collect_lineage_data(self, db_name: str) -> List[Dict]:
        """从MySQL收集血缘关系数据

        Args:
            db_name: 数据库名称

        Returns:
            血缘关系数据列表
        """
        connection = self.connect_to_mysql(db_name)
        try:
            with connection.cursor() as cursor:
                # 收集表间血缘关系
                cursor.execute("""
                    SELECT
                        source_table,
                        target_table,
                        source_column,
                        target_column,
                        transform_rule
                    FROM data_lineage_table_relations
                """)
                lineage_data = cursor.fetchall()

                return lineage_data
        finally:
            connection.close()

    def save_to_sqlite(self, metadata: Dict):
        """将元数据保存到SQLite数据库

        Args:
            metadata: 包含表和字段元数据的字典
        """
        conn = sqlite3.connect(self.sqlite_db_path)
        cursor = conn.cursor()

        # 保存表元数据
        if 'tables' in metadata:
            for table in metadata['tables']:
                cursor.execute('''
                    INSERT OR REPLACE INTO tables_meta
                    (table_schema, table_name, table_comment)
                    VALUES (?, ?, ?)
                ''', (
                    table['table_schema'],
                    table['table_name'],
                    table['table_comment']
                ))

        # 保存字段元数据
        if 'columns' in metadata:
            for column in metadata['columns']:
                cursor.execute('''
                    INSERT OR REPLACE INTO columns_meta
                    (table_schema, table_name, column_name, data_type, is_nullable, column_comment)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    column['table_schema'],
                    column['table_name'],
                    column['column_name'],
                    column['data_type'],
                    column['is_nullable'],
                    column['column_comment']
                ))

        # 保存血缘关系数据
        if 'lineage' in metadata:
            for relation in metadata['lineage']:
                cursor.execute('''
                    INSERT INTO table_lineage
                    (source_table, target_table, source_column, target_column, transform_rule)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    relation['source_table'],
                    relation['target_table'],
                    relation.get('source_column'),
                    relation.get('target_column'),
                    relation.get('transform_rule')
                ))

        conn.commit()
        conn.close()

    def sync_metadata(self):
        """同步所有元数据"""
        print("开始同步元数据...")

        # 收集bigdata_db的元数据
        try:
            print("正在收集bigdata_db元数据...")
            bigdata_metadata = self.collect_table_metadata('bigdata_db')
            print(f"收集到 {len(bigdata_metadata['tables'])} 个表，{len(bigdata_metadata['columns'])} 个字段")
        except Exception as e:
            print(f"收集bigdata_db元数据失败: {e}")
            bigdata_metadata = {'tables': [], 'columns': []}

        # 收集user_profile_db的血缘关系数据
        try:
            print("正在收集user_profile_db血缘关系数据...")
            lineage_data = self.collect_lineage_data('user_profile_db')
            print(f"收集到 {len(lineage_data)} 条血缘关系")
        except Exception as e:
            print(f"收集user_profile_db血缘关系数据失败: {e}")
            lineage_data = []

        # 保存到SQLite
        print("正在保存元数据到本地数据库...")
        self.save_to_sqlite({
            'tables': bigdata_metadata['tables'],
            'columns': bigdata_metadata['columns'],
            'lineage': lineage_data
        })

        print("元数据同步完成")

# 全局元数据采集器实例
metadata_collector = MetadataCollector()