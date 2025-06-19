import sqlite3
from typing import Optional, List, Dict, Any
from pathlib import Path


class SQLiteOp:
    # _instance = None

    # def __new__(cls, *args, **kwargs):
    #     if cls._instance is None:
    #         cls._instance = super(SQLiteOp, cls).__new__(cls)
    #     return cls._instance

    def __init__(self):
        """
        初始化 SQLite 连接参数
        """
        self.database = Path(__file__).parent.parent / "data" / "database.db"
        self.conn: Optional[sqlite3.Connection] = None

    def connect(self):
        """建立 SQLite 连接"""
        if self.conn is None:
            self.conn = sqlite3.connect(self.database, check_same_thread=False)
            # 设置返回字典格式的结果
            self.conn.row_factory = sqlite3.Row

    def disconnect(self):
        """关闭数据库连接"""
        try:
            if self.conn is not None:
                self.conn.close()
        except Exception as e:
            pass
        finally:
            self.conn = None

    def __enter__(self):
        """支持上下文管理器，进入时连接"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出时断开连接"""
        self.disconnect()

    def execute_query(self, sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        执行查询语句
        :param sql: SQL 查询语句
        :param params: 查询参数 (可选)
        :return: 查询结果 (字典列表)
        """
        if self.conn is None:
            self.connect()

        cursor = self.conn.cursor()
        try:
            cursor.execute(sql, params or ())
            # 将 sqlite3.Row 对象转换为字典
            return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            cursor.close()

    def execute_update(self, sql: str, params: Optional[tuple] = None, autocommit: bool = True):
        """
        执行更新语句 (INSERT/UPDATE/DELETE)
        :param sql: SQL 更新语句
        :param params: 查询参数 (可选)
        :param autocommit: 是否自动提交事务 (默认 True)
        :param is_insert: 是否为 INSERT 语句
        :return: 受影响的行数
        """
        if self.conn is None:
            self.connect()

        cursor = self.conn.cursor()
        try:
            cursor.execute(sql, params or ())
            if autocommit:
                self.conn.commit()
            return cursor.rowcount
        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            cursor.close()

    def commit(self):
        """手动提交事务"""
        if self.conn is not None:
            self.conn.commit()

    def rollback(self):
        """回滚事务"""
        if self.conn is not None:
            self.conn.rollback()

    def execute_many(self, sql: str, params_list: List[tuple], autocommit: bool = True) -> int:
        """
        批量执行SQL语句
        :param sql: SQL语句
        :param params_list: 参数列表
        :param autocommit: 是否自动提交
        :return: 受影响的行数
        """
        if self.conn is None:
            self.connect()

        cursor = self.conn.cursor()
        try:
            cursor.executemany(sql, params_list)
            if autocommit:
                self.conn.commit()
            return cursor.rowcount
        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            cursor.close()

    def create_table(self, table_name: str, columns: Dict[str, str], if_not_exists: bool = True):
        """
        创建表
        :param table_name: 表名
        :param columns: 列定义字典 {列名: 数据类型和约束}
        :param if_not_exists: 是否添加 IF NOT EXISTS 子句
        """
        if_not_exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        columns_sql = ", ".join([f"{name} {definition}" for name, definition in columns.items()])
        sql = f"CREATE TABLE {if_not_exists_clause}{table_name} ({columns_sql})"
        self.execute_update(sql)

    def table_exists(self, table_name: str) -> bool:
        """
        检查表是否存在
        :param table_name: 表名
        :return: 是否存在
        """
        sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        result = self.execute_query(sql, (table_name,))
        return len(result) > 0

    def check_tables(self):
        if not self.table_exists("flow_history"):
            self.create_table("flow_history", {
                "id": "TEXT PRIMARY KEY NOT NULL UNIQUE DEFAULT (lower(hex(randomblob(16))))",  # SQLite 使用 TEXT 替代 UUID
                "name": "TEXT NOT NULL",
                "description": "TEXT",
                "data": "JSON",  # SQLite 从 3.38.0 开始支持 JSON 类型
                "user_id": "TEXT REFERENCES user(id)",  # 简化了外键约束
                "is_component": "BOOLEAN",
                "updated_at": "TIMESTAMP",
                "icon": "TEXT",
                "icon_bg_color": "TEXT",
                "folder_id": "TEXT REFERENCES folder(id)",
                "endpoint_name": "TEXT",
                "webhook": "BOOLEAN",
                "gradient": "TEXT",
                "tags": "JSON",
                "locked": "BOOLEAN",
                "fs_path": "TEXT",
                "access_type": "TEXT DEFAULT 'PRIVATE' NOT NULL",  # 用 TEXT 替代 enum
                "mcp_enabled": "BOOLEAN",
                "action_name": "TEXT",
                "action_description": "TEXT",
                "version": "TEXT NOT NULL",
                "environment": "TEXT NOT NULL",
                "created_at": "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP",
                "is_exist": "BOOLEAN DEFAULT 'TRUE' NOT NULL",
                "old_id": "TEXT NOT NULL",
            }, if_not_exists=True)

        if not self.table_exists("fuse_history"):
            self.create_table("fuse_history", {
                "id": "TEXT PRIMARY KEY NOT NULL UNIQUE DEFAULT (lower(hex(randomblob(16))))",
                "history": "TEXT",
                "name": "TEXT",
                "version": "INTEGER",
                "label": "TEXT NOT NULL",
                "operation": "TEXT",
                "created_at": "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP",
            })


if __name__ == '__main__':
    with SQLiteOp() as op:
        op.create_table("users", {"id": "INTEGER PRIMARY KEY", "name": "TEXT"})
