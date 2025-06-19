import psycopg2
from sshtunnel import SSHTunnelForwarder
import paramiko


class PostgresOp:
    _instance = {}

    def __new__(cls, ssh_host, *args, **kwargs):
        key = str(kwargs)
        if key not in cls._instance:
            cls._instance[key] = super(PostgresOp, cls).__new__(cls)
        return cls._instance[key]

    def __init__(self, ssh_host, ssh_port, ssh_username, ssh_pkey=None,
                 use_ssh=True, host='localhost', port=5432,
                 database=None, username=None, password=None, **kwargs):
        """
        初始化 SSH 和 PostgreSQL 连接参数
        :param ssh_host: SSH 服务器地址
        :param ssh_port: SSH 服务器端口
        :param ssh_username: SSH 用户名
        :param ssh_pkey: SSH 私钥路径或 paramiko.PKey 对象（可选）
        :param host: 远程 PostgreSQL 地址（从 SSH 服务器可访问）
        :param port: 远程 PostgreSQL 端口
        :param database: 数据库名称
        :param username: 数据库用户名
        :param password: 数据库密码
        """
        self.use_ssh = use_ssh
        self.ssh_host = ssh_host
        self.ssh_port = ssh_port
        self.ssh_username = ssh_username
        self.ssh_pkey = ssh_pkey
        self.remote_db_host = host
        self.remote_db_port = port
        self.db_name = database
        self.db_user = username
        self.db_password = password

        self.tunnel = None
        self.conn = None

    def connect(self):
        if self.use_ssh:
            self.tunnel = SSHTunnelForwarder(
                (self.ssh_host, self.ssh_port),
                ssh_username=self.ssh_username,
                ssh_pkey=paramiko.RSAKey.from_private_key_file(self.ssh_pkey),
                remote_bind_address=(self.remote_db_host, self.remote_db_port)
            )
            self.tunnel.start()

            # 连接 PostgreSQL
            self.conn = psycopg2.connect(
                host='localhost',
                port=self.tunnel.local_bind_port,
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password
            )
        else:
            self.conn = psycopg2.connect(
                host=self.remote_db_host,
                port=self.remote_db_port,
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password
            )

    def disconnect(self):
        """关闭数据库连接和 SSH 隧道"""
        try:
            if self.conn and not self.conn.closed:
                self.conn.close()
        except Exception as e:
            pass
        finally:
            self.conn = None

        try:
            if self.tunnel and self.tunnel.is_active:
                self.tunnel.stop()
        except Exception as e:
            pass
        finally:
            self.tunnel = None

    def __enter__(self):
        """支持上下文管理器，进入时连接"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出时断开连接"""
        self.disconnect()

    def execute_query(self, sql, params=None, with_columns=False):
        """
        执行查询语句
        :param sql: SQL 查询语句
        :param params: 查询参数（可选）
        :param with_columns: 是否返回字段名（默认 False）
        :return: 查询结果（列表）
        """
        if self.conn is None:
            self.connect()
        if self.conn.closed:
            self.connect()
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql, params)
            # 获取查询结果
            rows = cursor.fetchall()
            if with_columns:
                # 获取字段名
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in rows]
            return rows
        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            cursor.close()

    def execute_update(self, sql, params=None, autocommit=True):
        """
        执行更新语句（INSERT/UPDATE/DELETE）
        :param sql: SQL 更新语句
        :param params: 查询参数（可选）
        :param autocommit: 是否自动提交事务（默认 True）
        :return: 受影响的行数
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql, params)
            rowcount = cursor.rowcount
            if autocommit:
                self.conn.commit()
            return rowcount
        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            cursor.close()

    def commit(self):
        """手动提交事务"""
        self.conn.commit()

    def rollback(self):
        """回滚事务"""
        self.conn.rollback()


if __name__ == '__main__':
    from config import CONFIG
    import pandas as pd

    with PostgresOp(**CONFIG.dev_postgres.model_dump(),
                    ssh_host=CONFIG.dev_postgres.ssh_config['host'],
                    ssh_pkey=CONFIG.dev_postgres.ssh_config['private_key_path'],
                    ssh_username=CONFIG.dev_postgres.ssh_config['username'],
                    ssh_port=CONFIG.dev_postgres.ssh_config['port']) as pg:
        result = pg.execute_query(
            "SELECT name,description,updated_at,data,endpoint_name,gradient,is_component,tags FROM flow "
            "where is_component=true limit 3")
        df = pd.DataFrame(result, columns=['name', 'description', 'updated_at', 'data', 'endpoint_name', 'gradient',
                                           'is_component', 'tags'])
        print(result)
