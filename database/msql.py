import pymysql
from sshtunnel import SSHTunnelForwarder
import paramiko


class MySQLOp:
    _instance = {}

    def __new__(cls, ssh_host, *args, **kwargs):
        key = str(kwargs)
        if key not in cls._instance:
            cls._instance[key] = super(MySQLOp, cls).__new__(cls)
        return cls._instance[key]

    def __init__(self, ssh_host, ssh_port, ssh_username, ssh_pkey=None,
                 use_ssh=True, host='localhost', port=3306,
                 database=None, username=None, password=None, **kwargs):
        """
        初始化 SSH 和 MySQL 连接参数
        :param ssh_host: SSH 服务器地址
        :param ssh_port: SSH 服务器端口
        :param ssh_username: SSH 用户名
        :param ssh_pkey: SSH 私钥路径或 paramiko.PKey 对象（可选）
        :param use_ssh: 是否使用SSH隧道（默认True）
        :param host: 远程 MySQL 地址（从 SSH 服务器可访问）
        :param port: 远程 MySQL 端口
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
        """建立SSH隧道和MySQL连接"""
        if self.use_ssh:
            self.tunnel = SSHTunnelForwarder(
                (self.ssh_host, self.ssh_port),
                ssh_username=self.ssh_username,
                ssh_pkey=paramiko.RSAKey.from_private_key_file(self.ssh_pkey) if self.ssh_pkey else None,
                remote_bind_address=(self.remote_db_host, self.remote_db_port)
            )
            self.tunnel.start()

            # 连接 MySQL
            self.conn = pymysql.connect(
                host='127.0.0.1',
                port=self.tunnel.local_bind_port,
                user=self.db_user,
                passwd=self.db_password,
                db=self.db_name,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
        else:
            self.conn = pymysql.connect(
                host=self.remote_db_host,
                port=self.remote_db_port,
                user=self.db_user,
                passwd=self.db_password,
                db=self.db_name,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )

    def disconnect(self):
        """关闭数据库连接和 SSH 隧道"""
        try:
            if self.conn and self.conn.open:
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

    def execute_query(self, sql, params=None):
        """
        执行查询语句
        :param sql: SQL 查询语句
        :param params: 查询参数（可选）
        :return: 查询结果（字典列表）
        """
        if self.conn is None or not self.conn.open:
            self.connect()

        with self.conn.cursor() as cursor:
            try:
                cursor.execute(sql, params)
                return cursor.fetchall()
            except Exception as e:
                self.conn.rollback()
                raise e

    def execute_update(self, sql, params=None, autocommit=False):
        """
        执行更新语句（INSERT/UPDATE/DELETE）
        :param sql: SQL 更新语句
        :param params: 查询参数（可选）
        :param autocommit: 是否自动提交事务（默认 True）
        :return: 受影响的行数
        """
        if self.conn is None or not self.conn.open:
            self.connect()

        with self.conn.cursor() as cursor:
            try:
                affected_rows = cursor.execute(sql, params)
                if autocommit:
                    self.conn.commit()
                return affected_rows
            except Exception as e:
                self.conn.rollback()
                raise e

    def commit(self):
        """手动提交事务"""
        self.conn.commit()

    def rollback(self):
        """回滚事务"""
        self.conn.rollback()

    def execute_many(self, sql, params_list, autocommit=True):
        """
        批量执行SQL语句
        :param sql: SQL语句
        :param params_list: 参数列表
        :param autocommit: 是否自动提交
        :return: 受影响的行数
        """
        if self.conn is None or not self.conn.open:
            self.connect()

        with self.conn.cursor() as cursor:
            try:
                affected_rows = cursor.executemany(sql, params_list)
                if autocommit:
                    self.conn.commit()
                return affected_rows
            except Exception as e:
                self.conn.rollback()
                raise e