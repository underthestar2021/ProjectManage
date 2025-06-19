import streamlit as st
from config import CONFIG
from database.msql import MySQLOp
from database.pg import PostgresOp
from database.sqlite import SQLiteOp


def init_session_state(key_name, value):
    if key_name not in st.session_state:
        setattr(st.session_state, key_name, value)


def get_sqlite():
    if "sqlite" not in st.session_state:
        sqlite_op = SQLiteOp()
        sqlite_op.connect()
        sqlite_op.check_tables()
        st.session_state.sqlite = sqlite_op
    return st.session_state.sqlite

def get_fuse_pg():
    if "fuse_pg" not in st.session_state:
        st.session_state.fuse_pg = get_pg(CONFIG.langfuse)
    return st.session_state.fuse_pg

def init_pg():
    if "dev_pg" not in st.session_state:
        st.session_state.dev_pg = get_pg(CONFIG.dev_postgres)
    if "test_pg" not in st.session_state:
        st.session_state.test_pg = get_pg(CONFIG.test_postgres)
    if "beta_pg" not in st.session_state:
        st.session_state.beta_pg = get_pg(CONFIG.beta_postgres)
    if "pro_pg" not in st.session_state:
        st.session_state.pro_pg = get_pg(CONFIG.pro_postgres)


def get_pg(config):
    pg = PostgresOp(**config.model_dump(),
                    ssh_host=config.ssh_config['host'],
                    ssh_pkey=config.ssh_config['private_key_path'],
                    ssh_username=config.ssh_config['username'],
                    ssh_port=config.ssh_config['port'])
    pg.connect()
    return pg


def get_env_pg(env):
    pg = None
    match env:
        case "dev" | "开发":
            pg = st.session_state.dev_pg
        case "test" | "测试":
            pg = st.session_state.test_pg
        case "stage" | "beta":
            pg = st.session_state.beta_pg
        case "pro" | "正式":
            pg = st.session_state.pro_pg
    if pg is not None and pg.conn.closed:
        pg.connect()
    return pg


def get_env_pg_user_info(env):
    match env:
        case "dev" | "开发":
            return CONFIG.dev_langflow
        case "test" | "测试":
            return CONFIG.test_langflow
        case "stage" | "beta":
            return CONFIG.beta_langflow
        case "pro" | "正式":
            return CONFIG.pro_langflow


@st.cache_data(ttl=300)
def get_user_id(env, username):
    current_pg = get_env_pg(env)
    result = current_pg.execute_query('select id from "user" where username=%s', (username,))
    current_pg.commit()
    return result[0][0]


def init_mysql():
    if "dev_mysql" not in st.session_state:
        st.session_state.dev_mysql = get_mysql(CONFIG.dev_mysql)
    if "test_mysql" not in st.session_state:
        st.session_state.test_mysql = get_mysql(CONFIG.test_mysql)
    if "beta_mysql" not in st.session_state:
        st.session_state.beta_mysql = get_mysql(CONFIG.beta_mysql)
    if "pro_mysql" not in st.session_state:
        st.session_state.pro_mysql = get_mysql(CONFIG.pro_mysql)


def get_mysql(config):
    mysql = MySQLOp(**config.model_dump(),
                    ssh_host=config.ssh_config['host'],
                    ssh_pkey=config.ssh_config['private_key_path'],
                    ssh_username=config.ssh_config['username'],
                    ssh_port=config.ssh_config['port'])
    mysql.connect()
    return mysql


def get_env_mysql(env):
    mysql = None
    match env:
        case "dev" | "开发":
            mysql = st.session_state.dev_mysql
        case "test" | "测试":
            mysql = st.session_state.test_mysql
        case "stage" | "beta":
            mysql = st.session_state.beta_mysql
        case "pro" | "正式":
            mysql = st.session_state.pro_mysql
    if mysql is not None and not mysql.conn.open:
        mysql.connect()
    return mysql
