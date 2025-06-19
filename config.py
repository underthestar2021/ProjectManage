from typing import Dict, Any
from pathlib import Path
from pydantic import BaseModel, field_validator

import toml


class DatabaseConfig(BaseModel):
    host: str
    port: int
    username: str
    password: str
    database: str
    use_ssh: bool
    ssh_config: Dict[str, Any]

    @field_validator('ssh_config')
    @classmethod
    def check_ssh_config(cls, v: Any):
        ssh_config = v
        if "private_key_path" in v:
            ssh_config["private_key_path"] = Path(__file__).parent.joinpath("config", v["private_key_path"]).resolve()
        return ssh_config


class LangflowConfig(BaseModel):
    url: str
    username: str
    password: str

class AppConfig(BaseModel):
    dev_postgres: DatabaseConfig
    test_postgres: DatabaseConfig
    beta_postgres: DatabaseConfig
    pro_postgres: DatabaseConfig
    dev_mysql: DatabaseConfig
    test_mysql: DatabaseConfig
    beta_mysql: DatabaseConfig
    pro_mysql: DatabaseConfig
    dev_langflow: LangflowConfig
    test_langflow: LangflowConfig
    beta_langflow: LangflowConfig
    pro_langflow: LangflowConfig

    langfuse: DatabaseConfig


# 读取和解析TOML文件
def load_config(file_path: str) -> AppConfig:
    path = Path(__file__).parent.joinpath("config", file_path).resolve()
    with open(path, 'r') as f:
        config_data = toml.load(f)

    return AppConfig(
        dev_postgres=DatabaseConfig(**config_data['dev_postgres']),
        test_postgres=DatabaseConfig(**config_data['test_postgres']),
        beta_postgres=DatabaseConfig(**config_data['beta_postgres']),
        pro_postgres=DatabaseConfig(**config_data['pro_postgres']),
        dev_mysql=DatabaseConfig(**config_data['dev_mysql']),
        test_mysql=DatabaseConfig(**config_data['test_mysql']),
        beta_mysql=DatabaseConfig(**config_data['beta_mysql']),
        pro_mysql=DatabaseConfig(**config_data['pro_mysql']),
        dev_langflow=LangflowConfig(**config_data['dev_langflow']),
        test_langflow=LangflowConfig(**config_data['test_langflow']),
        beta_langflow=LangflowConfig(**config_data['beta_langflow']),
        pro_langflow=LangflowConfig(**config_data['pro_langflow']),
        langfuse=DatabaseConfig(**config_data['langfuse']),
    )


CONFIG = load_config("config.toml")

# 使用示例
if __name__ == "__main__":
    config = load_config("config.toml")

    print(f"数据库连接: {config.dev_postgres.host}:{config.dev_postgres.port}")
