import configparser
import os
from typing import TypedDict, Union, Literal, Optional


class SourceConfig(TypedDict):
    host: str
    port: Optional[str]
    username: str
    password: str
    dbname: str
    # TODO: support user provided CA and client cert and key
    sslmode: Union[Literal["disable"], Literal["require"], Literal["verify-ca"], Literal["verify-full"]]
    schema: Optional[str]

    node: str
    replication_set: str

    # TODO: Add replication user and password


class TargetConfig(TypedDict):
    host: str
    port: Optional[str]
    username: str
    password: str
    dbname: str
    # TODO: support user provided CA and client cert and key
    sslmode: Union[Literal["disable"], Literal["require"], Literal["verify-ca"], Literal["verify-full"]]
    schema: Optional[str]

    node: str
    subscription: str

    replication_username: str
    replication_password: str


def load_config_from_ini(ini_file):
    config = configparser.ConfigParser()
    config.read(ini_file)
    return config


def load_config_from_env():
    config = {
        "source": SourceConfig(),
        "target": TargetConfig()
    }
    for section, typed_dict in [("source", SourceConfig), ("target", TargetConfig)]:
        for key in typed_dict.__annotations__.keys():
            env_key = f"{section}_{key}".upper()
            env_value = os.environ.get(env_key)
            if env_value is not None:
                config[section][key] = env_value
        if "sslmode" not in config[section]:
            config[section]["sslmode"] = "disable"
        if "port" not in config[section]:
            config[section]["port"] = "5432"
    return config
