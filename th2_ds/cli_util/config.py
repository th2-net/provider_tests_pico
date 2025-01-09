from __future__ import annotations
from copy import copy
from datetime import datetime
from typing import Optional, List, Union, Any, Dict
import click
import yaml
from click import Context
from pydantic import BaseModel, validator

DATA_SOURCE_CONFIG_PATH = "configs/data_sources.yaml"


class DataSourceImpl(BaseModel):
    lib: str
    version: str


class DataSource(BaseModel):
    version: str  # E.g. 5.3.20
    url: str = None
    chunk_length: Optional[int] = 65536
    ds_impl: DataSourceImpl = None
    cli_ds_class: str = None
    # requirements_path: str = None
    # custom_options: Optional[dict] = None


class RequestParams(BaseModel):
    start_timestamp: Optional[int] = None
    end_timestamp: Optional[int] = None
    streams: List[str] = None
    groups: List[str] = None
    filters: Union[str, List[str]] = None
    book_id: str = None
    scopes: List[str] = None

    @validator("filters")
    def convert_filters(cls, filters):
        # from th2_data_services import Filter
        if isinstance(filters, list):
            _fs = []
            for filter_string in filters:
                _fs.append(eval(filter_string))
            return _fs
        else:
            return eval(filters)

    # @validator("start_timestamp")
    # def convert_start_timestamp(cls, start_timestamp):
    #     if not isinstance(start_timestamp, datetime):
    #         return datetime.strptime(start_timestamp, "%Y-%m-%dT%H%M%S.%sZ")
    #     else:
    #         return start_timestamp
    #
    # @validator("end_timestamp")
    # def convert_end_timestamp(cls, end_timestamp):
    #     if not isinstance(end_timestamp, datetime):
    #         return datetime.strptime(end_timestamp, "%Y-%m-%dT%H%M%S.%sZ")
    #     else:
    #         return end_timestamp


class CliConfig(BaseModel):
    data_sources: Dict[str, DataSource]
    default_data_source: str = None
    get_messages_mode: str = 'ByGroups'
    request_params: RequestParams
    time_format: Optional[str] = None
    custom_plugin_params: Dict[str, Any] = dict()

    # TODO - add check that provided `default_data_source` in the `data_sources`


def _load_yaml(filename) -> dict:
    with open(filename, encoding='utf-8') as f:
        return yaml.safe_load(f)


def _get_cfg(ctx: Context, cfg_path) -> (CliConfig, dict):
    # Build extra params
    extra_params = dict()
    for item in ctx.args:
        extra_params.update([item.split("=")])

    yaml_cfg: dict = _load_yaml(cfg_path)

    for k in yaml_cfg:
        if k in extra_params:
            yaml_cfg[k] = extra_params[k]

    cfg: CliConfig = CliConfig(**yaml_cfg)
    return cfg, extra_params


def get_cfg(cfg_path: str, extra_params: Optional[dict] = None) -> CliConfig:
    """Returns common for all CLI config object."""

    def _get_key_chains_dict(d: dict, path='') -> List[str]:
        """
        {'a': {'b': 1, 'c': {'d':2, 'e': 3}}}

        a.b
        a.c.d
        a.c.e

        """
        lst = []
        for k, v in d.items():
            if not path:
                path_ = k
            else:
                path_ = path + '.' + k
            if isinstance(v, dict):
                keys_lst = _get_key_chains_dict(v, path_)
                lst.extend(keys_lst)
            else:
                lst.append(path_)

        return lst

    yaml_cfg: dict = _load_yaml(cfg_path)
    ds_yaml_cfg: dict = _load_yaml(DATA_SOURCE_CONFIG_PATH)
    yaml_cfg.update(ds_yaml_cfg)
    extra_params: dict = extra_params.copy()  # {param: val}
    key_chains_dict = _get_key_chains_dict(yaml_cfg)

    def get_dict_by_key_path(j, k: str):
        """
        k - 'a.b.c'

        """
        keys_path_list = k.split('.')  # ['a', 'b', 'c']
        rv = j
        for key in keys_path_list[:-1]:
            rv = rv[key]

        return rv

    if extra_params:
        for k in key_chains_dict:
            if k in extra_params:
                rv = get_dict_by_key_path(yaml_cfg, k)

                rv[k.split('.')[-1]] = copy(extra_params[k])
                del extra_params[k]

    if extra_params:
        click.secho(f"Unknown extra params: {extra_params}", bg='yellow')

    cfg: CliConfig = CliConfig(**yaml_cfg)
    return cfg
