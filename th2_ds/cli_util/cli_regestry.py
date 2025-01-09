from __future__ import annotations
from typing import List

from th2_ds.cli_util.interfaces.data_source_wrapper import IDataSourceWrapper


class CliRegistry:
    def __init__(self):
        self._data_sources: List[IDataSourceWrapper] = []

    def get_ds_by_cfg_name(self, value) -> IDataSourceWrapper:
        for ds in self._data_sources:
            if ds.__name__ == value:
                return ds
        raise RuntimeError(f"Data Source '{value}' not found")

    def register(self, cls: IDataSourceWrapper):
        """
        a way to register its own DataSourceWrapper
        """
        self._data_sources.append(cls)
