from __future__ import annotations
from abc import ABC, abstractmethod

from th2_ds.cli_util.interfaces.plugin import DSPlugin


class IDataSourceWrapper(ABC):
    """Interface for any Data source."""

    @abstractmethod
    def accept(self, plugin: DSPlugin, *args, **kwargs):
        pass

    @property
    @abstractmethod
    def ds_impl(self):
        pass


class ITh2DataSourceWrapper(IDataSourceWrapper):

    @abstractmethod
    def get_events_obj(self, ctx, command_kwargs=None):
        pass

    @abstractmethod
    def get_messages_obj(self, ctx, command_kwargs=None):
        pass

    @abstractmethod
    def get_groups_obj(self, ctx):
        pass

    @abstractmethod
    def get_aliases_obj(self, ctx):
        pass

    @abstractmethod
    def get_scopes_obj(self, ctx):
        pass

    @abstractmethod
    def get_books_obj(self, ctx):
        pass
