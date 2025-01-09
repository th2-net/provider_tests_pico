from __future__ import annotations
from typing import TYPE_CHECKING
from typing_extensions import override

from th2_ds.cli_util.interfaces.data_source_wrapper import ITh2DataSourceWrapper
from th2_ds.cli_util.utils import get_command_class_args
from th2_ds.cli_util.utils import truncate_timestamp

if TYPE_CHECKING:
    from th2_ds.cli_util.interfaces.plugin import DSPlugin


class CommonLogicForLwdpRelatedClasses(ITh2DataSourceWrapper):
    @override
    def __init__(self, url: str, chunk_length: int = 65536):
        from th2_data_services.data_source.lwdp.data_source import HTTPDataSource
        self._ds = HTTPDataSource(url, chunk_length)

    @override
    @property
    def ds_impl(self):
        return self._ds

    @override
    def get_events_obj(self, ctx, command_kwargs=None):
        from th2_data_services.data_source.lwdp.commands.http import GetEventsByBookByScopes
        return GetEventsByBookByScopes(**get_command_class_args(ctx.cfg, GetEventsByBookByScopes, command_kwargs))

    @override
    def get_messages_obj(self, ctx, command_kwargs=None):
        if ctx.cfg.get_messages_mode == "ByGroups":
            from th2_data_services.data_source.lwdp.commands.http import GetMessagesByBookByGroups
            return GetMessagesByBookByGroups(**get_command_class_args(ctx.cfg, GetMessagesByBookByGroups, command_kwargs))
        elif ctx.cfg.get_messages_mode == "ByStreams":
            from th2_data_services.data_source.lwdp.commands.http import GetMessagesByBookByStreams
            return GetMessagesByBookByStreams(**get_command_class_args(ctx.cfg, GetMessagesByBookByStreams, command_kwargs))
        else:
            raise ValueError(f"Unknown `messages_mode` value: {ctx.cfg.get_messages_mode}")

    @override
    def get_groups_obj(self, ctx):
        from th2_data_services.data_source.lwdp.commands.http import GetMessageGroups
        return GetMessageGroups(**get_command_class_args(ctx.cfg, GetMessageGroups))

    @override
    def get_aliases_obj(self, ctx):
        from th2_data_services.data_source.lwdp.commands.http import GetMessageAliases
        return GetMessageAliases(**get_command_class_args(ctx.cfg, GetMessageAliases))

    @override
    def get_scopes_obj(self, ctx):
        from th2_data_services.data_source.lwdp.commands.http import GetEventScopes
        return GetEventScopes(**get_command_class_args(ctx.cfg, GetEventScopes))

    @override
    def get_books_obj(self, ctx):
        from th2_data_services.data_source.lwdp.commands.http import GetBooks
        return GetBooks()


class CommonLogicForRdp5RelatedClasses(CommonLogicForLwdpRelatedClasses):
    @override
    def __init__(self, url: str, chunk_length: int = 65536):
        from th2_data_services.data_source.rdp.data_source import HTTPDataSource
        self._ds = HTTPDataSource(url, chunk_length)

    @override
    def get_aliases_obj(self, ctx):
        from th2_data_services.data_source.rdp.commands.http import GetMessageAliases
        return GetMessageAliases(**get_command_class_args(ctx.cfg, GetMessageAliases))

    @override
    def get_scopes_obj(self, ctx):
        from th2_data_services.data_source.rdp.commands.http import GetEventScopes
        return GetEventScopes(**get_command_class_args(ctx.cfg, GetEventScopes))

    @override
    def get_books_obj(self, ctx):
        from th2_data_services.data_source.rdp.commands.http import GetBooks
        return GetBooks()


class Lwdp1HttpDataSource(CommonLogicForLwdpRelatedClasses):
    @override
    def accept(self, plugin: DSPlugin, **kwargs):
        return plugin.visit_lwdp1_http_data_source(self, **kwargs)

    @override
    def get_events_obj(self, ctx, command_kwargs=None):
        return truncate_timestamp(super().get_events_obj(ctx, command_kwargs))

    @override
    def get_messages_obj(self, ctx, command_kwargs=None):
        if ctx.cfg.get_messages_mode == "ByGroups":
            raise Exception("Lwdp1 does not support groups!")
        elif ctx.cfg.get_messages_mode == "ByStreams":
            from th2_data_services.data_source.lwdp.commands.http import \
                GetMessagesByBookByStreams
            return truncate_timestamp(GetMessagesByBookByStreams(
                **get_command_class_args(ctx.cfg, GetMessagesByBookByStreams, command_kwargs)))

    @override
    def get_groups_obj(self, ctx):
        raise Exception("Lwdp1 does not support groups!")

    @override
    def get_aliases_obj(self, ctx):
        return truncate_timestamp(super().get_aliases_obj(ctx))


class Rpt5HttpDataSource(CommonLogicForRdp5RelatedClasses):
    @override
    def accept(self, plugin: DSPlugin, **kwargs):
        return plugin.visit_rpt5_http_data_source(self, **kwargs)

    @override
    def get_events_obj(self, ctx, command_kwargs=None):
        return truncate_timestamp(super().get_events_obj(ctx, command_kwargs))

    @override
    def get_messages_obj(self, ctx, command_kwargs=None):
        if ctx.cfg.get_messages_mode == "ByGroups":
            raise Exception("Rpt5 does not support groups!")
        elif ctx.cfg.get_messages_mode == "ByStreams":
            from th2_data_services.data_source.lwdp.commands.http import GetMessagesByBookByStreams
            return truncate_timestamp(GetMessagesByBookByStreams(
                **get_command_class_args(ctx.cfg, GetMessagesByBookByStreams, command_kwargs)))

    @override
    def get_groups_obj(self, ctx):
        raise Exception("Rpt5 does not support groups!")


class Lwdp2HttpDataSource(CommonLogicForLwdpRelatedClasses):
    @override
    def accept(self, plugin: DSPlugin, **kwargs):
        return plugin.visit_lwdp2_http_data_source(self, **kwargs)


class Lwdp3HttpDataSource(CommonLogicForLwdpRelatedClasses):
    @override
    def accept(self, plugin: DSPlugin, **kwargs):
        return plugin.visit_lwdp3_http_data_source(self, **kwargs)
