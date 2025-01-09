from __future__ import annotations
from abc import ABC, abstractmethod
import click
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from th2_ds.cli_util.interfaces.data_source_wrapper import IDataSourceWrapper


class DSPlugin(ABC):
    @abstractmethod
    def version(self) -> str:
        pass

    def get_root_group(self):
        gr = self.root()
        gr.params.insert(0,
                         click.Option(('--version',),
                                      is_flag=True,
                                      callback=self._print_version,
                                      expose_value=False,
                                      is_eager=True))
        return gr

    @abstractmethod
    def root(self) -> click.Command:
        """The group or command to attach to ds.py cli."""
        pass

    def _print_version(self, ctx, param, value):
        if not value or ctx.resilient_parsing:
            return
        click.echo(F"Version {self.version}")
        ctx.exit()

    @abstractmethod
    def visit_lwdp1_http_data_source(self, element: IDataSourceWrapper, **kwargs):
        pass

    @abstractmethod
    def visit_rpt5_http_data_source(self, element: IDataSourceWrapper, **kwargs):
        pass

    @abstractmethod
    def visit_lwdp2_http_data_source(self, element: IDataSourceWrapper, **kwargs):
        pass

    @abstractmethod
    def visit_lwdp3_http_data_source(self, element: IDataSourceWrapper, **kwargs):
        pass
