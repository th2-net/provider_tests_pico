import click
from click import Context

from th2_ds.cli_util.cli_regestry import CliRegistry
from th2_ds.cli_util.config import CliConfig, get_cfg


class CliContext:
    def __init__(self, ctx: Context, cfg_path: str, verbose: int, report_path: str):
        self.extra_params = self._get_extra_params(ctx)
        self.cfg: CliConfig = get_cfg(cfg_path, self.extra_params)
        self.verbose_level: int = verbose
        self.report_path: str = report_path
        self.cli_registry = CliRegistry()

        # FIXME
        #   Find some another way to register DataSourceWrappers
        #   Context shouldn't know about DS-wrappers
        from th2_ds.cli_util.impl.data_source_wrapper import Lwdp1HttpDataSource, \
            Lwdp2HttpDataSource, Lwdp3HttpDataSource, Rpt5HttpDataSource
        self.cli_registry.register(Lwdp1HttpDataSource)
        self.cli_registry.register(Lwdp2HttpDataSource)
        self.cli_registry.register(Lwdp3HttpDataSource)
        self.cli_registry.register(Rpt5HttpDataSource)

        # TODO - don't sure that ds lib installation is related to DSContext.
        install_ds_impl(self.cfg)

    def _get_extra_params(self, ctx: Context) -> dict:
        """Params that were added via CLI."""
        extra_params = dict()
        for item in ctx.args:
            extra_params.update([item.split("=")])
        return extra_params


def install_ds_impl(cfg):
    # FIXME -- pkg_resources is deprecated.
    import sys, subprocess, pkg_resources

    ds_impl = cfg.data_sources[cfg.default_data_source].ds_impl
    command = [sys.executable, "-m", "pip", "install", f"{ds_impl.lib}=={ds_impl.version}"]
    try:
        distr = pkg_resources.get_distribution(ds_impl.lib)
        if distr.version != ds_impl.version:
            click.secho(f"Version conflict: required version {ds_impl.version}, found {distr.version}", fg='red')
            click.secho(f"Installing {ds_impl.lib}:{ds_impl.version}", fg='blue')
            subprocess.check_call(command)
    except pkg_resources.DistributionNotFound:
        click.secho(f"Package {ds_impl.lib} not found", fg='red')
        click.secho(f"Installing {ds_impl.lib}:{ds_impl.version}", fg='blue')
        subprocess.check_call(command)
