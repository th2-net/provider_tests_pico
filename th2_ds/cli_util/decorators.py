from functools import wraps, update_wrapper
import click
import urllib3
from click import Context

from run_batch_testing import CFG_FILES
from th2_ds.cli_util.context import CliContext


def _cw(f):
    @wraps(f)
    def new_dec(*args, **kwargs):
        if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
            # actual decorated function
            return f(args[0])
        else:
            # decorator arguments
            return lambda realf: f(realf, *args, **kwargs)

    return new_dec


def http_error_wrapper(f):
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except urllib3.exceptions.ProtocolError as e:
            click.secho(f"urllib3.exceptions.ProtocolError: \n{e}", fg="red")
            exit(1)
        except urllib3.exceptions.HTTPError as e:
            click.secho(f"urllib3.exceptions.HTTPError: \n{e}", fg="red")
            exit(1)

    return update_wrapper(wrapper, f)


@_cw
def cli_command(f, name=None, group=None, required_cfg=True, **kwargs):
    _context_settings = dict(ignore_unknown_options=True, allow_extra_args=True)
    if group is None:
        group = click

    @group.command(name or f.__name__.lower().replace("_", "-"), context_settings=_context_settings, **kwargs)
    @click.option("-c", "--cfg-path", required=True)
    @click.option("-v", "--verbose", count=True)
    @click.option("-r", "--report-path")
    @click.pass_context
    @wraps(f)
    def new_func(ctx: Context, cfg_path: str, verbose: int, report_path: str, *args, **kwargs):
        ctx.obj = CliContext(ctx, cfg_path, verbose, report_path)
        return ctx.invoke(f, ctx.obj, *args, **kwargs)

    if required_cfg:
        @group.command(name or f.__name__.lower().replace("_", "-"), context_settings=_context_settings, **kwargs)
        @click.option("-c", "--cfg-path", required=True)
        @click.option("-v", "--verbose", count=True)
        @click.option("-r", "--report-path")
        @click.pass_context
        @wraps(f)
        def new_func(ctx: Context, cfg_path: str, verbose: int, report_path: str, *args, **kwargs):
            ctx.obj = CliContext(ctx, cfg_path, verbose, report_path)
            return ctx.invoke(f, ctx.obj, *args, **kwargs)

        return new_func

    else:
        @group.command(name or f.__name__.lower().replace("_", "-"), context_settings=_context_settings, **kwargs)
        @click.option("-v", "--verbose", count=True)
        @click.option("-r", "--report-path")
        @click.pass_context
        @wraps(f)
        def new_func_no_cfg(ctx: Context, verbose: int, report_path: str, *args, **kwargs):
            ctx.obj = CliContext(ctx, CFG_FILES[0], verbose, report_path)  # lw_dp.yaml used as dummy config
            return ctx.invoke(f, ctx.obj, *args, **kwargs)

        return new_func_no_cfg
