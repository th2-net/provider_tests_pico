from datetime import datetime, timedelta
import click

from th2_data_services.data import Data
from dsplugins.analysis import analysis
from th2_ds.cli_util.context import CliContext
from th2_ds.cli_util.decorators import http_error_wrapper, cli_command
from th2_ds.cli_util.impl import data_source_wrapper as ds_w
from th2_ds.cli_util.interfaces.plugin import DSPlugin
from th2_ds.cli_util.utils import counter, reset_counter, setup_counter, \
    not_implemented_err, get_command_class_args, show_info, get_ds_wrapper

# Frequently of intervals.
aggr_val_opt = click.option("--aggr-val", default=1, show_default=True, type=click.INT)
aggr_resolution_opt = click.option("--aggr-resolution", default="s", show_default=True, type=click.STRING)


# resolution: Datetime suffix for intervals.


def transform_time(record):
    try:
        time = datetime.fromtimestamp(record["timestamp"].get("epochSecond", 0))
        time += timedelta(microseconds=record["timestamp"].get("nano", 0)) / 1000
        return {"session_dir": record["sessionId"] + ":" + record["direction"], "time": time}
    except Exception as e:
        print(f"Exception: {e}")
        print(record)


@analysis.group()
def density():
    """Plots density chart"""


@cli_command(group=density, name="messages")
@aggr_val_opt
@aggr_resolution_opt
def density_messages(ctx: CliContext, aggr_val, aggr_resolution):
    """Plots density chart for messages"""
    data_source = get_ds_wrapper(ctx)
    data_source.accept(Plugin(), aggr_val=aggr_val, aggr_resolution=aggr_resolution, ctx=ctx)


@cli_command(group=density, name="events")
@aggr_val_opt
@aggr_resolution_opt
def density_events(ctx: CliContext, aggr_val, aggr_resolution):
    not_implemented_err()


@http_error_wrapper
def common_logic(messages: Data, command_class_args: dict, ctx: CliContext, aggr_val: int, aggr_resolution: str):
    # TODO - ADD TOTAL only param
    from th2_data_services_utils.utils import aggregate_by_intervals
    import plotly.express as px

    show_info(ctx.extra_params, command_class_args, urls=messages.metadata["urls"], get_messages_mode=ctx.cfg.get_messages_mode)

    transformed_messages = messages.map(counter).map(transform_time)

    if not messages.is_empty:
        setup_counter()
        output = aggregate_by_intervals(transformed_messages, "time", resolution=aggr_resolution, every=aggr_val)
        d_info = reset_counter()
        print(output)

        msgs_len = transformed_messages.len
        fig = px.line(output, x="time", y="count",
                      title=f"Density {ctx.cfg.request_params.start_timestamp} - {ctx.cfg.request_params.end_timestamp} | "
                            f"Msgs: {msgs_len}, size: {d_info['last_size_fmted']}, "
                            f"avg size: {d_info['avg_size_fmted']}, Aggr by {aggr_val}{aggr_resolution}")
        fig.show()
    else:
        click.secho("0 messages in the range", fg='red')


class Plugin(DSPlugin):
    def root(self) -> click.Command:
        return density

    def version(self) -> str:
        return '2.0.0'

    def _get_common_lwdp_objects_for_common_logic(self, ds_wrapper: ds_w.CommonLogicForLwdpRelatedClasses, **kwargs):
        ctx = kwargs['ctx']
        aggr_val = kwargs['aggr_val']
        aggr_resolution = kwargs['aggr_resolution']

        get_messages_cmd_obj = ds_wrapper.get_messages_obj(ctx)
        messages: Data = ds_wrapper.ds_impl.command(get_messages_cmd_obj)
        command_class_args = get_command_class_args(ctx.cfg, type(get_messages_cmd_obj))

        return dict(messages=messages,
                    command_class_args=command_class_args,
                    ctx=ctx,
                    aggr_val=aggr_val,
                    aggr_resolution=aggr_resolution)

    def visit_lwdp1_http_data_source(self, ds_wrapper: ds_w.Lwdp1HttpDataSource, **kwargs):
        cl_kw = self._get_common_lwdp_objects_for_common_logic(ds_wrapper, **kwargs)
        common_logic(**cl_kw)

    def visit_rpt5_http_data_source(self, ds_wrapper: ds_w.Rpt5HttpDataSource, **kwargs):
        cl_kw = self._get_common_lwdp_objects_for_common_logic(ds_wrapper, **kwargs)
        common_logic(**cl_kw)

    def visit_lwdp2_http_data_source(self, ds_wrapper: ds_w.Lwdp2HttpDataSource, **kwargs):
        cl_kw = self._get_common_lwdp_objects_for_common_logic(ds_wrapper, **kwargs)
        common_logic(**cl_kw)

    def visit_lwdp3_http_data_source(self, ds_wrapper: ds_w.Lwdp3HttpDataSource, **kwargs):
        cl_kw = self._get_common_lwdp_objects_for_common_logic(ds_wrapper, **kwargs)
        common_logic(**cl_kw)
