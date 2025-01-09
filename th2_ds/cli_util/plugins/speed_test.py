import json
import time
import click

from th2_data_services.data import Data

from th2_ds.cli_util.utils import show_info, get_command_class_args, get_ds_wrapper, generate_and_save_report, \
    get_exception_info
from th2_ds.cli_util.decorators import http_error_wrapper, cli_command
from th2_ds.cli_util.interfaces.plugin import DSPlugin
from th2_ds.cli_util.context import CliContext
from th2_ds.cli_util.impl import data_source_wrapper as ds_w

repetitions_opt = click.option("-n", "--repetitions", default=1, show_default=True)


def _speed_test(
        ctx: CliContext,
        repetitions: int,
        rtype: str,
        data: Data,
        urls: list[str],
        command_class_args: dict[str, object],
        show_info_flag=True
):
    # TODO - build in a check that the same amount of data is received each time
    # TODO - add average data size to output

    received_msgs = []
    times = []
    throughput_values: list[float] = []
    th2_2638 = False
    avg_per_second = None
    items_count, average_size, avg_attached_items = None, None, None
    results = {}
    exit_code = 0

    try:
        for i in range(repetitions):
            if i == 0 and show_info_flag:
                show_info(ctx.extra_params, command_class_args, urls=urls)

            start = time.time()
            len_ = 0
            for _ in data:
                len_ += 1

            t = time.time() - start
            received_msgs.append(len_)
            times.append(t)
            throughput_values.append(len_/t)
            if ctx.verbose_level > 0:
                print(f"Got: {len_} {rtype} in {t:.3f} seconds (~{len_ / t:.3f} per second), loop: {i + 1}")

        items_count, average_size, avg_attached_items = count_msg_stats(data, rtype)

        # Additional checks
        # [1] Check that user gets the same number of data each time (TH2-2638).
        if len(set(received_msgs)) != 1:
            click.secho(f"Got a different number of data each time (TH2-2638) - {received_msgs}", bg="red")
            th2_2638 = True

        # ----------------
        if ctx.verbose_level == 0:
            msgs_got = received_msgs[0] if not th2_2638 else received_msgs
            seconds = sum(times) / repetitions

            avg_per_second = 0
            for idx in range(len(received_msgs)):
                avg_per_second += received_msgs[idx] / times[idx]

            avg_per_second /= repetitions
            print(f"Got: {msgs_got} {rtype} in ~{seconds:.3f} seconds (AVG) (~{avg_per_second:.3f} per second), repetitions: {repetitions}. Average message size: {average_size}")

    except Exception as e:
        results["exception"] = get_exception_info(e)
        exit_code = 1

    test_params = {
        "repetitions": repetitions,
        "rtype": rtype,
    }

    attached_field_name = "avg_attached_events" if rtype == "messages" else "avg_attached_msgs"

    results["th2_2638"] = th2_2638
    results["items_count"] = items_count
    results["average_size_bytes"] = round(average_size, 1)
    results[attached_field_name] = round(avg_attached_items, 2)
    results["received_msgs"] = received_msgs
    results["times_sec"] = [round(num, 4) for num in times]
    results[f"throughput_values_{rtype}_per_sec"] = [round(num, 1) for num in throughput_values]

    if avg_per_second:
        results[f"avg_throughput_{rtype}_per_sec"] = round(avg_per_second, 1)

    generate_and_save_report(ctx=ctx, data=data, command_class_args=command_class_args, test_params=test_params, results=results)

    if exit_code != 0:
        exit(exit_code)


def count_msg_stats(data: Data, rtype: str) -> tuple[int, float, float]:
    size_sum = 0
    count = 0
    attached_items_sum = 0

    attached_field_name = None
    if rtype == "messages":
        attached_field_name = "attachedEventIds"
    elif rtype == "events":
        attached_field_name = "attachedMessageIds"

    for msg in data:
        count += 1
        json_string = json.dumps(msg)
        size_sum += len(json_string)
        if attached_field_name:
            attached_items = msg.get(attached_field_name)
            if attached_items:
                attached_items_sum += len(attached_items)

    average_size = size_sum / count if count != 0 else 0
    average_attached = attached_items_sum / count if count != 0 else 0
    return count, average_size, average_attached


@click.group()
def speed_test():
    """Calculate number of received events/messages"""


@cli_command(group=speed_test, name="messages")
@repetitions_opt
@http_error_wrapper
def speed_test_messages(ctx: CliContext, repetitions):
    data_source = get_ds_wrapper(ctx)
    data_source.accept(Plugin(), repetitions=repetitions, rtype="messages", ctx=ctx)


@cli_command(group=speed_test, name="events")
@repetitions_opt
@http_error_wrapper
def speed_test_events(ctx: CliContext, repetitions):
    data_source = get_ds_wrapper(ctx)
    data_source.accept(Plugin(), repetitions=repetitions, rtype="events", ctx=ctx)


def common_logic(data: Data, command_class_args: dict, ctx: CliContext, repetitions: int, rtype: str):
    _speed_test(ctx, repetitions, rtype, data, data.metadata["urls"], command_class_args)


class Plugin(DSPlugin):
    def version(self) -> str:
        return '1.1.0'

    def root(self) -> click.Command:
        """The group or command to attach to ds.py cli."""
        return speed_test

    def _get_common_lwdp_objects_for_common_logic(self, ds_wrapper: ds_w.CommonLogicForLwdpRelatedClasses, **kwargs):
        ctx = kwargs['ctx']
        rtype = kwargs['rtype']
        repetitions = kwargs['repetitions']

        if rtype == 'events':
            get_events_cmd_obj = ds_wrapper.get_events_obj(ctx)
            data: Data = ds_wrapper.ds_impl.command(get_events_cmd_obj)
            command_class_args = get_command_class_args(ctx.cfg, type(get_events_cmd_obj))

        elif rtype == 'messages':
            get_messages_cmd_obj = ds_wrapper.get_messages_obj(ctx)
            data: Data = ds_wrapper.ds_impl.command(get_messages_cmd_obj)
            command_class_args = get_command_class_args(ctx.cfg, type(get_messages_cmd_obj))

        else:
            raise RuntimeError(f'Unknown Rtype: {rtype}')

        return dict(data=data, command_class_args=command_class_args, ctx=ctx, repetitions=repetitions, rtype=rtype)

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
