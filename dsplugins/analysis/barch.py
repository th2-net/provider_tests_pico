import time
from datetime import datetime, timezone
from re import search
from typing import TYPE_CHECKING
import click

from th2_data_services.data import Data
from th2_data_services.data_source.lwdp.struct import MessageStruct, EventStruct
from dsplugins.analysis import analysis
from th2_ds.cli_util.context import CliContext
from th2_ds.cli_util.decorators import http_error_wrapper, cli_command
from th2_ds.cli_util.interfaces.plugin import DSPlugin
from th2_ds.cli_util.utils import unix_timestamp, get_command_class_args, show_info, data_counter, get_ds_wrapper, \
    generate_and_save_report, get_exception_info
from th2_ds.cli_util.impl import data_source_wrapper as ds_w

if TYPE_CHECKING:
    from th2_ds.cli_util.interfaces.plugin import DSPlugin

parts_num_opt = click.option("-n", "--parts-num", required=True, type=click.INT)


def map_add_unix_timestamp(m: dict):
    return {"unix_timestamp": unix_timestamp(m["timestamp"]), **m}


def date_range(start, end, intv):
    diff = (end - start) // intv
    for i in range(intv):
        yield start + diff * (i + 1)


def get_and_sort_messages_list(messages: Data):
    with data_counter(messages) as data:
        range_lst = list(data.map(map_add_unix_timestamp))
        # We sort in case the data is out of order
        range_lst = sorted(range_lst, key=lambda m: m["unix_timestamp"])
    return range_lst


def map_add_unix_timestamp_for_events(m: dict):
    return {"unix_timestamp": (unix_timestamp(m["startTimestamp"]), unix_timestamp(m.get("endTimestamp"))), **m}


def get_and_sort_events_list(events: Data):
    with data_counter(events) as data:
        range_lst = list(data.map(map_add_unix_timestamp_for_events))
        # We sort in case the data is out of order
        range_lst = sorted(range_lst, key=lambda m: m["unix_timestamp"])
    return range_lst


def get_parts(cfg, parts_num):
    parts = list(date_range(cfg.request_params.start_timestamp, cfg.request_params.end_timestamp, parts_num + 1))
    parts = parts[:-1]  # don't get the latest value == end_time
    parts.reverse()  # We do a reverse so that we start testing not with large pieces, but with small ones
    return parts


def write_records_to_file(obj, file_path):
    with open(file_path, "w") as f:
        for m in obj:
            print(m, file=f)


@analysis.group()
def barch():
    """Barch test"""
    pass


@cli_command(group=barch, name="messages")
@parts_num_opt
@http_error_wrapper
def messages(ctx: CliContext, parts_num: int):
    # TODO - 1. Add test for events.
    """The Barch test.

    !!! WITHOUT ADAPTERS NOW (3.03.2022)

    TBU

    Compatible versions:
        - rpt-data-provider 5
    """
    data_source = get_ds_wrapper(ctx)
    exit_code = data_source.accept(Plugin(), parts_num=parts_num, rtype="messages", ctx=ctx)
    exit(exit_code)


# @barch.command()
@cli_command(group=barch, name="events")
@parts_num_opt
@http_error_wrapper
def events(ctx: CliContext, parts_num: int):
    """The Barch test.

    !!! WITHOUT ADAPTERS NOW (3.03.2022)

    TBU

    Compatible versions:
        - rpt-data-provider 5
    """
    data_source = get_ds_wrapper(ctx)
    exit_code = data_source.accept(Plugin(), parts_num=parts_num, rtype="events", ctx=ctx)
    exit(exit_code)


def message_border_test(ds_wrapper: ds_w.CommonLogicForLwdpRelatedClasses, ctx: CliContext, message: dict[str, object]):
    message_timestamp: int = message["unix_timestamp"]
    message_id: str = message["messageId"]

    dt_start = message_timestamp + 0
    dt_end = message_timestamp + 10000000

    get_messages_obj = ds_wrapper.get_messages_obj(ctx, dict(start_timestamp=dt_start, end_timestamp=dt_end))
    data: Data = ds_wrapper.ds_impl.command(get_messages_obj)
    is_found = any(msg['messageId'] == message_id for msg in data)
    pass

def common_logic(ds_wrapper: ds_w.CommonLogicForLwdpRelatedClasses,
                 data: Data,
                 command_class_args: dict,
                 ctx: CliContext,
                 parts_num: int,
                 rtype: str):

    exit_code = 0
    test_params = {
        "parts_num": parts_num,
        "rtype": rtype
    }
    results = {}

    try:
        if rtype == 'events':
            all_events_dumped_flag = False
            # Configs.
            cfg = ctx.cfg

            data.use_cache(True)

            show_info(ctx.extra_params, command_class_args, urls=data.metadata["urls"])

            # Get and build long_range_lst.
            long_range_lst = get_and_sort_events_list(data)

            if not long_range_lst:
                failed_txt = '0 events received. There is no data in the range.'
                click.secho(failed_txt, bg='red')
                results["long_range"] = f"FAILED: {failed_txt}"
                generate_and_save_report(ctx=ctx, data=data, command_class_args=command_class_args, test_params=test_params,results=results)
                exit(1)

            msg = (f"\nInitial number of events in the long range: {len(long_range_lst)}\n"
                   f"Long range filter: 'm['unix_timestamp'] >= 'Lowest time in short range'\n")
            print(msg)
            results["long_range"] = msg
            parts = get_parts(cfg, parts_num)

            click.secho('Below all timestamps will be indicated in ns, but in fact they are ms * 10**6 \n'
                        'because The provider expects the time in ms, and the timestamp in messages in ns \n'
                        'in grpc it is possible to specify time in nanoseconds', bg='yellow')
            for idx, start_ts_part in enumerate(parts):
                get_events_obj = ds_wrapper.get_events_obj(ctx, dict(start_timestamp=start_ts_part))
                short_range: Data = ds_wrapper.ds_impl.command(get_events_obj)

                if isinstance(start_ts_part, datetime):
                    start_ns = int(start_ts_part.timestamp() * 10 ** 3) * 10 ** 6
                else:
                    start_ns = start_ts_part

                if isinstance(cfg.request_params.end_timestamp, datetime):
                    end_ns = int(cfg.request_params.end_timestamp.timestamp() * 10 ** 3) * 10 ** 6
                else:
                    end_ns = cfg.request_params.end_timestamp

                range_results: list[str] = ['PASSED']
                # Print request label.
                msg =(f"[{idx + 1:0>3}] Request time: {time.time()}"
                      f"Start: {start_ts_part} ({start_ns} ns)\n"
                      f"  End: {cfg.request_params.end_timestamp} ({end_ns} ns)")
                print(msg)
                range_results.append(msg)

                short_range_lst = get_and_sort_events_list(short_range)

                # Print Lowest & Highest time in short range.
                if short_range_lst:
                    lowest_time_in_short_range = short_range_lst[0]['unix_timestamp'][0]
                    highest_time_in_short_range = short_range_lst[-1]['unix_timestamp'][1]
                    if highest_time_in_short_range is None:
                        highest_time_in_short_range = short_range_lst[-1]['unix_timestamp'][0]
                    msg = (f"Lowest time in short range: {datetime.fromtimestamp(lowest_time_in_short_range, tz=timezone.utc)} [UTC] ({lowest_time_in_short_range} ns)\n"
                           f"Highest time in short range: {datetime.fromtimestamp(highest_time_in_short_range, tz=timezone.utc)} [UTC] ({highest_time_in_short_range} ns)\n")
                else:
                    msg = (f"Lowest time in short range: None\n"
                           f"Highest time in short range: None\n")

                print(msg)
                range_results.append(msg)

                long_data_for_the_range = Data(long_range_lst).filter(
                    lambda m: m["unix_timestamp"][0] >= start_ns)  # unix timestamp in ns

                # TODO: should we use this filter?
                #
                #     lambda m: m["unix_timestamp"][0] >= start_ns or (
                #             m["unix_timestamp"][1] is not None and start_ns < m["unix_timestamp"][1]))  # unix timestamp in ns

                msg = f"long.len in the range of short part: {long_data_for_the_range.len}, short.len: {len(short_range_lst)}"
                range_results.append(msg)
                if long_data_for_the_range.len == len(short_range_lst):
                    click.secho(msg, bg="green")
                else:
                    exit_code = 1
                    click.secho(msg, bg="red")
                    print()
                    range_results[0] = 'FAILED'

                    # Write All messages from long range to file.
                    if not all_events_dumped_flag:
                        long_all_events_name = F"long_all_events.json"
                        msg = f"See long_all_events events in the file '{long_all_events_name}'"
                        click.secho(msg)
                        range_results.append(msg)
                        write_records_to_file(obj=long_range_lst, file_path=long_all_events_name)
                        all_events_dumped_flag = True

                    # Write events from Long that is in short range by time.
                    long_range_events_name = F"long_range_events_{idx + 1}.json"
                    msg = f"See long_range events in the file '{long_range_events_name}'"
                    click.secho(msg)
                    range_results.append(msg)
                    write_records_to_file(obj=long_data_for_the_range, file_path=long_range_events_name)

                    short_range_events_name = F"short_range_events_{idx + 1}.json"
                    msg = f"See short_range events in the file '{short_range_events_name}'"
                    click.secho(msg)
                    range_results.append(msg)
                    write_records_to_file(obj=short_range_lst, file_path=short_range_events_name)

                    msg = "Please note, all data in the files are sorted!"
                    click.secho(msg, fg='yellow')
                    range_results.append(msg)

                    # if ctx.protocol == ProviderProtocol.HTTP:
                    output = echo_short_range(short_range.metadata["urls"])
                    range_results.extend(output)

                    msg = "\nUnknown event Ids:"
                    click.secho(msg)
                    range_results.append(msg)
                    click.secho(f"get long set")

                    # if get_major_provider_ver(ctx.cfg, protocol=ctx.protocol) in ('5',):
                    event_struct: EventStruct = ds_wrapper.ds_impl.event_struct
                    cur_provider_format = event_struct.EVENT_ID

                    long_set = set(m[cur_provider_format] for m in long_data_for_the_range)
                    click.secho(f"get short set")
                    short_set = set(m[cur_provider_format] for m in short_range_lst)
                    unknown_event_ids = list(long_set.symmetric_difference(short_set))

                    class EventInfo:
                        def __init__(self, id, start_ns, end_ns):
                            event_by_id_in_all = list(
                                Data(long_range_lst).find_by(record_field=cur_provider_format, field_values=[id]))
                            event_by_id_in_long_range = list(
                                long_data_for_the_range.find_by(record_field=cur_provider_format, field_values=[id]))
                            event_by_id_in_short_range = list(
                                Data(short_range_lst).find_by(record_field=cur_provider_format, field_values=[id]))

                            self.timestamp = ''

                            if event_by_id_in_all:
                                self.in_all = 'Y'
                                self.timestamp = event_by_id_in_all[0]['unix_timestamp'][0]
                            else:
                                self.in_all = 'N'

                            if event_by_id_in_long_range:
                                self.in_long = 'Y'
                                self.timestamp = event_by_id_in_long_range[0]['unix_timestamp'][0]
                            else:
                                self.in_long = 'N'

                            if event_by_id_in_short_range:
                                self.in_short = 'Y'
                                self.timestamp = event_by_id_in_short_range[0]['unix_timestamp'][0]
                            else:
                                self.in_short = 'N'

                            if self.timestamp:
                                fg_color = 'green' if start_ns <= self.timestamp <= end_ns else 'red'
                                self.timestamp = click.style(self.timestamp, fg=fg_color)

                            self.id = id

                        def __str__(self):
                            return F"{self.timestamp} | {self.id} | {self.in_all} | {self.in_long} | {self.in_short}"

                    unknown_event_info_lst = [EventInfo(id, start_ns, end_ns) for id in unknown_event_ids]

                    msg = "timestamp           | id                        | in_all | in_long | in_short"
                    print(msg)
#                    range_results.append(msg)

                    for x in unknown_event_info_lst:
                        print(x)
#                        range_results.append(str(x))

                results[f"range_{idx}"] = "\n".join(range_results)

        elif rtype == 'messages':
            all_msgs_dumped_flag = False
            # Configs.
            cfg = ctx.cfg

            data.use_cache(True)

            show_info(ctx.extra_params, command_class_args, urls=data.metadata["urls"])

            # Get and build long_range_lst.
            long_range_lst = get_and_sort_messages_list(data)

            if not long_range_lst:
                failed_txt = '0 messages received. There is no data in the range.'
                click.secho(failed_txt, bg='red')
                results["long_range"] = f"FAILED: {failed_txt}"
                generate_and_save_report(ctx=ctx, data=data, command_class_args=command_class_args, test_params=test_params,results=results)
                exit(1)

######################
            #first_message = long_range_lst[1]
            message_border_test(ds_wrapper, ctx, long_range_lst[0])
######################


            msg = (f"\nInitial number of messages in the long range: {len(long_range_lst)}\n"
                   f"Long range filter: 'm['unix_timestamp'] >= 'Lowest time in short range'\n")
            print(msg)
            results["long_range"] = msg

            parts = get_parts(cfg, parts_num)

            click.secho('Below all timestamps will be indicated in ns, but in fact they are ms * 10**6 \n'
                        'because The provider expects the time in ms, and the timestamp in messages in ns \n'
                        'in grpc it is possible to specify time in nanoseconds', bg='yellow')
            for idx, start_ts_part in enumerate(parts):
                get_messages_obj = ds_wrapper.get_messages_obj(ctx, dict(start_timestamp=start_ts_part))

                short_range: Data = ds_wrapper.ds_impl.command(get_messages_obj)
                # start_ns = int(start_ts_part.timestamp() * 10 ** 3) * 10 ** 6
                # end_ns = int(cfg.request_params.end_timestamp.timestamp() * 10 ** 3) * 10 ** 6

                range_results: list[str] = ['PASSED']

                # Print request label.
                msg = (f"[{idx + 1:0>3}] Request time: {time.time()}\n"
                       f"Start: {datetime.utcfromtimestamp(start_ts_part / 1_000_000_000)} ({start_ts_part} ns)\n"
                       f"  End: {datetime.utcfromtimestamp(cfg.request_params.end_timestamp / 1_000_000_000)} ({cfg.request_params.end_timestamp} ns)")
                print(msg)
                range_results.append(msg)

                short_range_lst = get_and_sort_messages_list(short_range)

                # Print Lowest & Highest time in short range.
                if short_range_lst:
                    lowest_time_in_short_range = short_range_lst[0]['unix_timestamp']
                    highest_time_in_short_range = short_range_lst[-1]['unix_timestamp']
                    if highest_time_in_short_range is None:
                        highest_time_in_short_range = short_range_lst[-1]['unix_timestamp'][0]
                    msg = (f"Lowest time in short range: {datetime.fromtimestamp(lowest_time_in_short_range / 1_000_000_000, tz=timezone.utc)} [UTC] ({lowest_time_in_short_range} ns)\n"
                           f"Highest time in short range: {datetime.fromtimestamp(highest_time_in_short_range / 1_000_000_000, tz=timezone.utc)} [UTC] ({highest_time_in_short_range} ns)\n")
                else:
                    msg = (f"Lowest time in short range: None\n"
                           f"Highest time in short range: None \n")

                print(msg)
                range_results.append(msg)

                long_data_for_the_range = Data(long_range_lst).filter(
                    lambda m: m["unix_timestamp"] >= start_ts_part)  # unix timestamp in ns

                msg = f"long.len in the range of short part: {long_data_for_the_range.len}, short.len: {len(short_range_lst)}"
                range_results.append(msg)
                if long_data_for_the_range.len == len(short_range_lst):
                    click.secho(msg, bg="green")
                else:
                    exit_code = 1
                    click.secho(msg, bg="red")
                    print()
                    range_results[0] = 'FAILED'

                    # Write All messages from long range to file.
                    if not all_msgs_dumped_flag:
                        long_all_msgs_name = F"long_all_messages.json"
                        msg = f"See long_all_messages messages in the file '{long_all_msgs_name}'"
                        click.secho(msg)
                        range_results.append(msg)
                        write_records_to_file(obj=long_range_lst, file_path=long_all_msgs_name)
                        all_msgs_dumped_flag = True

                    # Write messages from Long that is in short range by time.
                    long_range_msgs_name = F"long_range_messages_{idx + 1}.json"
                    msg = f"See long_range messages in the file '{long_range_msgs_name}'"
                    click.secho(msg)
                    range_results.append(msg)
                    write_records_to_file(obj=long_data_for_the_range, file_path=long_range_msgs_name)

                    short_range_msgs_name = F"short_range_messages_{idx + 1}.json"
                    msg = f"See short_range messages in the file '{short_range_msgs_name}'"
                    click.secho(msg)
                    range_results.append(msg)
                    write_records_to_file(obj=short_range_lst, file_path=short_range_msgs_name)

                    msg = "Please note, all data in the files are sorted!"
                    click.secho(msg, fg='yellow')
                    range_results.append(msg)

                    # if ctx.protocol == ProviderProtocol.HTTP:
                    output = echo_short_range(short_range.metadata["urls"])
                    range_results.extend(output)

                    msg = f"\nUnknown message Ids:"
                    click.secho(msg)
                    range_results.append(msg)
                    click.secho(f"get long set")
                    # FIXME:
                    #   there should be some another solution
                    # if get_major_provider_ver(ctx.cfg, protocol=ctx.protocol) in ('5',):
                    message_struct: MessageStruct = ds_wrapper.ds_impl.message_struct
                    cur_provider_format = message_struct.MESSAGE_ID

                    long_set = set(m[cur_provider_format] for m in long_data_for_the_range)
                    click.secho(f"get short set")
                    short_set = set(m[cur_provider_format] for m in short_range_lst)
                    unknown_msg_ids = list(long_set.symmetric_difference(short_set))

                    # TODO - is the time longer than the start time?

                    class MsgInfo:
                        def __init__(self, id, start_ns, end_ns):
                            msg_by_id_in_all = list(
                                Data(long_range_lst).find_by(record_field=cur_provider_format, field_values=[id]))
                            msg_by_id_in_long_range = list(
                                long_data_for_the_range.find_by(record_field=cur_provider_format, field_values=[id]))
                            msg_by_id_in_short_range = list(
                                Data(short_range_lst).find_by(record_field=cur_provider_format, field_values=[id]))

                            self.timestamp = ''

                            if msg_by_id_in_all:
                                self.in_all = 'Y'
                                self.timestamp = msg_by_id_in_all[0]['unix_timestamp']
                            else:
                                self.in_all = 'N'

                            if msg_by_id_in_long_range:
                                self.in_long = 'Y'
                                self.timestamp = msg_by_id_in_long_range[0]['unix_timestamp']
                            else:
                                self.in_long = 'N'

                            if msg_by_id_in_short_range:
                                self.in_short = 'Y'
                                self.timestamp = msg_by_id_in_short_range[0]['unix_timestamp']
                            else:
                                self.in_short = 'N'

                            if self.timestamp:
                                if start_ns <= self.timestamp <= end_ns:
                                    self.timestamp = click.style(self.timestamp, fg='green')
                                else:
                                    self.timestamp = click.style(self.timestamp, fg='red')
                            self.id = id

                        def __str__(self):
                            return F"{self.timestamp} | {self.id} | {self.in_all} | {self.in_long} | {self.in_short}"

                    unknown_msg_info_lst = [MsgInfo(id, start_ns, end_ns) for id in unknown_msg_ids]

                    msg = "timestamp           | id                        | in_all | in_long | in_short"
                    print(msg)
                    range_results.append(msg)
                    for x in unknown_msg_info_lst:
                        print(x)
                        range_results.append(str(x))

                results[f"range_{idx}"] = "\n".join(range_results)

        else:
            raise Exception(f'Unknown rtype ({rtype})')

        print()
        click.secho("Done", fg="green")

    except Exception as e:
        results["exception"] = get_exception_info(e)
        exit_code = 1

    generate_and_save_report(
        ctx=ctx,
        data=data,
        command_class_args=command_class_args,
        test_params=test_params,
        results=results
    )

    return exit_code


def echo_short_range(urls) -> list[str]:
    output: list[str] = []
    if len(urls) == 1:
        msg = f"\nUrl for short range: {urls[0]}"
        click.secho(msg)
        output.append(msg)
    elif len(urls) > 1:
        msg = f"\nUrls for short range:"
        click.secho(msg)
        output.append(msg)
        for url_item in urls:
            click.secho(url_item)
            output.append(url_item)
    return output


class Plugin(DSPlugin):
    def root(self) -> click.Command:
        return barch

    def version(self) -> str:
        return '3.0.0'

    def _get_common_lwdp_objects_for_common_logic(self, ds_wrapper: ds_w.CommonLogicForLwdpRelatedClasses, **kwargs):
        ctx = kwargs['ctx']
        rtype = kwargs['rtype']
        parts_num = kwargs['parts_num']

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

        return dict(ds_wrapper=ds_wrapper,
                    data=data,
                    command_class_args=command_class_args,
                    parts_num=parts_num,
                    ctx=ctx,
                    rtype=rtype)

    def visit_lwdp1_http_data_source(self, ds_wrapper: ds_w.Lwdp1HttpDataSource, **kwargs):
        cl_kw = self._get_common_lwdp_objects_for_common_logic(ds_wrapper, **kwargs)
        return common_logic(**cl_kw)

    def visit_rpt5_http_data_source(self, ds_wrapper: ds_w.Rpt5HttpDataSource, **kwargs):
        cl_kw = self._get_common_lwdp_objects_for_common_logic(ds_wrapper, **kwargs)
        return common_logic(**cl_kw)

    def visit_lwdp2_http_data_source(self, ds_wrapper: ds_w.Lwdp2HttpDataSource, **kwargs):
        cl_kw = self._get_common_lwdp_objects_for_common_logic(ds_wrapper, **kwargs)
        return common_logic(**cl_kw)

    def visit_lwdp3_http_data_source(self, ds_wrapper: ds_w.Lwdp3HttpDataSource, **kwargs):
        cl_kw = self._get_common_lwdp_objects_for_common_logic(ds_wrapper, **kwargs)
        return common_logic(**cl_kw)
