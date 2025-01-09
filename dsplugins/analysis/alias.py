import time
from pprint import pprint, pformat

import click
from th2_data_services.data import Data
# from th2_data_services.provider.v5.struct import Provider5MessageStruct
from th2_data_services.data_source.lwdp.struct import MessageStruct

from dsplugins.analysis import analysis
from th2_ds.cli_util.context import CliContext
from th2_ds.cli_util.decorators import http_error_wrapper, cli_command
from th2_ds.cli_util.impl import data_source_wrapper as ds_w
from th2_ds.cli_util.interfaces.plugin import DSPlugin
from th2_ds.cli_util.utils import counter, unix_timestamp, reset_counter, setup_counter, \
    get_command_class_args, show_info, data_counter, get_ds_wrapper, generate_and_save_report, get_exception_info


def map_add_unix_timestamp(m: dict):
    return {"unix_timestamp": unix_timestamp(m["timestamp"]), **m}


@cli_command(group=analysis, name="alias")
@http_error_wrapper
def alias(ctx: CliContext):
    """Alias test

    Only for messages.

    This test will download data for the entire period for all aliases, then
    will download data for each alias separately and check:
    - the number of messages is the same
    - messages are identical
    """
    data_source = get_ds_wrapper(ctx)
    return_code = data_source.accept(Plugin(), ctx=ctx)
    exit(return_code)


def common_logic(ds_wrapper: ds_w.CommonLogicForLwdpRelatedClasses,
                 messages: Data,
                 command_class_args: dict,
                 ctx: CliContext):
    exit_code = 0
    results: dict[str, object] = {}

    try:
        message_mode = ctx.cfg.get_messages_mode

        all_msgs_dumped_flag = False

        message_struct: MessageStruct = ds_wrapper.ds_impl.message_struct
        cur_format_id = message_struct.MESSAGE_ID

        show_info(ctx.extra_params, command_class_args, urls=messages.metadata["urls"])

        mode_to_name = {
            'ByGroups': 'groups',
            'ByStreams': 'streams',
        }

        click.secho(f'Get messages by all {mode_to_name[message_mode]}')
        all_messages_by_mode = {}
        with data_counter(messages) as data:
            for m in data:
                try:
                    all_messages_by_mode[m[cur_format_id]] = m
                except Exception:
                    print()
                    print(m[cur_format_id])
                    print(m)
                    print()
                    print(all_messages_by_mode)
                    print()
                    raise
        print()

        data_by_mode = []
        len_by_mode = 0
        err_msg = ''
        check1 = True
        check2 = True
        for idx, val in enumerate(getattr(ctx.cfg.request_params, mode_to_name[message_mode])):
            get_messages_obj = ds_wrapper.get_messages_obj(ctx, dict({mode_to_name[message_mode]: [val]}))
            msgs: Data = ds_wrapper.ds_impl.command(get_messages_obj)

            print(f"[{idx + 1:0>3}] Request time: {time.time()}")
            click.secho(F"Get messages by {mode_to_name[message_mode]}: {val}")
            setup_counter()
            messages_by_mode: Data = msgs.map(counter)
            data_by_mode.append(messages_by_mode)

            for m in messages_by_mode:
                msg_id = m[cur_format_id]
                # Check that messageID in the long range.
                if msg_id in all_messages_by_mode.keys():
                    # Check that the messages in the long range and in the short are equal.
                    if not m == all_messages_by_mode[msg_id]:
                        err_msg = 'Check2 - failed. Check that the messages in the long and short ranges are equal - not equal.'
                        print(f'msg_id: {msg_id}')
                        print('message from short range:')
                        pprint(m)
                        print()
                        print('message from long range:')
                        pprint(all_messages_by_mode[msg_id])
                        results["check_2"] = f'{err_msg} Message from short range: {pformat(m)}. Message from long range: {pformat(all_messages_by_mode[msg_id])}'
                        check2 = False
                        exit_code = 1
                else:
                    err_msg = 'Check1 - failed. Check that messageID in the long range - messageID NOT in the long range'
                    print(f'msg_id: {msg_id}')
                    print('message from short range:')
                    pprint(m)
                    results["check_1"] = f'{err_msg} Message from short range: {pformat(m)}.'
                    check1 = False
                    exit_code = 1

                if err_msg:
                    reset_counter()
                    click.secho(f"\n{err_msg}", bg="red")

                    if not all_msgs_dumped_flag:
                        click.secho(f"See all_messages_by_mode messages in the file 'all_messages_by_mode.txt'", bg="red")
                        with open("all_messages_by_mode.txt", "w") as f:
                            for m in all_messages_by_mode:
                                print(m, file=f)
                        all_msgs_dumped_flag = True

            len_by_mode += messages_by_mode.len

            reset_counter()
            print()

        # Check1
        if check1:
            check_txt = "Check1 - Check that messageID in the long range - Passed"
            results["check_1"] = check_txt
            click.secho(check_txt, bg="green")

        # Check2
        if check2:
            check_txt = "Check2 - Check that that the messages in the long range and in the short are equal - Passed"
            results["check_2"] = check_txt
            click.secho(check_txt, bg="green")

        # Check3
        if len(all_messages_by_mode) == len_by_mode:
            check_txt = f"Check3 - len(all_messages_by_mode) == len_by_mode - Passed. (len_by_mode = {len_by_mode})"
            bg="green"
        else:
            check_txt = f"Check3 - len(all_messages_by_mode) == len_by_mode - Failed. (len(all_messages_by_mode) = {len(all_messages_by_mode)}. len_by_mode = {len_by_mode})"
            exit_code = 1
            bg="red"

        click.secho(check_txt, bg=bg)
        print()
        results["check_3"] = check_txt

    except Exception as e:
        results["exception"] = get_exception_info(e)
        exit_code = 1

    generate_and_save_report(ctx=ctx, data=messages, command_class_args=command_class_args, results=results)
    return exit_code

class Plugin(DSPlugin):
    def root(self) -> click.Command:
        return alias

    def version(self) -> str:
        return '2.0.0'

    def _get_common_lwdp_objects_for_common_logic(self, ds_wrapper: ds_w.CommonLogicForLwdpRelatedClasses, **kwargs):
        ctx = kwargs['ctx']

        get_messages_cmd_obj = ds_wrapper.get_messages_obj(ctx)
        data: Data = ds_wrapper.ds_impl.command(get_messages_cmd_obj)
        command_class_args = get_command_class_args(ctx.cfg, type(get_messages_cmd_obj))

        return dict(ds_wrapper=ds_wrapper, messages=data, command_class_args=command_class_args, ctx=ctx)

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
