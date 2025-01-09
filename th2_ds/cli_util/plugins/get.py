from __future__ import annotations
import json
import os
import time
from typing import Optional
import click

from th2_data_services.data import Data
from th2_ds.cli_util.interfaces.data_source_wrapper import IDataSourceWrapper
from th2_ds.cli_util.interfaces.plugin import DSPlugin
from th2_ds.cli_util.context import CliContext
from th2_ds.cli_util.utils import not_implemented_err, get_command_class_args, show_info, data_counter, \
    get_ds_wrapper, create_ds_wrapper, generate_and_save_report, get_exception_info
from th2_ds.cli_util.decorators import http_error_wrapper, cli_command

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from th2_ds.cli_util.interfaces.plugin import DSPlugin
    from th2_ds.cli_util.impl import data_source_wrapper as ds_w


def write_data_to_file_json(data, out_file):
    with open(out_file, "w") as f:
        with data_counter(data) as data_:
            for m in data_:
                print(json.dumps(m, separators=(",", ":")), file=f)


def write_data_to_file_pickle(data, out_file):
    with open(out_file, "w") as f:
        with data_counter(data) as data_:
            data_: Data
            data_.build_cache(out_file)


def print_data_to_stdout(data):
    for m in data:
        print(json.dumps(m, separators=(",", ":")))


DEFAULT_FILE_FORMAT = 'json'

outfile_opt = click.option("-o", "--out-file")
format_opt = click.option("-f", "--format-file",
                          type=click.Choice(['json', 'pickle'], case_sensitive=False),
                          default=DEFAULT_FILE_FORMAT, show_default=True,
                          help='applicable with "out file" mode only')

@click.group()
def get():
    """Get events/messages from DataProvider"""


@cli_command(name='messages', group=get)
@outfile_opt
@format_opt
def get_messages(ctx: CliContext, out_file: Optional[str], format_file: str):
    """Get messages from DataProvider

    By default, messages will be printed to stdout.
    """
    # Note
    #   Here we don't know what exact class of ds_wrapper we have.
    #   But we will know inside the visitor method.
    ds_wrapper = get_ds_wrapper(ctx)
    ds_wrapper.accept(Plugin(), rtype="messages", ctx=ctx, out_file=out_file, format_file=format_file)


@cli_command(name='messages-by-id', group=get)
@outfile_opt
def get_messages_by_id(ctx: CliContext, out_file: Optional[str]):
    not_implemented_err()


@cli_command(name='groups', group=get)
@outfile_opt
@format_opt
def get_groups(ctx: CliContext, out_file: Optional[str], format_file: str):
    data_source = get_ds_wrapper(ctx)
    data_source.accept(Plugin(), rtype="groups", ctx=ctx, out_file=out_file, format_file=format_file)


@cli_command(name='books', group=get)
@outfile_opt
@format_opt
def get_books(ctx: CliContext, out_file: Optional[str], format_file: str):
    data_source = get_ds_wrapper(ctx)
    data_source.accept(Plugin(), rtype="books", ctx=ctx, out_file=out_file, format_file=format_file)


@cli_command(name='aliases', group=get)
@outfile_opt
@format_opt
def get_aliases(ctx: CliContext, out_file: Optional[str], format_file: str):
    data_source = get_ds_wrapper(ctx)
    data_source.accept(Plugin(), rtype="aliases", ctx=ctx, out_file=out_file, format_file=format_file)


@cli_command(name='events', group=get)
@outfile_opt
@format_opt
def get_events(ctx: CliContext, out_file: Optional[str], format_file: str):
    """Get events from DataProvider

    By default, events will be printed to stdout.
    """
    # Note
    #   Here we don't know what exact class of ds_wrapper we have.
    #   But we will know inside the visitor method.
    ds_wrapper = get_ds_wrapper(ctx)
    ds_wrapper.accept(Plugin(), rtype="events", ctx=ctx, out_file=out_file, format_file=format_file)


@cli_command(name='scopes', group=get)
@outfile_opt
@format_opt
def get_scopes(ctx: CliContext, out_file: Optional[str], format_file: str):
    """Get events from DataProvider

    By default, events will be printed to stdout.
    """
    ds_wrapper = get_ds_wrapper(ctx)
    ds_wrapper.accept(Plugin(), rtype="scopes", ctx=ctx, out_file=out_file, format_file=format_file)


@cli_command(name='events-by-id', group=get)
@outfile_opt
def get_events_by_id(ctx, cfg_path, out_file, verbose, protocol):
    not_implemented_err()

TEMP_OUTPUT_FILE = './temp_output.txt'
@cli_command(name='equivalence', group=get, required_cfg=False)
def get_tests(ctx: CliContext):
    report = dict[str, object]()

    try:
        if len(ctx.cfg.data_sources) < 2:
            # TODO: add record to report
            error_msg = f"Only {ctx.cfg.data_sources} data sources available. Minimum 2 data sources needed to run test."
            print(error_msg)
            report["error"] = error_msg
            generate_and_save_report(ctx=ctx, results=report)
            exit(2)

        # timestamps should not be used for this test because rpt-dp doesn't support timestamps for these requests
        ctx.cfg.request_params.start_timestamp = None
        ctx.cfg.request_params.end_timestamp = None

        exit_code = 0
        data_sources = list[tuple[str, IDataSourceWrapper]]()

        for name, ds_cfg in ctx.cfg.data_sources.items():
            data_sources.append((name, create_ds_wrapper(ctx.cli_registry, ds_cfg)))

        for rtype in ['books', 'aliases', 'scopes']:
            results = list[tuple[str, set[str]]]()
            for name, ds in data_sources:
                ds.accept(Plugin(), rtype=rtype, ctx=ctx, out_file=TEMP_OUTPUT_FILE, format_file='json')
                with open(TEMP_OUTPUT_FILE, 'r') as file:
                    results.append((name, set(line.strip().strip('"') for line in file)))
                try:
                    os.remove(TEMP_OUTPUT_FILE)
                except:
                    pass

            sets = iter(results)
            first_ds, first_set = next(sets)
            rtype_failed = False
            for current_ds, current_set in sets:
                if current_set != first_set:
                    result_txt = f"FAILED: '{rtype}' received from {current_ds} {current_set} and from {first_ds} {first_set} are different."
                    rtype_failed = True
                elif not first_set:
                    result_txt = f"FAILED: Empty result set for '{rtype}'."
                    rtype_failed = True
                else:
                    continue
                if rtype_failed:
                    print(result_txt)
                    report[rtype] = result_txt
                    exit_code = 1
                    break

            if not rtype_failed:
                result_txt = f"PASSED: '{rtype}' received from all data sources are equivalent: {first_set}."
                print(result_txt)
                report[rtype] = result_txt

    except Exception as e:
        report["exception"] = get_exception_info(e)
        exit_code = 1

    generate_and_save_report(ctx=ctx, results=report)
    exit(exit_code)


@http_error_wrapper
def common_logic(data: Data,
                 command_class_args: dict,
                 ctx: CliContext,
                 out_file: Optional[str],
                 format_file: str,
                 rtype: str):

    show_info(ctx.extra_params, command_class_args, urls=data.metadata["urls"])

    start = time.time()
    if out_file:
        # TODO
        #   Add jsonl, json.gz, jsonl.gz
        #   Implement them via Data method
        if format_file.lower() == 'json':
            write_data_to_file_json(data, out_file)
        elif format_file.lower() == 'pickle':
            write_data_to_file_pickle(data, out_file)

    else:
        print_data_to_stdout(data)

    t = time.time() - start
    avg_msgs_per_sec = data.len / t if t != 0 else 'n/a'
    print(f"Got: {data.len} {rtype} in {t} seconds (~{avg_msgs_per_sec} per second)")


class Plugin(DSPlugin):
    def version(self) -> str:
        return '1.1.0'

    def root(self) -> click.Command:
        """The group or command to attach to ds.py cli."""
        return get

    def _get_common_lwdp_objects_for_common_logic(self, ds_wrapper: ds_w.CommonLogicForLwdpRelatedClasses, **kwargs):
        ctx = kwargs['ctx']
        rtype = kwargs['rtype']
        out_file = kwargs['out_file']
        format_file = kwargs['format_file']

        if rtype == 'events':
            get_scopes_cmd_obj = ds_wrapper.get_events_obj(ctx)
            data: Data = ds_wrapper.ds_impl.command(get_scopes_cmd_obj)
            command_class_args = get_command_class_args(ctx.cfg, type(get_scopes_cmd_obj))

        elif rtype == 'scopes':
            get_scopes_cmd_obj = ds_wrapper.get_scopes_obj(ctx)
            data: Data = ds_wrapper.ds_impl.command(get_scopes_cmd_obj)

            if not isinstance(data, Data):
                data = Data(data)
                # FIXME: bug in the Lwdp
                data.metadata["urls"] = ['??']

            command_class_args = get_command_class_args(ctx.cfg, type(get_scopes_cmd_obj))

        elif rtype == 'messages':
            get_messages_cmd_obj = ds_wrapper.get_messages_obj(ctx)
            data: Data = ds_wrapper.ds_impl.command(get_messages_cmd_obj)
            command_class_args = get_command_class_args(ctx.cfg,
                                                        type(
                                                            get_messages_cmd_obj))
        elif rtype == 'groups':
            get_groups_cmd_obj = ds_wrapper.get_groups_obj(ctx)
            data: Data = ds_wrapper.ds_impl.command(get_groups_cmd_obj)
            command_class_args = get_command_class_args(ctx.cfg,
                                                        type(get_groups_cmd_obj))
        elif rtype == 'books':
            get_books_cmd_obj = ds_wrapper.get_books_obj(ctx)
            data: Data = Data(ds_wrapper.ds_impl.command(get_books_cmd_obj))
            # FIXME: bug in the Lwdp
            data.metadata["urls"] = ['??']
            command_class_args = get_command_class_args(ctx.cfg,
                                                        type(get_books_cmd_obj))

        elif rtype == 'aliases':
            get_aliases_cmd_obj = ds_wrapper.get_aliases_obj(ctx)
            data: Data = ds_wrapper.ds_impl.command(get_aliases_cmd_obj)

            if not isinstance(data, Data):
                data = Data(data)
                # FIXME: bug in the Lwdp
                data.metadata["urls"] = ['??']

            command_class_args = get_command_class_args(ctx.cfg, type(get_aliases_cmd_obj))

        else:
            raise RuntimeError(f'Unknown Rtype: {rtype}')

        return dict(data=data,
                    command_class_args=command_class_args,
                    ctx=ctx,
                    out_file=out_file,
                    format_file=format_file,
                    rtype=rtype)

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
