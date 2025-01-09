import datetime
import inspect
import json
import re
import sys
import time
import traceback
import contextlib
from importlib import import_module
from typing import List, Type
import click
from prettytable import PrettyTable
import threading

from th2_data_services.data import Data
# from th2_data_services.provider.interfaces.command import IProviderCommand, IHTTPProviderCommand, IGRPCProviderCommand
from th2_data_services.data_source.lwdp.interfaces.command import ICommand, IHTTPCommand
# from th2_data_services.provider.interfaces.data_source import IProviderDataSource, IGRPCProviderDataSource, IHTTPProviderDataSource
from th2_data_services.data_source.lwdp.interfaces.data_source import IHTTPDataSource
from th2_ds.cli_util.cli_regestry import CliRegistry
###from th2_data_services.data_source.lwdp.data_source.http import DataSource, HTTPDataSource
# from th2_data_services.data_source.lwdp.data_source.http import DataSource, HTTPDataSource
from th2_ds.cli_util.config import CliConfig, DataSource
from th2_ds.cli_util.context import CliContext
from th2_ds.cli_util.interfaces.data_source_wrapper import IDataSourceWrapper

_received_counter = 0
_last_cnt = 0
_total_sec = 1
_total_size = 0
_last_size_fmted = 0
_avg_size_fmted = 0


class StoppableThread(threading.Thread):
    """Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition."""

    def __init__(self, *args, **kwargs):
        super(StoppableThread, self).__init__(*args, **kwargs)
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()


_counter_thread: threading.Thread = None


def _sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def _print_recv_state(recv, last_recv, total_sec, total_size_bytes, **kwargs):
    # size = _sizeof_fmt(total_size_bytes)
    # if recv > 0:
    #     avg_size = _sizeof_fmt(total_size_bytes / recv)
    # else:
    #     avg_size = _sizeof_fmt(0)
    print(
        f"\rRecv: {recv}, speed: {recv - last_recv}/s, avg: {recv / total_sec:.0f}/s, "
        f"time: {datetime.timedelta(seconds=total_sec)}         ",
        # f"size: {size}, avg size: {avg_size}, time: {datetime.timedelta(seconds=total_sec)}         ",
        **kwargs)


def count_printer_thread():
    global _received_counter
    global _total_sec
    global _last_cnt

    t = threading.currentThread()
    while getattr(t, "do_run", True):
        time.sleep(1)
        _print_recv_state(recv=_received_counter,
                          last_recv=_last_cnt,
                          total_sec=_total_sec,
                          total_size_bytes=_total_size,
                          end="", flush=True
                          )
        _last_cnt = _received_counter
        _total_sec += 1


def setup_counter():
    global _received_counter
    global _last_cnt
    global _total_sec
    global _counter_thread
    global _total_size
    _total_size = 0
    _received_counter = 0
    _last_cnt = 0
    _total_sec = 1
    # _counter_thread = StoppableThread(target=count_printer_thread)
    _counter_thread = threading.Thread(target=count_printer_thread, daemon=True)
    _counter_thread.start()


def counter(r):
    global _received_counter
    # global _batch_size
    global _total_size
    _received_counter += 1
    _total_size += sys.getsizeof(r)
    return r


def reset_counter():
    global _received_counter
    global _last_cnt
    global _total_sec
    global _counter_thread
    global _last_size_fmted
    global _avg_size_fmted
    global _total_size
    _counter_thread.do_run = False
    _counter_thread.join()
    _print_recv_state(recv=_received_counter, last_recv=_last_cnt,
                      total_sec=_total_sec, total_size_bytes=_total_size)
    _counter_thread = None

    _last_size_fmted = _sizeof_fmt(_total_size)
    if _received_counter > 0:
        _avg_size_fmted = _sizeof_fmt(_total_size / _received_counter)
    else:
        _avg_size_fmted = _sizeof_fmt(0)

    return dict(last_size_fmted=_last_size_fmted, avg_size_fmted=_avg_size_fmted)


@contextlib.contextmanager
def data_counter(d: Data):
    new_d = d.map(counter)

    try:
        setup_counter()
        yield new_d
    finally:
        reset_counter()


def not_implemented_err():
    click.secho("Not implemented", bg="red")
    exit(1)


def _show_info(extra_params, cfg: CliConfig, data_obj=None):
    if extra_params:
        click.secho(f"Extra params: {extra_params}", bg="yellow")
    print(f"Start time: {cfg.request_params.start_timestamp}")
    print(f"  End time: {cfg.request_params.end_timestamp}")
    print(f"     Delta: {cfg.request_params.end_timestamp - cfg.request_params.start_timestamp}")
    print(f"Streams: {cfg.request_params.streams}")
    if data_obj is not None:
        try:
            print(f"URL: {data_obj.url}")
        except:
            pass


def show_info(extra_params: dict, command_params: dict, urls: list[str]=None, get_messages_mode=None):
    if extra_params:
        click.secho(f"Extra params: {extra_params}", bg="yellow")

    if 'start_timestamp' in command_params:
        print(f"Start time: {command_params['start_timestamp']}")

    if 'end_timestamp' in command_params:
        print(f"  End time: {command_params['end_timestamp']}")

    if 'start_timestamp' in command_params and 'end_timestamp' in command_params:
        if all(x is not None for x in [command_params['end_timestamp'], command_params['start_timestamp']]):
            print(f"     Delta: {command_params['end_timestamp'] - command_params['start_timestamp']}")

    for k, v in command_params.items():
        if k not in ('start_timestamp', 'end_timestamp'):
            print(F"{k}: {v}")

    if get_messages_mode:
        print(f"get_messages_mode: {get_messages_mode}")

    if urls is None:
        pass
    elif len(urls) == 1:
        print(f"URL: {urls[0]}")
    elif len(urls) > 1:
        print(f"URLs:")
        for url_item in urls:
            print(url_item)


def try_import_module(module):
    try:
        module = module.replace('.py', '').replace('/', '.')
        module = re.sub(r"^\W+", '', module)  # Cleaning non-letter symbols in the beginning of string
        return import_module(module)
    except ImportError as err:
        print(F"Can not import module '{module}'. ImportError: {err}", "red")
        print(F"Traceback:\n"
              F"{traceback.format_exc()}")
        return False


def my_import(name):
    components = name.split('.')
    mod = __import__(components[0])
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod


def unix_timestamp(ts: dict) -> int:
    # {"nano":477000000,"epochSecond":1634223323}
    return None if ts is None else int(f"{ts['epochSecond']}{ts['nano']:0>9}")


# FIXME:
#   commented because should be removed everywhere (another solution should be used)
#   --> USE get_ds_wrapper
# def get_datasource(cfg, protocol) -> IDataSource:
#     if protocol == 'HTTP':
#         return get_http_ds(cfg)
#     #    elif protocol == 'GRPC':
#     #        return get_grpc_ds(cfg)
#     else:
#         raise Exception('WTF? get_ds')

# def _get_ds(cfg, provider_class: IProviderDataSource):
#     if provider_class is HTTPProvider5DataSource:


# TODO -- IT IS A BUG!!!!
def get_major_provider_ver(cfg: CliConfig):
    """Returns major provider version.

    e.g. if version: 5.3.20 it returns '5'
    """
    # FIXME:
    #   there should be some another solution
    # if protocol == ProviderProtocol.HTTP:
    #     return cfg.http_data_source.version.split('.')[0]
    # elif protocol == ProviderProtocol.GRPC:
    #     return cfg.grpc_data_source.version.split('.')[0]
    # else:
    #     raise ValueError(
    #         f"Unknown protocol value '{protocol}', available values: ???")

    return cfg.http_data_source.version.split('.')[0]


def get_http_ds(cfg: CliConfig) -> IHTTPDataSource:
    # major_provider_ver = get_major_provider_ver(cfg, protocol=ProviderProtocol.HTTP)
    # ds_class_str_mod = F"th2_data_services.provider.v{major_provider_ver}.data_source"
    ds_class_str_mod = "th2_data_services.data_source.lwdp.data_source"

    data_source_module = try_import_module(ds_class_str_mod)
    if not data_source_module:
        exit(1)

    # HTTPProviderXDataSource = getattr(data_source_module, F"HTTPProvider{major_provider_ver}DataSource")
    HTTPProviderXDataSource = getattr(data_source_module, "HTTPDataSource")

    body = {}
    for k, v in cfg.http_data_source:
        # Exclude.
        if k in ('version'):
            continue
        # Special
        # if k == 'url':
        #     body['url'] = v[0]
        body[k] = v
    ds = HTTPProviderXDataSource(**body)

    return ds


# def get_grpc_ds(cfg: Cfg) -> IGRPCProviderDataSource:
#     major_provider_ver = get_major_provider_ver(cfg, protocol=ProviderProtocol.GRPC)
#     ds_class_str_mod = F"th2_data_services.provider.v{major_provider_ver}.data_source"
#
#     data_source_module = try_import_module(ds_class_str_mod)
#     if not data_source_module:
#         exit(1)
#
#     HTTPProviderXDataSource = getattr(data_source_module, F"GRPCProvider{major_provider_ver}DataSource")
#
#     body = {}
#     for k, v in cfg.grpc_data_source:
#         # Exclude.
#         if k in ('version'):
#             continue
#         # Special
#         # if k == 'url':
#         #     body['url'] = v[0]
#         body[k] = v
#     ds = HTTPProviderXDataSource(**body)
#
#     return ds


def get_command_class_args(cfg, command: Type[ICommand],
                           command_kwargs=None) -> dict:
    # FIXME:
    #   This function knows about someone DS command name. That is not ok.
    if command.__name__ == "GetMessagesByBookByStreams":
        command = inspect.unwrap(command.__init__)
    command_args: List[str] = inspect.getfullargspec(command)[0]
    command_kwargs_ = {k: v for k, v in cfg.request_params if k in command_args}
    if command_kwargs is not None:
        command_kwargs_.update(**command_kwargs)
    return command_kwargs_


# FIXME:
#   commented because should be removed everywhere (another solution should be used)
# def get_command_class(command_name) -> Type[ICommand]:
#     commands_module = try_import_module(
#         "th2_data_services.data_source.lwdp.commands.http")
#     if not commands_module:
#         exit(1)
#     return getattr(commands_module, command_name)


# FIXME:
#   commented because should be removed everywhere (another solution should be used)
# def get_events_command_obj(cfg: CliConfig,
#                            command_kwargs=None) -> ICommand:
#     GetEventsXCommand = get_command_class("GetEventsByBookByScopes")
#
#     commands_args = get_command_class_args(cfg, GetEventsXCommand,
#                                            command_kwargs)
#     return GetEventsXCommand(**commands_args)


# FIXME:
#   commented because should be removed everywhere (another solution should be used)
# def get_http_events_command(cfg: CliConfig, **kwargs) -> IHTTPCommand:
#     return get_events_command_obj('http', cfg, **kwargs)


# def get_grpc_events_command(cfg: Cfg, **kwargs) -> IGRPCProviderCommand:
#    return get_events_command_obj('grpc', cfg, **kwargs)


# FIXME:
#   commented because should be removed everywhere (another solution should be used)
# def get_messages_command_obj(protocol: str, cfg: CliConfig,
#                              command_kwargs=None) -> ICommand:
#     GetMessagesXCommand = get_command_class("GetMessagesByBookByGroups",
#                                             protocol,
#                                             get_major_provider_ver(cfg,
#                                                                    protocol=protocol))
#     commands_args = get_command_class_args(cfg, GetMessagesXCommand,
#                                            command_kwargs)
#     return GetMessagesXCommand(**commands_args)


class VerificationTableTypes:
    VERIFICATION = "verification"
    FIELD = "field"
    COLLECTION = "collection"


FIELD_NAME_POSITION = 0


def get_verification_table_rows(fields: dict) -> List[list]:  # List of rows.
    # ['Name', 'Expected', 'Actual', 'Status', 'Operation', 'Key', 'Hint']
    # TODO - this function should not update values e.g. PASSED to P
    rows: List[list] = []

    for field_name, val in fields.items():
        field_type = val['type']

        if field_type == VerificationTableTypes.FIELD:
            oper = val['operation']

            hint = val.get('hint', '')
            expected = val.get('expected')
            actual = val.get('actual')
            key = val.get('key')

            status = val['status']
            if status == 'PASSED':
                status = 'P'
            elif status == 'FAILED':
                status = 'F'

            rows.append([field_name,
                         expected,
                         actual,
                         status,
                         '==' if oper == 'EQUAL' else oper,
                         '+' if key else '',
                         hint,
                         ])

        elif field_type == VerificationTableTypes.COLLECTION:
            collection_rows = get_verification_table_rows(val['fields'])

            for row in collection_rows:
                row[FIELD_NAME_POSITION] = field_name + ' / ' + row[
                    FIELD_NAME_POSITION]

            rows += collection_rows

    return rows


def th2_verification_table_to_ascii(th2_table: dict):
    if not th2_table['type'] == VerificationTableTypes.VERIFICATION:
        raise Exception('Non verification type')

    rows = get_verification_table_rows(th2_table['fields'])
    t = PrettyTable()
    t.field_names = ['Name', 'Expected', 'Actual', 'Status', 'Operation', 'Key', 'Hint']
    for row in rows:
        t.add_row(row)

    return t


def get_ds_wrapper(ctx: CliContext) -> IDataSourceWrapper:
    """Factory method to create DataSourceWrapper"""
    ds_cfg = ctx.cfg.data_sources[ctx.cfg.default_data_source]
    return create_ds_wrapper(ctx.cli_registry, ds_cfg)


def create_ds_wrapper(cli_registry: CliRegistry, ds_cfg: DataSource) -> IDataSourceWrapper:
    # FixME:
    #   That works until I provide DataSourceWrapper with another __init__
    #   arguments.
    body = {}
    for k, v in ds_cfg:
        if k not in ('version', 'ds_impl', 'cli_ds_class'):
            body[k] = v

    return cli_registry.get_ds_by_cfg_name(ds_cfg.cli_ds_class)(**body)


def truncate_timestamp(obj):
    if obj._start_timestamp // 10000000000000 != 0:
        click.echo("Timestamps are converted from nanoseconds to milliseconds")
        obj._start_timestamp //= 1000000
        obj._end_timestamp //= 1000000
    return obj


def generate_and_save_report(
        *,
        ctx: CliContext,
        data: Data = None,
        command_class_args: dict[str, object] = None,
        test_params: dict[str, object] = None,
        results: dict[str, object]
):
    if not ctx.report_path:
        return

    report = {
        "common_params": {
            "args": command_class_args,
            "extra_params": ctx.extra_params,
            "cfg": ctx.cfg
        },
        "results": results
    }

    if test_params:
        report["test_params"] = test_params

    if data:
        report["ds_metadata"] = data.metadata

    def serializer(o):
        if isinstance(o, datetime.datetime):
            return o.isoformat()
        elif hasattr(o, "__dict__"):
            return o.__dict__

    with open(ctx.report_path, 'w', encoding='utf-8') as json_file:
        json.dump(report, json_file, default=serializer)

def get_exception_info(e: Exception) -> dict[str, str]:
    return {
        "type": str(type(e).__name__),
        "message": str(e),
        "traceback": traceback.format_exc()
    }
