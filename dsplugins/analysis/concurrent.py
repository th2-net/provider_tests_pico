import json
import os
import time
from copy import copy

import click
import urllib3

from th2_data_services.data import Data
from dsplugins.analysis import analysis
from th2_ds.cli_util.utils import get_ds_wrapper, get_command_class_args, generate_and_save_report, get_exception_info
from th2_ds.cli_util.decorators import http_error_wrapper, cli_command
from th2_ds.cli_util.interfaces.plugin import DSPlugin
from th2_ds.cli_util.context import CliContext
from th2_ds.cli_util.plugins.speed_test import common_logic
from multiprocessing import Process
from th2_ds.cli_util.impl import data_source_wrapper as ds_w

num_conc_req_opt = click.option("-n", "--num-concurrent-requests", "n_procs", type=click.IntRange(1), required=True)

@analysis.group()
def concurrent():
    """Do X concurrent requests to the server."""


@http_error_wrapper
def requester(data: Data, command_class_args: dict, ctx: CliContext, repetitions: int, rtype: str, _try=1):
    print(f"try: {_try}")
    try:
        common_logic(data, command_class_args, ctx, repetitions, rtype)
    except urllib3.exceptions.MaxRetryError:
        time.sleep(0.5)
        requester(data, command_class_args, ctx, repetitions, rtype, _try=_try + 1)


@cli_command(group=concurrent, name="messages")
@num_conc_req_opt
def concurrent_messages(ctx: CliContext, n_procs):
    data_source = get_ds_wrapper(ctx)
    exit_code = data_source.accept(Plugin(), n_procs=n_procs, rtype="messages", ctx=ctx)
    exit(exit_code)


@cli_command(group=concurrent, name="events")
@num_conc_req_opt
def concurrent_events(ctx: CliContext, n_procs):
    data_source = get_ds_wrapper(ctx)
    exit_code = data_source.accept(Plugin(), n_procs=n_procs, rtype="events", ctx=ctx)
    exit(exit_code)


def common(n_procs, **kwargs):
    exit_code = 0
    test_params = {
        "n_procs": n_procs,
        "repetitions": kwargs["repetitions"],
        "rtype": kwargs["rtype"]
    }

    results = {}
    ctx: CliContext = None

    try:
        ppool = []
        ctx = kwargs["ctx"]

        for i in range(n_procs):
            proc_kwarg = kwargs.copy()
            proc_ctx: CliContext = copy(ctx)
            proc_ctx.report_path = ctx.report_path + f"_{i}"
            proc_kwarg["ctx"] = proc_ctx
            ppool.append(Process(target=requester, kwargs=proc_kwarg))

        for p in ppool:
            p.start()
            time.sleep(1)

        for i, p in enumerate(ppool):
            p.join()
            process_report_filename = ctx.report_path + f"_{i}"
            with open(process_report_filename, 'r', encoding='utf-8') as json_file:
                 subprocess_res = json.load(json_file)
            try:
                os.remove(process_report_filename)
            except:
                pass

            results[p.name] = subprocess_res["results"]

    except Exception as e:
        results["exception"] = get_exception_info(e)
        exit_code = 1

    generate_and_save_report(
        ctx=ctx,
        data=kwargs["data"],
        command_class_args=kwargs.get("command_class_args", {}),
        test_params=test_params,
        results=results
    )

    if exit_code != 0:
        exit(exit_code)


class Plugin(DSPlugin):
    def root(self) -> click.Command:
        return concurrent

    def version(self) -> str:
        return '2.0.0'

    def _get_common_lwdp_objects_for_common_logic(self, ds_wrapper: ds_w.CommonLogicForLwdpRelatedClasses, **kwargs):
        ctx = kwargs['ctx']
        rtype = kwargs['rtype']
        n_procs = kwargs['n_procs']

        if rtype == 'events':
            get_events_cmd_obj = ds_wrapper.get_events_obj(ctx)
            data: Data = ds_wrapper.ds_impl.command(get_events_cmd_obj)
            command_class_args = get_command_class_args(ctx.cfg, type(get_events_cmd_obj))

        elif rtype == 'messages':
            get_messages_cmd_obj = ds_wrapper.get_messages_obj(ctx)
            data: Data = ds_wrapper.ds_impl.command(get_messages_cmd_obj)
            command_class_args = get_command_class_args(ctx.cfg,type(get_messages_cmd_obj))

        else:
            raise RuntimeError(f'Unknown Rtype: {rtype}')

        return n_procs, dict(data=data, command_class_args=command_class_args, ctx=ctx, repetitions=1, rtype=rtype)

    def visit_lwdp1_http_data_source(self, ds_wrapper: ds_w.Lwdp1HttpDataSource, **kwargs):
        n_procs, cl_kw = self._get_common_lwdp_objects_for_common_logic(ds_wrapper, **kwargs)
        return common(n_procs, **cl_kw)

    def visit_rpt5_http_data_source(self, ds_wrapper: ds_w.Rpt5HttpDataSource, **kwargs):
        n_procs, cl_kw = self._get_common_lwdp_objects_for_common_logic(ds_wrapper, **kwargs)
        return common(n_procs, **cl_kw)

    def visit_lwdp2_http_data_source(self, ds_wrapper: ds_w.Lwdp2HttpDataSource, **kwargs):
        n_procs, cl_kw = self._get_common_lwdp_objects_for_common_logic(ds_wrapper, **kwargs)
        return common(n_procs, **cl_kw)

    def visit_lwdp3_http_data_source(self, ds_wrapper: ds_w.Lwdp3HttpDataSource, **kwargs):
        n_procs, cl_kw = self._get_common_lwdp_objects_for_common_logic(ds_wrapper, **kwargs)
        return common(n_procs, **cl_kw)
