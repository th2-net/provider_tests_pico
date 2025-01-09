import json
import os
import re
import time
from typing import Optional
import click

from th2_data_services.data import Data
from th2_data_services.data_source.lwdp.event_tree import HttpETCDriver
from th2_data_services.event_tree.event_tree_collection import EventTreeCollection
from th2_data_services.utils.converters import Th2TimestampConverter
from th2_ds.cli_util.context import CliContext
from th2_ds.cli_util.decorators import http_error_wrapper, cli_command
from th2_ds.cli_util.interfaces.plugin import DSPlugin
from th2_ds.cli_util.utils import counter, reset_counter, setup_counter, \
    get_command_class_args, show_info, data_counter, get_ds_wrapper
from th2_ds.cli_util.impl import data_source_wrapper as ds_w


def write_data_to_file(data, out_file):
    with open(out_file, "w") as f:
        setup_counter()
        data = data.map(counter)
        for m in data:
            print(json.dumps(m, separators=(",", ":")), file=f)
        reset_counter()


def print_data_to_stdout(data):
    for m in data:
        print(json.dumps(m, separators=(",", ":")))


def get_etc(events: Data, ds):
    with data_counter(events) as data:
        driver = HttpETCDriver(data_source=ds, use_stub=True)
        etc = EventTreeCollection(driver)
        etc.build(data)

    return etc


outfile_opt = click.option("-o", "--out-dir", required=True)
filter_by_name_opt = click.option("--exclude-events-by-name",
                                  help="Excludes events with specified regexp")


@cli_command(name='events-tree')
@outfile_opt
@filter_by_name_opt
@http_error_wrapper
def events_tree(ctx: CliContext, out_dir: str, exclude_events_by_name: Optional[str]):
    """Get events from DataProvider and builds Events Tree and save to file

    Examples:

    [1] ./ds.py events-tree -c configs/qse.yaml -o trees --exclude-events-by-name='Checkpoint for session'

    """
    data_source = get_ds_wrapper(ctx)
    data_source.accept(Plugin(), out_dir=out_dir, exclude_events_by_name=exclude_events_by_name, ctx=ctx)


def common_logic(data: Data,
                 ds_wrapper: ds_w.CommonLogicForLwdpRelatedClasses,
                 command_class_args: dict,
                 ctx: CliContext,
                 out_dir: str,
                 exclude_events_by_name: str):
    # maj_version = version.split('.')[0]
    # if int(maj_version) != 5:
    #     click.secho('THIS PLUGIN SUPPORTS PROVIDER V5 ONLY', fg='red')
    #     exit(1)

    show_info(ctx.extra_params, command_class_args, urls=data.metadata["urls"])

    start = time.time()

    from th2_data_services.data_source.lwdp.struct import http_event_struct
    # from th2_data_services.provider.v5.struct import provider5_event_struct as event_struct

    if exclude_events_by_name:
        data = data.filter(
            lambda e: re.search(exclude_events_by_name, e[http_event_struct.NAME]) is None)

    data = data.map(lambda e: {
        http_event_struct.EVENT_ID: e[http_event_struct.EVENT_ID],
        http_event_struct.PARENT_EVENT_ID: e[http_event_struct.PARENT_EVENT_ID],
        http_event_struct.NAME: e[http_event_struct.NAME],
        http_event_struct.EVENT_TYPE: e[http_event_struct.EVENT_TYPE],
        http_event_struct.START_TIMESTAMP: e[http_event_struct.START_TIMESTAMP],
        http_event_struct.END_TIMESTAMP: e[http_event_struct.END_TIMESTAMP],
        http_event_struct.STATUS: e[http_event_struct.STATUS],
    })

    etc = get_etc(data, ds_wrapper.ds_impl)
    print(etc)

    trees = etc.get_trees()

    def get_line(e: dict, level: int, final=False):
        final_sign = '└──' if final else '├──'

        if level == 0:
            pre = ''
        else:
            pre = f"{'│   ' * (level - 1)}{final_sign} "

        st_time = Th2TimestampConverter.to_datetime(e[http_event_struct.START_TIMESTAMP])
        et_time = Th2TimestampConverter.to_datetime(e[http_event_struct.END_TIMESTAMP])

        status = '[P]' if e[http_event_struct.STATUS] else '[F]'

        return f"{pre}{status} {e[http_event_struct.NAME]:<{50 - level * 4}}  |  Type: {e[http_event_struct.EVENT_TYPE]}  |   {st_time}  -  {et_time}"

    def get_all_family(tree, e, level, file):
        line = get_line(e, level)
        print(line, file=file)
        for e in tree.get_children_iter(e[http_event_struct.EVENT_ID]):
            get_all_family(tree, e, level + 1, file)

    files = {}

    os.makedirs(out_dir, exist_ok=True)

    # tree.show()
    for tree in trees:
        file = tree.get_root_name()
        filename = out_dir + '/' + file + '.tree'  # UNIX only
        if file in files:
            files[file] += 1
            filename += '.' + str(files[file])
        else:
            filename += '.0'
            files[file] = 0

        f = open(filename, 'w')
        get_all_family(tree, tree.get_root(), level=0, file=f)
        f.close()

    t = time.time() - start
    print(f"Got: {data.len} events in {t} seconds (~{data.len / t} per second)")


class Plugin(DSPlugin):
    def version(self) -> str:
        return '0.3.0'

    def root(self) -> click.Command:
        """The group or command to attach to ds.py cli."""
        return events_tree

    def _get_common_lwdp_objects_for_common_logic(self, ds_wrapper: ds_w.CommonLogicForLwdpRelatedClasses, **kwargs):
        ctx = kwargs['ctx']
        out_dir = kwargs['out_dir']
        exclude_events_by_name = kwargs['exclude_events_by_name']

        get_events_cmd_obj = ds_wrapper.get_events_obj(ctx)
        data: Data = ds_wrapper.ds_impl.command(get_events_cmd_obj)
        command_class_args = get_command_class_args(ctx.cfg, type(get_events_cmd_obj))

        return dict(data=data,
                    ds_wrapper=ds_wrapper,
                    command_class_args=command_class_args,
                    ctx=ctx,
                    out_dir=out_dir,
                    exclude_events_by_name=exclude_events_by_name)

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
