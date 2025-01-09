import json
from typing import Union, List
import click

from th2_data_services.data import Data
from th2_ds.cli_util.context import CliContext
from th2_ds.cli_util.decorators import http_error_wrapper, cli_command
from th2_ds.cli_util.impl import data_source_wrapper as ds_w
from th2_ds.cli_util.interfaces.plugin import DSPlugin
from th2_ds.cli_util.utils import counter, reset_counter, setup_counter, show_info, \
    get_command_class_args, data_counter, get_ds_wrapper
from th2_ds.utils.summary import Metric, get_all_metric_combinations, SummaryCalculator, get_message_type


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


"""
FAQ
    1. What metrics will be the default and what combinations?
        ---
        Events 
            - status
            - 
        Messages
            - session
            - direction
            - messageType
    2. How to expand metrics? How to change default combinations?
        You can add a separate configuration for plugins

    3. Will there be separate summary for events and messages?
        --
        Yes

Requirements
        1. The module should be able to be imported and run as a script

TODO
    1. You will need to add a description in the readme of how to work with this module

"""


@click.group()
def summary():
    """Will provide such summary by messages

    Memory friendly function that works in a stream way.

    In essence, here we get a multidimensional matrix that is calculated on the fly.
    Splits into many two-dimensional matrices

    ----
    Time A - B
    Sessions: 5


    TOTAL by session
    session	| total |
    nft1	| 104 |

    TOTAL by direction
    direction	| total |
    OUT	| 104 |
    IN	| 104 |

    TOTAL by IN
    session	| total |
    nft1	| 104 |


    TOTAL by session/message type
    session	| message type	| cnt |
    nft1	| NOS			|	  |
                            | total |

    TOTAL by session/direction type
    session	| direction	| cnt |
    nft1	| IN			|	  |
                            | total |

    TOTAL by session/direction/message_type
    session	| direction	| cnt |

    BY MsgType
    NOS table
    """


@cli_command(name='messages', group=summary)
@http_error_wrapper
def messages(ctx: CliContext):
    """Get messages from DataProvider

    By default, messages will be printed to stdout.
    """

    direction_m = Metric('direction', lambda m: m['direction'])
    session_m = Metric('session', lambda m: m['sessionId'])
    message_type_m = Metric('messageType', get_message_type)

    metrics_list = [
        direction_m,
        session_m,
        message_type_m,
    ]

    all_metrics_combinations = get_all_metric_combinations(metrics_list)

    data_source = get_ds_wrapper(ctx)
    sc = data_source.accept(Plugin(), rtype="messages", ctx=ctx, metrics=metrics_list, combinations=all_metrics_combinations)
    sc.show()


@cli_command(name='events', group=summary)
def events(ctx: CliContext):
    """..
    """

    metrics_list = [
        Metric('type', lambda m: m['eventType']),
        Metric('successful', lambda m: m['successful']),
    ]

    all_metrics_combinations = get_all_metric_combinations(metrics_list)

    data_source = get_ds_wrapper(ctx)
    sc = data_source.accept(Plugin(), rtype="events", ctx=ctx, metrics=metrics_list, combinations=all_metrics_combinations)
    sc.show()


@http_error_wrapper
def common_logic(data: Data,
                 command_class_args: dict,
                 ctx: CliContext,
                 metrics: Union[List[str], List[Metric]],
                 combinations: list):
    show_info(ctx.extra_params, command_class_args, urls=data.metadata["urls"])

    sc = SummaryCalculator(metrics, combinations)

    with data_counter(data) as data_:
        for m in data_:
            sc.append(m)

    return sc


class Plugin(DSPlugin):
    def version(self) -> str:
        return '0.1.0'

    def root(self) -> click.Command:
        """The group or command to attach to ds.py cli."""
        return summary

    def _get_common_lwdp_objects_for_common_logic(
            self, ds_wrapper: ds_w.CommonLogicForLwdpRelatedClasses, **kwargs):
        ctx = kwargs['ctx']
        rtype = kwargs['rtype']
        metrics = kwargs["metrics"]
        combinations = kwargs["combinations"]

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

        return dict(data=data, command_class_args=command_class_args, ctx=ctx, metrics=metrics, combinations=combinations)

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
