from pprint import pprint
import click

from th2_ds.cli_util.config import _get_cfg
from th2_ds.cli_util.utils import get_datasource, _show_info
from th2_ds.cli_util.decorators import http_error_wrapper, common_wrapper
from th2_ds.cli_util.interfaces.plugin import DSPlugin
from th2_data_services.events_tree import EventsTree2
from th2_data_services.events_tree.events_tree import TreeNode


def get_data_tree(et, ds):
    data1_tree = {}  # {Full_name: attachedMessageIds_LIST}
    for root in et.roots:
        if 'sim-demo' in root.name:
            continue
        e: TreeNode
        for e in root.descendants:
            attached_ids_dict = {}
            for _id in e.data['attachedMessageIds']:
                try:
                    attached_ids_dict[_id] = ds.find_messages_by_id_from_data_provider(_id)
                except ValueError as er:
                    click.secho(e.path_str, bg='red')
                    click.secho(F"{er}\n"
                                F"Event:")
                    pprint(e.data)
                    print()
                    attached_ids_dict[_id] = None

            data1_tree[e.path_str] = attached_ids_dict
    return data1_tree


class Plugin(DSPlugin):
    @classmethod
    def get_plugin(cls, cli):
        analysis = cli.commands['analysis']

        @analysis.group()
        def links():
            """.."""
            pass

        @common_wrapper(group=links, command_name="1source")
        @http_error_wrapper
        def two_sources(ctx, cfg_path, verbose):
            cfg1, extra_params = _get_cfg(ctx, cfg_path)
            ds1 = get_datasource(cfg1)
            data1 = get_data_obj('events', ds1, cfg1, attachedMessages=True)
            _show_info(extra_params, cfg1, data1)
            print()

            for e in data1:
                if e['attachedMessageIds']:
                    print(F"eventName: {e['eventName']}\n "
                          F"eventId: {e['eventId']}, \n"
                          F"attachedMessageIds: {e['attachedMessageIds']}\n")

        @common_wrapper(group=links, command_name="2sources")
        @click.option("-c2", "--cfg-path2", required=True)
        @http_error_wrapper
        def two_sources(ctx, cfg_path, cfg_path2, verbose):
            """Links test

            Only for events.

            This test will download the data over the period and compare that the data is identical to the data from the transferred file

            Difficulties and problems of the test:
                1. There can be many messages linked in Checkpoint, it is not guaranteed that the same number will be on another server
                2. Check sequence rule may receive X messages, filter out Y, but will still link to X, which may not be the case on another server

            """

            cfg1, extra_params = _get_cfg(ctx, cfg_path)
            ds1 = get_datasource(cfg1)
            data1 = get_data_obj('events', ds1, cfg1, attachedMessages=True)
            _show_info(extra_params, cfg1, data1)
            print()
            cfg2, extra_params = _get_cfg(ctx, cfg_path2)
            ds2 = get_datasource(cfg2)
            data2 = get_data_obj('events', ds2, cfg2, attachedMessages=True)
            _show_info(extra_params, cfg2, data2)

            et1 = EventsTree2(data1, ds=ds1)
            data1_tree = get_data_tree(et1, ds1)

            et2 = EventsTree2(data2, ds=ds2)
            data2_tree = get_data_tree(et2, ds2)

            x = """
                        'direction': 'OUT',
              'sessionId': 'demo-conn1',
              'messageType': 'NewOrderSingle',
              'attachedEventIds': [],
              'type': 'message',
              """

            x = """
   x['demo-conn1:second:1638201856440573119']['body']['fields']
            {'OrderQty': {'simpleValue': '30'},
 'OrdType': {'simpleValue': '2'},
 'ClOrdID': {'simpleValue': '9797056'},
 'SecurityIDSource': {'simpleValue': '8'},
 'OrderCapacity': {'simpleValue': 'A'},
 'TransactTime': {'simpleValue': '2021-11-29T19:58:34'},
 'SecondaryClOrdID': {'simpleValue': '11111'},
 'AccountType': {'simpleValue': '1'},
 'trailer': {'messageValue': {'fields': {'CheckSum': {'simpleValue': '187'}}}},
 'Side': {'simpleValue': '1'},
 'Price': {'simpleValue': '65'},
 'TimeInForce': {'simpleValue': '0'},
 'TradingParty': {'messageValue': {'fields': {'NoPartyIDs': {'listValue': {'values': [{'messageValue': {'fields': {'PartyRole': {'simpleValue': '76'},
          'PartyID': {'simpleValue': 'DEMO-CONN1'},
          'PartyIDSource': {'simpleValue': 'D'}}}},
       {'messageValue': {'fields': {'PartyRole': {'simpleValue': '3'},
          'PartyID': {'simpleValue': '0'},
          'PartyIDSource': {'simpleValue': 'P'}}}},
       {'messageValue': {'fields': {'PartyRole': {'simpleValue': '122'},
          'PartyID': {'simpleValue': '0'},
          'PartyIDSource': {'simpleValue': 'P'}}}},
       {'messageValue': {'fields': {'PartyRole': {'simpleValue': '12'},
          'PartyID': {'simpleValue': '3'},
          'PartyIDSource': {'simpleValue': 'P'}}}}]}}}}},
 'SecurityID': {'simpleValue': 'INSTR6'},
 'header': {'messageValue': {'fields': {'BeginString': {'simpleValue': 'FIXT.1.1'},
    'SenderCompID': {'simpleValue': 'DEMO-CONN1'},
    'SendingTime': {'simpleValue': '2021-11-29T16:58:35.155'},
    'TargetCompID': {'simpleValue': 'FGW'},
    'MsgType': {'simpleValue': 'D'},
    'MsgSeqNum': {'simpleValue': '119'},
    'BodyLength': {'simpleValue': '249'}}}}}

            """

            print()
            print()

            # COMPARE
            common_event_names = set(list(data1_tree.keys())) & set(list(data2_tree.keys()))
            print(F"common_event_names:")
            pprint(common_event_names)

            for event_name in common_event_names:
                if 'Checkpoint' in event_name:
                    continue
                e1: dict = data1_tree[event_name]
                e2: dict = data2_tree[event_name]
                e1_messages = list(e1.values())
                e2_messages = list(e2.values())

                click.secho(event_name)
                if e1_messages == [] or e2_messages == []:
                    if e1_messages == e2_messages:
                        click.secho(F"{e1_messages} - Passed", bg='green')
                    else:
                        click.secho(F"{e1_messages} != {e2_messages}", bg='red')
                        print(e1_messages)
                        print(e2_messages)
                        print()

                else:
                    if len(e1_messages) == len(e2_messages):
                        click.secho(F"len check, msgs={len(e1_messages)} - Passed", bg='green')

                        if len(e1_messages) == 1:
                            if (e1_messages[0]['direction'] == e2_messages[0]['direction'] and
                                    e1_messages[0]['sessionId'] == e2_messages[0]['sessionId'] and
                                    e1_messages[0]['messageType'] == e2_messages[0]['messageType'] and
                                    e1_messages[0]['type'] == e2_messages[0]['type']):
                                click.secho(F"len check with len=1 - Passed", bg='green')
                            else:
                                click.secho(F"len check with len=1 - Failed", bg='red')
                                print(e1_messages)
                                print(e2_messages)
                                print()
                        else:
                            click.secho(F"Check manually!", bg='yellow')
                            print(e1_messages)
                            print(e2_messages)
                            print()

                    else:
                        click.secho(F"{len(e1_messages)} != {len(e2_messages)}", bg='red')
                        # print('e1_messages:')
                        # print([{**m} for m in e1_messages])
                        print(e1_messages)
                        print(e2_messages)
                        print()

                # if data1_tree[event_name] == data2_tree[event_name]:
                #     click.secho(F"{data1_tree[event_name]}", bg='green')
                # else:
                #     click.secho(F"{data1_tree[event_name]} != data2_tree[event_name]", bg='red')

            # for e in data1:
            #     if e['attachedMessageIds']:
            #         print(F"eventName: {e['eventName']}\n "
            #               F"eventId: {e['eventId']}, \n"
            #               F"attachedMessageIds: {e['attachedMessageIds']}\n")

        return links

    def version(self) -> str:
        return '0.0.1'
