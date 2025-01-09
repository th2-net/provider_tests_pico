#!/usr/bin/env python3
import traceback
from datetime import datetime, timezone
import click
import importlib
import pkgutil
from pathlib import Path

__version__ = '2.2.0'

from th2_ds.cli_util.interfaces.plugin import DSPlugin


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(F"Version {__version__}")
    ctx.exit()


@click.group()
@click.option("--version", is_flag=True, callback=print_version, expose_value=False, is_eager=True)
def cli():
    """Data Services CLI util"""


def import_plugins(root_gr, plugins_folder_path: str) -> dict:  # {plugin_name: plugin_module class}
    module_path = plugins_folder_path.replace('/', '.')
    builtin_plugins = {}
    for finder, name, ispkg in pkgutil.iter_modules([str(Path(plugins_folder_path).resolve())]):
        if not ispkg:
            full_module_path = F"{module_path}.{name}"
            try:
                plugin_module = importlib.import_module(full_module_path)
                builtin_plugins[name] = importlib.import_module(full_module_path)
                if 'Plugin' in dir(plugin_module):
                    plugin: DSPlugin = plugin_module.Plugin()
                    root_gr.add_command(plugin.get_root_group())
                # if 'ExtensionPlugin' in dir(plugin_module):
                #     plugin: DSPlugin = plugin_module.ExtensionPlugin()

            except Exception as e:
                click.secho(F"Cannot load plugin: '{full_module_path}'", bg='red')
                print(traceback.format_exc())
                print()
        else:
            path = module_path + '.' + name
            plugin_module = importlib.import_module(F"{path}.{'__init__'}")
            if 'Package' in dir(plugin_module):
                plugin: DSPlugin = plugin_module.Package()
                gr = plugin.get_root_group()
                root_gr.add_command(gr)
                r = import_plugins(gr, module_path + '/' + name)
                builtin_plugins.update(r)

    return builtin_plugins


def main():
    # name: importlib.import_module(F"th2_data_services.cli_util.plugins.{name}")
    import_plugins(cli, 'th2_ds/cli_util/plugins')
    import_plugins(cli, 'dsplugins')
    # from dsplugins_test import extend_get_plugin

    dt_object = datetime.now()
    local_datetime = dt_object.astimezone()
    local_tzinfo = local_datetime.tzinfo
    local_sec_start = local_datetime.timestamp()
    utc_datetime = dt_object.astimezone(timezone.utc)
    utc_tzinfo = utc_datetime.tzinfo
    utc_sec = utc_datetime.timestamp()
    click.secho(f'[ds cli] LOCAL: {local_datetime} [{local_tzinfo}]', fg='green')
    click.secho(f'           UTC: {utc_datetime} [{utc_tzinfo}] | sec: {utc_sec}', fg='green')
    print()

    try:
        cli()
    except Exception:
        raise
    finally:
        dt_object = datetime.now()
        local_datetime = dt_object.astimezone()
        local_tzinfo = local_datetime.tzinfo
        local_sec_end = local_datetime.timestamp()
        utc_datetime = dt_object.astimezone(timezone.utc)
        utc_tzinfo = utc_datetime.tzinfo
        utc_sec = utc_datetime.timestamp()

        print()
        click.secho(f'[ds cli] LOCAL: {local_datetime} [{local_tzinfo}]', fg='green')
        click.secho(f'           UTC: {utc_datetime} [{utc_tzinfo}] | sec: {utc_sec}', fg='green')
        click.secho(f'     Took time: {local_sec_end - local_sec_start}, s')


if __name__ == "__main__":
    """
    This is a script that allows you to receive data, check the speed of their upload and perform
    various types of data analysis

    It is possible to plug-in tests here

    ds get events -c var1.yaml -o outfile --print
    ds get messages
    ds get messages-by-id A1, A2 ... --from-file

    ds speed-test -t events
    ds speed-test -t messages

    # Functions for analyzing data over a period of time
    ds analysis density -t events
    ds analysis density -t messages

    ds analysis barch

    ds analysis duplicates

    ds analysis sequence --from-file
        Must build a graph based on x - sequence number of the message, on y - timestamp.
        Should answer the question whether the data received over the period is consistent
        Should return the intervals of normal growth and abnormal growth

        Each of their streams and directions should be separately

    ds demo
    """

    main()
