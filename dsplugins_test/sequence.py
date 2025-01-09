from datetime import datetime, timedelta
import click

from th2_ds.cli_util.config import _get_cfg
from th2_ds.cli_util.utils import get_datasource, _show_info, counter, reset_counter, setup_counter
from th2_ds.cli_util.decorators import http_error_wrapper, common_wrapper
from th2_ds.cli_util.interfaces.plugin import DSPlugin
from th2_data_services.data import Data


class Plugin(DSPlugin):
    @classmethod
    def get_plugin(cls, cli):
        @common_wrapper(group=cli.commands['analysis'], command_name="sequence", help="sequence")
        @click.option("--from-file")
        @http_error_wrapper
        def sequence(ctx, cfg_path, from_file, verbose):
            # TODO - We need to check that the data on streaming and direction is only growing!!!! and graph by key plot
            cfg, extra_params = _get_cfg(ctx, cfg_path)
            ds = get_datasource(cfg)

            messages: Data = ds.get_messages_from_data_provider(
                startTimestamp=cfg.start_time,
                endTimestamp=cfg.end_time,
                stream=cfg.streams,
            )
            messages.use_cache(True)

            _show_info(extra_params, cfg, messages)

            import pandas as pd
            import plotly.express as px

            def transform_time(record):
                try:
                    time = datetime.fromtimestamp(record["timestamp"].example("epochSecond", 0))
                    time += timedelta(microseconds=record["timestamp"].example("nano", 0)) / 1000
                    return {"session_dir": record["sessionId"] + ":" + record["direction"],
                            "seqnum": record["messageId"].split(":")[-1], "timestamp": time}
                except Exception as e:
                    print(f"Exception: {e}")
                    print(record)

            transformed_messages = messages.map(counter).map(transform_time)

            if not transformed_messages.is_empty:
                setup_counter()
                df = pd.DataFrame(transformed_messages)
                reset_counter()
                print(df)

                fig = px.line(df, x="seqnum", y="timestamp", color="session_dir",
                              title=f"{cfg.start_time} - {cfg.end_time} | Msgs: {transformed_messages.len}")  # title='v..',
                fig.show()
            else:
                click.secho('Data is empty', bg='red')

        return sequence

    def version(self) -> str:
        return '1.0.1'
