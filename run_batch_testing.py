import json
import os
import sys
import subprocess
from pathlib import Path
from subprocess import TimeoutExpired
import yaml
from datetime import datetime
import time

from testing_config import TestingConfig, TestCase
from th2_ds.cli_util.impl.data_source_wrapper import Lwdp3HttpDataSource
from th2_data_services.data_source.lwdp.commands.http import GetBooks, GetEventScopes, GetMessageAliases, \
    GetMessageGroups

TESTING_CONFIG_PATH = r'configs/batch_testing_config.yaml'
TEST_REPORT_PATH = r'./test_report.json'
DATA_SOURCES_CFG = r'./configs/data_sources.yaml'
CFG_FILES = [
    r'./lw_dp.yaml',
    r'./rpt_dp.yaml'
]

TIMEOUT_EXIT_CODE = 124

def prepare_config_files():
    with open(DATA_SOURCES_CFG, 'r') as file:
        lw_dp_datasource: dict[str, any] = yaml.safe_load(file)['data_sources']['lw_dp']

    # setting default values in config
    ds = Lwdp3HttpDataSource(lw_dp_datasource['url'])

    with open(testing_config.request_params_path, 'r') as file:
        request_params: dict[str, any] = yaml.safe_load(file) or {}

    book_id = request_params.get('book_id')
    if book_id is None:
        get_books_cmd_obj = GetBooks()
        books_list = list(ds.ds_impl.command(get_books_cmd_obj))
        if len(books_list) != 1:
            raise ValueError(f"If `book_id` is not specified, database should contain exactly one book. Actual books: {books_list}.")
        book_id = books_list[0]
        request_params['book_id'] = book_id

    end_timestamp = request_params.get('end_timestamp')
    if isinstance(end_timestamp, str):
        end_timestamp = datetime.strptime(end_timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
    elif isinstance(end_timestamp, int):
        end_timestamp = datetime.utcfromtimestamp(end_timestamp // 10**9)
    elif end_timestamp is None:
        end_timestamp = datetime.utcnow()
        request_params['end_timestamp'] = end_timestamp.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    start_timestamp = request_params.get('start_timestamp')
    if isinstance(start_timestamp, str):
        start_timestamp = datetime.strptime(start_timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
    elif isinstance(start_timestamp, int):
        start_timestamp = datetime.utcfromtimestamp(start_timestamp // 10**9)
    elif start_timestamp is None:
        start_timestamp = end_timestamp - testing_config.default_testing_interval_sec
        request_params['start_timestamp'] = request_params['start_timestamp'].strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    if 'scopes' not in request_params:
        get_scopes_cmd_obj = GetEventScopes(book_id, start_timestamp, end_timestamp)
        request_params['scopes'] = list(ds.ds_impl.command(get_scopes_cmd_obj))

    if 'streams' not in request_params:
        get_aliases_cmd_obj = GetMessageAliases(book_id, start_timestamp, end_timestamp)
        request_params['streams'] = list(ds.ds_impl.command(get_aliases_cmd_obj))

    if 'groups' not in request_params:
        get_groups_cmd_obj = GetMessageGroups(book_id, start_timestamp, end_timestamp)
        request_params['groups'] = list(ds.ds_impl.command(get_groups_cmd_obj))

    lw_request_params = request_params.copy()
    del lw_request_params['streams']
    lw_dp_cfg = {
        'default_data_source': 'lw_dp',
        'get_messages_mode': 'ByGroups',
        'request_params': lw_request_params
    }

    del request_params['groups']
    rpt_dp_cfg = {
        'default_data_source': 'rpt_dp',
        'get_messages_mode': 'ByStreams',
        'request_params': request_params
    }

    with open(CFG_FILES[0], 'w') as yaml_file:
        yaml.dump(lw_dp_cfg, yaml_file, default_flow_style=False)
        print(f"File {CFG_FILES[0]} saved. Content:")
        with open(CFG_FILES[0], 'r') as read_file:
            print(read_file.read())

    with open(CFG_FILES[1], 'w') as yaml_file:
        yaml.dump(rpt_dp_cfg, yaml_file, default_flow_style=False)
        print(f"File {CFG_FILES[1]} saved. Content:")
        with open(CFG_FILES[1], 'r') as read_file:
            print(read_file.read())

def execute_test_case(test_case: TestCase, cfg: str = None) -> tuple[str, list[str], float, int, float, str]:
    try:
        os.remove(TEST_REPORT_PATH)
    except:
        pass

    command_args = ['ds.py', *test_case.args, f'-r{TEST_REPORT_PATH}']
    if cfg:
        command_args.append('-c')
        command_args.append(cfg)
    print('./' + ' '.join(command_args))

    start_time = time.time()

    try:
        execution_result = subprocess.run([sys.executable, *command_args], timeout=test_case.timeout_sec)
        exit_code = execution_result.returncode
    except TimeoutExpired:
        exit_code = TIMEOUT_EXIT_CODE

    execution_time = time.time() - start_time

    try:
        with open(TEST_REPORT_PATH, 'r', encoding='utf-8') as file:
            test_report = json.load(file)
    except FileNotFoundError:
        test_report = {}

    report_message = f"{test_case.name} for {Path(cfg).stem}" if cfg else test_case.name
    return report_message, command_args, test_case.timeout_sec, exit_code, execution_time, test_report


def execute_tests() -> int:
    return_codes_sum = 0
    test_results = []

    for cfg in CFG_FILES:
        print('#'*100)
        print(cfg)
        print('#' * 100)

        for test_case in testing_config.test_cases:
            if not test_case.no_cfg:
                results = execute_test_case(test_case, cfg)
                test_results.append(results)

    for test_case in testing_config.test_cases:
        if test_case.no_cfg:
            results = execute_test_case(test_case)
            test_results.append(results)

    with open(testing_config.report_file_path, 'w') as report_file:
        for test_name, args, timeout, exit_code, execution_time, test_report in test_results:

            return_codes_sum += exit_code

            if exit_code == 0:
                status = 'PASSED'
            elif exit_code == TIMEOUT_EXIT_CODE:
                status = 'TIMED_OUT'
            else:
                status = 'FAILED'

            report_entry = {"name": test_name, "status": status, "execution_time": round(execution_time, 2),
                            "args": ' '.join(args), "timeout": timeout, "exit_code": exit_code,
                            'test_report': test_report}

            # only detailed reports for failed tests added
#            if exit_code != 0 and exit_code != TIMEOUT_EXIT_CODE:
#                report_entry['test_report'] = test_report

            report_file.write(json.dumps(report_entry) + '\n')

    return 0 if return_codes_sum == 0 else 1


if __name__ == "__main__":
    testing_config: TestingConfig = TestingConfig(TESTING_CONFIG_PATH)
    prepare_config_files()
    result = execute_tests()
    sys.exit(result)
