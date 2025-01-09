from typing import List, Optional
from pydantic import BaseModel, Field, PositiveFloat
from datetime import timedelta
import yaml


class TestCase(BaseModel):
    name: str
    args: List[str]
    timeout_sec: Optional[float]
    no_cfg: bool = Field(default=False)

    def __init__(self, **data):
        super().__init__(**data)


class TestingConfig(BaseModel):
    default_testing_interval_sec: timedelta = Field(default=timedelta(days=1))
    default_test_timeout_sec: PositiveFloat = Field(default=60.0)
    request_params_path: str = Field(default="./request_params.yaml")
    report_file_path: str = Field(default="./report.jsonl")
    test_cases: List[TestCase] = Field(default_factory=list)

    def __init__(self, filename: str):
        with open(filename, 'r') as f:
            config_data = yaml.safe_load(f)

        if 'default_testing_interval_sec' in config_data:
            config_data['default_testing_interval_sec'] = timedelta(seconds=config_data['default_testing_interval_sec'])

        super().__init__(**config_data)

        for test in self.test_cases:
            if test.timeout_sec is None:
                test.timeout_sec = self.default_test_timeout_sec
