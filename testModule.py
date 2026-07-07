from config import GlobalConfig
from config import OperationConfig
from config import ConfigLoader

from config import OperationConfig

op = OperationConfig(
    type="timezone_update",
    source_timezone="America/New_York",
    target_timezone="UTC",
    target_table_suffix="_utc",
)

print(op)

from config import ConfigLoader

cfg = ConfigLoader.load("config/timezone_conversion_config_v2.json")

print(cfg)
