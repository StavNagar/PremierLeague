import pytz
import datetime
import pandas as pd
from loggers.logger import get_logger


class Transformation:
    TYPE_MAP = {
        "int": int,
        "str": str,
        "bool": bool,
        "float": float
    }

    def __init__(self, transform_config: dict):
        self.transform_config = transform_config.get("CONFIG", {})
        self.extention = transform_config.get("EXTENTION", None)
        self.logger = get_logger(self.__class__.__name__)

    def extract_nested(self, data, path, default=None):
        if not path:
            return default
        if not isinstance(path, str):
            self.logger.warning(f"FAIL: Path should be string, got {type(path)}")
            return default

        current = data
        for key in path.split("."):
            if isinstance(current, list):
                try:
                    current = current[0]
                except (ValueError, IndexError):
                    return default
            elif isinstance(current, dict):
                current = current.get(key, default)
            else:
                return default

            if current is None:
                return default

        return current

    def cast(self, value, dtype):
        if value is None:
            return None
        try:
            if dtype == bool and isinstance(value, str):
                return value.lower() in ("true", "1", "yes")
            return dtype(value)
        except (ValueError, TypeError):
            self.logger.warning(f"Fail: Failed to cast value {value} to {dtype}, setting None")
            return None

    def filter_extra_data(self, event: dict):
        used_paths = [cfg[0] for cfg in self.transform_config.values()]

        def remove_used(d, prefix=""):
            if not isinstance(d, dict):
                return d
            result = {}
            for k, v in d.items():
                full_key = f"{prefix}.{k}" if prefix else k
                if any(full_key == path or path.startswith(full_key + ".") for path in used_paths):
                    continue
                if isinstance(v, dict):
                    filtered_v = remove_used(v, full_key)
                    if filtered_v:
                        result[k] = filtered_v
                elif isinstance(v, list):
                    filtered_list = []
                    for item in v:
                        if isinstance(item, dict):
                            f_item = remove_used(item, full_key)
                            if f_item:
                                filtered_list.append(f_item)
                        else:
                            filtered_list.append(item)
                    if filtered_list:
                        result[k] = filtered_list
                else:
                    result[k] = v
            return result

        filtered = remove_used(event)
        return filtered if filtered else None

    def transform_event(self, event: dict):
        transformed = {}
        for target_col, config in self.transform_config.items():
            if not isinstance(config, list) or len(config) != 2:
                self.logger.warning(f"FAIL: Invalid transform config for {target_col}: {config}")
                continue

            path, dtype_str = config
            raw_value = self.extract_nested(event, path)
            dtype = self.TYPE_MAP.get(dtype_str, str)
            transformed[target_col] = self.cast(raw_value, dtype)

        transformed["raw_json"] = self.filter_extra_data(event)
        transformed["updated_time"] = datetime.datetime.now(pytz.timezone("Israel"))
        return transformed

    def transform(self, msg: dict):
        events = self.extract_nested(msg, self.extention)[0] if self.extention else msg

        if events is None:
            return pd.DataFrame(columns=list(self.transform_config.keys()) + ["raw_json"])

        if isinstance(events, dict):
            events = [events]

        msg_df = [self.transform_event(event) for event in events]
        self.logger.info("SUCCESS: Success transform data")
        return pd.DataFrame(msg_df)
