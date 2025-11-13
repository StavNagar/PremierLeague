import pandas as pd
from loggers.logger import get_logger


class Transformation:
    def __init__(self, transform_config: dict):
        """
            transform_config (dict): Mapping of target column -> (source path, dtype)
        """
        self.transform_config = transform_config
        self.logger = get_logger(self.__class__.__name__)

    @staticmethod
    def _extract_nested(event: dict, path: str, default=None): 
        for key in path.split("_"):
            if isinstance(event, dict):
                event = event.get(key, default)
            elif isinstance(event, list):
                try:
                    idx = int(key)
                    event = event[idx] if idx < len(event) else default
                except (valueError, IndexError):
                    return default
            else:
                return default

            if event is None:
                return default

        return event
    
    def cast(self, value, dtype):
        if value is None:
            return None
        try:
            if dtype == bool:
                return bool(value)
            return dtype(value)
        except (ValueError, TypeError):
            self.logger.warning(f"Fail: Failed to cast value {value} to {dtype}, setting None")
            return None
    
    def transform_event(self, event: dict):
        transformed = {}
        for col, (path, dtype) in self.transform_config.items():
            raw_value = self._extract_nested(event, path)
            transformed[col] = self.cast(raw_value, dtype)
        
        extra_fields = {
            k: v for k, v in event.items()
            if k not in [col for col in self.transform_config]
        }
        transformed["raw_json"] = extra_fields if extra_fields else None

        return transformed

    def transform(self, msg: list):
        """
        msg (list) of events
        """
        if not msg:
            return pd.DataFrame(columns=self.transform_config.keys()) + ["raw_json"]
        
        msg_df = [self.transform_event(event) for event in msg]
        self.logger.info(f"SUCCESS: Success transform data")

        return pd.DataFrame(msg_df)