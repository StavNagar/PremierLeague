import os
import yaml


class Config:
    def __init__(self, config_source: str=None):
        self.config_source = config_source or os.getenv("CONDIG_SOURCE", "")
        self.config = self.load_config()

    def load_config(self, config_file_path: str):
        try:
            with open(config_file_path, "r", encoding="utf-8-sig") as file:
                config = yaml.safe_load(file)
            return config
        except FileNotFoundError as e:
            raise e
        except yaml.YAMLError as e:
            raise e
