import yaml


class Config:
    def __init__(self, config_file_name):
        self.config_file_name = config_file_name
        self.config = self.load_config()

    def load_config(self):
        try:
            with open(self.config_file_name, "r", encoding="utf-8-sig") as file:
                config = yaml.safe_load(file)
                if config is None:
                    raise ValueError(f"Config file '{self.config_file_name}' is empty or invalid.")
                return config

        except yaml.YAMLError as e:
            raise ValueError(f"YAML parsing error in {self.config_file_name}: {e}")