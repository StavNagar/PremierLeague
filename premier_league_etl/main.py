import os
from config import Config
from app import App
from etl import ETLPipeline
from https_reader import HTTPSReader
from parser import Parser
from transformation import Transformation
from bigquery_writer import BigQueryWriter


def run_pipeline(config_file_name):
    config = Config(config_file_name).load_config()
    App(
        HTTPSReader(config["reader"]),
        ETLPipeline(
            Parser(),
            Transformation(config["transformation"]),
            BigQueryWriter(config["writer"])
        )
    ).up()


def etl_entrypoint(request):
    request_json = request.get_json(silent=True) if request else None

    config_file_name = None

    if request_json and "ETL_CONFIG_NAME" in request_json:
        config_file_name = request_json["ETL_CONFIG_NAME"]

    if not config_file_name:
        config_file_name = os.getenv("ETL_CONFIG_NAME")
    
    if not config_file_name:
        return (
            "ERROR: No pipeline specified",
            400
        )

    run_pipeline(config_file_name)
