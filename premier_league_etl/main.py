from config import Config
from app import App
from etl import ETLPipeline
from https_reader import HTTPSReader
from parser import Parser
from transformation import Transformation
from bigquery_writer import BigQueryWriter



def main(config_file_name):
    config = Config(config_file_name).load_config()
    App(
        HTTPSReader(config["reader"]),
        ETLPipeline(
            Parser(),
            Transformation(config["transformation"]),
            BigQueryWriter(config["writer"])
        )
    ).up()

if __name__ == "__main__":
    main("configurations/api-football-teams.yml")