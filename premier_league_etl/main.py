from flask import Flask
from runner import run_pipeline

app = Flask(__name__)


@app.route("/api-sports-teams-pipeline", methods=["GET"])
def api_sports_team_pipeline():
    run_pipeline(config_file_name="api-sports-teams.yml")
    
    return "Pipeline named 'api-sports-teams completed", 200


@app.route("/api-sports-standings-pipeline", methods=["GET"])
def api_sports_team_pipeline():
    run_pipeline(config_file_name="api-sports-standings.yml")

    return "Pipeline named 'api-sports-standings completed", 200


@app.route("/api-football-teams-pipeline", methods=["GET"])
def api_sports_team_pipeline():
    run_pipeline(config_file_name="api-football-teams.yml")

    return "Pipeline named 'api-football-teams completed", 200


@app.route("/api-football-standings-pipeline", methods=["GET"])
def api_sports_team_pipeline():
    run_pipeline(config_file_name="api-football-standings.yml")

    return "Pipeline named 'api-football-standings completed", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
