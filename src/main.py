from tableau_api_lib import TableauServerConnection

from src.config.tableau_environments import authentication
from extracts.extract_manager import ExtractRefreshTaskManager


def main():
    tableau_connection = TableauServerConnection(authentication, env="tableau")
    tableau_connection.sign_in()
    extract_runner = ExtractRefreshTaskManager(conn=tableau_connection)
    # response = extract_runner.pause_workbook(workbook_name="Hackathon May2021")
    response = extract_runner.unpause_workbook(workbook_name="Hackathon May2021")
    print(response)
    tableau_connection.sign_out()


if __name__ == "__main__":
    main()
