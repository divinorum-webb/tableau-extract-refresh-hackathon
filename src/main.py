from tableau_api_lib import TableauServerConnection

from src.config.tableau_environments import authentication
from extracts.extract_manager import ExtractRefreshTaskManager


def main():
    tableau_connection = TableauServerConnection(authentication, env="tableau")
    tableau_connection.sign_in()

    extract_runner = ExtractRefreshTaskManager(conn=tableau_connection)
    # response = extract_runner.pause_workbook(workbook_name="Hackathon May2021")
    # print(response)
    response = extract_runner.unpause_workbook(workbook_name="Hackathon May2021")
    print(response)

    # extract_runner = ExtractRefreshTaskManager(conn=tableau_connection)
    # response = extract_runner.pause_datasource(datasource_name="BigQuery Extract1")
    # response = extract_runner.unpause_datasource(datasource_name="BigQuery Extract1")
    # print(response)

    # schedule_runner = ScheduleManager(conn=tableau_connection)
    # response = schedule_runner.pause_schedule(schedule_name="Devyx Test Weekly")
    # print(response)
    # response = schedule_runner.unpause_schedule(schedule_name="Devyx Test Weekly")
    # print(response)

    tableau_connection.sign_out()


if __name__ == "__main__":
    main()
