from tableau_api_lib import TableauServerConnection

from src.config.tableau_environments import authentication
from extracts.extract_manager import ExtractRefreshTaskManager
from utils.schedule_manager import ScheduleManager


# these values are unique to MY Tableau environment, be sure to set values relevant to your own
TEST_WORKBOOK_NAME = "Hackathon May2021"
TEST_WORKBOOK_ID = "912699c3-2b95-4668-95f4-fcf7d398be1a"
TEST_DATASOURCE_NAME = "BigQuery Extract1"
TEST_SCHEDULE_NAME = "Devyx Test Weekly"


def main() -> None:
    """Executes the code for this demo."""
    tableau_connection = TableauServerConnection(authentication, env="tableau")
    tableau_connection.sign_in()
    extract_runner = ExtractRefreshTaskManager(conn=tableau_connection)
    schedule_runner = ScheduleManager(conn=tableau_connection)

    # comment and uncomment the lines below as you see fit to sample the different functionality in this demo

    # response = schedule_runner.pause_schedule(schedule_name=TEST_SCHEDULE_NAME)
    # response = schedule_runner.unpause_schedule(schedule_name=TEST_SCHEDULE_NAME)

    response = extract_runner.pause_workbook(workbook_name=TEST_WORKBOOK_NAME)
    # response = extract_runner.unpause_workbook(workbook_name=TEST_WORKBOOK_NAME)

    # response = extract_runner.pause_workbook(workbook_id=TEST_WORKBOOK_ID, include_upstream=False)
    # response = extract_runner.unpause_workbook(workbook_id=TEST_WORKBOOK_ID, include_upstream=False)

    # response = extract_runner.pause_datasource(datasource_name=TEST_DATASOURCE_NAME)
    # response = extract_runner.unpause_datasource(datasource_name=TEST_DATASOURCE_NAME)

    print(response)
    tableau_connection.sign_out()


if __name__ == "__main__":
    main()
