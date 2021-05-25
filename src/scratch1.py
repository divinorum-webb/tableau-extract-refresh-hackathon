from typing import List, Optional

import pandas as pd
import requests
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import querying, flatten_dict_column
from typeguard import typechecked


@typechecked
class ExtractRefreshTaskManager:
    """Provides an interface for pausing and unpausing extract refresh tasks.

    This class supports two approaches to programmatically pausing extract refresh tasks using the Tabelau REST API:
    1) Supsending (pausing) and activating (unpausing) an entire schedule and all of its associated tasks.
    2) Deleting (pausing) and creating (unpausing) individual extract refresh tasks.
    """

    def __init__(self, conn: TableauServerConnection):
        self.conn = conn
        self._extract_schedules_df = None
        self._extract_refresh_tasks_df = None
        self._workbooks_df = None
        self._datasources_df = None

    # SCHEDULES

    @property
    def extract_schedules_df(self) -> pd.DataFrame:
        """Returns a Pandas DataFrame describing all schedules available to the Tableau connection."""
        if self._extract_schedules_df is None:
            schedules_df = querying.get_schedules_dataframe(self.conn)
            schedules_df = schedules_df[schedules_df["type"] == "Extract"]
            self._extract_schedules_df = schedules_df
        return self._extract_schedules_df

    def _get_schedule_id(self, schedule_name: str) -> str:
        """Returns the local unique identifier (luid) for the named Tableau Server schedule."""
        try:
            return self.extract_schedules_df[self.extract_schedules_df["name"] == schedule_name]["id"].to_list()[0]
        except IndexError:
            raise IndexError(f"No schedule with name `{schedule_name}` was found.")

    @staticmethod
    def _validate_schedule_inputs(schedule_name: Optional[str] = None, schedule_id: Optional[str] = None) -> None:
        """Raises an exception if neither a schedule name nor schedule ID (luid) were provided."""
        if not any([schedule_name, schedule_id]):
            raise ValueError(
                "To pause or unpause a schedule, you must provide a schedule name or a schedule ID (luid)."
            )

    def pause_schedule(
        self, schedule_name: Optional[str] = None, schedule_id: Optional[str] = None
    ) -> requests.Response:
        """Pauses an entire extract refresh schedule, halting all tasks associated with the targeted schedule.

        This method can be called on either the schedule name or the schedule ID (luid). Either value is valid, but at
        least one of them must be provided for this method to pause a schedule.

        Args:
            schedule_name: The name of the schedule being paused (suspended).
            schedule_id: The local unique identifier (luid) of the schedule being paused (suspended).
        Raises:
            ValueError: Neither a schedule name nor a schedule ID were provided.
        """
        self._validate_schedule_inputs(schedule_name=schedule_name, schedule_id=schedule_id)
        if schedule_name:
            schedule_id = self._get_schedule_id(schedule_name=schedule_name)
        return self.conn.update_schedule(schedule_id=schedule_id, schedule_state="Suspended")

    def unpause_schedule(
        self, schedule_name: Optional[str] = None, schedule_id: Optional[str] = None
    ) -> requests.Response:
        """Unpauses an entire extract refresh schedule, enabling all tasks associated with the targeted schedule.

        This method can be called on either the schedule name or the schedule ID (luid). Either value is valid, but at
        least one of them must be provided for this method to pause a schedule.

        Args:
            schedule_name: The name of the schedule being unpaused (activated).
            schedule_id: The local unique identifier (luid) of the schedule being unpaused (activated).
        Raises:
            ValueError: Neither a schedule name nor a schedule ID were provided.
        """
        self._validate_schedule_inputs(schedule_name=schedule_name, schedule_id=schedule_id)
        if schedule_name:
            schedule_id = self._get_schedule_id(schedule_name=schedule_name)
        return self.conn.update_schedule(schedule_id=schedule_id, schedule_state="Active")

    # EXTRACT REFRESH TASKS

    @property
    def extract_refresh_tasks_df(self) -> pd.DataFrame:
        """Returns a Pandas DataFrame describing all of the extract refresh tasks for the active Tableau site."""
        if self._extract_refresh_tasks_df is None:
            self._extract_refresh_tasks_df = querying.get_extract_refresh_tasks_dataframe(self.conn)
        return self._extract_refresh_tasks_df

    @property
    def workbook_extract_refresh_tasks_df(self) -> pd.DataFrame:
        """Returns a Pandas DataFrame describing all of the extract refresh tasks for workbooks."""
        workbook_tasks_df = self.extract_refresh_tasks_df[~self.extract_refresh_tasks_df["workbook"].isnull()].copy()
        workbook_tasks_df = flatten_dict_column(df=workbook_tasks_df, col_name="schedule", keys=["id", "name"])
        workbook_tasks_df = flatten_dict_column(df=workbook_tasks_df, col_name="workbook", keys=["id"])
        return workbook_tasks_df

    @property
    def datasource_extract_refresh_tasks_df(self) -> pd.DataFrame:
        """Returns a Pandas DataFrame describing all of the extract refresh tasks for datasources."""
        datasource_tasks_df = self.extract_refresh_tasks_df[
            ~self.extract_refresh_tasks_df["datasource"].isnull()
        ].copy()
        datasource_tasks_df = flatten_dict_column(df=datasource_tasks_df, col_name="schedule", keys=["id", "name"])
        datasource_tasks_df = flatten_dict_column(df=datasource_tasks_df, col_name="datasource", keys=["id"])
        return datasource_tasks_df

    @staticmethod
    def _write_extract_refresh_tasks_to_pause(extract_type: str, refresh_tasks_df: pd.DataFrame) -> None:
        """Writes the details for the extract refresh tasks that will be paused to a CSV file."""
        refresh_tasks_df.to_csv(f"paused_{extract_type}_extract_refresh_tasks.csv", index=False)

    @staticmethod
    def _read_extract_refresh_tasks_to_pause(extract_type: str) -> pd.DataFrame:
        """Reads the details from a CSV file for the extract refresh tasks that will be unpaused."""
        return pd.read_csv(filepath_or_buffer=f"paused_{extract_type}_extract_refresh_tasks.csv")

    def _delete_extract_refresh_tasks(self, refresh_tasks_df: pd.DataFrame) -> List[requests.Response]:
        """Deletes the extract refresh tasks described by the Pandas DataFrame provided."""
        deleted_task_responses = []
        for index, row in refresh_tasks_df.iterrows():
            deleted_task_response = self.conn.delete_extract_refresh_task(task_id=row["id"])
            deleted_task_responses.append(deleted_task_response)
        return deleted_task_responses

    def _create_workbook_extract_refresh_tasks(self, refresh_tasks_df: pd.DataFrame) -> List[requests.Response]:
        """Returns a list of server responses after requesting to create workbook extract refresh tasks."""
        responses = []
        for index, row in refresh_tasks_df.iterrows():
            response = self.conn.add_workbook_to_schedule(
                workbook_id=row["workbook_id"], schedule_id=row["schedule_id"]
            )
            responses.append(response)
        return responses

    def _create_datasource_extract_refresh_tasks(self, refresh_tasks_df: pd.DataFrame) -> List[requests.Response]:
        """Returns a list of server responses after requesting to create datasource extract refresh tasks."""
        responses = []
        for index, row in refresh_tasks_df.iterrows():
            response = self.conn.add_data_source_to_schedule(
                datasource_id=row["datasource_id"], schedule_id=row["schedule_id"]
            )
            responses.append(response)
        return responses

    # WORKBOOKS

    @property
    def workbooks_df(self) -> pd.DataFrame:
        """Returns a Pandas DataFrame describing all of the workbooks on the site."""
        if self._workbooks_df is None:
            self._workbooks_df = querying.get_workbooks_dataframe(self.conn)
        return self._workbooks_df

    def _get_workbook_id(self, workbook_name: str) -> str:
        """Returns the local unique identifier (luid) for the named Tableau Server workbook."""
        try:
            return self.workbooks_df[self.workbooks_df["name"] == workbook_name]["id"].to_list()[0]
        except IndexError:
            raise IndexError(f"No workbook with name `{workbook_name}` was found.")

    @staticmethod
    def _validate_workbook_inputs(workbook_name: Optional[str] = None, workbook_id: Optional[str] = None) -> None:
        """Raises an exception if neither a workbook name nor workbook ID (luid) were provided."""
        if not any([workbook_name, workbook_id]):
            raise ValueError(
                "To pause or unpause a workbook, you must provide a workbook name or a workbook ID (luid)."
            )

    def _get_workbook_refresh_tasks(self, workbook_id: str) -> pd.DataFrame:
        """Returns the extract refresh tasks associated with a specific workbook."""
        workbook_df = self.workbooks_df[self.workbooks_df["id"] == workbook_id]
        if workbook_df.empty:
            raise IndexError(f"No extract refresh tasks were found for the workbook (luid: {workbook_id}).")
        workbook_df = workbook_df[["name", "id", "project"]].rename(columns={"id": "workbook_id"})
        workbook_refresh_tasks_df = workbook_df.merge(self.workbook_extract_refresh_tasks_df, on=["workbook_id"])
        return workbook_refresh_tasks_df

    def pause_workbook(
        self, workbook_name: Optional[str] = None, workbook_id: Optional[str] = None
    ) -> List[requests.Response]:
        """Pauses extract refresh tasks for a workbook.

        This method can be called on either the workbook name or the workbook ID (luid). Either value is valid, but
        one of them must be provided for this method to pause a workbook.

        Args:
            workbook_name: The name of the workbook whose tasks will be paused (deleted).
            workbook_id: The local unique identifier (luid) of the workbook whose tasks will be unpaused (created).
        Raises:
            ValueError: Neither a workbook name nor a workbook ID were provided.
        """
        if workbook_name:
            workbook_id = self._get_workbook_id(workbook_name=workbook_name)
        workbook_refresh_tasks_df = self._get_workbook_refresh_tasks(workbook_id=workbook_id)
        self._write_extract_refresh_tasks_to_pause(extract_type="workbook", refresh_tasks_df=workbook_refresh_tasks_df)
        responses = self._delete_extract_refresh_tasks(refresh_tasks_df=workbook_refresh_tasks_df)
        return responses

    def unpause_workbook(
        self, workbook_name: Optional[str] = None, workbook_id: Optional[str] = None
    ) -> List[requests.Response]:
        """Unpauses extract refresh tasks for a workbook.

        This method can be called on either the workbook name or the workbook ID (luid). Either value is valid, but
        one of them must be provided for this method to unpause a workbook.

        Args:
            workbook_name: The name of the workbook whose extracts will be unpaused (deleted).
            workbook_id: The local unique identifier (luid) for the workbook whose extracts will be unpaused (created).
        Raises:
            ValueError: Neither a workbook name nor a workbook ID were provided.
        """
        if workbook_name:
            workbook_id = self._get_workbook_id(workbook_name=workbook_name)
        workbook_refresh_tasks_df = self._read_extract_refresh_tasks_to_pause(extract_type="workbook")
        workbook_refresh_tasks_df = workbook_refresh_tasks_df[workbook_refresh_tasks_df["workbook_id"] == workbook_id]
        responses = self._create_workbook_extract_refresh_tasks(refresh_tasks_df=workbook_refresh_tasks_df)
        return responses

    # DATASOURCES

    @property
    def datasources_df(self) -> pd.DataFrame:
        """Returns a Pandas DataFrame describing all of the datasources on the site."""
        if self._datasources_df is None:
            self._datasources_df = querying.get_datasources_dataframe(self.conn)
        return self._datasources_df

    def _get_datasource_id(self, datasource_name: str) -> str:
        """Returns the local unique identifier (luid) for the named Tableau Server datasources."""
        try:
            return self.datasources_df[self.datasources_df["name"] == datasource_name]["id"].to_list()[0]
        except IndexError:
            raise IndexError(f"No datasource with name `{datasource_name}` was found.")

    @staticmethod
    def _validate_datasource_inputs(datasource_name: Optional[str] = None, datasource_id: Optional[str] = None) -> None:
        """Raises an exception if neither a workbook name nor workbook ID (luid) were provided."""
        if not any([datasource_name, datasource_id]):
            raise ValueError(
                "To pause or unpause a workbook, you must provide a workbook name or a datasource ID (luid)."
            )

    def _get_datasource_refresh_tasks(self, datasource_id: str) -> pd.DataFrame:
        """Returns the extract refresh tasks associated with a specific datasource."""
        datasource_df = self.datasources_df[self.datasources_df["id"] == datasource_id]
        if datasource_df.empty:
            raise IndexError(f"No extract refresh tasks were found for the datasource (luid: {datasource_id}).")
        datasource_df = datasource_df[["name", "id", "project"]].rename(columns={"id": "datasource_id"})
        datasource_refresh_tasks_df = datasource_df.merge(self.workbook_extract_refresh_tasks_df, on=["datasource_id"])
        return datasource_refresh_tasks_df

    def pause_datasource(
        self, datasource_name: Optional[str] = None, datasource_id: Optional[str] = None
    ) -> List[requests.Response]:
        """Pauses extract refresh tasks for a datasource.

        This method can be called on either the datasource name or the datasource ID (luid). Either value is valid, but
        one of them must be provided for this method to pause a datasource.

        Args:
            datasource_name: The name of the datasource being paused (suspended).
            datasource_id: The local unique identifier (luid) of the datasource being paused (suspended).
        Raises:
            ValueError: Neither a datasource name nor a datasource ID were provided.
        """
        if datasource_name:
            datasource_id = self._get_datasource_id(datasource_name=datasource_name)
        datasource_refresh_tasks_df = self._get_datasource_refresh_tasks(datasource_id=datasource_id)
        self._write_extract_refresh_tasks_to_pause(
            extract_type="datasource", refresh_tasks_df=datasource_refresh_tasks_df
        )
        responses = self._delete_extract_refresh_tasks(refresh_tasks_df=datasource_refresh_tasks_df)
        return responses

    def unpause_datasource(
        self, datasource_name: Optional[str] = None, datasource_id: Optional[str] = None
    ) -> List[requests.Response]:
        """Unpauses extract refresh tasks for a datasource.

        This method can be called on either the datasource name or the datasource ID (luid). Either value is valid, but
        one of them must be provided for this method to unpause a datasource.

        Args:
            datasource_name: The name of the datasource whose extracts will be unpaused (deleted).
            datasource_id: The local unique identifier (luid) for the datasource whose extracts will be unpaused (created).
        Raises:
            ValueError: Neither a datasource name nor a datasource ID were provided.
        """
        if datasource_name:
            datasource_id = self._get_datasource_id(datasource_name=datasource_name)
        datasource_refresh_tasks_df = self._read_extract_refresh_tasks_to_pause(extract_type="datasource")
        datasource_refresh_tasks_df = datasource_refresh_tasks_df[
            datasource_refresh_tasks_df["datasource_id"] == datasource_id
        ]
        responses = self._create_datasource_extract_refresh_tasks(refresh_tasks_df=datasource_refresh_tasks_df)
        return responses
