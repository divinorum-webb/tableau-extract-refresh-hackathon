from typing import List, Optional, Tuple

import pandas as pd
import requests
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import querying, flatten_dict_column, flatten_dict_list_column
from typeguard import typechecked

from src.config.metadata_api import MetadataAPIConfig, MetadataColumns
from src.extracts.task_logger import ExtractRefreshTaskLogger


@typechecked
class ExtractRefreshTaskManager:
    """Provides an interface for pausing and unpausing extract refresh tasks.

    This class supports two approaches to programmatically pausing extract refresh tasks using the Tabelau REST API:
    1) Supsending (pausing) and activating (unpausing) an entire schedule and all of its associated tasks.
    2) Deleting (pausing) and creating (unpausing) individual extract refresh tasks.
    """

    def __init__(self, conn: TableauServerConnection):
        self.conn = conn
        self._extract_refresh_tasks_df = None
        self._workbooks_df = None
        self._datasources_df = None

    # EXTRACT REFRESH TASKS

    @property
    def extract_refresh_tasks_df(self) -> pd.DataFrame:
        """Returns a Pandas DataFrame describing all of the extract refresh tasks for the active Tableau site."""
        if self._extract_refresh_tasks_df is None:
            self._extract_refresh_tasks_df = querying.get_extract_refresh_tasks_dataframe(self.conn)
        return self._extract_refresh_tasks_df

    def _get_content_extract_refresh_tasks_df(self, content_type: str) -> pd.DataFrame:
        """Returns a Pandas DataFrame describing all of the extract refresh tasks for the content type.

        Args:
            content_type: The variety of content whose extract refresh tasks are being queried.

        Raises:
            ValueError: The content_type must be either 'workbook' or 'datasource'.
        """
        tasks_df = self.extract_refresh_tasks_df[~self.extract_refresh_tasks_df[content_type].isnull()].copy()
        tasks_df = flatten_dict_column(df=tasks_df, col_name="schedule", keys=["id", "name"])
        tasks_df = flatten_dict_column(df=tasks_df, col_name=content_type, keys=["id"])
        return tasks_df

    @property
    def workbook_extract_refresh_tasks_df(self) -> pd.DataFrame:
        """Returns a Pandas DataFrame describing all of the extract refresh tasks for workbooks."""
        return self._get_content_extract_refresh_tasks_df(content_type="workbook")

    @property
    def datasource_extract_refresh_tasks_df(self) -> pd.DataFrame:
        """Returns a Pandas DataFrame describing all of the extract refresh tasks for datasources."""
        return self._get_content_extract_refresh_tasks_df(content_type="datasource")

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
    ) -> Tuple[List[requests.Response], List[requests.Response]]:
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
        datasource_responses = self._pause_upstream_datasources(workbook_id=workbook_id)
        ExtractRefreshTaskLogger.write_extract_refresh_tasks_to_pause(
            extract_type="workbook", refresh_tasks_df=workbook_refresh_tasks_df
        )
        workbook_responses = self._delete_extract_refresh_tasks(refresh_tasks_df=workbook_refresh_tasks_df)
        return workbook_responses, datasource_responses

    def unpause_workbook(
        self, workbook_name: Optional[str] = None, workbook_id: Optional[str] = None
    ) -> Tuple[List[requests.Response], List[requests.Response]]:
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
        workbook_refresh_tasks_df = ExtractRefreshTaskLogger.read_extract_refresh_tasks_to_unpause(
            extract_type="workbook"
        )
        upstream_datasource_tasks_df = ExtractRefreshTaskLogger.read_extract_refresh_tasks_to_unpause(
            extract_type="upstream_datasource"
        )
        workbook_refresh_tasks_df = workbook_refresh_tasks_df[workbook_refresh_tasks_df["workbook_id"] == workbook_id]
        workbook_responses = self._create_workbook_extract_refresh_tasks(refresh_tasks_df=workbook_refresh_tasks_df)
        datasource_responses = self._create_datasource_extract_refresh_tasks(
            refresh_tasks_df=upstream_datasource_tasks_df
        )
        if all([True for response in workbook_responses if response.status_code == 200]):
            ExtractRefreshTaskLogger.remove_rows_for_unpaused_extract_refresh_tasks(
                extract_type="workbook", unpaused_tasks_df=workbook_refresh_tasks_df
            )
        else:
            raise ValueError(f"Some of the extracts for workbook `luid: {workbook_id}` failed to unpause.")
        if all([True for response in datasource_responses if response.status_code == 200]):
            ExtractRefreshTaskLogger.remove_rows_for_unpaused_extract_refresh_tasks(
                extract_type="upstream_datasource", unpaused_tasks_df=upstream_datasource_tasks_df
            )
        else:
            raise ValueError(
                f"Some of the extracts for upstream datasources `luid: {upstream_datasource_tasks_df[MetadataColumns.DATASOURCE_ID.value]}` failed to unpause."
            )
        return workbook_responses, datasource_responses

    # UPSTREAM DATASOURCES FOR WORKBOOKS

    def _get_upstream_datasource_extract_tasks_df(self, workbook_id: str) -> pd.DataFrame:
        """Returns the extract refresh tasks for the given upstream datasources."""
        upstream_datasources_df = self._get_workbook_upstream_datasources_df(workbook_id=workbook_id)
        upstream_datasource_extract_tasks_df = pd.DataFrame()
        for index, row in upstream_datasources_df.iterrows():
            datasource_tasks_df = self._get_datasource_refresh_tasks(
                datasource_id=row[MetadataColumns.DATASOURCE_ID.value]
            )
            upstream_datasource_extract_tasks_df = upstream_datasource_extract_tasks_df.append(datasource_tasks_df)
        return upstream_datasource_extract_tasks_df

    def _get_workbook_upstream_datasources_df(self, workbook_id: str) -> pd.DataFrame:
        """Returns a Pandas DataFrame describing the upstream datasources (for a workbook) having extracts."""
        response = self.conn.metadata_graphql_query(query=MetadataAPIConfig.UPSTREAM_DATASOURCES_QUERY.value)
        if response.status_code == MetadataAPIConfig.SUCCESS_STATUS_CODE.value:
            upstream_datasources_df = pd.DataFrame(
                response.json()[MetadataAPIConfig.METADATA_RESPONSE_OUTER_KEY.value][
                    MetadataAPIConfig.METADATA_RESPONSE_INNER_KEY.value
                ]
            )
            try:
                upstream_datasources_df = flatten_dict_list_column(
                    df=upstream_datasources_df, col_name=MetadataColumns.UPSTREAM_DATASOURCES.value
                )
                upstream_datasources_df = upstream_datasources_df[
                    upstream_datasources_df[MetadataColumns.WORKBOOK_ID.value] == workbook_id
                ]
                upstream_datasources_df = upstream_datasources_df[
                    upstream_datasources_df[MetadataColumns.HAS_EXTRACTS.value] == True
                ]
            except KeyError:
                upstream_datasources_df = pd.DataFrame()
            return upstream_datasources_df
        else:
            raise ConnectionError(f"Received an unexpected response from the Metadata API: {response.content}")

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
        datasource_refresh_tasks_df = datasource_df.merge(
            self.datasource_extract_refresh_tasks_df, on=["datasource_id"]
        )
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
        ExtractRefreshTaskLogger.write_extract_refresh_tasks_to_pause(
            extract_type="datasource", refresh_tasks_df=datasource_refresh_tasks_df
        )
        responses = self._delete_extract_refresh_tasks(refresh_tasks_df=datasource_refresh_tasks_df)
        return responses

    def _pause_upstream_datasources(self, workbook_id: str) -> List[requests.Response]:
        """Pauses extract refresh tasks for a datasource upstream from a workbook.

        Args:
            # upstream_datasources_df: A Pandas DataFrame describing the upstream datasource being paused.
            workbook_id: The local unique identifier (luid) of the workbook downstream from the datasource(s).
        """
        upstream_datasource_tasks_df = self._get_upstream_datasource_extract_tasks_df(workbook_id=workbook_id)
        upstream_datasource_tasks_df["workbook_id"] = workbook_id
        ExtractRefreshTaskLogger.write_extract_refresh_tasks_to_pause(
            extract_type="upstream_datasource", refresh_tasks_df=upstream_datasource_tasks_df
        )
        responses = self._delete_extract_refresh_tasks(refresh_tasks_df=upstream_datasource_tasks_df)
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
        datasource_refresh_tasks_df = ExtractRefreshTaskLogger.read_extract_refresh_tasks_to_unpause(
            extract_type="datasource"
        )
        datasource_refresh_tasks_df = datasource_refresh_tasks_df[
            datasource_refresh_tasks_df["datasource_id"] == datasource_id
        ]
        responses = self._create_datasource_extract_refresh_tasks(refresh_tasks_df=datasource_refresh_tasks_df)
        if all([True for response in responses if response.status_code == 200]):
            ExtractRefreshTaskLogger.remove_rows_for_unpaused_extract_refresh_tasks(
                extract_type="datasource", unpaused_tasks_df=datasource_refresh_tasks_df
            )
        else:
            raise ValueError(f"Some of the extracts for datasource `luid: {datasource_id}` failed to unpause.")
        return responses
