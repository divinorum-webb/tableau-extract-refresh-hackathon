from typing import List, Optional, Tuple

import pandas as pd
import requests
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import querying, flatten_dict_column, flatten_dict_list_column
from typeguard import typechecked

from src.config.constants import ContentManagerConfig, MetadataAPIConfig, DataFrameColumns, RestAPIConfig
from src.extracts.task_logger import ExtractRefreshTaskLogger
from src.utils.datasource_manager import DatasourceManager
from src.utils.workbook_manager import WorkbookManager


@typechecked
class ExtractRefreshTaskManager:
    """Provides an interface for pausing and unpausing extract refresh tasks.

    This class supports two approaches to programmatically pausing extract refresh tasks using the Tabelau REST API:
    1) Supsending (pausing) and activating (unpausing) an entire schedule and all of its associated tasks.
    2) Deleting (pausing) and creating (unpausing) individual extract refresh tasks.

    When pausing and unpausing a workbook, you have can include the optional `include_upstream` boolean flag.
    If `include_upstream` is True then all extract refresh tasks (including those belonging to embedded
    extracts and all associated upstream datasources) will be paused or unpaused.
    If `include_upstream` is False then only the workbook's embedded extracts are paused or unpaused.

    Example usage:
        extract_runner = ExtractRefreshTaskManager(conn=tableau_connection)
        response = extract_runner.pause_workbook(workbook_id=<YOUR_WORKBOOK_ID>, include_upstream=False)

    Args:
        conn: An active Tableau Server (or Tableau Online) connection signed in and having a valid auth token.
    """

    def __init__(self, conn: TableauServerConnection):
        self.conn = conn
        self.workbook_manager = WorkbookManager(conn=conn)
        self.datasource_manager = DatasourceManager(conn=conn)
        self._extract_refresh_tasks_df = None

    # EXTRACT REFRESH TASKS CORE

    @property
    def extract_refresh_tasks_df(self) -> pd.DataFrame:
        """Returns a Pandas DataFrame describing all of the extract refresh tasks for the active Tableau site."""
        if self._extract_refresh_tasks_df is None:
            self._extract_refresh_tasks_df = querying.get_extract_refresh_tasks_dataframe(self.conn)
        return self._extract_refresh_tasks_df

    def _create_workbook_extract_refresh_tasks(self, refresh_tasks_df: pd.DataFrame) -> List[requests.Response]:
        """Returns a list of server responses after requesting to create workbook extract refresh tasks."""
        responses = []
        for index, row in refresh_tasks_df.iterrows():
            response = self.conn.add_workbook_to_schedule(
                workbook_id=row[DataFrameColumns.WORKBOOK_ID.value], schedule_id=row[DataFrameColumns.SCHEDULE_ID.value]
            )
            responses.append(response)
        return responses

    def _create_datasource_extract_refresh_tasks(self, refresh_tasks_df: pd.DataFrame) -> List[requests.Response]:
        """Returns a list of server responses after requesting to create datasource extract refresh tasks."""
        responses = []
        for index, row in refresh_tasks_df.iterrows():
            response = self.conn.add_data_source_to_schedule(
                datasource_id=row[DataFrameColumns.DATASOURCE_ID.value],
                schedule_id=row[DataFrameColumns.SCHEDULE_ID.value],
            )
            responses.append(response)
        return responses

    def _delete_extract_refresh_tasks(self, refresh_tasks_df: pd.DataFrame) -> List[requests.Response]:
        """Deletes the extract refresh tasks described by the Pandas DataFrame provided."""
        deleted_task_responses = []
        for index, row in refresh_tasks_df.iterrows():
            deleted_task_response = self.conn.delete_extract_refresh_task(task_id=row[DataFrameColumns.TASK_ID.value])
            deleted_task_responses.append(deleted_task_response)
        return deleted_task_responses

    def _get_content_extract_refresh_tasks_df(self, content_type: str) -> pd.DataFrame:
        """Returns a Pandas DataFrame describing all of the extract refresh tasks for the content type.

        Args:
            content_type: The variety of content whose extract refresh tasks are being queried.

        Raises:
            ValueError: The content_type must be either 'workbook' or 'datasource'.
        """
        if not MetadataAPIConfig.is_valid_content_type(content_type=content_type):
            raise ValueError(
                f"The content type `{content_type}` is not valid. Valid types: {MetadataAPIConfig.CONTENT_TYPES.value}."
            )
        tasks_df = self.extract_refresh_tasks_df[~self.extract_refresh_tasks_df[content_type].isnull()].copy()
        tasks_df = flatten_dict_column(
            df=tasks_df, col_name=DataFrameColumns.SCHEDULE_NESTED.value, keys=DataFrameColumns.SCHEDULE_KEYS.value
        )
        tasks_df = flatten_dict_column(df=tasks_df, col_name=content_type, keys=[DataFrameColumns.CONTENT_ID.value])
        return tasks_df

    @staticmethod
    def _process_unpause_responses(
        responses: List[requests.Response], refresh_tasks_df: pd.DataFrame, content_type: str, content_id: str
    ) -> None:
        """Validates the responses received after issuing API requests to unpause the relevant extract refresh tasks."""
        if all(
            [True for response in responses if response.status_code == RestAPIConfig.TASK_CREATED_SUCCESS_CODE.value]
        ):
            ExtractRefreshTaskLogger.remove_rows_for_unpaused_extract_refresh_tasks(
                extract_refresh_task_type=content_type,
                refresh_tasks_df=refresh_tasks_df,
            )
        else:
            raise RuntimeError(f"Some of the extracts for {content_type} `luid: {content_id}` failed to unpause.")

    # EXTRACT REFRESH TASKS FOR WORKBOOKS

    @property
    def workbook_extract_refresh_tasks_df(self) -> pd.DataFrame:
        """Returns a Pandas DataFrame describing all of the extract refresh tasks for workbooks."""
        return self._get_content_extract_refresh_tasks_df(content_type=MetadataAPIConfig.CONTENT_TYPE_WORKBOOK.value)

    def _get_workbook_refresh_tasks(self, workbook_id: str) -> pd.DataFrame:
        """Returns the extract refresh tasks associated with a specific workbook."""
        workbook_df = self.workbook_manager.content_df[
            self.workbook_manager.content_df[DataFrameColumns.CONTENT_ID.value] == workbook_id
        ]
        if workbook_df.empty:
            raise IndexError(f"No extract refresh tasks were found for the workbook (luid: {workbook_id}).")
        workbook_df = workbook_df[["name", "id", "project"]].rename(columns={"id": DataFrameColumns.WORKBOOK_ID.value})
        workbook_refresh_tasks_df = workbook_df.merge(
            self.workbook_extract_refresh_tasks_df, on=[DataFrameColumns.WORKBOOK_ID.value]
        )
        return workbook_refresh_tasks_df

    def pause_workbook(
        self,
        workbook_name: Optional[str] = None,
        workbook_id: Optional[str] = None,
        include_upstream: Optional[bool] = True,
    ) -> Tuple[List[requests.Response], List[requests.Response]]:
        """Pauses extract refresh tasks for a workbook.

        This method can be called on either the workbook name or the workbook ID (luid). Either value is valid, but
        one of them must be provided for this method to pause a workbook.

        Args:
            workbook_name: The name of the workbook whose tasks will be paused (deleted).
            workbook_id: The local unique identifier (luid) of the workbook whose tasks will be paused (deleted).
            include_upstream: True if pausing upstream datasources for the workbook, False otherwise.
        Raises:
            ValueError: Neither a workbook name nor a workbook ID were provided.
        """
        datasource_responses = []
        if workbook_name:
            workbook_id = self.workbook_manager.get_content_id(content_name=workbook_name)
        workbook_refresh_tasks_df = self._get_workbook_refresh_tasks(workbook_id=workbook_id)
        if include_upstream is True:
            datasource_responses = self._pause_upstream_datasources(workbook_id=workbook_id)
        ExtractRefreshTaskLogger.write_extract_refresh_tasks_to_pause(
            extract_refresh_task_type=MetadataAPIConfig.CONTENT_TYPE_WORKBOOK.value,
            refresh_tasks_df=workbook_refresh_tasks_df,
        )
        workbook_responses = self._delete_extract_refresh_tasks(refresh_tasks_df=workbook_refresh_tasks_df)
        return workbook_responses, datasource_responses

    def unpause_workbook(
        self,
        workbook_name: Optional[str] = None,
        workbook_id: Optional[str] = None,
        include_upstream: Optional[bool] = True,
    ) -> Tuple[List[requests.Response], List[requests.Response]]:
        """Unpauses extract refresh tasks for a workbook.

        This method can be called on either the workbook name or the workbook ID (luid). Either value is valid, but
        one of them must be provided for this method to unpause a workbook.

        Args:
            workbook_name: The name of the workbook whose extracts will be unpaused (created).
            workbook_id: The local unique identifier (luid) for the workbook whose extracts will be unpaused (created).
            include_upstream: True if unpausing upstream datasources for the workbook, False otherwise.
        Raises:
            ValueError: Neither a workbook name nor a workbook ID were provided.
        """
        datasource_responses = []
        if workbook_name:
            workbook_id = self.workbook_manager.get_content_id(content_name=workbook_name)
        workbook_refresh_tasks_df = ExtractRefreshTaskLogger.read_extract_refresh_tasks_to_unpause(
            extract_refresh_task_type=MetadataAPIConfig.CONTENT_TYPE_WORKBOOK.value
        )
        workbook_refresh_tasks_df = workbook_refresh_tasks_df[
            workbook_refresh_tasks_df[DataFrameColumns.WORKBOOK_ID.value] == workbook_id
        ]
        workbook_responses = self._create_workbook_extract_refresh_tasks(refresh_tasks_df=workbook_refresh_tasks_df)
        if include_upstream is True:
            datasource_responses = self.unpause_upstream_datasource(workbook_id=workbook_id)
        self._process_unpause_responses(
            responses=workbook_responses,
            refresh_tasks_df=workbook_refresh_tasks_df,
            content_type=ContentManagerConfig.CONTENT_TYPE_WORKBOOK.value,
            content_id=workbook_id,
        )
        return workbook_responses, datasource_responses

    # EXTRACT REFRESH TASKS FOR DATASOURCES UPSTREAM FROM WORKBOOKS

    def _get_upstream_datasource_extract_tasks_df(self, workbook_id: str) -> pd.DataFrame:
        """Returns the extract refresh tasks for the given upstream datasources."""
        upstream_datasources_df = self._get_workbook_upstream_datasources_df(workbook_id=workbook_id)
        upstream_datasource_extract_tasks_df = pd.DataFrame()
        for index, row in upstream_datasources_df.iterrows():
            datasource_tasks_df = self._get_datasource_refresh_tasks(
                datasource_id=row[DataFrameColumns.DATASOURCE_ID.value]
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
                    df=upstream_datasources_df, col_name=DataFrameColumns.UPSTREAM_DATASOURCES.value
                )
                upstream_datasources_df = upstream_datasources_df[
                    upstream_datasources_df[DataFrameColumns.WORKBOOK_ID.value] == workbook_id
                ]
                upstream_datasources_df = upstream_datasources_df[
                    upstream_datasources_df[DataFrameColumns.HAS_EXTRACTS.value] == True
                ]
            except KeyError:
                upstream_datasources_df = pd.DataFrame()
            return upstream_datasources_df
        else:
            raise ConnectionError(f"Received an unexpected response from the Metadata API: {response.content}")

    def _pause_upstream_datasources(self, workbook_id: str) -> List[requests.Response]:
        """Pauses extract refresh tasks for a datasource upstream from a workbook.

        Args:
            workbook_id: The local unique identifier (luid) of the workbook downstream from the datasource(s).
        """
        upstream_datasource_tasks_df = self._get_upstream_datasource_extract_tasks_df(workbook_id=workbook_id)
        upstream_datasource_tasks_df[DataFrameColumns.WORKBOOK_ID.value] = workbook_id
        ExtractRefreshTaskLogger.write_extract_refresh_tasks_to_pause(
            extract_refresh_task_type=ContentManagerConfig.CONTENT_TYPE_UPSTREAM_DATASOURCE.value,
            refresh_tasks_df=upstream_datasource_tasks_df,
        )
        responses = self._delete_extract_refresh_tasks(refresh_tasks_df=upstream_datasource_tasks_df)
        return responses

    def unpause_upstream_datasource(self, workbook_id: str) -> List[requests.Response]:
        """Unpauses extract refresh tasks for a datasource upstream from the target workbook.

        Args:
            workbook_id: The local unique identifier (luid) for the workbook whose extracts will be unpaused (created).
        """
        upstream_datasource_tasks_df = ExtractRefreshTaskLogger.read_extract_refresh_tasks_to_unpause(
            extract_refresh_task_type=ContentManagerConfig.CONTENT_TYPE_UPSTREAM_DATASOURCE.value
        )
        upstream_datasource_tasks_df = upstream_datasource_tasks_df[
            upstream_datasource_tasks_df[DataFrameColumns.WORKBOOK_ID.value] == workbook_id
        ]
        responses = self._create_datasource_extract_refresh_tasks(refresh_tasks_df=upstream_datasource_tasks_df)
        self._process_unpause_responses(
            responses=responses,
            refresh_tasks_df=upstream_datasource_tasks_df,
            content_type=ContentManagerConfig.CONTENT_TYPE_UPSTREAM_DATASOURCE.value,
            content_id=workbook_id,
        )
        return responses

    # EXTRACT REFRESH TASKS FOR DATASOURCES

    @property
    def datasource_extract_refresh_tasks_df(self) -> pd.DataFrame:
        """Returns a Pandas DataFrame describing all of the extract refresh tasks for datasources."""
        return self._get_content_extract_refresh_tasks_df(content_type=MetadataAPIConfig.CONTENT_TYPE_DATASOURCE.value)

    def _get_datasource_refresh_tasks(self, datasource_id: str) -> pd.DataFrame:
        """Returns the extract refresh tasks associated with a specific datasource."""
        datasource_df = self.datasource_manager.content_df[
            self.datasource_manager.content_df[DataFrameColumns.CONTENT_ID.value] == datasource_id
        ]
        if datasource_df.empty:
            raise IndexError(f"No extract refresh tasks were found for the datasource (luid: {datasource_id}).")
        datasource_df = datasource_df[["name", "id", "project"]].rename(
            columns={"id": DataFrameColumns.DATASOURCE_ID.value}
        )
        datasource_refresh_tasks_df = datasource_df.merge(
            self.datasource_extract_refresh_tasks_df, on=[DataFrameColumns.DATASOURCE_ID.value]
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
            datasource_id = self.datasource_manager.get_content_id(content_name=datasource_name)
        datasource_refresh_tasks_df = self._get_datasource_refresh_tasks(datasource_id=datasource_id)
        ExtractRefreshTaskLogger.write_extract_refresh_tasks_to_pause(
            extract_refresh_task_type=MetadataAPIConfig.CONTENT_TYPE_DATASOURCE.value,
            refresh_tasks_df=datasource_refresh_tasks_df,
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
            datasource_name: The name of the datasource whose extracts will be unpaused (created).
            datasource_id: The local unique identifier (luid) for the datasource whose extracts will be unpaused (created).
        Raises:
            ValueError: Neither a datasource name nor a datasource ID were provided.
        """
        if datasource_name:
            datasource_id = self.datasource_manager.get_content_id(content_name=datasource_name)
        datasource_refresh_tasks_df = ExtractRefreshTaskLogger.read_extract_refresh_tasks_to_unpause(
            extract_refresh_task_type=MetadataAPIConfig.CONTENT_TYPE_DATASOURCE.value
        )
        datasource_refresh_tasks_df = datasource_refresh_tasks_df[
            datasource_refresh_tasks_df[DataFrameColumns.DATASOURCE_ID.value] == datasource_id
        ]
        responses = self._create_datasource_extract_refresh_tasks(refresh_tasks_df=datasource_refresh_tasks_df)
        self._process_unpause_responses(
            responses=responses,
            refresh_tasks_df=datasource_refresh_tasks_df,
            content_type=ContentManagerConfig.CONTENT_TYPE_DATASOURCE.value,
            content_id=datasource_id,
        )
        return responses
