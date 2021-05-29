from typing import Optional

import pandas as pd
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import querying, flatten_dict_column


class DatasourceManager:
    """Provides control over datasources available in a Tableau environment."""

    def __init__(self, conn: TableauServerConnection):
        self.conn = conn
        self._datasources_df = None

    @property
    def datasources_df(self) -> pd.DataFrame:
        """Returns a Pandas DataFrame describing all of the datasources on the site."""
        if self._datasources_df is None:
            self._datasources_df = querying.get_datasources_dataframe(self.conn)
        return self._datasources_df

    def get_datasource_id(self, datasource_name: str) -> str:
        """Returns the local unique identifier (luid) for the named Tableau Server datasources."""
        try:
            target_datasource_df = self.datasources_df[self.datasources_df["name"] == datasource_name].copy()
            self.enforce_single_datasource_requirement(
                target_datasource_df=target_datasource_df, datasource_name=datasource_name
            )
            return target_datasource_df["id"].to_list()[0]
        except IndexError:
            raise IndexError(f"No datasource with name `{datasource_name}` was found.")

    @staticmethod
    def enforce_single_datasource_requirement(target_datasource_df: pd.DataFrame, datasource_name: str) -> None:
        """Raises an exception if multiple datasources match the given datasource name."""
        if target_datasource_df.shape[0] > 1:
            target_datasource_df = flatten_dict_column(df=target_datasource_df, col_name="project", keys=["name", "id"])
            raise NameError(
                f"""
            Multiple datasources match the name `{datasource_name}`. Try using the datasource ID (luid) instead:
            {target_datasource_df[["project_name", "name", "id"]]}
            """
            )

    @staticmethod
    def validate_datasource_inputs(datasource_name: Optional[str] = None, datasource_id: Optional[str] = None) -> None:
        """Raises an exception if neither a workbook name nor workbook ID (luid) were provided."""
        if not any([datasource_name, datasource_id]):
            raise ValueError(
                "To pause or unpause a workbook, you must provide a workbook name or a datasource ID (luid)."
            )
