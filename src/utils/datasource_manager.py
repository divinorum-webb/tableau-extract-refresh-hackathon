from typing import Optional

import pandas as pd
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import querying


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
            return self.datasources_df[self.datasources_df["name"] == datasource_name]["id"].to_list()[0]
        except IndexError:
            raise IndexError(f"No datasource with name `{datasource_name}` was found.")

    @staticmethod
    def validate_datasource_inputs(datasource_name: Optional[str] = None, datasource_id: Optional[str] = None) -> None:
        """Raises an exception if neither a workbook name nor workbook ID (luid) were provided."""
        if not any([datasource_name, datasource_id]):
            raise ValueError(
                "To pause or unpause a workbook, you must provide a workbook name or a datasource ID (luid)."
            )
