from typing import Optional

import pandas as pd
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import querying


class WorkbookManager:
    """Provides control over workbooks available in a Tableau environment."""

    def __init__(self, conn: TableauServerConnection):
        self.conn = conn
        self._workbooks_df = None

    @property
    def workbooks_df(self) -> pd.DataFrame:
        """Returns a Pandas DataFrame describing all of the workbooks on the site."""
        if self._workbooks_df is None:
            self._workbooks_df = querying.get_workbooks_dataframe(self.conn)
        return self._workbooks_df

    def get_workbook_id(self, workbook_name: str) -> str:
        """Returns the local unique identifier (luid) for the named Tableau Server workbook."""
        try:
            return self.workbooks_df[self.workbooks_df["name"] == workbook_name]["id"].to_list()[0]
        except IndexError:
            raise IndexError(f"No workbook with name `{workbook_name}` was found.")

    @staticmethod
    def validate_workbook_inputs(workbook_name: Optional[str] = None, workbook_id: Optional[str] = None) -> None:
        """Raises an exception if neither a workbook name nor workbook ID (luid) were provided."""
        if not any([workbook_name, workbook_id]):
            raise ValueError(
                "To pause or unpause a workbook, you must provide a workbook name or a workbook ID (luid)."
            )
