from typing import Optional

import pandas as pd
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import querying, flatten_dict_column


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
            target_workbook_df = self.workbooks_df[self.workbooks_df["name"] == workbook_name].copy()
            self.enforce_single_workbook_requirement(target_workbook_df=target_workbook_df, workbook_name=workbook_name)
            return target_workbook_df["id"].to_list()[0]
        except IndexError:
            raise IndexError(f"No workbook with name `{workbook_name}` was found.")

    @staticmethod
    def enforce_single_workbook_requirement(target_workbook_df: pd.DataFrame, workbook_name: str) -> None:
        """Raises an exception if multiple workbooks match the given workbook name."""
        if target_workbook_df.shape[0] > 1:
            target_workbook_df = flatten_dict_column(df=target_workbook_df, col_name="project", keys=["name", "id"])
            raise NameError(
                f"""
            Multiple workbooks match the name `{workbook_name}`. Try using the workbook ID (luid) instead:
            {target_workbook_df[["project_name", "name", "id"]]}
            """
            )

    @staticmethod
    def validate_workbook_inputs(workbook_name: Optional[str] = None, workbook_id: Optional[str] = None) -> None:
        """Raises an exception if neither a workbook name nor workbook ID (luid) were provided."""
        if not any([workbook_name, workbook_id]):
            raise ValueError(
                "To pause or unpause a workbook, you must provide a workbook name or a workbook ID (luid)."
            )
