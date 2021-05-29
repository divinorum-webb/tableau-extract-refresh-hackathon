from abc import ABC, abstractmethod
from typing import Optional

import pandas as pd
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import flatten_dict_column
from typeguard import typechecked


@typechecked
class ContentManager(ABC):
    """Defines an abstract base class used to implement common content management functionality.

    While the extract manager plays the role of pausing and unpausing extract refresh tasks, there are other content
    managers that help the extract manager obtain information about various types of Tableau content.

    The content managers all inherit a common set of validation and getter functions.
    The BaseManager class implements this common functionality from within a single abstract base class.
    """

    def __init__(self, conn: TableauServerConnection, content_type: str):
        self.conn = conn
        self.content_type = content_type
        self._content_df = None

    @property
    @abstractmethod
    def content_df(self) -> pd.DataFrame:
        """Returns a Pandas DataFrame describing details for the content the manager class is intended to manage."""
        pass

    def get_content_id(self, content_name) -> str:
        """Returns the local unique identifier (luid) for the named Tableau Server content."""
        try:
            target_content_df = self.content_df[self.content_df["name"] == content_name].copy()
            self.enforce_single_object_requirement(target_content_df=target_content_df, content_name=content_name)
            return target_content_df["id"].to_list()[0]
        except IndexError:
            raise IndexError(f"No {self.content_type} with name `{content_name}` was found.")

    def enforce_single_object_requirement(self, target_content_df: pd.DataFrame, content_name: str) -> None:
        """Raises an exception if multiple pieces of content match the given content name."""
        if target_content_df.shape[0] > 1:
            target_content_df = flatten_dict_column(df=target_content_df, col_name="project", keys=["name", "id"])
            raise NameError(
                f"""
            Multiple {self.content_type}s match the name `{content_name}`.\n
            Try using the {self.content_type}'s local unique identifier ("id") instead:\n
            {target_content_df[["project_name", "name", "id"]].to_markdown(index=False)}
            """
            )

    def validate_workbook_inputs(self, content_name: Optional[str] = None, content_id: Optional[str] = None) -> None:
        """Raises an exception if neither the content name nor the content ID (luid) are provided."""
        if not any([content_name, content_id]):
            raise ValueError(
                f"To pause or unpause a {self.content_type}, provide its name or ID value (luid)."
            )
