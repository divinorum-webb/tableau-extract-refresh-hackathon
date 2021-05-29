import pandas as pd
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import querying
from typeguard import typechecked

from src.config.constants import ContentManagerConfig
from src.utils.content_manager import ContentManager


@typechecked
class ScheduleManager(ContentManager):
    """Provides control over schedules available in a Tableau Server environment."""

    def __init__(self, conn: TableauServerConnection):
        super().__init__(conn=conn, content_type=ContentManagerConfig.CONTENT_TYPE_SCHEDULE.value)

    @property
    def content_df(self) -> pd.DataFrame:
        """Returns a Pandas DataFrame describing all schedules available to the Tableau connection."""
        if self._content_df is None:
            content_df = querying.get_schedules_dataframe(self.conn)
            content_df = content_df[content_df["type"] == "Extract"]
            self._content_df = content_df
        return self._content_df
