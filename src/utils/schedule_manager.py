from typing import Optional

import pandas as pd
import requests
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
        self.validate_content_inputs(content_name=schedule_name, content_id=schedule_id)
        if schedule_name:
            schedule_id = self.get_content_id(content_name=schedule_name)
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
        self.validate_content_inputs(content_name=schedule_name, content_id=schedule_id)
        if schedule_name:
            schedule_id = self.get_content_id(content_name=schedule_name)
        return self.conn.update_schedule(schedule_id=schedule_id, schedule_state="Active")
