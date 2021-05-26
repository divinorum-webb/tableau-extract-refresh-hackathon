from typing import Optional

import pandas as pd
import requests
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import querying


class ScheduleManager:
    """Provides control over schedules available in a Tableau Server environment."""

    def __init__(self, conn: TableauServerConnection):
        self.conn = conn
        self._extract_schedules_df = None

    @property
    def extract_schedules_df(self) -> pd.DataFrame:
        """Returns a Pandas DataFrame describing all schedules available to the Tableau connection."""
        if self._extract_schedules_df is None:
            schedules_df = querying.get_schedules_dataframe(self.conn)
            schedules_df = schedules_df[schedules_df["type"] == "Extract"]
            self._extract_schedules_df = schedules_df
        return self._extract_schedules_df

    def get_schedule_id(self, schedule_name: str) -> str:
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
            schedule_id = self.get_schedule_id(schedule_name=schedule_name)
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
            schedule_id = self.get_schedule_id(schedule_name=schedule_name)
        return self.conn.update_schedule(schedule_id=schedule_id, schedule_state="Active")
