import pandas as pd


class ExtractRefreshTaskLogger:
    """Defines logic for storing and retrieving details for paussed extract refresh tasks.

    You can use this class to build your own logic driving the process of reading and writing details
    for extract refresh tasks that are currently paused (deleted) and can be unpaused (re-created).
    """

    @staticmethod
    def write_extract_refresh_tasks_to_pause(extract_type: str, refresh_tasks_df: pd.DataFrame) -> None:
        """Writes the details for the extract refresh tasks that will be paused to a CSV file."""
        refresh_tasks_df.to_csv(f"data/paused_{extract_type}_extract_refresh_tasks.csv")

    @staticmethod
    def read_extract_refresh_tasks_to_unpause(extract_type: str) -> pd.DataFrame:
        """Reads the details from a CSV file for the extract refresh tasks that will be unpaused."""
        return pd.read_csv(f"data/paused_{extract_type}_extract_refresh_tasks.csv")

    @classmethod
    def remove_rows_for_unpaused_extract_refresh_tasks(
        cls, extract_type: str, unpaused_tasks_df: pd.DataFrame
    ) -> None:
        """Deletes rows  from a CSV file for extract refresh tasks that have been unpaused."""
        existing_tasks_df = cls.read_extract_refresh_tasks_to_unpause(extract_type=extract_type)
        existing_tasks_df = existing_tasks_df[~existing_tasks_df["id"].isin(unpaused_tasks_df["id"])]
        existing_tasks_df.to_csv(f"data/paused_{extract_type}_extract_refresh_tasks.csv", mode="w")