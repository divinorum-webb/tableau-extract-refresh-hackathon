import pandas as pd
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import querying
from typeguard import typechecked

from src.config.constants import ContentManagerConfig
from src.utils.content_manager import ContentManager


@typechecked
class DatasourceManager(ContentManager):
    """Provides control over datasources available in a Tableau environment."""

    def __init__(self, conn: TableauServerConnection):
        super().__init__(conn=conn, content_type=ContentManagerConfig.CONTENT_TYPE_DATASOURCE.value)

    @property
    def content_df(self) -> pd.DataFrame:
        """Returns a Pandas DataFrame describing all of the datasources on the site."""
        if self._content_df is None:
            self._content_df = querying.get_datasources_dataframe(self.conn)
        return self._content_df
