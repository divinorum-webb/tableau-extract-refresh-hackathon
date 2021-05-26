from enum import Enum


class MetadataAPIConfig(Enum):
    """Defines constants for interacting with the Metadata API."""

    UPSTREAM_DATASOURCES_QUERY = """
        {
          workbooks {
            workbook_name: name
            workbook_id: luid
            upstreamDatasources {
              datasource_name: name
              datasource_id: luid
              has_extracts: hasExtracts
            }
          }
        }
        """
    METADATA_RESPONSE_OUTER_KEY = "data"
    METADATA_RESPONSE_INNER_KEY = "workbooks"
    SUCCESS_STATUS_CODE = 200


class MetadataColumns(Enum):
    """Defines column names for data returned by the Metadata API queries."""

    DATASOURCE_ID = "datasource_id"
    DATASOURCE_NAME = "datasource_name"
    HAS_EXTRACTS = "has_extracts"
    WORKBOOK_ID = "workbook_id"
    WORKBOOK_NAME = "workbook_name"
    UPSTREAM_DATASOURCES = "upstreamDatasources"
