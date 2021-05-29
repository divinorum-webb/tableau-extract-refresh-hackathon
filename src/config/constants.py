from enum import Enum


class ContentManagerConfig(Enum):
    """Defines constants for Tableau content managers."""
    CONTENT_TYPE_DATASOURCE = "datasource"
    CONTENT_TYPE_WORKBOOK = "workbook"
    CONTENT_TYPE_SCHEDULE = "schedule"
    CONTENT_TYPES = [CONTENT_TYPE_DATASOURCE, CONTENT_TYPE_WORKBOOK, CONTENT_TYPE_SCHEDULE]


class DataFrameColumns(Enum):
    """Defines column names for data returned by the Metadata API queries."""

    CONTENT_ID = "id"
    DATASOURCE_ID = "datasource_id"
    HAS_EXTRACTS = "has_extracts"
    SCHEDULE_ID = "schedule_id"
    SCHEDULE_KEYS = ["id", "name"]
    SCHEDULE_NESTED = "schedule"
    TASK_ID = "id"
    WORKBOOK_ID = "workbook_id"
    UPSTREAM_DATASOURCES = "upstreamDatasources"


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
    CONTENT_TYPE_WORKBOOK = "workbook"
    CONTENT_TYPE_DATASOURCE = "datasource"
    CONTENT_TYPES = [CONTENT_TYPE_WORKBOOK, CONTENT_TYPE_DATASOURCE]

    @classmethod
    def is_valid_content_type(cls, content_type: str) -> bool:
        """Returns True if the content type is valid."""
        return content_type in cls.CONTENT_TYPES.value


class RestAPIConfig(Enum):
    """Defines constants for interacting with the REST API."""

    TASK_CREATED_SUCCESS_CODE = 200
    TASK_DELETED_SUCCESS_CODE = 204
