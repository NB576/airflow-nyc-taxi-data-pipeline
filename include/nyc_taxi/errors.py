class DataQualityError(Exception):
    """Base exception for all data quality failures"""

    def __new__(cls, *args, **kwargs):
        if cls is DataQualityError:
            raise TypeError("DataQualityError cannot be instantiated directly.")
        return super().__new__(cls)

class SchemaValidationError(DataQualityError):
    """Schema (column/type) validation failure"""

    def __init__(self, msg):
        super().__init__(msg) 

class NullThresholdError(DataQualityError):
    """Raised when null percentage exceeds threshold for a column"""

    def __init__(self, column, null_pct, threshold_pct):
        msg = f"Null threshold exceeded: '{column}' null_pct={null_pct:.1f}%, threshold={threshold_pct}%"
        super().__init__(msg)

class MiniumumRowsError(DataQualityError):
    """Raised when minimum row threshold not exceeded"""

    def __init__(self, rows , min_rows_threshold):
        msg = f"Minimum row threshold not exceeded: rows={rows}, min_threshold={min_rows_threshold}"
        super().__init__(msg)

class NegativeDurationThresholdError(DataQualityError):
    """Raised when negative duration percentage exceeded"""
    def __init__(self,  neg_dur_pct, max_rows_threshold):
        msg = f"Maximum negative duration threshold exceeded: negative_duation_pct={neg_dur_pct:.1f}%, max_threshold={max_rows_threshold}%"
        super().__init__(msg)

class DataSourceMissingError(DataQualityError):
    """Required input file not found in remote storage."""
    def __init__(self, path: str):
        msg = f"Required input file not found: {path}"
        super().__init__(msg)

class ColumnNotFoundError(DataQualityError):
    "Column not found in data source"
    def __init__(self, cols: list, source: str):
        cols_str = ', '.join(cols)
        msg = f'Column(s) [{cols_str}] not found in {source}.'

