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

class NonNullColumnError(DataQualityError):
    """Raised when pickup or dropoff columns contain a null value"""
    def __init__(self, col: str, null_count: int):
        super().__init(f"Non null column {col} contains {null_count} nulls")

class NullThresholdError(DataQualityError):
    """Raised when null percentage exceeds threshold for a column"""

    def __init__(self, cols : list,threshold_pct: float):
        cols_str = ', '.join(cols)
        msg = f"Null threshold {threshold_pct}% exceeded for columns [{cols_str}]"
        super().__init__(msg)

class TotalNullsThresholdError(DataQualityError):
    "Raised when total nulls exceeds specified threshold"

    def __init__(self, total_null_pct, total_null_threshold_pct):
        msg = f"Total null {total_null_pct}% exceeds threshold of {total_null_threshold_pct}"

class MiniumumRowsError(DataQualityError):
    """Raised when minimum row threshold not exceeded"""

    def __init__(self, rows , min_rows_threshold):
        msg = f"Minimum row threshold not exceeded: rows={rows}, min_threshold={min_rows_threshold}"
        super().__init__(msg)

class NegativeDurationThresholdError(DataQualityError):
    """Raised when negative duration percentage exceeded"""
    def __init__(self,  neg_dur_pct, max_rows_threshold_pct):
        msg = f"Maximum negative duration threshold exceeded: negative_duation_pct={neg_dur_pct}%, max_threshold={max_rows_threshold_pct}%"
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

