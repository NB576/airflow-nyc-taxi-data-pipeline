from pyspark.sql import functions as F

# returns an F.when expr with cases for each mapping in mappings param provided
def map_col(mapping: dict, input_col: str):
    expr = F.when(F.lit(False), F.lit(None).cast("string"))
    
    # iteratively add a when clause for each key in mapping
    for k, v in mapping.items():
        expr = F.when(F.col(input_col) == k, v)
    
    # chain otherwise onto expr and return
    return expr.otherwise("Unknown")