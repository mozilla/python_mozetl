from pyspark.sql import DataFrame


def tranform(self, f):
    """Apply generic tranform function to a DataFrame
    
    Allows us to chain custom transformations to DataFrames.
    See: https://github.com/MrPowers/quinn/blob/master/quinn/extensions/dataframe_ext.py
    """
    return f(self)

DataFrame.tranform = transform
