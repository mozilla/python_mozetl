""""Utility functions for Churn"""
from pyspark.sql import functions as F


DS = "YYYY-MM-DD"
DS_NODASH = "YYYYMMDD"


def format_date(start_date, format, offset=None):
    """Format a date given an optional offset.

    :param start_date:  an Arrow object representing the start date
    :param format:      the format string
    :param offset:      an offset measured in days
    :return:            a formatted datestring
    """

    if offset is not None:
        start_date = start_date.shift(days=offset)
    return start_date.format(format)


def to_datetime(col, format="yyyyMMdd"):
    """Convert a StringType column into a DateType column.

    Default format is `yyyyMMdd`. Formats are specified by SimpleDateFormats_.
    See `pyspark.sql.functions.to_timestamp` in the PySpark 2.2 docs for an
    up to date implementation of this function.

    .. _SimpleDateFormats: http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html
    """
    return F.from_unixtime(F.unix_timestamp(col, format)).alias(
        "to_datetime({}, {})".format(col, format)
    )


def format_spark_path(bucket, prefix):
    return "s3://{}/{}".format(bucket, prefix)


def preprocess_col_expr(mapping):
    """Pre-process the expression dictionary.

     This replaces values with their appropriate column expressions. For
     convenience, the expression dictionary accepts values of None which maps
     the value to the key. In addition, it will accept string expressions that
     are acceptable in `DataFrame.selectExpr(...)`. For example::

        >>> preprocess_col_expr({
        ...    "foo": None,
        ...    "bar": "count(*)"
        ...})
        {'foo': Column<foo>, 'bar': Column<count(1)>}

     :param mapping:    A dictionary mapping a string column to an expression
     :returns:          A dictionary where all values are valid spark column
                        expressions
     """
    select_expr = {}

    for name, expr in mapping.items():
        column_expr = expr if expr is not None else name
        if isinstance(column_expr, str):
            column_expr = F.expr(column_expr)
        select_expr[name] = column_expr.alias(name)

    return select_expr


def build_col_expr(mapping):
    """Build a dictionary into a list of column expressions.

    This allows the building of fairly complex expressions by manipulating
    dictionaries. These dictionaries can be reused across multiple expressions.

    :param mapping:    A dictionary mapping column name to column expression. A
                        value of None will use the key as the column expression.
    :returns:           A list that can be used in `df.select(...)`
    """
    select_expr = []

    preprocessed = preprocess_col_expr(mapping)
    for name, expr in preprocessed.items():
        select_expr.append(expr.alias(name))
    return select_expr
