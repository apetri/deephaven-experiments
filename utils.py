import typing

from deephaven import new_table,empty_table
from deephaven.table import Table
import deephaven.numpy as dhnp

def hmerge(t1:Table,t2:Table) -> Table:
    """
    t1,t2 have the same number of rows.
    Returns a table whose columns are the union of the columns of t1 and t2

    """

    if(t1.size!=t2.size):
        raise ValueError("Tables do not have same length")

    t12 = t1.update("__idx = i").natural_join(t2.update("__idx=i"),on="__idx")
    return t12.drop_columns("__idx")

def pivot(t:Table):
    return empty_table(0)

def unpivot(t:Table):
    return empty_table(0)