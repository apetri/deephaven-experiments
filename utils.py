import typing

from deephaven import agg,merge,new_table
from deephaven.table import Table
from deephaven.column import InputColumn
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

def pivot(t:Table,keycols:typing.List[str],namecol:str,valuecol:str) -> Table:
    """
    Pivot a table
    """

    ctyps = {r["Name"]:r["DataType"] for r in t.meta_table.iter_dict()}
    if not ctyps[namecol]=="java.lang.String":
        raise ValueError(f"{namecol} column is not a string")

    # Construct the index
    pvt = t.select_distinct(keycols)

    # Pivoted column names
    pcols = dhnp.to_numpy(t.select_distinct(namecol))[:,0]

    # Add the columns one at a time via join
    for pc in pcols:
        tf = t.where(f"{namecol}=`{pc}`").agg_by(agg.first(f"{pc} = {valuecol}"),by=keycols)
        pvt = pvt.natural_join(tf,on=keycols)

    # Done
    return pvt

def unpivot(t:Table,keycols:typing.List[str],keyname:str,valuename:str):

    """
    Unpivot a table
    """

    vtyps = [ r["DataType"] for r in t.meta_table.iter_dict() if not r["Name"] in keycols ]

    if not all([v==vtyps[0] for v in vtyps]):
        raise ValueError("Inconsistent value types")

    chnks = []
    for cn in [c for c in t.column_names if not c in keycols]:
        chnks.append(t.select(keycols + [f"{keyname} = `{cn}`",f"{valuename} = {cn}"]))

    return merge(chnks)

def binColumn(t:Table,col:InputColumn,out:InputColumn|None=None,signed:bool=True) -> Table:

    namein = col.j_column.name()
    nameout = out.j_column.name() if out is not None else namein + "_bin"

    bkts = new_table([col,out] if out is not None else [col])
    if out is None:
        bkts = bkts.update(f"{nameout} = {namein}")

    types = [ x["DataType"] for x in bkts.meta_table.iter_dict() ]

    t = t.update(f"__aux = abs({namein})" if signed else f"__aux = {namein}")
    t = t.aj(table=bkts,on=f"__aux>={namein}",joins=f"{nameout}")

    if signed:
        t = t.update(f"{nameout} = {nameout} * ({types[1]})Math.signum({namein})")

    return t.drop_columns(["__aux"])