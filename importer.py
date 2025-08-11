import pandas as pd
import numpy as np

from deephaven.pandas import to_table
from deephaven.table import Table
import jpy

ZoneOffset = jpy.get_type("java.time.ZoneOffset")
UTC = ZoneOffset.UTC

import databento as db

def dbn2df(path:str) -> pd.DataFrame:

    df = db.DBNStore.from_file(path).to_df()

    # Cast unsigned to signed (java compatibility)
    for c in df.dtypes[df.dtypes==np.uint8].index:
        df[c] = df[c].astype(np.int32)
    
    for c in df.dtypes[df.dtypes==np.uint16].index:
        df[c] = df[c].astype(np.int32)
    
    for c in df.dtypes[df.dtypes==np.uint32].index:
        df[c] = df[c].astype(np.int64)
    
    for c in df.dtypes[df.dtypes==np.uint64].index:
        df[c] = df[c].astype(np.int64)

    return df

def dbn2table(path:str) -> Table:
    return to_table(dbn2df(path))

#########################################
#########################################

def optionslist(path:str) -> Table:
    opts = dbn2table(path)
    
    cols = ["date"] + [c.name for c in opts.columns]
    
    opts = opts.update([
        "date = ts_event.atZone(UTC).toLocalDate()",
        "expiration = expiration.atZone(UTC).toLocalDate()"
    ])

    return opts.select(cols).drop_columns("ts_event")