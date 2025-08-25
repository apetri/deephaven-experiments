import typing

import pandas as pd
import numpy as np

import deephaven.pandas as dhpd
from deephaven.table import Table
from deephaven import new_table
from deephaven.column import long_col,double_col

import databento as db

import utils
from . import Client

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

#########################################
#########################################

class DBHClient(Client):

    @property
    def feeds(self) -> Table:
        return dhpd.to_table(self._feeds)
    
    def ls(self) -> Table:
        return dhpd.to_table(super().ls())

    def lsbatch(self) -> Table:
        return dhpd.to_table(super().lsbatch())

    def read(self,path:str):
        return dhpd.to_table(dbn2df(path))

    def plan(self,queries:Table):

        cost = []
        num_records = []

        # Calculate cost with databento api
        for i,qry in dhpd.to_pandas(queries).iterrows():
            qry["date"] = qry["date"].strftime(r"%Y%m%d")
            res = self.cost(**qry)
            cost.append(res["cost"])
            num_records.append(res["num_records"])

        # Enrich original table
        ctbl = new_table([
            long_col("num_records",num_records),
            double_col("cost",cost)
        ])

        return utils.hmerge(queries,ctbl)

    ################################################################################
    ################################################################################

    def options(self,date:str) -> Table:

        opts = self.read(self.path(date,"OPRA.PILLAR","definition"))

        opts = opts.update([
            "date = ts_event.atZone('UTC').toLocalDate()",
            "expiry = expiration.atZone('UTC').toLocalDate()",
            "days2expiry = numberBusinessDates(ts_event,expiration) - 1"
            ])

        opts = opts.sort("expiry")

        return opts.move_columns_up(["underlying","date","expiry","days2expiry"])

    @staticmethod
    def byDays2exp(opts:Table) -> Table:
        return opts.select(["date","underlying","symbols = symbol","days2expiry"]).group_by(["date","underlying","days2expiry"]).update("num_securities = symbols.size()")

    def makeQueryTable(self,opts:Table,filt:typing.Callable[[Table],Table]=byDays2exp,start="09:30",end="16:00",schema="trades",dataset="OPRA.PILLAR"):

        qrys = filt(opts)

        if not all([c in qrys.column_names for c in ["date","symbols"]]):
            raise RuntimeError("Missing columns in query table")

        qrys = qrys.update([
            f"start = `{start}`",
            f"end = `{end}`",
            f"dataset = `{dataset}`",
            f"schema = `{schema}`"
        ])

        return qrys