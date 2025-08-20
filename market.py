import typing

import pandas as pd
import numpy as np

from deephaven import agg,merge,new_table
from deephaven.column import string_col
from deephaven.pandas import to_table
from deephaven.table import Table
from deephaven.updateby import rolling_formula_tick

import jpy

import databento as db

import gui
import data

UTC = jpy.get_type("java.time.ZoneOffset").UTC
EST = jpy.get_type("java.time.ZoneId").of("America/New_York")

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

def ls():
    clnt = data.Client()
    return to_table(clnt.ls())

#########################################
#########################################

# Analysis for MBP-1 schema
class MBP1(object):

    TIMELAGS = new_table([
        string_col("horizon",["10ms","100ms","1s","10s","1min"]),
        string_col("durationstr",["PT"+x for x in ["0.01s","0.1s","1s","10s","1m"]])
    ]
    ).update("duration = parseDuration(durationstr)")

    # Calculate mid
    @staticmethod
    def mid(t:Table) -> Table:
        return t.update("mid = 0.5*(bid_px_00 + ask_px_00)")

    # Binning buckets
    @staticmethod
    def buckets(t:Table):
        t = t.update("date = ts_event.atZone(EST).toLocalDate()")
        t = t.update("hour = lowerBin(ts_event,HOUR).atZone(EST).toLocalTime()")

        return t.move_columns_up(["date","hour"])

    # Calculate returns
    @staticmethod
    def returns(samples:Table,universe:Table,lag:typing.Dict) -> Table:

        print(f"[+] Calculating {lag['horizon']} mid returns.")

        samples = samples.update("ts_fwd = ts_event + '{0}'".format(lag["durationstr"]))
        samples = samples.aj(table=universe,on=["ts_fwd>=ts_event"],joins=["mid_fwd = mid"])
        samples = samples.update([f"mid_change_{lag['horizon']} = mid_fwd - mid",f"mid_ret_{lag['horizon']} = 1e4 * mid_change_{lag['horizon']} / mid"])
        samples = samples.drop_columns(["ts_fwd","mid_fwd"])

        return samples

    @staticmethod
    def analyzeTrades(t:Table,by=["side"],timelags:Table=TIMELAGS,ticklags = [1,5,10,50,100]) -> Table:

        trd = MBP1.buckets(t.where("action=`T`"))
        tagg = []

        # Time based lags
        for it in timelags.iter_dict():

            trdret = MBP1.returns(trd,t,it)

            tagg.append(trdret.agg_by([agg.count_("nsamples"),agg.avg(f"forecast = mid_change_forecast"),agg.avg(f"mid_change = mid_change_{it['horizon']}")],by=by).update([f"horizon = `{it['horizon']}`","unit = `price`","clock = `physical`"]))
            tagg.append(trdret.update("mid_change_forecast = 1e4*mid_change_forecast / mid").agg_by([agg.count_("nsamples"),agg.avg(f"forecast = mid_change_forecast"),agg.avg(f"mid_change = mid_ret_{it['horizon']}")],by=by).update([f"horizon = `{it['horizon']}`","unit = `bps`","clock = `physical`"]))

        # Tick based lags
        for tk in ticklags:

            trdret = trd.update_by(rolling_formula_tick(formula="mid_fwd = last(mid)",fwd_ticks=tk)).update(["mid_change = mid_fwd - mid","mid_ret = 1e4 * mid_change / mid"])

            tagg.append(trdret.agg_by([agg.count_("nsamples"),agg.avg(f"forecast = mid_change_forecast"),agg.avg(f"mid_change")],by=by).update([f"horizon = `{tk}`","unit = `price`","clock = `ticks`"]))
            tagg.append(trdret.update("mid_change_forecast = 1e4*mid_change_forecast / mid").agg_by([agg.count_("nsamples"),agg.avg(f"forecast = mid_change_forecast"),agg.avg(f"mid_change = mid_ret")],by=by).update([f"horizon = `{tk}`","unit = `bps`","clock = `ticks`"]))

        return merge(tagg)

#########################################
#########################################

# Visualize in GUI
class Visualization(gui.dashboard.Manager):

    def aggregations(self) -> typing.Dict:
        return  {
            "nsamples": agg.sum_("nsamples"),
            "mid_change": agg.weighted_avg(wcol="nsamples",cols=["mid_change"]),
            "forecast": agg.weighted_avg(wcol="nsamples",cols=["forecast"])
        }

    def canFilter(self,data:Table) -> typing.List[str]:
        return [c for c in data.column_names if not c in ["mid_change","nsamples","forecast"]]

    def canSort(self,data:Table) -> typing.List[str]:
        return [c for c in data.column_names if not c in ["horizon"]]

    def mustConstrain(self) -> typing.List[str]:
        return ["horizon","clock","unit"]

    def featureBuckets(self) -> typing.List[str]:
        return ["feature_value"]