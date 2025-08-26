import typing

from deephaven import agg,merge,new_table
from deephaven.column import string_col,double_col
from deephaven.table import Table
from deephaven.updateby import rolling_formula_tick

from . import dbclient
import gui

#########################################
#########################################

def binColumn(t:Table|None,orig:str,binned:str,binning:typing.Tuple) -> Table:

    bkts = new_table([
        double_col(f"{orig}",binning[0]),
        double_col(f"{binned}",binning[1])
        ]
    )

    if t is None:
        return bkts

    t = t.update(f"aux_abs = abs({orig})")
    t = t.aj(table=bkts,on=f"aux_abs>={orig}",joins=f"{binned}")

    t = t.update(f"{binned} = {binned} * Math.signum({orig})")

    return t.drop_columns(["aux_abs"])

#########################################
#########################################

# Analysis for MBP-1 schema (equities)
class MBP1(object):

    TIMELAGS = new_table([
        string_col("horizon",["10ms","100ms","1s","10s","1min"]),
        string_col("durationstr",["PT"+x for x in ["0.01s","0.1s","1s","10s","1m"]])
    ]
    ).update("duration = parseDuration(durationstr)")

    def __init__(self,dbclient:dbclient.DBHClient,path:str) -> None:

        self._dbclient = dbclient

        data = dbclient.read(path) if path.startswith(dbclient._root) else dbclient.readbatch(path)
        data = data.update("mid = 0.5*(bid_px_00 + ask_px_00)")

        self._universe = data

    @property
    def dbclient(self) -> dbclient.DBHClient:
        return self._dbclient

    @property
    def universe(self) -> Table:
        return self._universe

    def trades(self) -> Table:
        trd = self.universe.where("action = `T`")
        trd = trd.update("sideimpl = price<=bid_px_00 ? -1 : (price>=ask_px_00 ? 1 : NULL_INT)")
        return trd

    ################################################################################################

    # Binning buckets
    @staticmethod
    def buckets(t:Table):
        t = t.update("date = ts_event.atZone('EST').toLocalDate()")
        t = t.update("hour = lowerBin(ts_event,HOUR).atZone('EST').toLocalTime()")

        return t.move_columns_up(["date","hour"])

    # Calculate returns
    def returns(self,samples:Table,lag:typing.Dict) -> Table:

        samples = samples.update("ts_fwd = ts_event + '{0}'".format(lag["durationstr"]))
        samples = samples.aj(table=self.universe,on=["ts_fwd>=ts_event"],joins=["mid_fwd = mid"])
        samples = samples.update([f"mid_change_{lag['horizon']} = mid_fwd - mid",f"mid_ret_{lag['horizon']} = 1e4 * mid_change_{lag['horizon']} / mid"])
        samples = samples.drop_columns(["ts_fwd","mid_fwd"])

        return samples

    def analyzeEvents(self,evs:Table,feature_names:typing.List[str]=[],timelags:Table=TIMELAGS,ticklags = [1,5,10,50,100]) -> Table:

        tagg = []

        # Time based lags
        for it in timelags.iter_dict():

            evret = self.returns(evs,it)

            aggr_price = [agg.count_("nsamples"),agg.avg(f"forecast"),agg.avg(f"realized = mid_change_{it['horizon']}")]
            aggr_bps =  [agg.count_("nsamples"),agg.avg(f"forecast = forecast_bps"),agg.avg(f"realized = mid_ret_{it['horizon']}")]

            for feat in feature_names:

                evret = evret.rename_columns([f"forecast = forecast_{feat}"]).update([f"feature_value = (double){feat}","forecast_bps = 1e4*forecast / mid"])

                tagg.append(evret.agg_by(aggr_price,by=["feature_value"]).update([f"feature_name = `{feat}`",f"horizon = `{it['horizon']}`","unit = `price`","clock = `physical`"]))
                tagg.append(evret.agg_by(aggr_bps,by=["feature_value"]).update([f"feature_name = `{feat}`",f"horizon = `{it['horizon']}`","unit = `bps`","clock = `physical`"]))

        # Tick based lags
        aggr_price = [agg.count_("nsamples"),agg.avg(f"forecast"),agg.avg(f"realized = mid_change")]
        aggr_bps =  [agg.count_("nsamples"),agg.avg(f"forecast = forecast_bps"),agg.avg(f"realized = mid_ret")]

        for tk in ticklags:

            univret = self.universe.update_by(rolling_formula_tick(formula="mid_fwd = last(mid)",fwd_ticks=tk)).update(["mid_change = mid_fwd - mid","mid_ret = 1e4 * mid_change / mid"])
            evret = evs.natural_join(univret.agg_by([agg.last("mid_change"),agg.last("mid_ret")],by="ts_event"),on="ts_event",joins=["mid_change","mid_ret"])

            for feat in feature_names:

                evret = evret.rename_columns([f"forecast = forecast_{feat}"]).update([f"feature_value = (double){feat}","forecast_bps = 1e4*forecast / mid"])

                tagg.append(evret.agg_by(aggr_price,by=["feature_value"]).update([f"feature_name = `{feat}`",f"horizon = `{tk}`","unit = `price`","clock = `ticks`"]))
                tagg.append(evret.agg_by(aggr_bps,by=["feature_value"]).update([f"feature_name = `{feat}`",f"horizon = `{tk}`","unit = `bps`","clock = `ticks`"]))

        return merge(tagg)

# Analysis for TCBBO schema (option trades)
class TCBBO(object):

    def __init__(self,dbclient:dbclient.DBHClient,path:str,date:str="20250801") -> None:

        self._dbclient = dbclient

        opts = dbclient.options(date)
        data = dbclient.read(path) if path.startswith(dbclient._root) else dbclient.readbatch(path)

        data = data.natural_join(dbclient.feeds,on="publisher_id",joins="venue")
        data = data.natural_join(opts,on="instrument_id",joins=["days2expiry","typ = instrument_class","strike_price"])

        # Replace NaN
        NEG_INF_DOUBLE = float("-inf")
        POS_INF_DOUBLE = float("inf")
        data = data.update(["bid_px_00 = isNaN(bid_px_00) ? NEG_INF_DOUBLE : bid_px_00","ask_px_00 = isNaN(ask_px_00) ? POS_INF_DOUBLE : ask_px_00"])

        # Implied side
        data = data.update("sideimpl = price<=bid_px_00 ? -1 : (price>=ask_px_00 ? 1 : NULL_INT)")
        self._universe = data

    @property
    def dbclient(self) -> dbclient.DBHClient:
        return self._dbclient

    @property
    def universe(self) -> Table:
        return self._universe

#########################################
#########################################

# Visualize in GUI
class Visualization(gui.dashboard.Manager):

    def aggregations(self) -> typing.Dict:
        return  {
            "nsamples": agg.sum_("nsamples"),
            "realized": agg.weighted_avg(wcol="nsamples",cols=["realized"]),
            "forecast": agg.weighted_avg(wcol="nsamples",cols=["forecast"])
        }

    def aggregateTable(self,tfilt:Table,chart_type:str,by_values:typing.List[str],metric_values:typing.List[str]) -> Table:

        calcs = set(["nsamples"] + metric_values if chart_type=="ovp" else metric_values)
        byv = [b for b in by_values if b!="NONE"]
        tagg = tfilt.agg_by([self.aggregations()[m] for m in calcs],by=byv).sort([b for b in byv if b in self.sortable])

        if "feature_value" in by_values:
            tagg = tagg.sort("feature_value")

        return tagg

    def canFilter(self,data:Table) -> typing.List[str]:
        return [c for c in data.column_names if not c in ["nsamples","realized","forecast"]]

    def canSort(self,data:Table) -> typing.List[str]:
        return [c for c in data.column_names if not c in ["horizon"]]

    def mustConstrain(self) -> typing.List[str]:
        return ["horizon","clock","unit","feature_name"]

    def featureBuckets(self) -> typing.List[str]:
        return ["feature_value"]