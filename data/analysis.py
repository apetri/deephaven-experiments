import typing

from deephaven import agg,merge,new_table
from deephaven.column import int_col,string_col
from deephaven.table import Table
from deephaven.updateby import rolling_formula_tick

from . import dbclient

import utils
import gui

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
        data = data.sort("ts_event")

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

    AGGREGATIONS = [
        agg.count_("num_samples"),
        agg.sum_("num_contracts = size"),
        agg.formula("net_contracts_delta = sum(size*sidedelta)"),
        agg.formula("net_contracts = sum(size*sideimpl)")
    ]

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
        data = data.update([
            "sideimpl = price<=bid_px_00 ? -1 : (price>=ask_px_00 ? 1 : NULL_INT)",
            "sidedelta = sideimpl * (typ = `C` ? 1 : -1)"])

        self._universe = data

    @property
    def dbclient(self) -> dbclient.DBHClient:
        return self._dbclient

    @property
    def universe(self) -> Table:
        return self._universe

    ##################################################

    def asof(self,oth:Table,joins:typing.List[str]) -> Table:
        return self._universe.aj(oth,on="ts_event",joins=["ts_eq = ts_event"] + joins)

    ##################################################

    def analyzeEvents(self,oth:Table,features:typing.List[str],bys:typing.List[str]) -> Table:

        # Ignore unsigned trades
        optrd = self.asof(oth,features).where("!isNull(sideimpl)")

        # Bucketing
        optrd = utils.binColumn(optrd,col=int_col("days2expiry",[0,1,10,21,100]),signed=False)
        optrd = utils.binColumn(optrd,col=int_col("days2expiry",[0,1]),out=string_col("expiry_type",["zdte","other"]),signed=False)

        # Collect aggregations
        trdagg = []
        for fn in features:
            trdagg.append(optrd.where(f"!isNull({fn})").rename_columns([f"feature_value = {fn}"]).agg_by(self.AGGREGATIONS,by=["feature_value"] + bys).sort(bys + ["feature_value"]).update([f"feature_name = `{fn}`","feature_value_abs = abs(feature_value)"]))

        # Done
        return merge(trdagg)

#########################################
#########################################

# Visualize in GUI: MBP1 analysis
class Mbp1Gui(gui.dashboard.Manager):

    def aggregations(self) -> typing.Dict:
        return  {
            "nsamples": agg.sum_("nsamples"),
            "realized": agg.weighted_avg(wcol="nsamples",cols=["realized"]),
            "forecast": agg.weighted_avg(wcol="nsamples",cols=["forecast"])
        }

    def derived(self) -> typing.Dict:
        return {}

    def canFilter(self,data:Table) -> typing.List[str]:
        return [c for c in data.column_names if not c in self.aggregations().keys()]

    def canSort(self,data:Table) -> typing.List[str]:
        return [c for c in data.column_names if not c in ["horizon"]]

    def mustConstrain(self) -> typing.List[str]:
        return ["horizon","clock","unit","feature_name"]

    def featureBuckets(self) -> typing.List[str]:
        return ["feature_value"]

    def featureTraces(self, metrics: typing.List[str]) -> typing.Dict:
        return {
            "Aggregation metric (prediction)": ["forecast"],
            "Aggregation metric (observation)": ["realized"]
        }

# Visualize in GUI: OPRA trades
class OpraGui(gui.dashboard.Manager):

    def aggregations(self) -> typing.Dict:
        return  {
            x: agg.sum_(x) for x in ["num_samples","num_contracts","net_contracts","net_contracts_delta"]
        }

    def derived(self) -> typing.Dict[str,typing.Tuple[str,typing.List[str]]]:
        return {
            "delta_imbalance" : ("net_contracts_delta/num_contracts",["net_contracts_delta","num_contracts"]),
            "net_imbalance" : ("net_contracts/num_contracts",["net_contracts","num_contracts"])
        }

    def canFilter(self,data:Table) -> typing.List[str]:
        return [c for c in data.column_names if not c in self.aggregations().keys()]

    def canSort(self,data:Table) -> typing.List[str]:
        return data.column_names

    def mustConstrain(self) -> typing.List[str]:
        return ["feature_name"]

    def featureBuckets(self) -> typing.List[str]:
        return ["feature_value"]

    def featureTraces(self, metrics: typing.List[str]) -> typing.Dict:
        return {
            "Trace": ["delta_imbalance"]
        }
