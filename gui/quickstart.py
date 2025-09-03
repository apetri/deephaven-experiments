import typing

from deephaven import empty_table,time_table,agg
from deephaven.table import Table
from deephaven.updateby import cum_sum
from deephaven.appmode import ApplicationState, get_app_state

from gui import dashboard
from globalscope import *

class Example(dashboard.Manager):

    @classmethod
    def random(cls,t:Table):
        t = t.update([
            "cat1 = rndString(`a`,`b`,`c`)",
            "cat2 = rndString(`DD`,`EE`,`FF`,`GG`)",
            "cat3 = rndString(`fff`,`ggg`,`hhh`,`iii`,`lll`)",
            "n1 = rndInt(1,2,3,4)",
            "n2 = rndInt(10,20,30)",
            "value1 = rndUnif(0,1)",
            "value2 = n1 + rndUnif(0,1)",
            "value3 = n2 + 10*n1*n1 + rndUnif(0,1)",
            "valuePred = 4*n1",
            "valueObs = rndUnif(-2.5,2.5) + 4*n1",
            "date = '2025-01-01' + 'P1D' * (int)(i/10)",
            "idx = 1"
        ]
        )

        # Add intraday (local) time types
        t = t.update_by(ops=[cum_sum(cols="idx")],by=["date"]).update(["minute = '09:29:00'.plusMinutes(idx)","second = '10:00:00'.plusSeconds(idx)"])

        return t.drop_columns(["idx"])

    @classmethod
    def static(cls,nrows:int=1000):
        return cls(cls.random(empty_table(nrows)))
    
    @classmethod
    def ticking(cls,period:str="PT1s"):
        return cls(cls.random(time_table(period)))

    def aggregations(self) -> typing.Dict:
        """
        Definitions of allowed aggregations
        """
        return  {
            "count": agg.count_("count"),
            "sum1": agg.sum_("sum1 = value1"),
            "average1": agg.avg("average1 = value1"),
            "average2": agg.avg("average2 = value2"),
            "average3": agg.avg("average3 = value3"),
            "valueObs": agg.avg("valueObs"),
            "valuePred": agg.avg("valuePred"),
            "sXY": agg.formula("sXY = sum(valuePred*valueObs)"),
            "sXX": agg.formula("sXX = sum(valuePred*valuePred)"),
            "sYY": agg.formula("sYY = sum(valueObs*valueObs)"),
        }

    def derived(self) -> typing.Dict:
        """
        Derived aggregations: name -> (definition,[dependencies])
        """

        return {
            "ratio12" : ("average1/average2",["average1","average2"]),
            "beta"    : ("sXY/sXX",["sXY","sXX"]),
            "r2"      : ("1 - (sXX + sYY - 2*sXY) / sYY",["sXX","sYY","sXY"])
        }

    def selectableMetrics(self, metriclist: typing.List[str]) -> typing.List[str]:
        return [m for m in metriclist if not m in ["sXY","sXX","sYY"]]

    def canFilter(self,data:Table) -> typing.List[str]:
        """
        Returns a list of columns that can be filtered on
        """
        return [c for c in data.column_names if (not c.startswith("value") and not c=="Timestamp")]

    def multipleSelect(self) -> typing.List[str]:
        return ["cat2","n2"]

    def mustConstrain(self) -> typing.List[str]:
        return ["cat3"]

    def featureBuckets(self) -> typing.List[str]:
        return ["n1"]

    def featureTraces(self, metrics: typing.List[str]) -> typing.List[str]:
        return ["valueObs","valuePred"]

###################################################
###################################################

def make_static_example(nrows:int=100):
    return Example.static(nrows)

def make_dynamic_example(period:str="PT1s"):
    return Example.ticking(period)

###################################################
###################################################

def initializeApp():
    app = get_app_state()

    global st
    global static_data

    st = make_static_example(1000)
    static_data = st.data
    app["static"] = st.render()

    global dyn
    global ticking_data

    dyn = make_dynamic_example("PT1s")
    ticking_data = dyn.data
    app["ticking"] = dyn.render()

if __name__=="__main__":
    initializeApp()