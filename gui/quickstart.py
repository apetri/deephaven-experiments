import typing

from deephaven import empty_table,time_table,agg
from deephaven.table import Table
from deephaven.updateby import cum_sum
from deephaven.appmode import ApplicationState, get_app_state

from gui import dashboard

class Example(dashboard.Manager):

    @classmethod
    def random(cls,t:Table):
        t = t.update([
            "cat1 = new String[]{`a`,`b`,`c`}[randomInt(0,3)]",
            "cat2 = new String[]{`DD`,`EE`,`FF`,`GG`}[randomInt(0,4)]",
            "cat3 = new String[]{`fff`,`ggg`,`hhh`,`iii`,`lll`}[randomInt(0,4)]",
            "n1 = new int[]{1,2,3,4}[randomInt(0,4)]",
            "n2 = new int[]{10,20,30}[randomInt(0,3)]",
            "value1 = random()",
            "value2 = n1 + random()",
            "value3 = n2 + 10*n1*n1 + random()",
            "valuePred = 4*n1",
            "valueObs = -2.5 + random()*5 + 4*n1",
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
            "valuePred": agg.avg("valuePred")
        }

    def aggregateTable(self,tfilt:Table,chart_type:str,by_values:typing.List[str],metric_values:typing.List[str]) -> Table:

        calcs = set(["count"] + metric_values if chart_type=="ovp" else metric_values)
        byv = [b for b in by_values if b!="NONE"]
        tagg = tfilt.agg_by([self.aggregations()[m] for m in calcs],by=byv).sort([b for b in byv if b in self.sortable])

        return tagg

    def canFilter(self,data:Table) -> typing.List[str]:
        """
        Returns a list of columns that can be filtered on
        """
        return [c for c in data.column_names if (not c.startswith("value") and not c=="Timestamp")]

    def mustConstrain(self) -> typing.List[str]:
        return ["cat3"]

    def featureBuckets(self) -> typing.List[str]:
        return ["n1"]

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

    global static_data
    global ticking_data

    st = make_static_example(1000)
    static_data = st.data
    app["static"] = st.render()

    dyn = make_dynamic_example("PT1s")
    ticking_data = dyn.data
    app["ticking"] = dyn.render()

if __name__=="__main__":
    initializeApp()