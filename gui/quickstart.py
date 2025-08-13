import typing

from deephaven.table import Table
from deephaven import empty_table,agg

from . import dashboard

def randomTable(nrows:int) -> Table:

    t = empty_table(nrows)
    t = t.update(
        [
            "cat1 = new String[]{`a`,`b`,`c`}[randomInt(0,3)]",
            "cat2 = new String[]{`DD`,`EE`,`FF`,`GG`}[randomInt(0,4)]",
            "cat3 = new String[]{`fff`,`ggg`,`hhh`,`iii`,`lll`}[randomInt(0,4)]",
            "n1 = new int[]{1,2,3,4}[randomInt(0,4)]",
            "n2 = new int[]{10,20,30}[randomInt(0,3)]",
            "value1 = random()",
            "value2 = n1 + random()",
            "value3 = n2 + 10*n1*n1 + random()",
            "valuePred = 4*n1",
            "valueObs = -2.5 + random()*5 + 4*n1"
        ]
    )

    return t

class Static(dashboard.Manager):

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

    def canFilter(self,data:Table) -> typing.List[str]:
        """
        Returns a list of columns that can be filtered on
        """
        return [c for c in data.column_names if not c.startswith("value")]

    def featureBuckets(self) -> typing.List[str]:
        return ["n1"]

def make_static_example():
    return Static.fetch(randomTable,nrows=100)