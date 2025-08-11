from deephaven.table import Table
from deephaven import empty_table

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
            "value2 = 10*random()",
            "value3 = random() + 10*n1",
            "valuePred = 4*n1",
            "valueObs = -2.5 + random()*5 + 4*n1"
        ]
    )

    return t