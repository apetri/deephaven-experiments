import typing

from deephaven import ui,agg

from deephaven.table import Table
from deephaven.pandas import to_pandas

from . import traces

# Globals
CLAUSE = {
    "int" : lambda c,v:f"{c}={v}",
    "double" : lambda c,v:f"{c}={v}",
    "java.lang.String" : lambda c,v:f"{c}=`{v}`"
}

CONFIG = {
    "chart_types" : ["bars"],
    "filterable" : lambda t: [c for c in t.column_names if not c.startswith("value")],
    "aggregations":{
        "count": agg.count_("count"),
        "sum1": agg.sum_("sum1 = value1"),
        "average1": agg.avg("average1 = value1"),
        "average2": agg.avg("average2 = value2"),
        "average3": agg.avg("average3 = value3")
    }

}

# Filtering
def filteringControls(t:Table,filterable:typing.List[str],filter_values:typing.Dict,set_filter_values:typing.Callable) -> typing.Dict:

    # Text box
    txt = ui.text(f"Filters: " + str({k:v for k,v in filter_values.items() if v is not None}))

    # Filter buttons
    filter_buttons = [

        ui.combo_box(t.select_distinct(c).sort(c),
                     key=c,
                     label=c,
                     selected_key=filter_values[c] if c in filter_values else None,
                     on_change=lambda v,x=c: set_filter_values({**filter_values,x:v}))

        for c in filterable
    ]

    # Clear all filters
    clear = ui.button("Clear all filters",on_press=lambda b: set_filter_values({x:None for x in filter_buttons}))

    # Done
    return {
        "filter_text" : txt,
        "filter_buttons" : filter_buttons,
        "clear_button" : clear
    }


def filterTable(t:Table,filter_values:typing.Dict) -> typing.Dict:

    ctyp = ui.use_memo(lambda: dict(to_pandas(t.meta_table)[["Name","DataType"]].values),[t])

    # Do the filtering
    clauses = ui.use_memo(lambda:[CLAUSE[ctyp[x]](x,filter_values[x]) for x in filter_values if filter_values[x] is not None],[filter_values])
    tfilt = ui.use_memo(lambda: t.where(clauses),[t,clauses])

    return {
        "filter_clauses" : clauses,
        "filtered_table" : tfilt
    }

# Aggregations
def aggregationControls(filterable:typing.List[str],by_values:typing.List,set_by_values:typing.Callable,metrics:typing.List[str],metric_value:str,set_metric_value:typing.Callable) -> typing.Dict:

    # Set buttons
    by_buttons = [
        ui.picker(*filterable,selected_key=by_values[0],on_change=lambda v:set_by_values([v,by_values[1],by_values[2]]),label="Primary by"),
        ui.picker(*["NONE"]+filterable,selected_key=by_values[1],on_change=lambda v:set_by_values([by_values[0],v,by_values[2]]),label="Secondary by"),
        ui.picker(*["NONE"]+filterable,selected_key=by_values[2],on_change=lambda v:set_by_values([by_values[0],by_values[1],v]),label="Tertiary by")
    ]

    metric_button = ui.picker(*metrics,selected_key=metric_value,on_change=set_metric_value,label="Aggregation metric")

    # Done
    return {
        "by_buttons" : by_buttons,
        "metric_button" : metric_button,
    }

def aggregateTable(tfilt:Table,by_values:typing.List,metric_value:str) -> typing.Dict:

    # Run aggregations
    byv = [b for b in by_values if b!="NONE"]
    tagg = ui.use_memo(lambda:tfilt.agg_by([CONFIG["aggregations"][metric_value]],by=byv).sort(byv),[tfilt,by_values,metric_value])

    return {
        "aggregated_table": tagg
    }

# Charting
def chartControls(chart_type:str,set_chart_type:typing.Callable) -> typing.Dict:

    # Graph type button
    chart_button = ui.picker(*CONFIG["chart_types"],selected_key=chart_type,on_change=set_chart_type,label="Chart type")

    return {
        "chart_button": chart_button
    }

def chartTable(tagg:Table,bys:typing.List[str],metric:str,chart_type:str):

    byv = [b for b in bys if b!="NONE"]

    match chart_type:
        case "bars":
            return ui.use_memo(lambda:traces.bars(tagg,byv,metric),[tagg,byv,metric])
        case _:
            raise ValueError(f"Chart type:{chart_type} not implemented")


##########################################################
##########################################################

@ui.component
def arrange(t:Table):

    filterable = CONFIG["filterable"](t)
    metrics = list(CONFIG["aggregations"].keys())

    # State management
    filter_values,set_filter_values = ui.use_state({})
    by_values,set_by_values = ui.use_state([filterable[0],"NONE","NONE"])
    metric_value,set_metric_value = ui.use_state(metrics[0])
    chart_type,set_chart_type = ui.use_state(CONFIG["chart_types"][0])

    # Filtering
    filt = {}
    filt.update(filteringControls(t,filterable,filter_values,set_filter_values))
    filt.update(filterTable(t,filter_values))

    # Aggregations
    aggs = {}
    aggs.update(aggregationControls(filterable,by_values,set_by_values,metrics,metric_value,set_metric_value))
    aggs.update(aggregateTable(filt["filtered_table"],by_values,metric_value))

    # Charting
    chrt = chartControls(chart_type,set_chart_type)
    chrt["chart"] = chartTable(aggs["aggregated_table"],by_values,metric_value,chart_type)

    # Arrange
    return ui.column(
        ui.row(
            ui.panel(filt["filter_text"],filt["clear_button"]," AND ".join(filt["filter_clauses"]),ui.flex(*filt["filter_buttons"],wrap="wrap"),title="Filtering controls"),
            ui.panel(ui.flex(*aggs["by_buttons"],wrap="wrap"),aggs["metric_button"],title="Aggregation controls")),
        ui.row(
            ui.panel(filt["filtered_table"],title="Filtered table"),
            ui.panel(aggs["aggregated_table"],title="Aggregated table")
        ),
        ui.row(ui.panel(chrt["chart_button"],chrt["chart"],title="Charted aggregated table"),height=60)
    )