import typing

from deephaven import ui,agg

from deephaven.table import Table
from deephaven.pandas import to_pandas

from . import traces

class Manager(object):

    FORMATCLAUSE = {
        "int" : lambda c,v:f"{c}={v}",
        "double" : lambda c,v:f"{c}={v}",
        "java.lang.String" : lambda c,v:f"{c}=`{v}`"
    }

    @staticmethod
    def amendList(l0:typing.List,n:int,v):
         l1 = l0.copy()
         l1[n] = v
         return l1

    def __init__(self,data:Table):
        self.data_ = data
        self.ctypes_ = dict(to_pandas(data.meta_table)[["Name","DataType"]].values)
        self.filterable_ = self.canFilter(data)
    
    @property
    def data(self) -> Table:
        return self.data_

    @property
    def ctypes(self) -> typing.Dict:
        return self.ctypes_

    @property
    def filterable(self) -> typing.List[str]:
        return self.filterable_
    
    @classmethod
    def fetch(cls,callable:typing.Callable,*args,**kwargs):

        """
        Callable must return a deephaven Table
        """

        return cls(callable(*args,**kwargs))

    ##########################################################

    ## Editable by user


    def aggregations(self) -> typing.Dict:
        """
        Definitions of allowed aggregations
        """
        return  {
            "count": agg.count_("count"),
        }

    def chartTypes(self) -> typing.List[str]:
        return ["bars","oc_lines","ovp"]

    def canFilter(self,data:Table) -> typing.List[str]:
        """
        Returns a list of columns that can be filtered on
        """
        return data.column_names

    def featureBuckets(self) -> typing.List[str]:
        return []

    ##############################################
    ##############################################

    ### Below typically not touched by user

    ## Filtering
    def filteringControls(self,filter_values:typing.Dict,set_filter_values:typing.Callable) -> typing.Dict:

        # Text box
        txt = ui.text(f"Filters: " + str({k:v for k,v in filter_values.items() if v is not None}))

        # Filter buttons
        filter_buttons = [

            ui.combo_box(self.data_.select_distinct(c).sort(c),
                         key=c,
                         label=c,
                         selected_key=filter_values[c] if c in filter_values else None,
                         on_change=lambda v,x=c: set_filter_values({**filter_values,x:v}))

            for c in self.filterable
        ]

        # Clear all filters
        clear = ui.button("Clear all filters",on_press=lambda b: set_filter_values({x:None for x in filter_buttons}))

        # Done
        return {
            "filter_text" : txt,
            "filter_buttons" : filter_buttons,
            "clear_button" : clear
        }


    def filterTable(self,filter_values:typing.Dict) -> typing.Dict:

        ctyp = ui.use_memo(lambda: dict(to_pandas(self.data_.meta_table)[["Name","DataType"]].values),[self.data_])

        # Do the filtering
        clauses = ui.use_memo(lambda:[self.FORMATCLAUSE[ctyp[x]](x,filter_values[x]) for x in filter_values if filter_values[x] is not None],[filter_values])
        tfilt = ui.use_memo(lambda: self.data_.where(clauses),[self.data_,clauses])

        return {
            "filter_clauses" : clauses,
            "filtered_table" : tfilt
        }

    ## Aggregations
    def byChoices(self,chart_type:str) -> typing.Dict:

        filterable = self.filterable

        match chart_type:
            case "bars":
                return {
                    "Primary by": filterable,
                    "Secondary by": ["NONE"] + filterable,
                    "Tertiary by": ["NONE"] + filterable
                }
            case "oc_lines":
                return {
                    "Trace by": filterable,
                    "Sweep by": filterable
                }
            case "ovp":
                return {
                    "Primary by": filterable,
                    "Secondary by": ["NONE"] + filterable,
                    "Feature bucket": self.featureBuckets()
                }
            case _:
                raise ValueError("Chart type not implemented")

    def metricChoices(self,chart_type:str) -> typing.Dict:

        metrics = list(self.aggregations().keys())

        match chart_type:
            case "bars":
                return {
                    "Aggregation metric": metrics,
                }
            case "oc_lines":
                return {
                    "Aggregation metric X": metrics,
                    "Aggregation metric Y": metrics[1:] + [metrics[0]]
                }
            case "ovp":
                return {
                    "Aggregation metric (prediction)": metrics,
                    "Aggregation metric (observation)": metrics[1:] + [metrics[0]]
                }
            case _:
                raise ValueError("Chart type not implemented")


    def aggregationControls(self,chart_type:str,by_values:typing.List,set_by_values:typing.Callable,metric_values:typing.List[str],set_metric_values:typing.Callable) -> typing.Dict:

        # By buttons
        by_choices = self.byChoices(chart_type)

        by_buttons = [
            ui.picker(*f,selected_key=by_values[i],on_change=lambda v,i=i:set_by_values(self.amendList(by_values,i,v)),label=k)
            for i,(k,f) in enumerate(by_choices.items())
        ]

        # Aggregation metric buttons
        metric_choices = self.metricChoices(chart_type)

        metric_buttons = [
            ui.picker(*m,selected_key=metric_values[i],on_change=lambda v,i=i:set_metric_values(self.amendList(metric_values,i,v)),label=n)
            for i,(n,m) in enumerate(metric_choices.items())
        ]

        # Done
        return {
            "by_buttons" : by_buttons,
            "metric_button" : metric_buttons,
        }

    def aggregateTable(self,tfilt:Table,by_values:typing.List[str],metric_values:typing.List[str]) -> Table:

        # Run aggregations
        byv = [b for b in by_values if b!="NONE"]
        tagg = tfilt.agg_by([self.aggregations()[m] for m in set(metric_values)],by=byv).sort(byv)

        return tagg

    ## Charting
    def toggleChartType(self,chart_type:str,set_chart_type:typing.Callable,set_by_values:typing.Callable,set_metric_values:typing.Callable):
        set_chart_type(chart_type)
        set_by_values([v[0] for n,v in self.byChoices(chart_type).items()])
        set_metric_values([v[0] for n,v in self.metricChoices(chart_type).items()])

    def chartControls(self,chart_type:str,set_chart_type:typing.Callable,set_by_values:typing.Callable,set_metric_values:typing.Callable) -> typing.Dict:

        # Graph type button
        chart_button = ui.picker(*self.chartTypes(),selected_key=chart_type,on_change=lambda v:self.toggleChartType(str(v),set_chart_type,set_by_values,set_metric_values),label="Chart type")

        return {
            "chart_button": chart_button
        }

    def chartTable(self,chart_type:str,tagg:Table,bys:typing.List[str],metrics:typing.List[str]):

        byv = [b for b in bys if b!="NONE"]

        match chart_type:
            case "bars":
                return traces.bars(tagg,byv,metrics[0])
            case "oc_lines":
                return traces.oc_lines(tagg,byv[0],metrics[0],metrics[1])
            case "ovp":
                return traces.ovp(tagg,byv[:-1],byv[-1],metrics)
            case _:
                raise ValueError(f"Chart type:{chart_type} not implemented")

    @ui.component
    def arrange(self):

        # State management
        chart_type,set_chart_type = ui.use_state(self.chartTypes()[0])

        filter_values,set_filter_values = ui.use_state({})

        by_values,set_by_values = ui.use_state([m[0] for n,m in self.byChoices(chart_type).items()])
        metric_values,set_metric_values = ui.use_state([m[0] for n,m in self.metricChoices(chart_type).items()])

        # Filtering
        filt = {}
        filt.update(self.filteringControls(filter_values,set_filter_values))
        filt.update(self.filterTable(filter_values))

        # Charting controls
        chrtcntrl = self.chartControls(chart_type,set_chart_type,set_by_values,set_metric_values)

        # Aggregations
        aggcntrl = ui.use_memo(lambda:self.aggregationControls(chart_type,by_values,set_by_values,metric_values,set_metric_values),[chart_type,by_values,metric_values])
        aggtbl = ui.use_memo(lambda:self.aggregateTable(filt["filtered_table"],by_values,metric_values),[filt["filtered_table"],by_values,metric_values])

        # Chart
        chrt = ui.use_memo(lambda:self.chartTable(chart_type,aggtbl,by_values,metric_values),[chart_type,aggtbl,by_values,metric_values])

        # Arrange
        return ui.column(
            ui.row(
                ui.panel(filt["filter_text"],filt["clear_button"]," AND ".join(filt["filter_clauses"]),ui.flex(*filt["filter_buttons"],wrap="wrap"),title="Filtering controls"),
                ui.panel(ui.flex(chrtcntrl["chart_button"]),ui.flex(*aggcntrl["by_buttons"],wrap="wrap"),ui.flex(aggcntrl["metric_button"],wrap="wrap"),title="Aggregation controls")),
            ui.row(
                ui.panel(filt["filtered_table"],title="Filtered table"),
                ui.panel(aggtbl,title="Aggregated table")
            ),
            ui.row(ui.panel(chrt,title=f"Charted aggregation: {chart_type}"),height=60)
        )