import typing

from deephaven import ui,agg

from deephaven.table import Table
from deephaven.pandas import to_pandas

from . import traces

class Manager(object):

    @staticmethod
    def formatClause(typ:str,c:str,v) -> str:
        match typ:
            case "java.lang.String":
                return f"{c}=`{v}`"
            case "java.time.LocalDate":
                return f"{c}='{v}'"
            case "java.time.LocalTime":
                return f"{c}='{v}'"
            case _:
                return f"{c}={v}"

    @staticmethod
    def amendList(l0:typing.List,n:int,v) -> typing.List:
         l1 = l0.copy()
         l1[n] = v
         return l1

    def __init__(self,data:Table):

        self._data = data
        self._ctypes = dict(to_pandas(data.meta_table)[["Name","DataType"]].values)
        self._filterable = self.canFilter(data)
        self._sortable = self.canSort(data)

        # State management, assigned later upon dashboard rendering
        self._chart_type = "NONE"
        self._set_chart_type = lambda v:None

        self._filter_values = {"None":"None"}
        self._set_filter_values = lambda v:None

        self._by_values = ["None"]
        self._set_by_values = lambda v:None

        self._metric_values = ["None"]
        self._set_metric_values = lambda v:None
    
    @classmethod
    def fetch(cls,callable:typing.Callable,*args,**kwargs):

        """
        Callable must return a deephaven Table
        """

        return cls(callable(*args,**kwargs))

    # Property getters
    @property
    def data(self) -> Table:
        return self._data

    @property
    def ctypes(self) -> typing.Dict[str,str]:
        return self._ctypes

    @property
    def filterable(self) -> typing.List[str]:
        return self._filterable
    
    @property
    def sortable(self) -> typing.List[str]:
        return self._sortable

    @property
    def chart_type(self) -> str:
        return self._chart_type

    @property
    def filter_values(self) -> typing.Dict:
        return self._filter_values

    @property
    def by_values(self) -> typing.List[str]:
        return self._by_values

    ##########################################################

    ## Editable by user


    def aggregations(self) -> typing.Dict:
        """
        Definitions of allowed aggregations
        """
        return  {
            "count": agg.count_("count"),
        }

    def canFilter(self,data:Table) -> typing.List[str]:
        """
        Returns a list of columns that can be filtered on
        """
        return data.column_names

    def canSort(self,data:Table) -> typing.List[str]:
        """
        Returns a list of columns that can be explicitly be sorted on upon aggregation
        """
        return data.column_names

    def featureBuckets(self) -> typing.List[str]:
        return []

    ##############################################
    ##############################################

    ### Below typically not touched by user

    ## time-like columns (can be used in timeseries)
    def timeCols(self) -> typing.List[str]:
        return [ c for c,t in self.ctypes.items() if t in ["java.time.LocalDate","java.time.LocalTime"] ]

    ## Supported chart types
    def chartTypes(self) -> typing.List[str]:

        chrts = ["bars","oc_lines"]

        if len(self.timeCols()):
            chrts.append("timeseries")
    
        if len(self.featureBuckets())>0:
            chrts.append("ovp")
    
        return chrts

    ## Filtering
    def filteringControls(self) -> typing.Dict:

        # Text box
        txt = ui.text(f"Filters: " + str({k:v for k,v in self._filter_values.items() if v is not None}))

        # Filter buttons
        filter_buttons = [

            ui.combo_box(self._data.select_distinct(c).sort(c),
                         key=c,
                         label=c,
                         selected_key=self._filter_values[c] if c in self._filter_values else None,
                         on_change=lambda v,x=c: self._set_filter_values({**self._filter_values,x:v}))

            for c in self.filterable
        ]

        # Clear all filters
        clear = ui.button("Clear all filters",on_press=lambda b: self._set_filter_values({x:None for x in filter_buttons}))

        # Done
        return {
            "filter_text" : txt,
            "filter_buttons" : filter_buttons,
            "clear_button" : clear
        }


    def filterTable(self,filter_values:typing.Dict) -> typing.Dict:

        # Do the filtering
        clauses = ui.use_memo(lambda:[self.formatClause(self.ctypes[x],x,filter_values[x]) for x in filter_values if filter_values[x] is not None],[filter_values])
        tfilt = ui.use_memo(lambda: self._data.where(clauses),[self._data,clauses])

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
            case "timeseries":
                return {
                    "Time": self.timeCols(),
                    "Trace by": [c for c in filterable if not c in self.timeCols() ]
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
            case "timeseries":
                return {
                    "Aggregation metric": metrics
                }
            case "ovp":
                return {
                    "Aggregation metric (prediction)": metrics,
                    "Aggregation metric (observation)": metrics[1:] + [metrics[0]]
                }
            case _:
                raise ValueError("Chart type not implemented")


    def aggregationControls(self) -> typing.Dict:

        # By buttons
        by_choices = self.byChoices(self._chart_type)

        by_buttons = [
            ui.picker(*f,selected_key=self._by_values[i],on_change=lambda v,i=i:self._set_by_values(self.amendList(self._by_values,i,v)),label=k)
            for i,(k,f) in enumerate(by_choices.items())
        ]

        # Aggregation metric buttons
        metric_choices = self.metricChoices(self._chart_type)

        metric_buttons = [
            ui.picker(*m,selected_key=self._metric_values[i],on_change=lambda v,i=i:self._set_metric_values(self.amendList(self._metric_values,i,v)),label=n)
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
        tagg = tfilt.agg_by([self.aggregations()[m] for m in set(metric_values)],by=byv).sort([b for b in byv if b in self.sortable])

        return tagg

    ## Charting
    def toggleChartType(self,chart_type:str):
        self._set_chart_type(chart_type)
        self._set_by_values([v[0] for n,v in self.byChoices(chart_type).items()])
        self._set_metric_values([v[0] for n,v in self.metricChoices(chart_type).items()])

    def chartControls(self,chart_type:str) -> typing.Dict:

        # Graph type button
        chart_button = ui.picker(*self.chartTypes(),selected_key=chart_type,on_change=lambda v:self.toggleChartType(str(v)),label="Chart type")

        return {
            "chart_button": chart_button
        }

    def chartTable(self,chart_type:str,tagg:Table,bys:typing.List[str],metrics:typing.List[str]):

        byv = [b for b in bys if b!="NONE"]

        match chart_type:
            case "bars":
                return traces.bars(tagg,bys=byv,metric=metrics[0])
            case "oc_lines":
                return traces.oc_lines(tagg,by=byv[0],mX=metrics[0],mY=metrics[1])
            case "timeseries":
                return traces.timeseries(tagg,tc=byv[0],by=byv[1],metric=metrics[0])
            case "ovp":
                return traces.ovp(tagg,bys=byv[:-1],feat=byv[-1],metrics=metrics)
            case _:
                raise ValueError(f"Chart type:{chart_type} not implemented")

    @ui.component
    def arrange(self):

        # State management
        self._chart_type,self._set_chart_type = ui.use_state(self.chartTypes()[0])

        self._filter_values,self._set_filter_values = ui.use_state({})

        self._by_values,self._set_by_values = ui.use_state([m[0] for n,m in self.byChoices(self._chart_type).items()])
        self._metric_values,self._set_metric_values = ui.use_state([m[0] for n,m in self.metricChoices(self._chart_type).items()])

        # Filtering
        filt = {}
        filt.update(self.filteringControls())
        filt.update(self.filterTable(self._filter_values))

        # Charting controls
        chrtcntrl = self.chartControls(self._chart_type)

        # Aggregations
        aggcntrl = ui.use_memo(lambda:self.aggregationControls(),[self._chart_type,self._by_values,self._metric_values])
        aggtbl = ui.use_memo(lambda:self.aggregateTable(filt["filtered_table"],self._by_values,self._metric_values),[filt["filtered_table"],self._by_values,self._metric_values])

        # Chart
        chrt = ui.use_memo(lambda:self.chartTable(self._chart_type,aggtbl,self._by_values,self._metric_values),[self._chart_type,aggtbl,self._by_values,self._metric_values])

        # Arrange
        return ui.column(
            ui.row(
                ui.panel(filt["filter_text"],filt["clear_button"]," AND ".join(filt["filter_clauses"]),ui.flex(*filt["filter_buttons"],wrap="wrap"),title="Filtering controls"),
                ui.panel(ui.flex(chrtcntrl["chart_button"]),ui.flex(*aggcntrl["by_buttons"],wrap="wrap"),ui.flex(aggcntrl["metric_button"],wrap="wrap"),title="Aggregation controls")),
            ui.row(
                ui.panel(filt["filtered_table"],title="Filtered table"),
                ui.panel(aggtbl,title="Aggregated table")
            ),
            ui.row(ui.panel(chrt,title=f"Charted aggregation: {self._chart_type}"),height=60)
        )

    def render(self):
        return ui.dashboard(self.arrange())