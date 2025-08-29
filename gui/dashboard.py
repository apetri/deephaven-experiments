import typing

from deephaven import ui,agg
from deephaven.table import Table

from . import traces

class Manager(object):

    def formatLiteral(self,typ:str,c:str,v) -> str:
        match typ:
            case "java.lang.String":
                return f"{c}=`{v}`"
            case "java.time.LocalDate":
                return f"{c}='{v}'"
            case "java.time.LocalTime":
                return f"{c}='{v}'"
            case "java.time.Duration":
                return f"{c}='{v}'"
            case _:
                return f"{c}={v}"

    def formatClause(self,typ:str,c:str,vs:typing.List) -> str:
        return " || ".join([self.formatLiteral(typ,c,v) for v in vs])

    @staticmethod
    def amendList(l0:typing.List,n:int,v) -> typing.List:
         l1 = l0.copy()
         l1[n] = v
         return l1

    def selectDistinct(self,data:Table,col:str,typ:str) -> Table:

        match typ:
            case "java.time.Duration":
                return data.select_distinct(col).sort(col).update(f"{col} = {col}.toString()")
            case _:
                return data.select_distinct(col).sort(col)

    def __init__(self,data:Table):

        self._data = data
        self._ctypes = { r["Name"]:r["DataType"] for r in data.meta_table.iter_dict() }

        self._filterable = self.canFilter(data)
        self._constrained = self.mustConstrain()
        self._free = [f for f in self._filterable if not f in self._constrained]

        self._sortable = self.canSort(data)
        self._multiple_filters = [ f for f in self.multipleSelect() if (f in self._filterable) and (not f in self._constrained) ]
        self._single_filters = [f for f in self._filterable if not f in self._multiple_filters ]

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
    def constrained(self) -> typing.List[str]:
        return self._constrained

    @property
    def free(self) -> typing.List[str]:
        return self._free

    @property
    def single_filters(self) -> typing.List[str]:
        return self._single_filters

    @property
    def multiple_filters(self) -> typing.List[str]:
        return self._multiple_filters

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

    @property
    def metric_values(self) -> typing.List[str]:
        return self._metric_values

    ##########################################################

    ## Editable by user


    def aggregations(self) -> typing.Dict:
        """
        Definitions of allowed aggregations
        """
        return  {
            "count": agg.count_("count"),
        }

    def derived(self) -> typing.Dict:
        """
        Derived aggregations
        """

        return {
            "one" : ("count/count",["count"])
        }

    def canFilter(self,data:Table) -> typing.List[str]:
        """
        Returns a list of columns that can be filtered on
        """
        return data.column_names

    def multipleSelect(self) -> typing.List[str]:

        """
        Returns list of columns that can have a multiple select filter
        """
        return []

    def canSort(self,data:Table) -> typing.List[str]:
        """
        Returns a list of columns that can be explicitly be sorted on upon aggregation
        """
        return data.column_names

    def mustConstrain(self) -> typing.List[str]:
        """
        Return list of columns that must have a value selected
        """
        return []

    def featureBuckets(self) -> typing.List[str]:
        return []

    def featureTraces(self,metrics:typing.List[str]) -> typing.List:
        return []

    ##############################################
    ##############################################

    ### Below typically not touched by user

    ## time-like columns (can be used in timeseries)
    def timeCols(self) -> typing.List[str]:
        return [ c for c,t in self.ctypes.items() if t in ["java.time.LocalDate","java.time.LocalTime"] ]

    ## Supported chart types
    def chartTypes(self) -> typing.List[str]:

        chrts = ["bars","lines"]

        if len(self.timeCols()):
            chrts.append("timeseries")
    
        if len(self.featureBuckets())>0:
            chrts.append("featurelines")
    
        return chrts

    ## Filtering
    def filteringControls(self) -> typing.Dict:

        # Filter buttons
        filter_buttons_single = [

            ui.combo_box(self.selectDistinct(self.data,c,self.ctypes[c]),
                         key=c,
                         label=c,
                         selected_key=self._filter_values.get(c),
                         on_change=lambda v,x=c: self._set_filter_values({**self._filter_values,x:[v] if v is not None else []}))

            for c in self.free if not c in self.multiple_filters
        ]

        filter_buttons_single += [
            ui.picker(self.selectDistinct(self.data,c,self.ctypes[c]),
                      key=c,
                      label=c,
                      selected_key=self._filter_values.get(c),
                      on_change=lambda v,x=c: self._set_filter_values({**self._filter_values,x:[v]}))

            for c in self.constrained
        ]

        filter_buttons_multiple = [

            ui.checkbox_group(*[x[c] for x in self.selectDistinct(self.data,c,self.ctypes[c]).iter_dict()],
                              label=c,
                              value=self._filter_values.get(c),
                              on_change=lambda v,x=c: self._set_filter_values({**self._filter_values,x:v}),
                              orientation="horizontal")

            for c in self.multiple_filters
        ]

        # Clear all filters
        clear = ui.button("Clear all filters",on_press=lambda b: self._set_filter_values({k:([] if k in self.free else v) for k,v in self._filter_values.items() }))

        # Done
        return {
            "filter_buttons_single" : filter_buttons_single,
            "filter_buttons_multiple" : filter_buttons_multiple,
            "clear_button" : clear
        }

    def filterTable(self,filter_values:typing.Dict,bys:typing.List[str]) -> typing.Dict:

        # Do the filtering
        # Exclude "must constrain clauses if that clause is in a by"
        excl = [c for c in self.mustConstrain() if c in bys]

        clauses = [self.formatClause(self.ctypes[x],x,filter_values[x]) for x in filter_values if (len(filter_values[x])>0) and (not x in excl)]
        tfilt = self._data.where(clauses)

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
            case "lines":
                return {
                    "Trace by": filterable,
                    "Sweep by": filterable
                }
            case "timeseries":
                return {
                    "Time": self.timeCols(),
                    "Trace by": [c for c in filterable if not c in self.timeCols() ]
                }
            case "featurelines":
                return {
                    "Primary by": filterable,
                    "Secondary by": ["NONE"] + filterable,
                    "Feature bucket": self.featureBuckets()
                }
            case _:
                raise ValueError("Chart type not implemented")

    def metricChoices(self,chart_type:str) -> typing.Dict:

        metrics = list(self.aggregations().keys()) + list(self.derived().keys())

        match chart_type:
            case "bars":
                return {
                    "Aggregation metric": metrics,
                }
            case "lines":
                return {
                    "Aggregation metric X": metrics,
                    "Aggregation metric Y": metrics[1:] + [metrics[0]]
                }
            case "timeseries":
                return {
                    "Aggregation metric": metrics
                }
            case "featurelines":
                return {"traces" : self.featureTraces(metrics) }
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

        metric_buttons = []
        if self._chart_type=="featurelines":
            metric_buttons += [
                ui.checkbox_group(
                    *metric_choices["traces"],label="traces",value=self._metric_values,on_change=lambda v:self._setMetrics(v),orientation="horizontal"
                )
            ]
        else:
            metric_buttons += [
                ui.picker(*m,selected_key=self._metric_values[i],on_change=lambda v,i=i:self._set_metric_values(self.amendList(self._metric_values,i,v)),label=n)
                for i,(n,m) in enumerate(metric_choices.items())
            ]

        # Done
        return {
            "by_buttons" : by_buttons,
            "metric_button" : metric_buttons,
        }

    def aggregateTable(self,tfilt:Table,chart_type:str,by_values:typing.List[str],metric_values:typing.List[str]) -> Table:

        ## Find out which metrics we need to calculate
        aggr = self.aggregations()
        derv = self.derived()

        calclist = dict()
        dervlist = []

        ## Aggregations + derived metrics
        for m in metric_values:
            if m in aggr:
                calclist[m] = aggr[m]
            else:
                for dep in derv[m][1]:
                    calclist[dep] = aggr[dep]
                dervlist.append(f"{m} = {derv[m][0]}")

        # Run aggregations
        byv = [b for b in by_values if b!="NONE"]
        srt = set([x for x in self.featureBuckets() + byv if (x in byv) and x in self.sortable])

        tagg = tfilt.agg_by(aggs=list(calclist.values()),by=byv).sort(list(srt))

        # Calculate derived stats if any
        if(len(dervlist)>0):
            tagg = tagg.update(dervlist)

        # Done
        return tagg

    ## Charting
    def _toggleChartType(self,chart_type:str):
        self._set_chart_type(chart_type)
        self._set_by_values([v[0] for n,v in self.byChoices(chart_type).items()])
        self._set_metric_values([v[0] for n,v in self.metricChoices(chart_type).items()])

    def _setMetrics(self,v):

        # Prevent from selecting zero traces
        if(len(v)==0):
            return

        self._set_metric_values(v)

    def chartControls(self,chart_type:str) -> typing.Dict:

        # Graph type button
        chart_button = ui.picker(*self.chartTypes(),selected_key=chart_type,on_change=lambda v:self._toggleChartType(str(v)),label="Chart type")

        return {
            "chart_button": chart_button
        }

    def chartTable(self,chart_type:str,tagg:Table,bys:typing.List[str],metrics:typing.List[str]):

        byv = [b for b in bys if b!="NONE"]

        match chart_type:
            case "bars":
                return traces.bars(tagg,bys=byv,metric=metrics[0])
            case "lines":
                return traces.lines(tagg,by=byv[0],mX=metrics[0],mY=metrics[1])
            case "timeseries":
                return traces.timeseries(tagg,tc=byv[0],by=byv[1],metric=metrics[0])
            case "featurelines":
                return traces.featurelines(tagg,bys=byv[:-1],feat=byv[-1],metrics=metrics)
            case _:
                raise ValueError(f"Chart type:{chart_type} not implemented")

    @ui.component
    def arrange(self):

        # State management
        self._chart_type,self._set_chart_type = ui.use_state(self.chartTypes()[0])

        mustcnstr = self.mustConstrain()
        dflt:typing.Dict = {x:[] for x in self.filterable}
        if len(mustcnstr)>0:
            dflt.update( {k:[v] for k,v in next(self._data.iter_dict(cols=mustcnstr)).items()} )

        self._filter_values,self._set_filter_values = ui.use_state(dflt)

        self._by_values,self._set_by_values = ui.use_state([m[0] for n,m in self.byChoices(self._chart_type).items()])
        self._metric_values,self._set_metric_values = ui.use_state([m[0] for n,m in self.metricChoices(self._chart_type).items()])

        # Controls
        filtcntrl = self.filteringControls()
        chrtcntrl = self.chartControls(self._chart_type)
        aggcntrl = ui.use_memo(lambda:self.aggregationControls(),[self._chart_type,self._by_values,self._metric_values])

        # Filtering
        filt = ui.use_memo(lambda:self.filterTable(self._filter_values,self._by_values),[self._filter_values,self._by_values])

        # Aggregations
        aggtbl = ui.use_memo(lambda:self.aggregateTable(filt["filtered_table"],self._chart_type,self._by_values,self._metric_values),[filt,self._chart_type,self._by_values,self._metric_values])

        # Charting
        chrt = ui.use_memo(lambda:self.chartTable(self._chart_type,aggtbl,self._by_values,self._metric_values),[self._chart_type,aggtbl,self._by_values,self._metric_values])

        # Arrange
        return ui.column(
            ui.row(
                ui.column(
                    ui.panel(filtcntrl["clear_button"],
                             ui.flex(*filtcntrl["filter_buttons_single"],wrap="wrap"),
                             ui.column(*filtcntrl["filter_buttons_multiple"]),
                             title="Filtering controls"
                             )
                        ),
                ui.column(
                    ui.panel(ui.flex(chrtcntrl["chart_button"]),ui.flex(*aggcntrl["by_buttons"],wrap="wrap"),ui.flex(aggcntrl["metric_button"],wrap="wrap"),title="Aggregation controls")
                )
            ),
            ui.row(
                ui.stack(
                    ui.panel(ui.text(" AND ".join(filt["filter_clauses"])),filt["filtered_table"],title="Filtered table"),
                    ui.panel(aggtbl,title="Aggregated table"),
                    ui.panel(chrt,title=f"Chart: {self._chart_type}"),height=60
                ),
                height=60
            )
        )

    def render(self):
        return ui.dashboard(self.arrange())