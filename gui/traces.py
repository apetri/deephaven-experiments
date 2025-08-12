import typing

import deephaven.plot.express as dx

from deephaven.table import Table
from deephaven.pandas import to_pandas

import plotly.express as px
import plotly.graph_objects as go

def bars(t:Table,bys:typing.List[str],metric:str) -> go.Figure:

    df = to_pandas(t)

    fig = go.Figure()
    N = len(bys)

    if N==1:
        fig.add_trace(go.Bar(x=df[bys[0]],y=df[metric],name=bys[0]))
    elif N>1:
        for n in df[bys[N-1]].unique():
            fig.add_trace(go.Bar(x=df[df[bys[N-1]]==n][bys].T,y=df[df[bys[N-1]]==n][metric],name=str(n)))
    else:
        raise ValueError()

    fig.update_layout(xaxis_title=".".join(bys),yaxis_title=metric)

    return fig

def scatter(t:Table,bys:typing.List[str],mX:str,mY:str) -> dx.DeephavenFigure:
    return dx.scatter(t,x=mX,y=mY,by=bys[0])