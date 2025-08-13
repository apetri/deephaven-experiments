import typing

import pandas as pd

import deephaven.plot.express as dx

from deephaven.table import Table
from deephaven.pandas import to_pandas

import plotly.express as px
import plotly.graph_objects as go

# Bars has to be implemented through px cause dx does not support nested categories
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
        raise ValueError("N <= 0")

    fig.update_layout(xaxis_title=".".join(bys),yaxis_title=metric)

    return fig

def oc_lines(t:Table,by:str,mX:str,mY:str) -> dx.DeephavenFigure:
    return dx.line(t,x=mX,y=mY,by=by,markers=True)

# OVP has to be implemented through px
# dx does not support the necessary control granularity over traces
def ovp(t:Table,bys:typing.List[str],feat:str,metrics:typing.List[str]) -> go.Figure:

    # Tag line chunks
    xc = "X".join(bys)
    if len(bys)>1:
        t = t.update(f"{xc} = {bys[0]} + `.` + {bys[1]}")

    lncnk = t.select([xc , f"{feat} = `` + {feat}"] + metrics)

    # Insert None between line chunks

    X = [[],[]]
    Y = [[] for i in range(len(metrics))]

    for tbl in lncnk.partition_by(xc).constituent_tables:
        df = to_pandas(tbl)
        for i,row in df.iterrows():
            X[0].append(row[xc])
            X[1].append(row[feat])

            for j,m in enumerate(metrics):
                Y[j].append(row[m])

        X[0].append(None)
        X[1].append(None)

        for j,m in enumerate(metrics):
            Y[j].append(None)

    # Ready to plot now
    fig = go.Figure()
    for j,m in enumerate(metrics):
        fig.add_trace(go.Scatter(x=X,y=Y[j],mode="markers+lines",name=m))

    fig.update_layout(xaxis_title=xc,yaxis_title="metrics")

    return fig