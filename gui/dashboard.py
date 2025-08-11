from deephaven import ui,agg

from deephaven.table import Table
from deephaven.pandas import to_pandas

from . import traces

CLAUSE = {
    "int" : lambda c,v:f"{c}={v}",
    "double" : lambda c,v:f"{c}={v}",
    "java.lang.String" : lambda c,v:f"{c}=`{v}`"
}


def filtering(t:Table,filter_values,set_filter_values):

    ctyp = ui.use_memo(lambda: dict(to_pandas(t.meta_table)[["Name","DataType"]].values),[t])

    # Text box
    txt = ui.text(f"Filters: " + str({k:v for k,v in filter_values.items() if v is not None}))

    # Filter buttons
    filter_buttons = [

        ui.combo_box(t.select_distinct(c).sort(c),
                     key=c,
                     label=c,
                     selected_key=filter_values[c] if c in filter_values else None,
                     on_change=lambda v,x=c: set_filter_values({**filter_values,x:v}))

        for c in t.column_names if not c.startswith("value")
    ]

    # Clear all filters
    clear = ui.button("Clear all filters",on_press=lambda b: set_filter_values({x:None for x in filter_buttons}))

    # Filter clauses
    clauses = ui.use_memo(lambda:[CLAUSE[ctyp[x]](x,filter_values[x]) for x in filter_values if filter_values[x] is not None],[filter_values])
    tfilt = ui.use_memo(lambda: t.where(clauses),[t,clauses])

    return {
        "filter_text" : txt,
        "filter_buttons" : filter_buttons,
        "clear_button" : clear,
        "filter_clauses" : clauses,
        "filtered_table" : tfilt
    }


@ui.component
def arrange(t:Table):

    # State management
    filter_values,set_filter_values = ui.use_state({})

    # Filtering
    filt = filtering(t,filter_values,set_filter_values)

    ui.panel(filt["filter_text"],filt["clear_button"]," AND ".join(filt["filter_clauses"]),ui.flex(*filt["filter_buttons"],wrap="wrap"),title="Filtering controls")

    ui.column(
            ui.panel(filt["filter_text"],filt["clear_button"]," AND ".join(filt["filter_clauses"]),ui.flex(*filt["filter_buttons"],wrap="wrap"),title="Filtering controls"),
            ui.panel(filt["filtered_table"],title="Filtered table")
        )

    # Arrange
    return ui.column(
        ui.panel(filt["filter_text"],filt["clear_button"]," AND ".join(filt["filter_clauses"]),ui.flex(*filt["filter_buttons"],wrap="wrap"),title="Filtering controls"),
        ui.panel(filt["filtered_table"],title="Filtered table")
    )