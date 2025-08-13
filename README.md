# deephaven-experiments
My own experiments with DeepHaven data labs

## GUI quickstart

Run this in a deephaven IDE

    from deephaven import ui
    import gui.quickstart

    dm = gui.quickstart.make_static_example()
    dash = ui.dashboard(dm.arrange())
