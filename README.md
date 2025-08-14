# deephaven-experiments
My own experiments with DeepHaven data labs

## GUI quickstart

Run this in a deephaven IDE

    from deephaven import ui
    import gui.quickstart

    # Static example with 1000 rows
    dstatic = gui.quickstart.make_static_example(1000)
    dash = dstatic.render()

    # Dynamic example (not all charts update correctly)
    dyn = gui.quickstart.make_dynamic example("PT1s")
    dash = dyn.render()
