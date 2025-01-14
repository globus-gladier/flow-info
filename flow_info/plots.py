import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import plotly.express as px


def plot_histogram(flow_logs, include=None):
    """Create a histogram of the step runtimes

    Args:
        include (list, optional): The list of steps to plot, e.g. ['flow', '1', '2']. Defaults to None.
    """
    cols = []
    labels = []
    for c in flow_logs.keys():
        if "_step_time" in c:
            if not include or c.replace("_step_time", "") in include:
                cols.append(c)
                if c == "total_step_time":
                    labels.append("Total runtime")
                else:
                    labels.append(c.replace("_step_time", ""))

    df = flow_logs[cols]

    f = plt.figure()
    df.plot.hist(bins=20, alpha=0.5, ax=f.gca())
    plt.legend(labels, loc="center left", bbox_to_anchor=(1.0, 0.5), fontsize=18)
    plt.ylabel("Frequency", fontsize=18, color="black")
    plt.xlabel("Time (s)", fontsize=18, color="black")
    plt.tick_params(axis="both", which="major", pad=4, labelsize=18, labelcolor="black")
    plt.savefig("histogram.png", bbox_inches="tight", pad_inches="layout")


def plot_gantt(
    flow_logs, flow_order, limit=None, show_relative_time=True, include=None
):
    """Plot a Gantt Chart of flow runs.

    Args:
        limit (str, optional): The number of most recent flows to plot
        show_relative_time (bool, optional): show relative time on x axis. Default: True
    """
    sns.set(style="white", palette="Set2", color_codes=False)
    sns.set_style("ticks")
    colors = sns.color_palette("Set2")

    tasks = flow_logs.copy()

    # Shrink the task list to the last n rows
    if limit:
        tasks = tasks.tail(limit)
        tasks = tasks.reset_index(drop=True)

    # Convert to relative time
    if show_relative_time:
        pd.options.display.float_format = "{:.5f}".format
        convert_columns = list(tasks.columns)[1:]  # Not inlcude action id
        # start = tasks['start'].min()
        # for c in convert_columns:
        #     print(c)
        #     if "_runtime" not in c:
        #         tasks[c] = tasks[c] - start

    # Plot from dataframe
    fig, gnt = plt.subplots(figsize=(12, 9))
    gnt.set_ylim(0, (len(tasks) + 1) * 10)
    gnt.grid(True)
    gnt.set_yticks([(i + 1) * 10 + 3 for i in range(len(tasks))])
    gnt.set_yticklabels(range(len(tasks)))

    for i, task in tasks.iterrows():
        # flow_start = task['start']
        for j, step in enumerate(flow_order):
            step_start, step_end = (
                task[f"{step}_start"],
                task[f"{step}_end"] - task[f"{step}_start"],
            )
            # if j == 0:
            # gnt.broken_barh([(flow_start, step_start-flow_start)], ((i+1)*10, 6), facecolor=colors[0], edgecolor='black')
            gnt.broken_barh(
                [(step_start, step_end)],
                ((i + 1) * 10, 6),
                facecolor=colors[j],
                edgecolor="black",
            )
        flow_end = task["end"] - task[f"{step}_end"]
    gnt.legend(
        flow_order,
        #'Flow finishing'],
        fontsize=25,
        loc="upper left",
    )
    gnt.set_ylabel("Flow", fontsize=25, color="black")
    gnt.set_xlabel("Time (s)", fontsize=25, color="black")
    gnt.tick_params(axis="both", which="major", pad=5, labelsize=25, labelcolor="black")
    plt.savefig("gantt.png", bbox_inches="tight", pad_inches="layout")


def plot_over_time(df: pd.DataFrame):
    fig = px.line(
        df,
        x="start_hour",
        y="runs_per_hour",
        title="Time Series with Range Slider and Selectors",
    )

    fig.update_xaxes(
        rangeslider_visible=True,
        rangeselector=dict(
            buttons=list(
                [
                    dict(count=1, label="1m", step="month", stepmode="backward"),
                    dict(count=6, label="6m", step="month", stepmode="backward"),
                    dict(count=1, label="YTD", step="year", stepmode="todate"),
                    dict(count=1, label="1y", step="year", stepmode="backward"),
                    dict(step="all"),
                ]
            )
        ),
    )
    fig.show()
