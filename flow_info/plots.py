from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import matplotlib.dates as mdates
import matplotlib.pyplot as plt


def plot_runs_over_time(datetimes, name="mybeamline", frequency="D"):
    """
    Plot runs over time, given a list of datetimes and the name of the file to save
    as. This bins the datetimes into a range per-month, over the course of about a
    year or whatever range takes place for the given set of datetimes.

    frequency: A valid pandas frequency (W-Mon for weekly, ME for monthly). More see here:
    https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
    """
    # Create a DataFrame with a count column
    df = pd.DataFrame({'datetime': datetimes, 'count': 1})
    bins = df.groupby(pd.Grouper(key='datetime', freq=frequency)).sum()
    bins = list(bins.to_dict()["count"].keys())
    bins = [d.date() for d in pd.to_datetime(bins)]

    # Plot histogram with bi-monthly bins
    plt.figure(figsize=(12, 6))
    plt.hist(datetimes, bins=bins, color='cornflowerblue', edgecolor='black')

    # Format x-axis to show bi-monthly labels
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    # plt.xlabel('Bi-Monthly Interval Start')
    plt.ylabel('Runs')
    plt.title(f'Runs over time for {name}')
    plt.xticks(rotation=45)
    plt.tight_layout()

    filename = f"{name}-runs-over-time.png"
    plt.savefig(filename, bbox_inches="tight", pad_inches="layout")
    return filename


def plot_step_times(flow_logs, name: str = "mybeamline"):
    """Create a histogram of the step runtimes

    Args:
        include (list, optional): The list of steps to plot, e.g. ['flow', '1', '2']. Defaults to None.
    """
    # create the graph
    steps = [s for s in flow_logs.columns if s.endswith("_step_time")]
    new_columns = {s:s.replace("_step_time", "") for s in steps}
    flow_logs.rename(columns=new_columns, inplace=True)
    columns = list(new_columns.values())
    labels = {'x': f'Flow step runtimes for {name}', 'y': 'Time (s)'}
    fig = px.bar(x=columns, y=[flow_logs[c].mean() for c in columns], labels=labels)

    # Save the figure.
    filename = f"{name}_step_times.png"
    fig.write_image(filename)
    return filename


def plot_gantt(
    flow_logs, flow_order, name="mybeamline"):
    """Plot a Gantt Chart of flow runs.

    Args:
        limit (str, optional): The number of most recent flows to plot
        show_relative_time (bool, optional): show relative time on x axis. Default: True
    """
    flow_steps = []
    start = 0
    for step in flow_order:
        if "Transfer" in step:
            resource = "Transfer"
        else:
            resource = "Compute"

        step_time = start + flow_logs[f"{step}_step_time"].mean()
        flow_steps.append(
            dict(
                Task=step,
                Start=datetime.fromtimestamp(int(start)).isoformat(),
                Finish=datetime.fromtimestamp(int(step_time)).isoformat(),
                Resource=resource
            ),
        )
        start = step_time
    df = pd.DataFrame(flow_steps)
    timeline_order = flow_order.copy()
    timeline_order.reverse()
    fig = px.timeline(df, x_start="Start", x_end="Finish", y="Task", color="Resource", category_orders={"Task": timeline_order})

    fig.update_layout(xaxis=dict(
                    title='Average Time in Seconds', 
                    tickformat = '%S'))
    fig.update_yaxes(autorange="reversed")
    filename = f"{name}_gantt.png"
    fig.write_image(filename)
    return filename
