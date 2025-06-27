# flow-info

A profiling and visualization tool for Globus Flows that provides comprehensive analysis of flow execution metrics, transfer statistics, and runtime performance.

## Features

- **Flow Execution Analysis**: Profile and analyze Globus Flow runs with detailed metrics
- **Transfer Statistics**: Track data transfer volumes, file counts, and transfer efficiency
- **Runtime Profiling**: Measure and visualize step execution times and compute performance
- **Data Visualization**: Generate histograms, Gantt charts, and time-series plots
- **Caching System**: Efficient local caching of flow data for improved performance
- **CLI Interface**: User-friendly command-line interface with rich output formatting

## Installation

```bash
pip install flow-info
```

Or install from source:

```bash
git clone https://github.com/globus-gladier/flow_info.git
cd flow_info
pip install -e .
```

## Requirements

- Python 3.6+
- globus-sdk
- pandas
- matplotlib
- seaborn
- typer
- rich

## Usage

### Basic Commands

#### Get Flow Summary
```bash
flow-info summary [--name FLOW_NAME]
```
Display a summary of flows including run counts, flow definitions, and cache status.

#### Update Flow Data
```bash
flow-info update [--name FLOW_NAME] [--no-gui]
```
Download and cache the latest flow data, runs, and execution logs.

#### Analyze Transfer Usage
```bash
flow-info transfer-usage [--name FLOW_NAME] [--limit N]
```
Show data transfer statistics including bytes transferred, files moved, and transfer efficiency.

#### Profile Runtime Performance
```bash
flow-info runtimes [--name FLOW_NAME] [--limit N] [--compute-only]
```
Display execution time statistics for flow steps and overall runtime analysis.

#### Generate Visualizations
```bash
# Create runtime histogram
flow-info histogram [--name FLOW_NAME] [--limit N]

# Plot execution over time
flow-info plot-over-time [--name FLOW_NAME]

# Generate Gantt chart (planned feature)
flow-info gantt [--name FLOW_NAME]
```

### Configuration

The tool uses a configuration file `beamlines.cfg` to define flow configurations and connection settings. Flow data is cached locally for efficient repeated analysis.

### Example Workflow

1. **Initialize and update flow data:**
   ```bash
   flow-info update --name xpcs
   ```

2. **Get overview of your flows:**
   ```bash
   flow-info summary --name xpcs
   ```

3. **Analyze transfer performance:**
   ```bash
   flow-info transfer-usage --name xpcs --limit 100
   ```

4. **Profile runtime performance:**
   ```bash
   flow-info runtimes --name xpcs --compute-only
   ```

5. **Generate visualizations:**
   ```bash
   flow-info histogram --name xpcs
   flow-info plot-over-time --name xpcs
   ```

## API Reference

The `flow_info` module provides programmatic access to flow analysis capabilities:

```python
from flow_info import FlowInfo

# Initialize flow analyzer
fi = FlowInfo("xpcs")

# Load flow execution data
list(fi.load(limit=50))

# Get statistical summary
stats = fi.get_flow_stats()

# Extract transfer metrics
transfer_data = fi.extract_bytes_transferred(flow_id, flow_logs)

# Analyze step execution times
step_times = fi.extract_step_times(flow_logs)
```

## Architecture

- **`flow_info.py`**: Core analysis engine for processing flow execution data
- **`flows_cache.py`**: Local caching system for flow definitions and execution logs
- **`cli.py`**: Command-line interface built with Typer and Rich
- **`plots.py`**: Visualization utilities using matplotlib and seaborn

## Contributing

This project is part of the Globus Gladier ecosystem. Contributions are welcome!

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## Links

- **GitHub**: https://github.com/globus-gladier/flow_info
- **Globus Flows**: https://www.globus.org/platform/flows
- **Gladier**: https://gladier.org