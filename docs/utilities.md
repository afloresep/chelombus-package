# Monitoring Utilities Documentation

This documentation provides detailed instructions on how to use the monitoring utilities included in your package, specifically the `TimeTracker`, `RAMTracker`, and `RAMAndTimeTracker` classes. These utilities allow you to monitor the execution time and memory usage of specific parts of your code. Additionally, we provide guidance on how to configure logging using the `setup_logging` function.

## Table of Contents

- [Overview](#overview)
- [Logging Configuration](#logging-configuration)
  - [Setting Up Logging](#setting-up-logging)
- [Monitoring Classes](#monitoring-classes)
  - [TimeTracker](#timetracker)
    - [Usage Example](#timetracker-usage-example)
  - [RAMTracker](#ramtracker)
    - [Usage Example](#ramtracker-usage-example)
  - [RAMAndTimeTracker](#ramandtimetracker)
    - [Usage Example](#ramandtimetracker-usage-example)
- [Advanced Usage](#advanced-usage)
  - [Custom Loggers](#custom-loggers)
  - [Adjusting Monitoring Intervals](#adjusting-monitoring-intervals)
- [Best Practices](#best-practices)
- [FAQs](#faqs)
- [Conclusion](#conclusion)

---

## Overview

The monitoring utilities are designed to help you:

- **Track Execution Time**: Measure how long specific parts of your code take to execute.
- **Monitor Memory Usage**: Keep track of current and peak RAM usage during execution.
- **Log Performance Metrics**: Record performance data using Python's `logging` module.

These tools are especially useful for long-running processes that may take minutes, hours, or even days to complete.

---

## Logging Configuration

The monitoring classes use Python's built-in `logging` module to record performance metrics. To get the most out of these utilities, it's important to configure logging according to your needs.

### Setting Up Logging

We provide a `setup_logging` function in the `utils/logging.py` module to help you configure logging easily.

```python
# utils/logging.py
import logging

def setup_logging(log_level=logging.INFO, log_file=None):
    """
    Configures logging to output to console and optionally to a file.

    Args:
        log_level (int): Logging level (e.g., logging.INFO, logging.DEBUG).
        log_file (str, optional): Path to a log file. If None, logs will not be written to a file.
    """
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    handlers = [logging.StreamHandler()]  # Always log to console

    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(level=log_level, format=log_format, handlers=handlers)
```

**Usage Example:**

```python
from utils.logging import setup_logging

# Set up logging to output to console and to a file
setup_logging(log_level=logging.INFO, log_file='performance.log')
```

**Notes:**

- **Log Level**: Adjust the `log_level` parameter to control the verbosity (e.g., `logging.DEBUG`, `logging.INFO`, `logging.WARNING`).
- **Log File**: If you specify a `log_file`, logs will be written to that file in addition to the console.

---

## Monitoring Classes

The monitoring utilities are implemented as context managers, allowing you to use them with Python's `with` statement for clean and efficient resource management.

### TimeTracker

The `TimeTracker` class measures the total execution time of a code block.

**Implementation:**

```python
# utils/helper_functions.py
import time
import logging

class TimeTracker:
    def __init__(self, description="", logger=None):
        self.description = description
        self.logger = logger or logging.getLogger(__name__)

    def __enter__(self):
        self.start_time = time.time()
        self.logger.info(f"{self.description} started.")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.end_time = time.time()
        self.elapsed_time = self.end_time - self.start_time
        self.logger.info(f"{self.description} completed.")
        self.logger.info(f"Total time elapsed: {self.format_time(self.elapsed_time)}")

    @staticmethod
    def format_time(seconds):
        hours, rem = divmod(seconds, 3600)
        minutes, seconds = divmod(rem, 60)
        return f"{int(hours)} hours, {int(minutes)} minutes, {seconds:.2f} seconds"
```

#### Usage Example

```python
from utils.helper_functions import TimeTracker

with TimeTracker(description="Processing data"):
    # Place the code you want to time here
    process_data()
```

**Output:**

```
2023-10-15 12:00:00 - __main__ - INFO - Processing data started.
2023-10-15 12:05:00 - __main__ - INFO - Processing data completed.
2023-10-15 12:05:00 - __main__ - INFO - Total time elapsed: 0 hours, 5 minutes, 0.00 seconds
```

### RAMTracker

The `RAMTracker` class monitors RAM usage during the execution of a code block.

**Implementation:**

```python
import time
import psutil
import threading
import logging

class RAMTracker:
    def __init__(self, description="", interval=1.0, logger=None):
        self.description = description
        self.interval = interval
        self.process = psutil.Process()
        self._stop_event = threading.Event()
        self.max_ram = 0
        self.logger = logger or logging.getLogger(__name__)

    def _monitor_ram(self):
        while not self._stop_event.is_set():
            ram_usage = self.process.memory_info().rss / (1024 ** 2)  # Convert to MB
            if ram_usage > self.max_ram:
                self.max_ram = ram_usage
            time.sleep(self.interval)

    def __enter__(self):
        self.logger.info(f"{self.description} RAM monitoring started.")
        self._thread = threading.Thread(target=self._monitor_ram)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._stop_event.set()
        self._thread.join()
        self.logger.info(f"{self.description} RAM monitoring completed.")
        self.logger.info(f"Peak RAM usage: {self.max_ram:.2f} MB")
```

#### Usage Example

```python
from utils.helper_functions import RAMTracker

with RAMTracker(description="Processing data", interval=2.0):
    # Place the code you want to monitor here
    process_data()
```

**Notes:**

- **Interval**: Adjust the `interval` parameter to control how frequently RAM usage is sampled (in seconds).

### RAMAndTimeTracker

The `RAMAndTimeTracker` class combines both time tracking and RAM monitoring. It also displays real-time progress in the console.

**Implementation:**

```python
import time
import psutil
import threading
import logging

class RAMAndTimeTracker:
    def __init__(self, description="", interval=1.0, logger=None, display_progress=True):
        self.description = description
        self.interval = interval
        self.process = psutil.Process()
        self._stop_event = threading.Event()
        self.max_ram = 0
        self.logger = logger or logging.getLogger(__name__)
        self.display_progress = display_progress

    def _monitor(self):
        while not self._stop_event.is_set():
            current_time = time.time()
            elapsed_time = current_time - self.start_time
            ram_usage = self.process.memory_info().rss / (1024 ** 2)  # Convert to MB
            if ram_usage > self.max_ram:
                self.max_ram = ram_usage
            if self.display_progress:
                print(
                    f"\r{self.description}: Elapsed time: {elapsed_time:.2f}s, "
                    f"Current RAM: {ram_usage:.2f} MB, Peak RAM: {self.max_ram:.2f} MB",
                    end=""
                )
            time.sleep(self.interval)
        if self.display_progress:
            print()  # Move to the next line after progress output

    def __enter__(self):
        self.start_time = time.time()
        self.logger.info(f"{self.description} started.")
        self._thread = threading.Thread(target=self._monitor)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._stop_event.set()
        self._thread.join()
        end_time = time.time()
        elapsed_time = end_time - self.start_time
        self.logger.info(f"{self.description} completed.")
        self.logger.info(f"Total time elapsed: {self.format_time(elapsed_time)}")
        self.logger.info(f"Peak RAM usage: {self.max_ram:.2f} MB")

    @staticmethod
    def format_time(seconds):
        hours, rem = divmod(seconds, 3600)
        minutes, seconds = divmod(rem, 60)
        return f"{int(hours)} hours, {int(minutes)} minutes, {seconds:.2f} seconds"
```

#### Usage Example

```python
from utils.helper_functions import RAMAndTimeTracker

with RAMAndTimeTracker(description="Processing data", interval=1.0):
    # Place the code you want to monitor here
    process_data()
```

**Console Output:**

```
Processing data: Elapsed time: 5.24s, Current RAM: 125.30 MB, Peak RAM: 140.50 MB
```

**Log Output:**

```
2023-10-15 12:00:00 - __main__ - INFO - Processing data started.
2023-10-15 12:05:00 - __main__ - INFO - Processing data completed.
2023-10-15 12:05:00 - __main__ - INFO - Total time elapsed: 0 hours, 5 minutes, 0.00 seconds
2023-10-15 12:05:00 - __main__ - INFO - Peak RAM usage: 140.50 MB
```

---

## Advanced Usage

### Custom Loggers

If you have a custom logging setup or want to direct logs to a specific logger, you can pass a `logger` instance to the monitoring classes.

**Example:**

```python
import logging
from utils.helper_functions import RAMAndTimeTracker

# Create a custom logger
custom_logger = logging.getLogger('my_custom_logger')
custom_logger.setLevel(logging.DEBUG)
handler = logging.FileHandler('custom_log.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
handler.setFormatter(formatter)
custom_logger.addHandler(handler)

# Use the monitoring class with the custom logger
with RAMAndTimeTracker(description="Processing data", logger=custom_logger):
    process_data()
```

### Adjusting Monitoring Intervals

You can adjust the `interval` parameter in the monitoring classes to control how frequently the RAM usage and progress are updated.

**Example:**

```python
from utils.helper_functions import RAMAndTimeTracker

# Update every 5 seconds instead of every 1 second
with RAMAndTimeTracker(description="Processing data", interval=5.0):
    process_data()
```

---
## FAQs

**1. _Do I need to install any additional packages?_**

Yes, you need to install the `psutil` package to use the RAM monitoring features.

```bash
pip install psutil
```

**2. _Can I disable the console output?_**

Yes, you can disable the real-time console output by setting `display_progress=False` in the `RAMAndTimeTracker` class.

**Example:**

```python
with RAMAndTimeTracker(description="Processing data", display_progress=False):
    process_data()
```

**3. _How can I change the logging format?_**

You can customize the logging format by modifying the `setup_logging` function or by configuring logging directly in your application code.

**Example:**

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s:%(message)s',
    handlers=[logging.StreamHandler()]
)
```

**4. _Will monitoring impact the performance of my code?_**

Monitoring introduces minimal overhead, especially when the `interval` is set appropriately for long-running tasks. However, for extremely performance-sensitive applications, you may want to increase the `interval` or disable monitoring.

---

## Conclusion

The monitoring utilities provided in this package are powerful tools for tracking the performance of your code. By following the guidelines and examples in this documentation, you can effectively monitor execution time and memory usage, helping you optimize and debug your applications.