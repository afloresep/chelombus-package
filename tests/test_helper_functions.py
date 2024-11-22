import time
import logging
from chelombus.utils.helper_functions import RAMAndTimeTracker 

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RAMAndTimeTrackerTest")

def workload():
    """A simple workload function that allocates memory and sleeps."""
    large_list = []
    for _ in range(100000):
        # Allocate memory
        large_list.extend([0] * (10**5))
        time.sleep(0.1)  # Simulate work by sleeping
    del large_list  # Free memory

if __name__ == "__main__":
    description = "RAM and Time Tracker Performance Test"
    
    # Run the RAM and Time Tracker
    with RAMAndTimeTracker(description=description, interval=0.4, logger=logger, display_progress=True):
        workload()

    print("Test completed.")
