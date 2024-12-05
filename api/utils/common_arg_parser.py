import argparse
from api.utils.config import LOGGING_LEVEL, N_JOBS

def common_arg_parser(description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--config", type=str, help="Path to the user configuration file.")
    parser.add_argument('--log-level', type=str, default=LOGGING_LEVEL,
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help="Logging verbosity level.")
    parser.add_argument('--n-jobs', type=int, default=N_JOBS, help="Number of CPU cores to use.")
    parser.add_argument('--resume', type=int, default=0, help="Resume from a specific chunk.")
    parser.add_argument('--log', type=bool, default=False, help="Saving logs to output.log file. Default False")
    
    return parser