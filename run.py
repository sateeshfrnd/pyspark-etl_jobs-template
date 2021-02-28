"""

"""
import json
import argparse
from importlib import import_module
from etl_application.common.spark_utils import get_spark_session


def _parse_arguments():
    """
    Parse arguments provided by spark-submit command
    Returns:
        args : argparse.Namespace
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", required=True, help='Name of the job')
    parser.add_argument("--config_file", default='config.json')
    args = parser.parse_args()
    return args

def _get_config(config_file_name: str) -> dict:
    """
    Get the configurations from config file
    Args:
        config_file_name:

    Returns:

    """
    with open(config_file_name, "r") as config_file :
        config = json.load(config_file)

    return config



def run():
    """
    Main function that will be executed by spark-submit command.
    Returns:

    """
    args = _parse_arguments()
    config = _get_config(args.config_file)

    job_module = import_module(f"etl_application.etl_jobs.{args.job}.run_job")
    spark = get_spark_session(config.get(args.job))
    job_module.run(spark, config)



if __name__ == "__main__":
    run()