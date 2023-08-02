import logging
import datetime
from .sub_steps.s_entrypoint_transactions import run as s_entrypoint_transactions
from .sub_steps.s_entrypoint_time_series import run as s_entrypoint_time_series
from .sub_steps.s_entrypoint_smart_contracts import run as s_entrypoint_smart_contracts
from .sub_steps.s_entrypoint_summary import run as s_entrypoint_summary

expected_system_params = {
    "mongodb": True,
    "s3_client": True,
    "s3_resource": True
}

sub_steps = [
    s_entrypoint_transactions,
    s_entrypoint_time_series,
    s_entrypoint_smart_contracts,
    s_entrypoint_summary
]

# sub_steps = [s_entrypoint_smart_contracts]

def runTask(task_params={}, system_params={}):
    entrypoint = task_params.get("entrypoint")
    if entrypoint is None:
        raise Exception("Missing parameter entrypoint")
    
    today_date = task_params.get("today_date")
    logging.info(f"Running statistics task for entrypoint: {entrypoint}, on date {today_date}")

    for step in sub_steps:
        step_result = step(
            task_params=task_params,
            system_params=system_params
            )

    logging.info(f"Finished task tezos_entrypoint_statistics for entrypoint :: {entrypoint} ::")
    return True