import datetime
from test_utils.verbose_timedelta import verbose_timedelta
import os

def runTask(task,
            task_params=False,
            system_params=False
            ):
    
    task_execution_params = False
    if task_params:
        task_execution_params = task_params
    else:
        task_execution_params = getattr(task, "TEST_PARAMS", None)
    print(" ")
    print('=' * 20, " Task: ",task.__name__, '=' * 20)
    print("Task execution params: ")
    print(task_execution_params)
    print(" ")
    start_time = datetime.datetime.now()
    task_result = False
    task_error = ''
    try:
        if hasattr(task, "expected_system_params"):
            for param in task.expected_system_params:
                if param not in system_params:
                    raise Exception(f"Task {task.__name__} requires secondary param {param} but it was not provided")
            task_result = task.runTask(
                task_params=task_execution_params,
                system_params=system_params)
        else:
            print(f"Task {task.__name__} does not have expected system params defined")
            task_result = task.runTask(task_params=task_execution_params)
    except Exception as e:
        print(f"Task {task.__name__} failed with exception: {e}")
        task_error = str(e)
        task_result = False
        
    end_time = datetime.datetime.now()
    end_time = datetime.datetime.now()
    step_run_time = end_time - start_time
    run_time_verbose = verbose_timedelta(step_run_time)

    print(f"Completed task: {task.__name__} in {run_time_verbose} ({str(step_run_time)})")
    print(" ")
    print('=' * 20, " Task end ", '=' * 20)
    print(" ")
    return {
        "task": task.__name__,
        "result": task_result,
        "error": task_error,
        "run_time": str(step_run_time),
        "run_time_verbose": run_time_verbose
    }


def findMissingImports(tasks):
    task_names = [task.__name__.split('.')[-1] for task in tasks]

    tasks_dir = os.listdir(os.path.join(os.getcwd(), 'tasks'))
    filter_dirs = [
        "__pycache__",
        "__init__.py"
    ]
    tasks_dir = [task for task in tasks_dir if task not in filter_dirs]

    tasks_imported = [t for t in tasks_dir if t in task_names]
    print("Tasks imported:")
    print(tasks_imported)

    tasks_not_imported = [t for t in tasks_dir if t not in task_names]
    print("- " * 20)
    if len(tasks_not_imported) == 0:
        print("✅ All tasks imported")
    else:
        print("Tasks not imported:")
        for t in tasks_not_imported:
            print("❌ Missing import: ", t)

