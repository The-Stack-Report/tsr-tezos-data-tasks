from tasks import tasks, tasksByKey
import os
import datetime
from test_utils.verbose_timedelta import verbose_timedelta

# Run file to test the modules

def runTask(task):
    test_params = getattr(task, "TEST_PARAMS", None)
    print(" ")
    print('=' * 20, " Task: ",task.__name__, '=' * 20)
    print("Test params: ")
    print(test_params)
    print(" ")
    start_time = datetime.datetime.now()
    task.runTask(test_params)
    end_time = datetime.datetime.now()
    end_time = datetime.datetime.now()
    step_run_time = end_time - start_time
    run_time_verbose = verbose_timedelta(step_run_time)

    print(f"Completed task: {task.__name__} in {run_time_verbose} ({str(step_run_time)})")
    print(" ")
    print('=' * 20, " Task end ", '=' * 20)
    print(" ")

def runTasks():
    for task in tasks:
        if hasattr(task, 'runTask'):
            runTask(task)
        else:
            print('>>> Task ',task.__name__,'does not have runTask function')
        print("Finished task: " + task.__name__)

def findMissingImports():
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


if __name__ == '__main__':
    print("Running tests")
    # runTasks()
    findMissingImports()

    # Quickly test specific tasks
    # runTask(tasksByKey['tezos_accounts_index'])
    runTask(tasksByKey['tezos_contract_statistics'])
    runTask(tasksByKey['tezos_contract_statistics'])

    
    

    


