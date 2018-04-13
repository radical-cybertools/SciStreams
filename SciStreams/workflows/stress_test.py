"""
    Test a dag that spawns subDAGs that spawns more subDAGs

    main_dag will span some test_dag's which spawns test_subdags

    If the queue names are not setup properly, this will deadlock
"""

from lightflow.models import Dag, Parameters, Option
from lightflow.tasks import PythonTask
from lightflow.models.task_data import TaskData, MultiTaskData

from celery.contrib import rdb
import logging
from logging.handlers import SysLogHandler

def get_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler('/home/xf11bm/logs/stress_test.log')
    fh.setLevel(logging.INFO)
    # host is your server that used for the log
    formatter = logging.Formatter('%(name)s: %(levelname)s %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger


parameters = Parameters([
        Option('N', help='Specify number of frames to test', default=10, type=int),
])

def main_func(data, store, signal, context):
    store.set("foo", "bar")
    print("in main func")
    dag_names = list()
    N = store.get("N")
    print(N)
    logger = get_logger()
    logger.info("test")
    for i in range(N):
        print("iteration {} of {}".format(i, N))
        dag_names.append(signal.start_dag(sub_dag, data=data))

    signal.join_dags(dag_names)


from collections import deque
def sub_func(data, store, signal, context):
    N = 10
    dag_names = deque()
    for i in range(N):
        # test a debugger
        print("iteration {} of {}".format(i, N))
        dag_names.append(signal.start_dag(subsub_dag, data=data))

    #rdb.set_trace()
    logger = get_logger()
    logger.info("starting DAG search loop")
    stopped = False
    while not stopped:
        if len(dag_names):
            dag_name = dag_names.popleft()
            if not signal._is_stopped(dag_name=dag_name):
                logger.info("{} not stopped".format(dag_name))
                dag_names.append(dag_name)
            else:
                logger.info("{} is stopped".format(dag_name))

            if len(dag_names) == 0:
                stopped = True
        else:
            time.sleep(.1)

    #signal.join_dags(dag_names)


def subsub_func(data, store, signal, context):
    print("completed")


main_task = PythonTask(name="main_task",
                       callback=main_func, queue='cms-main-task')
main_dag_dict = {
    main_task: None,
    }

main_dag = Dag("main_dag", autostart=True, queue='cms-main')
main_dag.define(main_dag_dict)

sub_task = PythonTask(name="test_task",
                       callback=sub_func, queue='cms-primary-task')

sub_dag_dict = {
    sub_task: None,
    }

sub_dag = Dag("test_dag", autostart=False, queue='cms-primary')
sub_dag.define(sub_dag_dict)


from functools import partial
OneImageTask = partial(PythonTask, queue='cms-oneimage-task')
subsub_task = OneImageTask(name="testsub_task",
                           callback=subsub_func)#, queue='cms-oneimage-task')

subsub_dag_dict = {
    subsub_task: None,
    }

subsub_dag = Dag("testsubdag_dag", autostart=False, queue='cms-oneimage')
subsub_dag.define(subsub_dag_dict)
