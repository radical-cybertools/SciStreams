"""
    test lib import
"""

from lightflow.models import Dag, Parameters, Option
from lightflow.tasks import PythonTask
from lightflow.models.task_data import TaskData, MultiTaskData

# TODO : make callback something else callback
# 
from databroker import Broker
import matplotlib.pyplot as plt
import numpy as np

from SciStreams.config import config
config['foo'] = 'bar'

parameters = Parameters([
        Option('N', help='Specify a start time', type=str),
])

def main_func(data, store, signal, context):
    store.set("foo", "bar")
    print("in main func")
    dag_names = list()
    N = int(store.get("N"))
    print(N)
    for i in range(N):
        print("iteration {} of {}".format(i, N))
        dag_names.append(signal.start_dag(test_dag, data=data))

    signal.join_dags(dag_names)

def test_func(data, store, signal, context):
    print("completed\n")


# create the main DAG that spawns others
#img_dag = Dag('img_dag')
test_task = PythonTask(name="test",
                       callback=test_func)
main_task = PythonTask(name="main",
                       callback=main_func)
main_dag_dict = {
    main_task: None,
    }

main_dag = Dag("main", autostart=True)
main_dag.define(main_dag_dict)

test_dag_dict = {
    test_task: None,
    }

test_dag = Dag("test", autostart=False)
test_dag.define(test_dag_dict)
