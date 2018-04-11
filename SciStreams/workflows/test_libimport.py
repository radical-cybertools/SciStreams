"""
    test lib import
"""

from lightflow.models import Dag
from lightflow.tasks import PythonTask
from lightflow.models.task_data import TaskData, MultiTaskData

# TODO : make callback something else callback
# 
from databroker import Broker
import matplotlib.pyplot as plt
import numpy as np

from SciStreams.config import config
config['foo'] = 'bar'

def test_func(data, store, signal, context):
    print("printing config\n\n")
    print(config['foo'])
    print("done\n\n")
    config['foo'] = 'far'



# create the main DAG that spawns others
#img_dag = Dag('img_dag')
test_task = PythonTask(name="main",
                       callback=test_func)
test_task2 = PythonTask(name="main2",
                       callback=test_func)
test_dag_dict = {
    test_task: test_task2,
    }

test_dag = Dag("test", autostart=True)
test_dag.define(test_dag_dict)
