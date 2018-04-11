from lightflow.models import Dag, Parameters, Option
from lightflow.tasks import PythonTask
from lightflow.models.task_data import TaskData, MultiTaskData

def test_func(data, store, signal, context):
    import logging
    logging.basicConfig(filename='/home/xf11bm/SciStreams/SciStreams/test.log', level=logging.DEBUG)
    logging.debug('Testing a log write again')
    #logging.info('So should this')
    #logging.warning('And this, too')

test_task = PythonTask(name="test func", callback=test_func, queue='test')

test_dag_dict = {
    test_task : None,
    }

test_dag = Dag("test", autostart=True)
test_dag.define(test_dag_dict)
