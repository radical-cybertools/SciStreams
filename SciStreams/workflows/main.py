"""
    This is the main dag. It reads data, then
    executes another DAG to run on.

    Efficiency Question : should we spawn new DAG's for each subtask or create one big DAG
    then spawn?
"""

from lightflow.models import Dag
from lightflow.tasks import PythonTask

# TODO : make callback something else callback
# 
from databroker import Broker
import matplotlib.pyplot as plt
import numpy as np
import time

from SciStreams.workflows.img import img_dag

#from SciStreams.workflows.circavg import ciravg_dag

#dbname = 'cms'

def get(d, key, default):
    if key in d:
        return d['key']
    else:
        return default


def main_func(data, store, signal, context):
    # this grabs from the args
    start_time = get(data, 'start_time', time.time()-3600*24)
    stop_time = get(data, 'stop_time', time.time())
    dbname = get(data, 'dbname', 'cms')

    # get the databroker instance
    # TODO: this should be store eventually
    # we might want to think about how to wrap to this
    # or write our own
    db = Broker.named(dbname)
    hdrs = db(start_time=start_time, stop_time=stop_time)
    # first search by uid and send them
    # TODO : some filtering of data
    dag_names = list()
    cnt = 0
    for hdr in hdrs:
        cnt += 1
        uid = hdr.start['uid']
        data['uid'] = uid
        # grab the metadata already since it's cheapr
        data['md'] = dict(hdr.start)
        data['dbname'] = dbname
        dag_name = signal.start_dag(img_dag, data=data)
        print("dag name: {}".format(dag_name))
        dag_names.append(dag_name)
    print("Main job submission finished, found {} images".format(cnt))

    #signal.join_dags(dag_names)



# create the main DAG that spawns others
main_dag = Dag('main_dag')
main_task = PythonTask(name="main",
                       callback=main_func)
main_dag.define({
    main_task: None,
    })
