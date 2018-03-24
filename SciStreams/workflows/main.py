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


# the primary dag that reads and processes the data
# ALL dags used need to be imported at top level
# TODO : sort this out
from SciStreams.workflows.primary import primary_dag
from SciStreams.workflows.one_image import one_image_dag

#from SciStreams.workflows.circavg import ciravg_dag

#dbname = 'cms'

def get(d, key, default):
    if key in d:
        return d['key']
    else:
        return default


def main_func(data, store, signal, context):
    '''
        Here we're querying databroker and submitting events into the pipeline.
        These events should then be parsed by something else in the pipeline.

        The data returned is a dictionary with the fields:
            run_start : the uid of the run start
            md : the metadata
            data : the data dictionary. This data is not necessarily filled here
                    (for optimization).
            descriptor : the descriptor for this data
    '''
    # this grabs from the args
    # send data in event by event from headers
    start_time = get(data, 'start_time', time.time()-3600*24)
    stop_time = get(data, 'stop_time', time.time())
    dbname = get(data, 'dbname', 'cms')

    # get the databroker instance
    # TODO: this should be store eventually
    # we might want to think about how to wrap to this
    # or write our own
    db = Broker.named(dbname)
    print(start_time)
    print(stop_time)
    hdrs = db(start_time=start_time, stop_time=stop_time)
    # for now test with this
    #hdrs = [db["00ca7bd0-3589-4a39-bced-e78febceba85"]]
    # first search by uid and send them
    # TODO : some filtering of data
    dag_names = list()
    cnt = 0
    for hdr in hdrs:
        cnt += 1
        uid = hdr.start['uid']
        # make the descriptor dictionary
        descriptor_dict = make_descriptor_dict(hdr.descriptors)
        stream_names = hdr.stream_names
        for stream_name in stream_names:
            events = hdr.events(stream_name)
            for event in events:
                data['run_start'] = uid
                # grab the metadata already since it's cheapr
                data['md'] = dict(hdr.start)
                data['md']['seq_num'] = event['seq_num']
                data['dbname'] = dbname
                data['event'] = event['data']
                data['descriptor'] = descriptor_dict[event['descriptor']]
                dag_name = signal.start_dag(primary_dag, data=data)
                print("dag name: {}".format(dag_name))
                dag_names.append(dag_name)
        if cnt == 8:
            break
    print("Main job submission finished, found {} images".format(cnt))

    signal.join_dags(dag_names)


def make_descriptor_dict(descriptors):
    desc_dict = dict()
    for descriptor in descriptors:
        desc_dict[descriptor['uid']] = descriptor
    return desc_dict


# create the main DAG that spawns others
main_dag = Dag('main_dag')
main_task = PythonTask(name="main",
                       callback=main_func)
main_dag.define({
    main_task: None,
    })
