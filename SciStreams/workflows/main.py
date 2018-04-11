"""
    This DAG is the main DAG. It will:
        - pick out images from the stream from detectors.
        - ensure that the attributes coming in have all needed keys
            If they don't, it won'process them

        - This spawns the one_image_dag DAG for events
            with the images expected and attributes
"""
import matplotlib
matplotlib.use("Agg")

import time

from lightflow.models import Dag, Parameters, Option
from lightflow.tasks import PythonTask
from lightflow.models.task_data import TaskData, MultiTaskData

# TODO : make callback something else callback
from databroker import Broker
import matplotlib.pyplot as plt
import numpy as np


from SciStreams.workflows.one_image import one_image_dag


print("In main workflow")
parameters = Parameters([
        Option('start_time', help='Specify a start time', type=str),
        Option('stop_time', help='specify a stop time', type=str),
        Option('dbname', help='specify a stop time', type=str),
        Option('max_images', help='specify max num of images', default=0, type=str),
        #Option('iterations', default=1, help='The number of iterations', type=int),
        #Option('threshold', default=0.4, help='The threshold value', type=float)
])

# filter a streamdoc with certain attributes (set in the yml file)
# required_attributes, typesdict globals needed
def filter_attributes(attr, type='main'):
    '''
        Filter attributes.

        Note that this ultimately checks that attributes match what is seen in
        yml file.
    '''
    from SciStreams.config import required_attributes, typesdict
    #print("filterting attributes")
    # get the sub required attributes
    reqattr = required_attributes['main']
    for key, val in reqattr.items():
        if key not in attr:
            print("bad attributes")
            print("{} not in attributes".format(key))
            return False
        elif not isinstance(attr[key], typesdict[val]):
            print("bad attributes")
            print("key {} not an instance of {}".format(key, val))
            return False
    #print("good attributes")
    return True


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
    print("In main function")
    #start_time = get(store, 'start_time', time.time()-3600*24)
    #stop_time = get(store, 'stop_time', time.time())
    start_time = store.get('start_time')
    stop_time = store.get('stop_time')
    max_images = int(store.get('max_images'))
    #start_time = "2017-11-06"
    #stop_time = "2017-11-07"
    dbname = store.get('dbname')

    # get the databroker instance
    # TODO: this should be store eventually
    # we might want to think about how to wrap to this
    # or write our own
    db = Broker.named(dbname)
    #print(time.ctime(start_time))
    #print(time.ctime(stop_time))
    print("searching for hdrs")
    hdrs = db(start_time=start_time, stop_time=stop_time)
    # for now test with this
    #hdrs = [db["00ca7bd0-3589-4a39-bced-e78febceba85"]]
    # first search by uid and send them
    # TODO : some filtering of data
    dag_names = list()
    cnt = 0
    # maximum number of jobs to submit
    # this was a temp tweak
    #MAXNUM = 10
    for hdr in hdrs:
        print("reading a header")
        cnt += 1
        uid = hdr.start['uid']
        # make the descriptor dictionary
        descriptor_dict = make_descriptor_dict(hdr.descriptors)
        stream_names = hdr.stream_names
        for stream_name in stream_names:
            print("stream name : {}".format(stream_name))
            events = hdr.events(stream_name)
            eventnum = 0
            for event in events:
                print('on event # {}'.format(eventnum))
                eventnum +=1
                data['run_start'] = uid
                # grab the metadata already since it's cheapr
                #data['md'] = dict(hdr.start)
                #data['md']['seq_num'] = event['seq_num']
                #data['dbname'] = dbname
                #data['event'] = event['data']
                #data['descriptor'] = descriptor_dict[event['descriptor']]

                descriptor = descriptor_dict[event['descriptor']]
                md = dict(hdr.start)
                md['seqnum'] = event['seq_num']
                run_start = uid
                # now just get data from event
                event_data = event['data']
                #dag_name = signal.start_dag(main_dag, data=data)
                #print("dag name: {}".format(dag_name))
                #dag_names.append(dag_name)
                # pasting the main stream here
                #run_start = data['run_start']
                # md is also 'start'
                #md = data['md']
                #dbname = data['dbname']
                #descriptor = data['descriptor']
                #event = data['event']

                #db = Broker.named(dbname)
                #hdr = db[run_start]

                # pull the strema name
                #stream_name = descriptor['name']

                fields = hdr.fields(stream_name)

                print("looking for an image with keys {}".format(fields))
                # TODO: when we can name DAG queues, make this another DAG with
                # specialized queue
                # eventually iterate and spawn new tasks
                detector_keys = ['pilatus2M_image']
                for detector_key in detector_keys:
                    if detector_key in fields:
                        print("Found , saving an image")
                        try:
                            imgs = hdr.data(detector_key, fill=True)
                            # differentiate image sequences
                            imgseq = 0
                            first = True
                            print("Taking average of image")
                            for img in imgs:
                                if first:
                                    imgtot = img*0
                                    first = False
                                imgtot += img
                                imgseq += 1
                            imgtot = imgtot/float(imgseq)

                            # give it some provenance and data
                            new_data = dict(img=img)
                            new_data['run_start'] = run_start
                            new_data['md'] = md.copy()
                            new_data['md']['detector_key'] = detector_key
                            new_data['md']['image_sequence'] = imgseq
                            new_data['descriptor'] = descriptor
                            good_attr = filter_attributes(new_data['md'])

                            new_data = TaskData(data=new_data)
                            new_data = MultiTaskData(dataset=new_data)

                            data['img'] = imgtot
                            data['run_start'] = run_start
                            data['md'] = md.copy()
                            data['md']['detector_key'] = detector_key
                            data['md']['image_sequence'] = imgseq
                            data['descriptor'] = descriptor

                            if good_attr:
                                print("got a good image")
                                dag_name = signal.start_dag(one_image_dag, data=new_data)
                                print("main node, dag name: {}".format(dag_name))
                                dag_names.append(dag_name)
                            else:
                                print("Did not find a good image")
                                print(new_data['md'])
                            #break # break on first image
                        except FileNotFoundError as exc:
                            print("Error, could not find file, ignoring...")
                            print(exc)
                #break #break after first event
        if max_images > 0 and cnt >= max_images:
            print("Reached maximum images, breaking...")
            break
    signal.join_dags(dag_names)
    print("Main job submission finished, found {} images".format(cnt))

    #signal.join_dags(dag_names)





def get(d, key, default):
    if key in d:
        return d['key']
    else:
        return default




def make_descriptor_dict(descriptors):
    desc_dict = dict()
    for descriptor in descriptors:
        desc_dict[descriptor['uid']] = descriptor
    return desc_dict

# create the main DAG that spawns others
#img_dag = Dag('img_dag')
image_task = PythonTask(name="main",
                       callback=main_func, queue="main")
main_dag_dict = {
    image_task: None,
    }

main_dag = Dag("main", autostart=True)
main_dag.define(main_dag_dict)
