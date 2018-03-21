"""
"""

from lightflow.models import Dag
from lightflow.tasks import PythonTask

# TODO : make callback something else callback
# 
from databroker import Broker
import matplotlib.pyplot as plt
import numpy as np


from SciStreams.interfaces.plotting_mpl import store_results_mpl

def image_func(data, store, signal, context):
    uid = data['uid']
    md = data['md']
    dbname = data['dbname']

    db = Broker.named(dbname)
    hdr = db[uid]

    stream_name = 'primary'
    fields = hdr.fields(stream_name)

    # eventually iterate and spawn new tasks
    detector_keys = ['pilatus2M_image']
    for detector_key in detector_keys:
        if detector_key in hdr.fields():
            print("Found , saving an image")
            imgs = hdr.data(detector_key, fill=True)
            for img in imgs:
                sdoc = dict()
                sdoc['kwargs'] = dict(img=img)
                sdoc['attributes'] = md
                store_results_mpl(sdoc, images=['img'])



# create the main DAG that spawns others
#img_dag = Dag('img_dag')
image_task = PythonTask(name="main",
                       callback=image_func)
img_dag_dict = {
    image_task: None,
    }

img_dag = Dag("img", autostart=False)
img_dag.define(img_dag_dict)
