"""


"""

from lightflow.models import Dag
from lightflow.tasks import PythonTask

# TODO : make callback something else callback
# 
from databroker import Broker
import matplotlib.pyplot as plt
import numpy as np

dbname = 'cms'


# the callback function for the task that stores the value 5
def put_data(data, store, signal, context):
    # chicken or egg problem. replace this with a root callback

    # grab uid of some run
    # I grabbed one that I found
    uid = "20799599-9572-4487-bc33-3570368a9da9"
    data['uid'] = uid
    data['dbname'] = 'cms'

    print('Task {task_name} being run in DAG {dag_name} '
          'for workflow {workflow_name} ({workflow_id}) '
          'on {worker_hostname}'.format(**context.to_dict()))

def grab_image(data, store, signal, context):
    ''' This function grabs the image.'''
    dbname = data['dbname']
    db = Broker.named(dbname)
    uid = data['uid']

    df = db[uid].table(fill=True)
    data['image'] = df.pilatus2M_image.iloc[0]

def plot_image(data, store, signal, context):
    ''' Store the image.'''
    plot_path = "/home/xf11bm/test.png"
    img = data['image']
    print(img)
    print(np)
    plt.figure(0);plt.clf();
    plt.imshow(np.log(img))
    plt.savefig(plot_path)


# create the main DAG that spawns others
main_dag = Dag('main_dag')
main_task = PythonTask(name="main",
                       callback=main)
main_dag.define({
    main_task: None,
    })


from SciStreams.XS_Streams import filter_attributes, pick_allowed_detectors




# the secondary circular average task
# create the two tasks for storing and retrieving data
put_task = PythonTask(name='put_task',
                      callback=put_data)
grab_image_task = PythonTask(name='grab_image',
                      callback=grab_image)
plot_image_task = PythonTask(name='plot_image',
                      callback=plot_image)


# set up the graph of the DAG, in which the put_task has to be executed first,
# followed by the print_task.
circavg_dag_dict = {
        put_task: grab_image_task,
        grab_image_task: plot_image_task,
        plot_image_task: None
    }

#circavg_dag = Dag('circavg')
