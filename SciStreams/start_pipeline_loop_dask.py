# Things for the ZMQ communication
# Needs the lightflow environment

import matplotlib 
matplotlib.use('Agg')
from databroker import Broker

import workflows.main_local_dask as main_local_dask

data = dict()
data['data_folder'] = "/home/georgeha/data/fa466942"
import time
t1 = time.time()
main_local_dask.primary_func(data)

t2 = time.time()
print('took {} sec'.format(t2-t1))

