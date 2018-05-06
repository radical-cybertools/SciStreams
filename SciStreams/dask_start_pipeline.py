# Things for the ZMQ communication
# Needs the lightflow environment

import matplotlib 
matplotlib.use('Agg')
import time
import os
from distributed import Client
import sys, argparse
import workflows.main_local_dask as main_local_dask
from dask.distributed import wait


def partition_files(data,files_per_process):
        
    data_folder = data['data_folder'] 
    files = os.listdir(data_folder)
    
    files = [ os.path.join(data_folder,filename)  for filename in files
             if os.path.isfile(os.path.join(data_folder,filename))]

    files_per_process_list = list()

    for i in range(0,len(files),files_per_process):
        temp = list()
        for j in range(files_per_process):
            temp.append(files[i+j])
        files_per_process_list.append(temp)
    
    
    
    return files_per_process_list # 2d list 


if __name__=='__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", help="Dont' put a / at the end!", default= '/data/03662/tg829618/SciStreams/data/fa466942')
    parser.add_argument("--files_per_process", help="provide the base cost of the account", type=float, default=1)
    parser.add_argument("--dask_client", help="dask client url",  default=None)

    args = parser.parse_args() 
    files_per_process = args.files_per_process

    data = dict()
    #data['data_folder'] = "/data/03662/tg829618/SciStreams/data/fa466942"
    data['data_folder'] = args.data_dir
    
    t1 = time.time()
    
    files_per_process_list = partition_files(data,files_per_process)
    
    client = Client(args.dask_client)
    
    image_analysis_pipeline = list()
    
    for i in range(0,len(files_per_process_list)):
        image_analysis_pipeline.append(client.submit(main_local_dask.primary_func, data,files_per_process_list[i]))  #files_per_process_list[i]  type : list
    
    wait(image_analysis_pipeline)
        
    
    t2 = time.time()
    print('took {} sec'.format(t2-t1))

