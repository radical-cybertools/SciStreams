import os
import shutil
from uuid import uuid4

from ...config import config


tau_config = config['modules']['tau']

TRACEDIR = tau_config['tracedir']
TRACEFILES = tau_config['tracefiles']
TRACEFILES = [TRACEDIR + "/" + TRACEFILE for TRACEFILE in TRACEFILES]

PROFILEDIR = tau_config['profiledir']
PROFILEFILE = tau_config['profilefile']
PROFILEFILE = PROFILEDIR + "/" + PROFILEFILE


def copytracefile(name):
    ''' convenience routine to copy trace file. need better method later.
        name : name of prefix directory
    '''
    # always copy from same file
    source = PROFILEFILE
    # make directory if doesn't exist
    dest_dir = PROFILEDIR + "/" + name
    os.makedirs(dest_dir, exist_ok=True)
    dest_file = dest_dir+"/" + str(uuid4()) + ".tau.profile"
    print('moving trace file from {} to {}'.format(source, dest_dir))
    shutil.copy(source, dest_file)
    print("done")
