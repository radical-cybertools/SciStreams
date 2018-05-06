srun --pty -p normal  -t 1:00:00 -n  96  /bin/bash -l


hostnodes=`scontrol show hostnames $SLURM_NODELIST`

echo $hostnodes  > dask_hostfile.txt

dask-ssh --hostfile dask_hostfile.txt  --nthreads 1 --nprocs 24


