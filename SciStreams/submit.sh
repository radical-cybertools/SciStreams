#!/bin/bash

#SBATCH -J peakfinder             # Job name
#SBATCH -o slurm.%j.out                  # STDOUT (%j = JobId)
#SBATCH -e slurm.%j.err                  # STDERR (%j = JobId)
#SBATCH --partition=compute
# #SBATCH --constraint="large_scratch"
#SBATCH --nodes=1                        # Total number of nodes requested (16 cores/node). You may delete this line if wanted
#SBATCH --ntasks-per-node=24             # Total number of mpi tasks requested
#SBATCH --export=ALL
#SBATCH -t 04:00:00                      # wall time (D-HH:MM)
#SBATCH --mail-user=gc481e@scarletmail.rutgers.edu     # email address
#SBATCH --mail-type=all                  # type of mail to send

#The next line is required if the user has more than one project
# #SBATCH -A A-yourproject # <-- Allocation name to charge job against

my_file=dask_start_pipeline
#my_file=test
#my_file=find_dir

SCHEDULER=`hostname`
echo SCHEDULER: $SCHEDULER
dask-scheduler --port=8786 &
sleep 5

hostnodes=`scontrol show hostnames $SLURM_NODELIST`
echo $hostnodes

for host in $hostnodes; do
    echo "Working on $host ...."
    ssh $host  dask-worker --nprocs 24 --nthreads 1 $SCHEDULER:8786 &
    
    sleep 1
done

sleep 5
echo "====-run script-===="

ssh $SCHEDULER
echo 'sshed scheduler'
python $my_file.py --dask_client $SCHEDULER:8786  --nodes 2  --images 2852
echo 'script completed at '


