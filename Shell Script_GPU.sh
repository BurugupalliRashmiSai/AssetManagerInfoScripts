#!/bin/bash

#SBATCH --export=NONE # do not export current env to the job
#SBATCH --job-name=MSvd2 # keep job name short with no spaces
#SBATCH --time=1-00:00:00 # request 1 day; Format: days-hours:minutes:seconds
#SBATCH --ntasks=10                  #Request 1 task
#SBATCH --mem=40G # request 7.5GB total memory per node; can use 7G but not 7.5G
#SBATCH --output=msv1op # save stdout to a file with job name and JobID appended to file name
#SBATCH --error=msv1err # save stdout to a file with job name and JobID appended to file name
#SBATCH --gres=gpu:2                 #Request 2 GPU per node can be 1 or 2
#SBATCH --partition=gpu              #Request the GPU partition/queue

# unload any modules to start with a clean environment
module purge

cd $SCRATCH

# load software modules
module load GCC/9.3.0 Python/3.8.2
source pip_envs/RA_ALG1-python3/bin/activate
# run commands
python Manager_Seller_ALG1_Data2.py