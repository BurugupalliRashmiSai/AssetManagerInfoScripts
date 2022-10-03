#!/bin/bash

#SBATCH --export=NONE # do not export current env to the job
#SBATCH --job-name=MSv1 # keep job name short with no spaces
#SBATCH --time=1-00:00:00 # request 1 day; Format: days-hours:minutes:seconds
#SBATCH --nodes=1 # request 1 node (optional since default=1)
#SBATCH --ntasks-per-node=1 # request 1 task (command) per node
#SBATCH --cpus-per-task=48 # request 1 cpu (core, thread) per task
#SBATCH --mem=40G # request 7.5GB total memory per node; can use 7G but not 7.5G
#SBATCH --output=msv1op # save stdout to a file with job name and JobID appended to file name
#SBATCH --error=msv1err # save stdout to a file with job name and JobID appended to file name
# unload any modules to start with a clean environment
module purge

cd $SCRATCH

# load software modules
module load GCC/9.3.0 Python/3.8.2
source pip_envs/RA_ALG1-python3/bin/activate
# run commands
python Manager_Seller_ALG1_Data1.py
