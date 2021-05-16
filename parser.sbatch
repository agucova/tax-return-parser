#!/bin/bash
#SBATCH --job-name=tax-return-parser
#SBATCH --nodes=1
#SBATCH --gres=gpu:0
#SBATCH --cpus-per-task=90
#SBATCH --mem=32GB
#SBATCH --time=60:00:00:00

PROJECT_DIR=$HOME/parser
MAIN=parser.py

module purge
module load python/intel/3.8.6

export RAYPORT=$(shuf -i 10000-65500 -n 1)
/usr/bin/ssh -N -f -R $RAYPORT:localhost:$RAYPORT log-1
/usr/bin/ssh -N -f -R $RAYPORT:localhost:$RAYPORT log-2
/usr/bin/ssh -N -f -R $RAYPORT:localhost:$RAYPORT log-3

source $SCRATCH/.env/bin/activate
cd $PROJECT_DIR && $SCRATCH/poetry/bin/poetry install
/usr/bin/time -v $SCRATCH/.env/bin/python3 -u $PROJECT_DIR/$MAIN

module unload python/intel/3.8.6
