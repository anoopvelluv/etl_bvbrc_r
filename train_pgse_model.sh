#!/bin/bash -l
#SBATCH -D ./
#SBATCH --export=ALL
#SBATCH -J pgse_training
#SBATCH -p local
#SBATCH -o slurm_out/slurm-pgse-training-%A_%a.out
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=3-00:00:00

set -e
date
hostname

cd ~/etl_bvbrc_r/

source hpc_setup.sh

echo "Restoring renv..."
Rscript --vanilla -e "renv::restore(prompt = FALSE, rebuild = TRUE)"


###############################################
# Run main job
###############################################
echo "Running genome download script..."
Rscript --vanilla R/ML_Training/train_pgse.R