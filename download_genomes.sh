#!/bin/bash -l
#SBATCH -D ./
#SBATCH --export=ALL
#SBATCH -J genomes_download
#SBATCH -p local
#SBATCH -o slurm_out/slurm-genomes-download-%A_%a.out
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=3-00:00:00
#SBATCH --array=0-7

date
hostname
echo "Starting genome downloads"

cd ~/etl_bvbrc_r/

###############################################
# SET SAFE LIBRARY PATHS FOR HPC
###############################################

# Your own R library (avoid system dirs)
export R_LIBS_USER=$HOME/R/library
mkdir -p "$R_LIBS_USER"

# renv cache in your home directory
export RENV_PATHS_CACHE=$HOME/.cache/R/renv
mkdir -p "$RENV_PATHS_CACHE"


echo "Using R user library: $R_LIBS_USER"
echo "Using renv cache: $RENV_PATHS_CACHE"

###############################################
# OPTIONAL: Clean renv cache
###############################################
echo "Cleaning renv cache..."
rm -rf
rm -rf $RENV_PATHS_CACHE/source/*


###############################################
#  Restore renv environment
###############################################
echo "Restoring renv environment..."

Rscript --vanilla -e '
Sys.setenv(R_INSTALL_STAGED = FALSE)
# FORCE renv to use project library, not system
Sys.setenv(RENV_PATHS_LIBRARY = file.path(getwd(), "renv/library"))

if (!requireNamespace("renv", quietly = TRUE)) {
    install.packages("renv", repos = "https://cloud.r-project.org")
}
renv::restore(prompt = FALSE, rebuild = TRUE)
'

###############################################
# Run main job
###############################################
echo "Running genome download script..."
Rscript --vanilla R/download_genomes.R