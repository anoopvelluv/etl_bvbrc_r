#!/bin/bash -l

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
'