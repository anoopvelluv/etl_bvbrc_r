## BV-BRC Genome ETL & PGSE/KMER Training Pipeline

This repository contains scripts to download BV-BRC genome files, generate label files, and train PGSE & KMER models.

BV-BRC FTP reference: [BV-BRC FTP Documentation](https://www.bv-brc.org/docs/quick_references/ftp.html)

------------------------------------------------------------------------

<small>

### Pipeline Steps

**Local Development Environment**

1.  Download genome files\
    `Rscript etl_bvbrc_r/R/download_genomes.R`

2.  Generate label files for PGSE training\
    `Rscript etl_bvbrc_r/R/generate_label_files.R`

3.  Train PGSE model\
    Navigate to `ML_Training` directory and run training scripts as needed.

**HPC Environment**

1.  Download genome files\
    `sbatch download_genomes.sh`

2.  Generate Label files\
    `sbatch create_labels_pgse.sh`
    
3.  Train PGSE models\
    `sbatch train_pgse_model.sh`
