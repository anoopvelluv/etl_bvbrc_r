library(dplyr)
library(yaml)

source(here::here("R/constants.R"))
source(here::here("R/utils.R"))

#' Generate Label Files and Metadata YAML from Audit Logs
#'
#' This function processes audit logs and label data to generate structured
#' CSV files for each genome and antibiotic combination. It also produces a
#' metadata YAML configuration summarizing output paths.
#'
#' @details
#' The function performs the following tasks:
#' \itemize{
#'   \item Loads configuration from a YAML file.
#'   \item Reads audit logs to identify genome names and IDs.
#'   \item For each genome, extracts labels data from a source PATRIC database.
#'   \item Filters for specified antibiotics and valid measurement values.
#'   \item Saves the labels in CSV format per organism and antibiotic.
#'   \item Outputs a YAML configuration file mapping organisms to their label files.
#' }
#'
#' @return This function has no return value. It is called for its side effects:
#' saving CSV label files and a metadata YAML file to disk.
#'
main <- function() {
  
  logger <- setup_logging(LOG_FOLDER, LOG_FILE_NAME_LABELS)
  
  # Load Parameters
  config <- yaml::yaml.load_file(ETL_CONFIG_FILE)
  
  # Select config automatically
  env <- if(is_hpc()) "prod" else "dev"
  app_config <- config[[env]]
  log4r::info(logger, paste("Using config for:", env))
  
  
  # Read Audit Log
  audit_logs <- read.csv(AUDIT_LOG)
  stopifnot(nrow(audit_logs) > 0)
  
  # Collapse audit logs by genome name
  genomes_ingested <- audit_logs %>%
    dplyr::group_by(genome_name) %>%
    dplyr::summarise(genome_ids = list(genome_id), .groups = "drop")
  
  meta_yaml <- list()
  mo_meta_data <- list()
  
  # Iterate over each genome
  for (row in seq_len(nrow(genomes_ingested))) {
    genome_name <- genomes_ingested$genome_name[row]
    genome_ids  <- genomes_ingested$genome_ids[[row]]
    
    log4r::info(
      logger,
      paste0(
        "genome_name: ", genome_name,
        " genome_ids_count: ", length(genome_ids)
      ))
    
    # Read labels data
    labels_data <- read_patric_db(
      PATRIC_DATA_PATH,
      genome_name,
      MO_STD_MAPPING
    )
    
    # Filter data for ingested genome IDs
    labels_data <- labels_data %>%
      dplyr::filter(genome_id %in% genome_ids)
    
    abx_meta_data <- list()
    
    # Iterate over each antibiotic
    for (abx in config$anti_biotics) {
      labels_data_abx <- labels_data %>%
        dplyr::filter(
          antibiotic == abx &
            (!is.na(measurement_value) | measurement_value != "NA")
        ) %>%
        dplyr::mutate(file_name = paste0(genome_id, ".fna")) %>%
        dplyr::select(measurement_value, file_name)
      
      # Rename columns
      colnames(labels_data_abx) <- c("labels", "files")
      
      if (nrow(labels_data_abx) == 0) {
        log4r::info(logger, 
          paste0("Antibiotic - ", abx, " not found in Patric DB for microorganism ",
                 genome_name, " in selected genome_ids. Skipping...")
        )
        next
      }
      
      # Prepare output paths
      genome_dir <- stringr::str_replace_all(genome_name, " ", "_")
      abx_dir <- abx %>%
        stringr::str_replace_all(" ", "_") %>%
        stringr::str_replace_all("/", "_")
      
      output_file_name <- paste0(genome_dir, "-", abx_dir, "_labels.csv")
      genome_output_folder <- file.path(app_config$LABELS_OUTPUT_FOLDER, genome_dir, abx_dir)
      genome_output_file <- file.path(genome_output_folder, output_file_name)
      
      # Create output directory
      dir.create(genome_output_folder, recursive = TRUE, showWarnings = FALSE)
      
      # Save metadata
      abx_meta_data[[abx]] <- list(
        label_file = genome_output_file,
        data_dir = file.path(app_config$GENOME_OUTPUT_FOLDER, genome_dir)
      )
      #Convert labels to logarithmic value
      labels_data_abx <- labels_data_abx %>%    
                          mutate(
                            labels = as.numeric(sub("/.*", "",labels)),
                            labels = log(labels, base = 2))
      
      # Write label file
      write.csv(labels_data_abx, genome_output_file, row.names = FALSE)
    }
    
    # Add to microorganism metadata if any antibiotics were processed
    if (length(abx_meta_data) > 0) {
      mo_meta_data[[genome_name]] <- abx_meta_data
    }
  }
  
  # Create YAML output config
  meta_yaml[["micro-organism"]] <- mo_meta_data
  yaml_output <- yaml::as.yaml(meta_yaml)
  writeLines(yaml_output, file.path(app_config$LABELS_OUTPUT_FOLDER,META_LABEL_FILE))
  
  log4r::info(logger, "Labels Generated")
}

# Run the main function
main()
