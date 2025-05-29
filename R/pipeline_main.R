source("renv/activate.R")
library(MIC)
library(glue)

source(here::here("R/utils.R"))
source(here::here("R/constants.R"))
source(here::here("R/mic.R"))

#' Ingest PATRIC Data from FTP
#'
#' Downloads and updates the local PATRIC database if a new version is detected on the FTP server.
#'
#' @param logger Alogger object used to log the ingestion process.
#' @param retry An integer specifying the number of retry attempts if ingestion fails. Default is 2.
#' @param timeout_secs An integer specifying the timeout duration (in seconds) for the download operation. Default is 120.
#'
#' @return Returns TRUE if the file has not changed, or the result (logical) of the ingestion success status.
#'
#' @details
#' - Checks whether the remote file has been updated using is_file_updated_in_ftp()
#' - If the file is new or updated, it attempts to download the PATRIC database up to retry times using MIC::download_patric_db().
#' - On success, the temporary file is moved to the final data path via replace_file().
#' - The local WAL (write-ahead log) is updated using update_wal().
#' - Temporary files are cleaned using clear_temp_folder().
ingest_patric_data <- function(logger,
                               retry = 2,
                               timeout_secs = 120){
  
  ftp_file_meta <- is_file_updated_in_ftp(PATRIC_FTP_FOLDER,
                                          PATRIC_FTP_FILE)
  
  if (!isTRUE(ftp_file_meta$change_detected)) {
    log4r::info(logger,"Patric DB : File not updated in server, Skipping Data Ingestion ")
    return(TRUE)
  }
  
  options(timeout=timeout_secs)
  
  for (i in 1:retry) {
    # Remove temp file if it exists
    if (file.exists(TEMP_PATRIC_PATH)) {
      file.remove(TEMP_PATRIC_PATH)
    }
    
    # Try to ingest data and capture success/failure
    status <- tryCatch({
      status <- MIC::download_patric_db(TEMP_PATRIC_PATH)
    }, error = function(e) {
      log4r::info(logger,paste0("Patric DB : Error during ingestion on attempt ", i, ": ", e$message))
      FALSE
    })
    
    # Log result and exit loop if successful
    if (isTRUE(status)) {
      log4r::info(logger,paste0("Patric DB :Data Ingested successfully on attempt ", i))
      replace_file(TEMP_PATRIC_PATH, PATRIC_DATA_PATH)
      update_wal(PATRIC_FTP_FILE, ftp_file_meta$latest_mod_time) 
      break
    } else {
      log4r::info(logger,paste0("Patric DB : Data Ingestion Failed. Retrying... Attempt ", i))
      Sys.sleep(2)  #Wait before retrying
    }
  }
  clear_temp_folder(TEMP_PATRIC_PATH)
  return(status)
}

#' Ingest PATRIC Genomes for a Microorganism
#'
#' Downloads genome files from the PATRIC FTP server for a specified microorganism and set of genome IDs.
#' Only downloads genomes that have been updated since the last ingestion, with retry attempts on failure.
#'
#' @param mo_name A character string with the microorganism name.
#' @param genome_ids A character vector of genome IDs to ingest.
#' @param n_genomes_to_ingest Integer specifying how many genomes to ingest; defaults to 10.
#'   If NA or length zero, or if fewer genomes are available, all genomes will be ingested.
#' @param logger A logger object used for logging progress and errors.
#' @param retry Integer number of retry attempts for each genome ingestion (default is 2).
#' @param timeout_secs Integer timeout (in seconds) for network operations (default is 120).
#'
#' @return Invisibly returns NULL. Function is called for its side effects (downloading and logging).
#'
#' @details
#' - Creates microorganism-specific folders for output and temporary files.
#' - Checks for updates to each genome file on the FTP server before downloading.
#' - Retries failed downloads up to retry times with a short delay between attempts.
#' - Updates the WAL (write-ahead log) on successful ingestion.
#' - Clears temporary folders after ingestion completes.
ingest_patric_genomes <- function(mo_name,
                                  genome_ids,
                                  n_genomes_to_ingest = 10,
                                  logger,
                                  retry = 2,
                                  timeout_secs = 120){
  
  options(timeout=timeout_secs)
  
  if (is.na(n_genomes_to_ingest) || length(n_genomes_to_ingest) == 0 || length(genome_ids) < n_genomes_to_ingest ) {
    n_genomes_to_ingest <- length(genome_ids)
  }
  
  
  #Creating microorganism specific directories
  genome_folders <- setup_genome_folders(GENOME_OUTPUT_FOLDER, GENOME_OUTPUT_TEMP_FOLDER, mo_name)
  GENOME_OUTPUT_FOLDER <- genome_folders$GENOME_OUTPUT_FOLDER
  GENOME_OUTPUT_TEMP_FOLDER <- genome_folders$GENOME_OUTPUT_TEMP_FOLDER
    
  i <- 1
  while (i <= n_genomes_to_ingest) {
    
    ftp_folder <- file.path(GENOME_FTP_PATH, genome_ids[[i]],"/")
    ftp_file_meta <- is_file_updated_in_ftp(ftp_folder,
                                            paste0(genome_ids[[i]],".fna"))
    
    #Check whether ftp remote file modified since last ingestion
    if(isTRUE(ftp_file_meta$change_detected)){
      for (j in 1:retry) {
        status <- pull_PATRIC_genome(GENOME_OUTPUT_TEMP_FOLDER,
                                     genome_ids[[i]])
        
        status_text <- if (status) "SUCCESS" else "FAILURE"
        
        if(isTRUE(status)){
          
          downloaded_file <- file.path(GENOME_OUTPUT_TEMP_FOLDER, paste0(genome_ids[[i]],".fna"))
          target_path <-  file.path(GENOME_OUTPUT_FOLDER, paste0(genome_ids[[i]],".fna"))
          
          if (file.exists(downloaded_file)) {
            
            replace_file(file.path(GENOME_OUTPUT_TEMP_FOLDER, paste0(genome_ids[[i]],".fna")),
                         file.path(GENOME_OUTPUT_FOLDER, paste0(genome_ids[[i]],".fna")))
            
            message_text <- glue("pull_PATRIC_genome: Genome Ingestion completed for {genome_ids[[i]]}. Status: {status_text}")
            log4r::info(logger, message_text)
            
          }else{
            message_text <- glue("pull_PATRIC_genome: Genome Ingestion completed. but seems like file is empty. No file downloaded -  {genome_ids[[i]]}")
            log4r::info(logger, message_text)
          }
          update_wal(paste0(genome_ids[[i]],".fna"), ftp_file_meta$latest_mod_time) 
          break
        }
        else{
          log4r::info(logger,paste0("pull_PATRIC_genome : Error during ingestion on attempt ", j, ": ", e$message))
          log4r::info(logger,"Retrying...")
          Sys.sleep(2)  #Wait before retrying
        }
      }
    }else{
      log4r::info(logger,paste0("Genome Ingestion : File not updated in server, Skipping Data Ingestion "," Genome Id : ", genome_ids[[i]]))
    }
    i <- i + 1
  }
  clear_temp_folder(GENOME_OUTPUT_TEMP_FOLDER)
}

#' Main Pipeline Function for Data Ingestion
#'
#' Executes the complete ingestion pipeline which includes:
#' - Loading configuration parameters from a YAML file.
#' - Ingesting the PATRIC database if updated.
#' - Iterating over specified microorganisms to ingest genome data.
#'
#' @details
#' This function:
#'  - Creates a timestamped log file and initializes a logger.
#   - Loads ETL parameters from a YAML configuration file.
#'  - Calls ingest_patric_data() to update the PATRIC database.
#'  - For each microorganism listed in the configuration:
#'       - Reads the PATRIC database for that microorganism.
#'       - Retrieves genome IDs filtered by MIC data.
#'       - Calls ingest_patric_genomes() to download genome files.
#'Logs progress and completion messages.
#'
#' @return Invisibly returns NULL. The function is called for its side effects.
pipeline_main <- function(){
    log_file <- paste0(LOG_FOLDER, "/", LOG_FILE_NAME, "_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".log")
    logger <- log4r::logger(appenders = log4r::file_appender(log_file, append = FALSE))
    
    # #load Parameters
    config = yaml::yaml.load_file(ETL_CONFIG_FILE)
    log4r::info(logger, paste0("Loaded Parameters \n",yaml::as.yaml(config)))
    
    #Patric DB Ingestion Logic###############
    ingest_patric_data(logger,
                       retry = 2,
                       timeout_secs = config$ftp_connection_timeout)
    
    
    #Genome Ingestion Logic###############
    for(mo_name in config$micro_organisms){
      
      patric_data <- read_patric_db(patric_db = PATRIC_DATA_PATH,
                                    mo_name = mo_name,
                                    logger)
      
      genome_ids <- get_genome_ids(database = patric_data,
                                   filter = "MIC")
      message(paste0("Pulling data for genome : ", mo_name))
      log4r::info(logger, paste0("Pulling  top ", config$top_n_genomes_to_ingest," genome ids from Patric DB. Micro Organism : ",mo_name))
      ingest_patric_genomes(mo_name,
                            genome_ids,
                            config$top_n_genomes_to_ingest,
                            logger,
                            retry = 2,
                            config$ftp_connection_timeout)
    }
    log4r::info(logger, "Data Ingestion Completed.  ")
    message("Data Ingestion Completed. Please check logs for details")
}
pipeline_main()