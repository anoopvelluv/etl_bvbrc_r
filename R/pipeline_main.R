library(MIC)
library(glue)

source(here::here("R/utils.R"))
source(here::here("R/constants.R"))
source(here::here("R/mic.R"))


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
            
            update_wal(paste0(genome_ids[[i]],".fna"), ftp_file_meta$latest_mod_time) 
            
            message_text <- glue("pull_PATRIC_genome: Genome Ingestion completed for {genome_ids[[i]]}. Status: {status_text}")
            log4r::info(logger, message_text)
          }else{
            
            message_text <- glue("pull_PATRIC_genome: Genome Ingestion completed. but seems like file is empty. No file downloaded -  {genome_ids[[i]]}")
            log4r::info(logger, message_text)
          }
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