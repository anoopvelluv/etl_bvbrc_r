library(MIC)

source(here::here("R/utils.R"))
source(here::here("R/constants.R"))
source(here::here("R/mic.R"))


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
    log4r::info(logger, paste0("Pulling  top ", config$top_n_genomes_to_ingest," genome ids from Patric DB "))
    genome_ids <- get_genome_ids(database = PATRIC_DATA_PATH,
                   filter = "MIC")
  
    i <- 1
    while (i <= config$top_n_genomes_to_ingest) {
    
      ftp_folder <- file.path(GENOME_FTP_PATH, genome_ids[[i]],"/")
      ftp_file_meta <- is_file_updated_in_ftp(ftp_folder,
                                              paste0(genome_ids[[i]],".fna"))
    
      #Check whether ftp remote file modified since last ingestion
      if(isTRUE(ftp_file_meta$change_detected)){
        
        status <- pull_PATRIC_genome(GENOME_OUTPUT_TEMP_FOLDER,
                                     genome_ids[[i]],
                                     retry = 2,
                                     timeout_secs = config$ftp_connection_timeout,
                                     logger)
        
        if(isTRUE(status)){
          file.remove(file.path(GENOME_OUTPUT_FOLDER, paste0(genome_ids[[i]],".fna")))
          file.copy(file.path(GENOME_OUTPUT_TEMP_FOLDER, paste0(genome_ids[[i]],".fna")),
                    file.path(GENOME_OUTPUT_FOLDER, paste0(genome_ids[[i]],".fna")))
          
          update_wal(paste0(genome_ids[[i]],".fna"), ftp_file_meta$latest_mod_time) 
        }
      }else{
        log4r::info(logger,paste0("Genome Ingestion : File not updated in server, Skipping Data Ingestion "," Genome Id : ", genome_ids[[i]]))
      }
      i <- i + 1
    }
    clear_temp_folder(GENOME_OUTPUT_TEMP_FOLDER)
    message("Data Ingestion Completed. Please check logs for details")
}

pipeline_main()