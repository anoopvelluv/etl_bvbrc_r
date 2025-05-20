
#Customized function from MIC package for more control
pull_PATRIC_genome <- function(output_directory, 
                               genome_id,
                               retry = 2,
                               timeout_secs = 120,
                               logger) {
  
    status <- TRUE
    
    genome_path <- glue::glue(
      "ftp://ftp.patricbrc.org/genomes/{genome_id}/{genome_id}.fna"
    )
    
    target_path <- file.path(output_directory,
                             glue::glue("{genome_id}.fna"))
    
    options(timeout=timeout_secs)
    
    for (i in 1:retry) {
      if (file.exists(target_path)) {
        message(glue::glue("Genome {genome_id} already exists"))
      } else {
        message(glue::glue("Downloading file..."))
        message(genome_path)
        message(target_path)
        tryCatch(utils::download.file(genome_path,
                                      destfile = target_path,
                                      mode = "wb"),
                 error = function(e) {
                   status <- FALSE
                   message(glue::glue("Unable to download {genome_id}"))
                   log4r::info(logger,paste0("pull_PATRIC_genome : Error during ingestion on attempt ", i, ": ", e$message))
                 }
        )
      }
      if(isTRUE(status)){
        message_text <- sprintf(
          "pull_PATRIC_genome : Genome Ingestion completed for %s. Status: %s",
          genome_id,
          ifelse(status, "SUCCESS", "FAILURE")
        )
        log4r::info(logger, message_text)
        break
      }
    }
    return(status)
}


#Customized function from MIC package for more control
get_genome_ids <- function(taxonomic_name = NULL,
                            database = patric_ftp_path,
                            filter = "MIC") {
    supported_modality_filters <- c("all", "mic", "disc")
    filter <- tolower(filter)
    filter <- ifelse(filter == "disk", "disc", filter)
    
    if (!filter %in% supported_modality_filters) {
      stop(glue::glue("Unable to recognise filter {filter}, please use one of:
      {glue_collapse(supported_modality_filters, sep=', ')}"))
    }
    
    if (inherits(database, "patric_db")) {
      patric_amr_list <- database
    } else {
      patric_amr_list <- load_patric_db(database)
    }
    
    if (is.null(taxonomic_name)) {
      filtered_data <- patric_amr_list
    } else {
      filtered_data <- patric_amr_list[grep(taxonomic_name, patric_amr_list$genome_name), ]
    }
    
    filtered_data <- filtered_data |> dplyr::filter(dplyr::case_when(
      filter == "mic" & measurement_unit == "mg/L" ~ TRUE,
      filter == "disc" & laboratory_typing_method == "Disk diffusion" ~ TRUE,
      filter == "all" ~ TRUE
    ))
    
    genome_ids <- unique(filtered_data$genome_id)
}


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
      file.remove(PATRIC_DATA_PATH)
      file.copy(TEMP_PATRIC_PATH, PATRIC_DATA_PATH)
      log4r::info(logger, paste0("Patric DB : Data moved to \n", PATRIC_DATA_PATH ))
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
