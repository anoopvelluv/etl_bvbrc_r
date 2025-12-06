

source(here::here("R/constants.R"))

#Customized function from MIC package for more control

#' Retrieve Genome IDs from PATRIC Database
#'
#' This function extracts genome IDs from a PATRIC antimicrobial resistance (AMR) database,
#' optionally filtered by taxonomic name and measurement modality.
#'
#' @param taxonomic_name Character string. Optional. A pattern to filter genome names by taxonomic name.
#'        If NULL, no filtering on taxonomic name is performed.
#' @param database Either a `patric_db` object or a file path to the PATRIC AMR database.
#'        Default is `patric_db_path`.
#' @param filter Character string. Filter to apply based on measurement modality.
#'        Options are `"all"`, `"mic"`, or `"disc"`. Case-insensitive.
#'        `"mic"` filters for MIC measurements (`measurement_unit == "mg/L"`),
#'        `"disc"` filters for disk diffusion tests (`laboratory_typing_method == "Disk diffusion"`),
#'        and `"all"` applies no filtering. Default is `"MIC"`.
#'
#' @return A character vector of unique genome IDs matching the criteria.
#'
#' @details
#' The function supports filtering based on measurement modality: MIC values (minimum inhibitory concentration),
#' disk diffusion, or all data. The `taxonomic_name` supports partial matching using `grep`.
#'
#' If the `database` argument is a file path, the function will load it using `load_patric_db()`.
#' If the database is already a `patric_db` object, it uses it directly.
#'
get_genome_ids <- function(taxonomic_name = NULL,
                            database = patric_db_path,
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

#Customized function from MIC package for more control
#' Download a PATRIC Genome FASTA File
#'
#' Downloads the genome FASTA file for a specified genome ID from the PATRIC FTP server
#' and saves it to a given output directory.
#'
#' @param output_directory Character string. The directory where the genome FASTA file
#'        will be saved.
#' @param genome_id Character string. The genome ID corresponding to the PATRIC genome
#'        to download.
#'
#' @return Logical. Returns `TRUE` if the genome file already exists or if the download
#'         is successful. Returns `FALSE` if the download fails.
#'
#' @details
#' This function constructs the FTP URL based on the genome ID, then downloads the
#' corresponding FASTA file (.fna) to the specified output directory.
#' If the file already exists in the output directory, it will not download it again.
#'
pull_PATRIC_genome <- function(output_directory, 
                               genome_id,
                               logger) {
  genome_path <- glue::glue(
    "{GENOME_FTP_PATH}/{genome_id}/{genome_id}.fna"
  )
  
  target_path <- file.path(output_directory,
                           glue::glue("{genome_id}.fna"))
  
  
  if (file.exists(target_path)) {
    message(glue::glue("Genome {genome_id} already exists"))
    return(TRUE)
  } else {
    message(glue::glue("Downloading file..."))
    message(genome_path)
    
    handle <- curl::new_handle()
    curl::handle_setopt(handle,
                        use_ssl = 3, 
                        userpwd = "anonymous:",
                        timeout = 7200,  # 2 hours
                        connecttimeout = 300,
                        low_speed_limit = 1,
                        low_speed_time  = 3600)
    
    status <- tryCatch(
      {
        curl::curl_download(genome_path,
                            target_path,
                            handle = handle, mode = "wb")
        TRUE
      },
      error = function(e) {
        log4r::info(logger, glue::glue("Unable to download {genome_id}: {e}"))
        message(glue::glue("Unable to download {genome_id}: {e}"))
        FALSE
      }
    )
  }
  return(status)
}

