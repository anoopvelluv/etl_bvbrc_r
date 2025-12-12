
library(curl)
source(here::here("R/constants.R"))

#' Parse FTP Directory Listing Line
#'
#' Extracts the file name and last modified time from a line in an FTP directory listing.
#'
#' @param lines A character string representing the full FTP directory listing (multiple lines).
#' @param file_name A character string representing the name of the file to find within the listing.
#'
#' @return A list containing:
#'   filename- The name of the matched file (character).
#'   last_modified - The POSIXct object representing the last modified date and time of the file.
#' 
#' Returns NULL if the file is not found or the date format is unparseable.

parse_ftp_line <- function(lines, file_name) {
  
  lines_splitted <- unlist(strsplit(lines, "\n"))
  file_line <- grep(file_name, lines_splitted, value = TRUE)
  
  parts <- strsplit(file_line, "\\s+")[[1]]
  if (length(parts) < 9) return(NULL)
    filename <- parts[length(parts)]
    date_str <- paste(parts[6:8], collapse = " ")
    file_size <- suppressWarnings(as.numeric(parts[5]))
  
  # Try parsing with time first, then with year
  mod_time <- suppressWarnings(as.POSIXct(date_str, format = "%b %d %H:%M", tz = "GMT"))
  if (is.na(mod_time)) {
      mod_time <- suppressWarnings(as.POSIXct(date_str, format = "%b %d %Y", tz = "GMT"))
  }
  if (is.na(mod_time)) return(NULL)
  
  list(filename = filename, last_modified = mod_time, file_size = file_size)
}

#' Load WAL (Write-Ahead Log) File
#'
#' Loads a WAL file containing file metadata, such as filenames and their last modified timestamps.
#'
#' @param path A character string specifying the path to the WAL CSV file.
#'
#' @return A data.frame with columns:
#'  filename - Character vector of file names.
#'  last_modified - POSIXct vector representing the last modified timestamps (in GMT).
#'
#' If the file does not exist, returns an empty data frame with the appropriate columns.
load_wal <- function(path) {
  if (file.exists(path)) {
    wal <- read.csv(path, stringsAsFactors = FALSE)
    wal$last_modified <- as.POSIXct(wal$last_modified, tz = "GMT")
    wal
  } else {
    data.frame(filename = character(), last_modified = as.POSIXct(character()), stringsAsFactors = FALSE)
  }
}

#' Update WAL (Write-Ahead Log) Entry
#'
#' Updates the WAL by replacing or adding a file entry with the given modification date.
#'
#' @param file A character string specifying the file name to update in the WAL.
#' @param mod_date A POSIXct object representing the last modified timestamp of the file.
#'
#' @return Invisibly returns the updated WAL data frame.
#'
#' @details This function loads the existing WAL using the global variable WAL_PATH,
#' removes any existing entry with the same file name, and adds the new entry.
update_wal <- function(file, mod_date) {
  wal <- load_wal(WAL_PATH)
  wal_updated <- wal[wal$filename != file, ]
  
  wal_updated <- rbind(
    wal_updated,
    data.frame(filename = file, last_modified = mod_date, stringsAsFactors = FALSE)
  )
  
  # Create the folder if it doesn't exist
  dir.create(dirname(WAL_PATH), recursive = TRUE, showWarnings = FALSE)
  
  write.csv(wal_updated, WAL_PATH, row.names = FALSE)
}

#' Check if FTP File is Updated
#'
#' Determines whether a specific file on an FTP server has been updated compared to the local WAL.
#'
#' @param ftp_dir A character string specifying the URL of the FTP directory.
#' @param ftp_file A character string specifying the file name to check for updates.
#'
#' @return A list containing:
#'   - change_detected - Logical indicating whether the file has been updated or is new.
#'   - latest_mod_time - POSIXct timestamp of the latest modification time from the FTP server.
#'
#' @details Compares the last modified time of the file on the FTP server with the time stored in the local WAL,
#' which is loaded from the global variable WAL_PATH. If the file is new or the timestamp has changed,
#' change_detected is set to TRUE.
is_file_updated_in_ftp<- function(ftp_dir,
                           ftp_file,
                           logger,
                           retry = 3,
                           sleep_sec = 5){
  # Create a curl handle
  h <- curl::new_handle()
  curl::handle_setopt(
    h,
    use_ssl = 3,          # same as your example
    userpwd = "anonymous:" ,  # username & password,
    timeout = 7200,  # 2 hours
    connecttimeout = 300,
    low_speed_limit = 1,
    low_speed_time  = 3600
  )
  
  attempt = 1
  while (attempt <= retry) {
    # Get directory listing
    listing <- tryCatch(
      {
        res <- curl::curl_fetch_memory(
          url = ftp_dir,
          handle = h
        )
        rawToChar(res$content)
      },
      error = function(e) {
        if(attempt >= retry){
          log4r::info(logger,paste0("Patric DB : Failed to collect metadata. Exiting. command :  RCurl::getURL ",ftp_dir ,e ))
          stop("Failed to collect metadata from BVBRC. Stopped Execution")
        }else{
          log4r::info(logger,paste0("Patric DB : Failed to collect metadata.command :  RCurl::getURL ",ftp_dir, e))
        }
      }
    )
    if (!is.null(listing)) {
      break
    }else{
      log4r::info(logger,paste0("Retrying (attempt = ",attempt,")"))
      attempt <- attempt + 1
      Sys.sleep(sleep_sec)
    }
  }
  
  meta_now <- parse_ftp_line(listing, ftp_file)
  
  wal <- load_wal(WAL_PATH)
  prev_meta <- wal[wal$file == ftp_file, ]
  
  change_detected <- ifelse(nrow(prev_meta) == 0 || prev_meta$last_modified != meta_now$last_modified,TRUE,FALSE)
  
  return(list(change_detected = change_detected,
              latest_mod_time = meta_now$last_modified,
              file_size = meta_now$file_size))
}

#' Clear Temporary Folder or File
#'
#' Deletes all files within a specified folder. If the path points to a file instead of a folder, it deletes the file.
#'
#' @param folder_path A character string specifying the path to a folder or a file.
#'
#' @return Invisibly returns TRUE for successful file deletions or FALSE if deletions fail.
#'
#' @details
#' - If the path is a file (i.e., does not exist as a directory but exists as a file), it deletes the file.
#' - If the path is a directory, it deletes all files inside it (but not the folder itself).
clear_temp_folder <- function(folder_path) {
  if(!dir.exists(folder_path) && file.exists(folder_path)){
    file.remove(folder_path)
  }else{
    files_to_remove <- list.files(folder_path, full.names = TRUE)
    file.remove(files_to_remove)
  }
}

#' Replace a File
#'
#' Replaces a destination file with a source file. If the destination exists, it is first removed before copying.
#'
#' @param src A character string specifying the path to the source file.
#' @param dest A character string specifying the path to the destination file.
#'
#' @return This function is called for its side effects and returns nothing. If the file copy fails, an error is thrown.
replace_file <- function(src, dest) {
  
  if (file.exists(dest)) {
    file.remove(dest)
  }
  
  success <- file.copy(src, dest, overwrite = TRUE)
  
  if (!success) {
    stop(sprintf("Failed to copy file from %s to %s", src, dest))
  }
}

#' Read and Filter PATRIC Database for a Microorganism
#'
#' Loads a PATRIC database and filters it for a standardized microorganism name using a mapping file.
#'
#' @param patric_db A character string specifying the path to the PATRIC database file.
#' @param mo_name A character string specifying the microorganism name to filter for.
#' @param logger A log4r logger object used for logging informational messages.
#'
#' @return A data.frame containing records from the PATRIC database that match the standardized microorganism name.
#'
#' @details
#' - Uses AMR::mo_name() to standardize the input microorganism name.
#' - Loads the database using  faLearn::load_patric_db().
#' - Loads a standard mapping file from MO_STD_MAPPING (assumed to be a globally defined path to an RDS file).
#' - If the standardized name is not found in the mapping, it updates the mapping via update_standard_mapping().
#' - The database is then joined with the mapping and filtered.
read_patric_db <- function(patric_db, 
                           mo_name, 
                           standard_mapping_file,
                           recalculate_std_mapping = FALSE,
                           logger = NULL) {
  # Standardize microorganism name
  mo_name <- AMR::mo_name(mo_name)
  
  # Load the PATRIC database
  patric_database <- faLearn::load_patric_db(patric_db)
  
  # Load the genome-to-standard mapping
  mo_standard_mapping <- readRDS(standard_mapping_file)
  
  # Clean up quotes in genome names
  mo_standard_mapping$genome_name <- gsub('""', '"', mo_standard_mapping$genome_name)
  
  unique_patric_mo <- na.omit(unique(patric_database$genome_name))
  unique_std_mapping_mo <- na.omit(unique(mo_standard_mapping$genome_name))
  
  if (!all(unique_patric_mo %in% unique_std_mapping_mo)) {
    missing_items <- setdiff(unique_patric_mo, unique_std_mapping_mo)
    if(! is.null(logger))
      log4r::info(logger, paste("Missing genome names in standard mapping:", paste(missing_items, collapse = ", ")))
  }
  
  if(!(mo_name %in% mo_standard_mapping$std_mo_name) & ! is.null(logger)){
    log4r::info(logger, paste("Missing genome name in std mapping - ", mo_name))
  }
  
  # Recalculate standard mapping if missing or outdated
  if (recalculate_std_mapping && 
      (!(mo_name %in% mo_standard_mapping$std_mo_name) ||
      !all(unique_patric_mo %in% unique_std_mapping_mo) )) {
    if(! is.null(logger))
      log4r::info(logger, paste0("Missing ", mo_name, " in standard mapping file. Recalculating mapping..."))
    mo_standard_mapping <- update_standard_mapping(patric_database)
  }
  
  # Join standard mapping to PATRIC DB
  patric_database <- dplyr::left_join(patric_database, mo_standard_mapping, by = "genome_name")
  
  # Filter for standardized organism name
  patric_database <- patric_database[patric_database$std_mo_name == mo_name, ]
  
  # Rename and clean up columns
  names(patric_database)[names(patric_database) == "genome_name"] <- "old_genome_name"
  names(patric_database)[names(patric_database) == "std_mo_name"] <- "genome_name"
  
  # Drop old genome name
  patric_database <- patric_database |> dplyr::select(-old_genome_name)
  return(patric_database)
}


#' Update Standard Microorganism Mapping
#'
#' Generates and saves a mapping of genome names to standardized microorganism names.
#'
#' @param database A data.frame containing at least a genome_name column.
#'
#' @return A data.frame with columns:
#'.  - genome_name - Original genome name from the database.
#'   - std_mo_name - Standardized microorganism name using AMR::mo_name().
#' 
update_standard_mapping <- function(database) {
  unique_genome_names <- unique(database$genome_name)
  std_mapping <- data.frame(
    genome_name = unique_genome_names,
    std_mo_name = AMR::mo_name(unique_genome_names),
    stringsAsFactors = FALSE
  )
  
  writeRDS(std_mapping, MO_STD_MAPPING)
  return(std_mapping)
}

#' Set Up Genome Output and Temporary Folders
#'
#' Creates output and temporary directories for a specific microorganism, ensuring folder-safe naming.
#'
#' @param base_output_folder A character string specifying the base directory where output folders should be created.
#' @param base_temp_folder A character string specifying the base directory for temporary folders.
#' @param mo_name A character string specifying the microorganism name. Spaces will be replaced with underscores.
#'
#' @return A named list with two elements:
#' - GENOME_OUTPUT_FOLDER - Path to the created output folder.
#' - GENOME_OUTPUT_TEMP_FOLDER - Path to the created temporary folder.
#' 
setup_genome_folders <- function(base_output_folder, base_temp_folder, mo_name) {
  mo_name <-  stringr::str_replace_all(mo_name, " ", "_")
  genome_output_folder <- file.path(base_output_folder, mo_name)
  genome_temp_folder <- file.path(base_temp_folder, mo_name)
  
  dir.create(genome_output_folder, recursive = TRUE, showWarnings = FALSE)
  dir.create(genome_temp_folder, recursive = TRUE, showWarnings = FALSE)
  
  return(list(
    GENOME_OUTPUT_FOLDER = genome_output_folder,
    GENOME_OUTPUT_TEMP_FOLDER = genome_temp_folder
  ))
}

#' Audit and Save Pulled Genome IDs
#'
#' This function logs genome names and genome IDs to a CSV file for auditing purposes.
#' It can either append to an existing file or overwrite it.
#'
#' @param genome_name A character vector of genome names.
#' @param genome_id A character vector of genome IDs corresponding to `genome_name`.
#' @param file_path A string specifying the path to the CSV file where the data will be saved.
#' @param append Logical. If TRUE (default), the data will be appended to the existing file. 
#'               If FALSE, any existing file at `file_path` will be deleted before writing.
#'
#' @return Invisibly returns the data frame written to file.
audit_pulled_genome_ids <- function(genome_name = NULL, genome_id = NULL, file_path = "data/audit_log.csv", append = TRUE) {
  # Remove file if append is FALSE
  if (!append && file.exists(file_path)) {
    file.remove(file_path)
  }
  
  # Create data frame
  df <- data.frame(
    ingestion_time = Sys.time(),
    genome_name = genome_name,
    genome_id = genome_id,
    stringsAsFactors = FALSE
  )
  
  col_names <- !file.exists(file_path) 
  # Append or write new
  write.table(
    df,
    file = file_path,
    sep = ",",
    row.names = FALSE,
    col.names = col_names ,
    append = append,
    quote = TRUE
  )
}


#' Detect Whether the Environment is an HPC Cluster (Production)
#'
#' This function attempts to determine whether the current R session
#' is running on a High-Performance Computing (HPC) cluster.  
#' It checks for common environment variables used by job schedulers
#' (e.g., SLURM, PBS) and hostname patterns often found on compute nodes.
#'
#' This can be used to automatically switch between *development* and
#' *production* configurations depending on the environment.
is_hpc <- function() {
  # Example checks:
  # 1. SLURM job variable exists
  if(!is.na(Sys.getenv("SLURM_JOB_ID", unset = NA))) return(TRUE)
  
  # 2. PBS job variable exists
  if(!is.na(Sys.getenv("PBS_JOBID", unset = NA))) return(TRUE)
  
  # 3. Check hostname pattern (adjust as per your HPC cluster)
  hostname <- Sys.info()[["nodename"]]
  if(grepl("compute|hpc|cluster", hostname, ignore.case = TRUE)) return(TRUE)
  
  return(FALSE)
}


#' Set Up Logging Directory and Create a Logger
#'
#' This function ensures that a specified log directory exists and then
#' creates a log file inside that directory using the \pkg{log4r} package.
#' The log filename will include a timestamp for easy identification.
#'
setup_logging <- function(log_folder, log_file_name = "app") {
  # Create directory if needed
  dir.create(log_folder, recursive = TRUE, showWarnings = FALSE)
  
  # Build full log file path
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  log_file <- file.path(log_folder, paste0(log_file_name, "_", timestamp, ".log"))
  
  # Create logger
  logger <- log4r::logger(
    appenders = log4r::file_appender(log_file, append = FALSE)
  )
  
  return(logger)
}
