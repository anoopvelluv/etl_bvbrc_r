
library(RCurl)
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
  
  # Try parsing with time first, then with year
  mod_time <- suppressWarnings(as.POSIXct(date_str, format = "%b %d %H:%M", tz = "GMT"))
  if (is.na(mod_time)) {
      mod_time <- suppressWarnings(as.POSIXct(date_str, format = "%b %d %Y", tz = "GMT"))
  }
  if (is.na(mod_time)) return(NULL)
  
  list(filename = filename, last_modified = mod_time)
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
                           ftp_file){
  
  # Get directory listing
  listing <- tryCatch(
    {
      RCurl::getURL(ftp_dir, dirlistonly = FALSE)
    },
    error = function(e) {
      stop("Failed to collect metadata. Exiting.")
    }
  )
  
  meta_now <- parse_ftp_line(listing, ftp_file)
  
  wal <- load_wal(WAL_PATH)
  prev_meta <- wal[wal$file == ftp_file, ]
  
  change_detected <- ifelse(nrow(prev_meta) == 0 || prev_meta$last_modified != meta_now$last_modified,TRUE,FALSE)
  
  return(list(change_detected = change_detected, latest_mod_time = meta_now$last_modified))
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
#' - Loads the database using MIC::load_patric_db().
#' - Loads a standard mapping file from MO_STD_MAPPING (assumed to be a globally defined path to an RDS file).
#' - If the standardized name is not found in the mapping, it updates the mapping via update_standard_mapping().
#' - The database is then joined with the mapping and filtered.
read_patric_db <- function(patric_db, 
                           mo_name,
                           logger) {
  # Standardize microorganism name
  mo_name <- AMR::mo_name(mo_name)
  
  # Load the PATRIC database
  patric_database <- MIC::load_patric_db(patric_db)
  
  # Load mapping of genome names to standard microorganism names
  mo_standard_mapping <- readRDS(MO_STD_MAPPING)
  
  mo_standard_mapping$genome_name <- gsub('""', '"', mo_standard_mapping$genome_name)
  
  #Re-calculate standard mapping file
  if (!(mo_name %in% mo_standard_mapping$std_mo_name)) {
    log4r::info(logger, paste0("Missing ",mo_name," in standard mapping file. Re - calculating standard mapping file."))
    mo_standard_mapping <- update_standard_mapping(patric_database)
  }
  
  # Join PATRIC data with standard mapping on genome_name
  patric_database <- dplyr::left_join(patric_database, mo_standard_mapping, by = "genome_name")
  
  # Filter rows matching the standardized microorganism name
  patric_database <- patric_database[patric_database$std_mo_name == mo_name, ]
  
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
