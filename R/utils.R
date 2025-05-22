
library(RCurl)
source(here::here("R/constants.R"))

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

# Load WAL
load_wal <- function(path) {
  if (file.exists(path)) {
    wal <- read.csv(path, stringsAsFactors = FALSE)
    wal$last_modified <- as.POSIXct(wal$last_modified, tz = "GMT")
    wal
  } else {
    data.frame(filename = character(), last_modified = as.POSIXct(character()), stringsAsFactors = FALSE)
  }
}

# Update WAL
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

is_file_updated_in_ftp<- function(ftp_dir,
                           ftp_file){
  
  # Get directory listing
  listing <- tryCatch(
    {
      getURL(ftp_dir, dirlistonly = FALSE)
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

clear_temp_folder <- function(folder_path) {
  if(!dir.exists(folder_path) && file.exists(folder_path)){
    file.remove(folder_path)
  }else{
    files_to_remove <- list.files(folder_path, full.names = TRUE)
    file.remove(files_to_remove)
  }
}

replace_file <- function(src, dest) {
  
  if (file.exists(dest)) {
    file.remove(dest)
  }
  
  success <- file.copy(src, dest, overwrite = TRUE)
  
  if (!success) {
    stop(sprintf("Failed to copy file from %s to %s", src, dest))
  }
}

read_patric_db <- function(patric_db, mo_name) {
  # Standardize microorganism name
  mo_name <- AMR::mo_name(mo_name)
  
  # Load the PATRIC database
  patric_database <- MIC::load_patric_db(patric_db)
  
  # Load mapping of genome names to standard microorganism names
  mo_standard_mapping <- readRDS(MO_STD_MAPPING)
  
  # #Re-calculate standard mapping file
  if ( ! mo_name %in% mo_standard_mapping$std_mo_name ) {
    mo_standard_mapping <- update_standard_mapping(patric_database)
  }
  
  # Join PATRIC data with standard mapping on genome_name
  patric_database <- dplyr::left_join(patric_database, mo_standard_mapping, by = "genome_name")
  
  # Filter rows matching the standardized microorganism name
  patric_database <- patric_database[patric_database$std_mo_name == mo_name, ]
  
  return(patric_database)
}

#Update standard mo mapping file if found new genome name
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
