
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
