library(PGSE)
library(dplyr)
source(here::here("R/constants.R"))

# Load Parameters
main_config <- yaml::yaml.load_file(ETL_CONFIG_FILE)
# Select config automatically
env <- if(is_hpc()) "prod" else "dev"
app_config <- main_config[[env]]

config = yaml::yaml.load_file(file.path(app_config$LABELS_OUTPUT_FOLDER,META_LABEL_FILE))
K_Values = c(6, 8, 10)

#' This function  iterates through a set of microorganism and antibiotic
#' configurations defined in a YAML file and triggers training using the `pgse-train` command-line tool.
#'
#' @details
#' The training process:
#' \itemize{
#'   \item Loads configuration from `META_LABEL_CONFIG` YAML file.
#'   \item For each microorganism and antibiotic combination:
#'   \itemize{
#'     \item Cleans up names (spaces and slashes).
#'     \item Constructs paths for label files and training output.
#'     \item Iterates over multiple values of K (e.g., 6, 8, 10).
#'     \item Runs `pgse-train` with appropriate arguments.
#'   }
#'   \item Outputs model progress files and result exports into organized directories.
#' }
#'
cmd <- "pgse-train"
train_all_models <- function() {
  # Iterate over micro-organisms
  for (mo_name in names(config$`micro-organism`)) {
    abx_list <- config$`micro-organism`[[mo_name]]
    
    for (abx_name in names(abx_list)) {
      label_file <- abx_list[[abx_name]]$label_file
      data_dir   <- abx_list[[abx_name]]$data_dir
      
      # Standardize names
      mo_name_clean <- stringr::str_replace_all(mo_name, " ", "-")
      abx_name_clean <- abx_name %>%
        stringr::str_replace_all(" ", "_") %>%
        stringr::str_replace_all("/", "_")
      
      for (k in K_Values) {
          export_file <- file.path(
            "data", "output", mo_name_clean, paste0("K", k, "/")
          )
          
          message(paste0("Training started: Microorganism = ", mo_name_clean, 
                         ", Antibiotic = ", abx_name_clean))
          message(paste0("label_file: ", label_file))
          message(paste0("data_dir: ", data_dir))
          message(paste0("export_file: ", export_file))
          
          # Define file paths
          progress_file <- paste0(export_file, abx_name_clean, "_K", k, ".save")
          export_result <- paste0(export_file, "result-K", k, "-", abx_name_clean)
          
          # Create directory
          dir.create(export_file, showWarnings = FALSE, recursive = TRUE)
  
          
          # Build arguments for system call
          args <- c(
            "--label-file", label_file,
            "--data-dir", data_dir,
            # "--save-file", progress_file,
            "--export-file", export_result,
            "--workers", "8",
            "--features", "10000",
            "--dist", "0",
            "--k", as.character(k),
            "--target", "70",
            "--ext", "2",
            "--lr", "0.001",
            "--num-rounds", "6000",
            "--folds", "5"
            # "--ea-max", "64",
            # "--ea-min", "0"
          )
          train_model(args)
          message(paste0("Training completed with K = ", k))
      }
    }
  }
}

train_model <-  function(args){
  
  command <- paste(
    "source ~/miniconda3/etc/profile.d/conda.sh &&",
    "conda activate pgse &&",
    "pgse-train",
    paste(args, collapse = " ")
  )
  
  system2("bash", args = c("-c", shQuote(command)))
}
train_all_models()