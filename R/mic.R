
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

#Customized function from MIC package for more control
pull_PATRIC_genome <- function(output_directory, 
                               genome_id) {
  
  genome_path <- glue::glue(
    "ftp://ftp.patricbrc.org/genomes/{genome_id}/{genome_id}.fna"
  )
  
  target_path <- file.path(output_directory,
                           glue::glue("{genome_id}.fna"))
  
  
  if (file.exists(target_path)) {
    message(glue::glue("Genome {genome_id} already exists"))
    return(TRUE)
  } else {
    message(glue::glue("Downloading file..."))
    message(genome_path)
    status <- tryCatch(
      {
        utils::download.file(genome_path,
                             destfile = target_path,
                             mode = "wb")
        TRUE
      },
      error = function(e) {
        message(glue::glue("Unable to download {genome_id}"))
        FALSE
      }
    )
  }
  return(status)
}

