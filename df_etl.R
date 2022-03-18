#!/usr/bin/env Rscript

##################################################
# Libraries
##################################################

source("df_etl_lib/config.R")
source("df_etl_lib/data.R")
source("df_etl_lib/pipelines.R")

##################################################
# Imports
##################################################

options(
  warn = -1, 
  readr.num_columns = 0
)

library(optparse)
suppressPackageStartupMessages(library(tidyverse)) 

##################################################
# Constants
##################################################

# Output file prefix
OUT_SUBDIR = get_subdir_prefix()

##################################################
# Option list
##################################################

option_list = list(
  make_option(
    c("-c", "--config"), 
    type = "character", 
    default = "config.yaml", 
    help = "Pipeline configuration file", 
    metavar = "character"
  ),
  make_option(
    c("-o", "--out_dir"), 
    type = "character", 
    default = OUT_SUBDIR, 
    help = "Subdirectory name for outputs", 
    metavar = "character"
  )
) 

##################################################
# Main
##################################################

main = function() {
  # Read args
  opt_parser = OptionParser(option_list = option_list)
  args = parse_args(opt_parser)
  
  # Load configuration
  CONFIG = get_config(args$config)
  CONFIG = validate_config(CONFIG, MERGE_BY_OPTIONS)

  # Create output directory
  out_dir_name = str_c(args$out_dir, "_", CONFIG$name)
  out_dir = create_out_dir(out_dir_name)
  copy_config(args, out_dir)

  # Execute pipeline
  processed_dfs = execute_pipeline(CONFIG$data, READER_PLUGINS, WRITER_PLUGINS)
  save_dfs(processed_dfs, out_dir, WRITER_PLUGINS, CONFIG$merge_by)
  message("Pipeline completed.")
} 

main()