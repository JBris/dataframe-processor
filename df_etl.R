#!/usr/bin/env Rscript

##################################################
# Libraries
##################################################

source("df_etl_lib/config.R")
source("df_etl_lib/data.R")
source("df_etl_lib/pipelines.R")
source("df_etl_lib/utils.R")

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
    help = "Pipeline configuration file.", 
    metavar = "character"
  ),
  make_option (
    c("-e", "--env"), 
    type = "character", 
    default = NULL, 
    help = "The environment variable file.", 
    metavar = "character"
  ),
  make_option(
    c("-o", "--out_dir"), 
    type = "character", 
    default = OUT_SUBDIR, 
    help = "Subdirectory name for outputs.", 
    metavar = "character"
  ),
  make_option(
    c("-p", "--prehook"), 
    type = "character", 
    default = NULL, 
    help = "A prehook script for modifying and extending the pipeline's default functionality.", 
    metavar = "character"
  ),
  make_option(
    c("-q", "--posthook"), 
    type = "character", 
    default = NULL, 
    help = "A posthook script for modifying the processed dataframes before merging and saving occurs.", 
    metavar = "character"
  ),
  make_option(
    c("-d", "--delete_out"), 
    action = "store_true",
    default = F, 
    help = "Delete the contents of the output directory before running the pipeline."
  )
) 

##################################################
# Main
##################################################

main = function() {
  # Read args
  opt_parser = OptionParser(option_list = option_list)
  args = parse_args(opt_parser)
  load_env_vars(args$env)
  call_hook(args$prehook, "Prehook")
  clear_out_dir(args$delete_out)
  
  # Load configuration
  CONFIG = get_config(args$config)
  CONFIG = validate_config(CONFIG, MERGE_BY_OPTIONS)

  # Create output directory
  out_dir_name = str_c(args$out_dir, "_", CONFIG$name)
  out_dir = create_out_dir(out_dir_name)
  copy_config(args, out_dir)
  
  # Execute pipeline
  processed_items <<- execute_pipeline(CONFIG)
  call_hook(args$posthook, "Posthook")
  save_dfs(processed_items, out_dir, CONFIG)
  message("Pipeline completed.")
} 

main()