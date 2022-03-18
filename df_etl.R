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

options(warn = -1)

library(optparse)
library(tidyverse)

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
  CONFIG = validate_config(CONFIG)

  # Create output directory
  out_dir_name = str_c(args$out_dir, "_", CONFIG$name)
  out_dir = create_out_dir(out_dir_name)
  file.copy(args$config, out_dir, overwrite = T)

  print(CONFIG)
  message("Hello")
  

} 

main()