#!/usr/bin/env Rscript

##################################################
# Libraries
##################################################

source("df_etl_lib/data.R")
source("df_etl_lib/pipelines.R")

##################################################
# Imports
##################################################

options(warn = -1)

library(optparse)
library(tidyverse)
library(yaml)

options(warn = 0)

##################################################
# Constants
##################################################

# Output file prefix
CURRENT_DATE = Sys.Date() %>% format("%Y_%m_%d")
CURRENT_TIME = Sys.time() %>% format("%H_%M")
OUT_SUBDIR = str_c(CURRENT_DATE, "_", CURRENT_TIME)

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
  )
) 

##################################################
# Main
##################################################

main = function() {
  # Read args
  opt_parser = OptionParser(option_list = option_list)
  args = parse_args(opt_parser)

  message("Hello")
  
} 

main()