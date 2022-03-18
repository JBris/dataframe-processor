##################################################
# Imports
##################################################

library(yaml)

##################################################
# Library
##################################################

get_config = function(config_file) {
  if(!file.exists(config_file)) {
    stop(str_c("Configuration file does not exist: ", config_file))
  }
  config = read_yaml(config_file)
}

copy_config = function(args, out_dir, overwrite = T) {
  file.copy(args$config, out_dir, overwrite = overwrite)
}

validate_config = function(config, merge_by_options) {
  mandatory_keys = list("name", "reader", "destination", "writer", "data", "merge_by")
  config_keys = names(config)

  for(mandatory_key in mandatory_keys) {
    if (!mandatory_key %in% config_keys) { 
      stop(str_c("Mandatory key is missing in configuration: ", mandatory_key)) 
    }
  }

  for(str_item in list("name", "reader", "destination", "writer", "merge_by")) {
    if(!is.character(config[[str_item]])) {
      stop(str_c("Configuration item must be a string: ", str_item))
    }
  }

  merge_by = config$merge_by
  merge_by_options_str = toString(merge_by_options)
  if(!merge_by %in% merge_by_options) {
    stop(str_interp("Configuration value '${merge_by}' for merge_by is invalid. Must be one of '[${merge_by_options_str}]'."))
  }

  if(!is.list(config$data)) {
    stop("Configuration data definitions must be a list.")
  }

  config$data = validate_data_def(config)
  config
}

validate_data_def = function(config) {
  mandatory_keys = list("source")
  optional_keys = list("reader", "destination", "writer")
  data_definitions = config$data
  data_keys = names(data_definitions)

  for(data_key in data_keys) {
    data_definition = data_definitions[[data_key]]
    definition_keys = names(data_definition)

    for(mandatory_key in mandatory_keys) {
      if (!mandatory_key %in% definition_keys) { 
        stop(str_interp("Mandatory key '${mandatory_key}' is missing in data definition '${data_key}'"))
      }
    }

    for(optional_key in optional_keys) {
      if (!optional_key %in% definition_keys) { 
        data_definition[[optional_key]] = config[[optional_key]]
      }
    } 

    for(str_item in list("source", "reader", "destination", "writer")) {
      if(!is.character(data_definition[[str_item]])) {
        stop(str_interp("Element '${str_item}' in data definition '${data_key}' must be a string."))
      }
    }

    data_definitions[[data_key]] = data_definition
  }

  data_definitions
}