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

  for(str_item in list("name", "destination")) {
    if(!is.character(config[[str_item]])) {
      stop(str_c("Configuration item must be a string: ", str_item))
    }
  }

  for(list_item in list("data")) {
    if(!is.list(config[[list_item]])) {
      stop(str_c("Configuration item must be a list: ", list_item))
    }
  }

  for(func_item in list("reader", "writer", "merge_by")) {
    func_definition = config[[func_item]]
    config[[func_item]] = validate_func_definition(func_definition, func_item)
  }

  config$data = validate_data_def(config)
  config
}

validate_func_definition = function(func_definition, func_item) {
  if(!is.list(func_definition)) {
    stop(str_c("Function definition must be a list: ", func_item))
  }

  if(!is.null(func_definition$eval)) {
    return(func_definition)
  }
  
  if(is.null(func_definition$name)) {
    stop(str_c("Function definition requires a name: ", func_item))
  }

  if(!is.character(func_definition$name)) {
    stop(str_c("Function definition name must be a string: ", func_item))
  }

  if(is.null(func_definition$args)) {
    func_definition$args = list()
  }

  if(!is.list(func_definition$args)) {
    stop(str_c("Function definition arguments must be a list: ", func_item))
  }

  func_definition
}

validate_data_def = function(config) {
  mandatory_keys = list("source", "pipeline")
  optional_keys = list("reader", "destination", "writer", "merge_by")
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

    for(str_item in list("source", "destination")) {
      if(!is.character(data_definition[[str_item]])) {
        stop(str_interp("Element '${str_item}' in data definition '${data_key}' must be a string."))
      }
    }

    for(func_item in list("reader", "writer")) {
      if(!is.list(data_definition[[func_item]])) {
        stop(str_interp("Element '${func_item}' in data definition '${data_key}' must be a list."))
      }
      data_definition[[func_item]] = validate_func_definition(
        data_definition[[func_item]], 
        str_interp("'${func_item}' in data definition '${data_key}'.")
      )
    }

    data_definition$pipeline = validate_pipeline(data_definition$pipeline, data_key)
    data_definitions[[data_key]] = data_definition
  }

  data_definitions
}

validate_pipeline = function(data_pipeline, data_key) {
    if(!is.list(data_pipeline)) {
      stop(str_interp("Pipeline in data definition '${data_key}' must be a list of data processors."))
    }

    if(length(data_pipeline) == 0) {
        return(data_pipeline)
    }

    stage_keys = c("preprocess", "map", "postprocess")
    stages = names(data_pipeline)

    stage_intersect = Reduce(intersect, list(stage_keys, stages))
    if(length(stage_intersect) == 0) {
        return(df)
    }

    for(i in seq_along(stage_keys)) {
      stage_key = stage_keys[[i]]
      stage = data_pipeline[[stage_key]]
      
      for(j in seq_along(stage)) {
        stage_item = stage[[j]]
        
        if(is.null(stage_item$source)) {
          stop(str_interp("Data source in item '${j}' in stage '${stage_key}' in data definition '${data_key}' is required."))
        }
        if(!is.character(stage_item$source)) {
          stop(
            str_interp("Data source in item '${j}' in stage '${stage_key}' in data definition '${data_key}' must be a string or list of strings.")
          )
        }

        if(is.null(stage_item$destination)) {
          if(length(stage_item$source) == 1) {
            stage_item$destination = stage_item$source
          } else {
            stop(str_interp("Data destination in item '${j}' in stage '${stage_key}' in data definition '${data_key}' must be provided."))
          }
        }

        if(is.null(stage_item$steps)) {
          stage_item$steps = list()
        }

        if(!is.list(stage_item$steps)) {
          stop(str_interp("Data steps in item '${j}' in stage '${stage_key}' in data definition '${data_key}' must be a list."))
        }

        for(k in seq_along(stage_item$steps)) {
          step = stage_item$steps[[k]]
          step = validate_func_definition(
            step, 
            str_interp("Step '${k}' of data steps in item '${i}' in stage '${stage_key}' in data definition '${data_key}'.")
          )
          stage_item$steps[[k]] = step
        }
        stage[[j]] = stage_item
      }
      
      data_pipeline[[stage_key]] = stage
    }
    
    data_pipeline    
}