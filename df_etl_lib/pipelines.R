##################################################
# Library
##################################################

execute_pipeline = function(config) {
    data_definitions = config$data

    data_keys = names(data_definitions)
    destination_lists = list()

    for(data_definition in data_definitions) {
        data_destination = data_definition$destination
        data_writer = data_definition$writer$name
        data_merge_by = data_definition$merge_by$name
        destination_key = str_interp("${data_destination}_${data_writer}_${data_merge_by}")
        if(is.null(destination_lists[[destination_key]])) {
            destination_lists[[destination_key]] = list(
                destination = data_destination,
                writer = data_definition$writer,
                merge_by = data_definition$merge_by,
                dfs = list()
            )
        }
    }

    for(data_key in data_keys) {
        message(str_c("Processing data definition: ", data_key))
        data_definition = data_definitions[[data_key]]
        data_source = data_definition$source
        data_reader = data_definition$reader
        data_destination = data_definition$destination
        data_writer = data_definition$writer$name
        data_merge_by = data_definition$merge_by$name
        destination_key = str_interp("${data_destination}_${data_writer}_${data_merge_by}")

        if(!file.exists(data_source)) {
            stop(str_interp("Data source '${data_source}' does not exist for '${data_key}' data definition."))
        }
        
        df = do.call(data_reader$name, c(data_source, data_reader$args))
        df = process_df(df, data_definition$prepipeline, data_key)
        processed_df = process_pipeline(df, data_definition$pipeline, data_key)
        processed_df = process_df(processed_df, data_definition$postpipeline, data_key)
        destination_lists[[destination_key]][["dfs"]][[data_key]] = processed_df
    }
    destination_lists
}

process_df = function(df, steps, data_key) {
    if(length(steps) == 0) {
        return(df)
    } 

    for(step in steps) {
        if(!is.null(step$eval)) {
            df = eval(str2expression(step$eval))
        } else {
            df = do.call(step$name, c(list(df), step$args))  
        }
    }

    df
} 

process_pipeline = function(df, pipeline, data_key) {
    if(length(pipeline) == 0) {
        return(df)
    }    
    df = process_stage(df, pipeline, "preprocess", data_key = data_key)
    df = process_stage(df, pipeline, "map", new_df = T, data_key = data_key)
    df = process_stage(df, pipeline, "postprocess", data_key = data_key)
    df
}

process_stage = function(df, pipeline, stage_key, new_df = F, data_key = "") {
    stage = pipeline[[stage_key]]
    if(is.null(stage)) {
        return(df)
    }
    
    if(new_df) {
        processing_df = tibble(.rows = nrow(df))
    } 
    
    for(stage_item in stage) {
        source_name = stage_item$source
        destination_name = stage_item$destination
        steps = stage_item$steps

        if(length(source_name) == 1) {
            data_source = df[[source_name]] 
        } else {
            data_source = df[source_name] 
        }    
        
        for(step in steps) {
            if(!is.null(step$eval)) {
                data_source = eval(str2expression(step$eval))
            } else {
                data_source = do.call(step$name, c(list(data_source), step$args))  
            }
        }

        data_source = as_tibble(data_source)
        data_ndims = ncol(data_source)
        if(data_ndims == 1) {        
            if(new_df) {
                processing_df[destination_name] = data_source
            } else {
                df[destination_name] = data_source
            }
        } else {
            stop(str_interp(
                "Invalid number of dimensions '${data_ndims}' for destination '${destination_name}' in stage ${stage_key} for '${data_key}' data definition."
            )) 
        }
        
    }

    if(new_df) {
        processing_df  
    } else {
        df
    }
}

save_dfs = function(processed_items, out_dir, config) {
    saved_df_list = list()
    for(processed_item in processed_items) {
        data_destination = processed_item$destination
        data_writer = processed_item$writer
        merge_by = processed_item$merge_by
        dfs = unname(processed_item$dfs)
        out_file = file.path(out_dir, data_destination)
        merged_dfs = do.call(merge_by$name, list(dfs, merge_by$args))
        do.call(data_writer$name, c(list(merged_dfs), out_file, data_writer$args))
        saved_df_list[[data_destination]] = list(destination = data_destination, df = merged_dfs)
        message(str_c("Created file: ", out_file))
    }
    saved_df_list
}
