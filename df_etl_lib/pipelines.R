##################################################
# Library
##################################################

execute_pipeline = function(data_definitions, reader_plugins, writer_plugins) {
    data_keys = names(data_definitions)
    reader_keys = names(reader_plugins)
    writer_keys = names(writer_plugins)
    destination_lists = list()

    for(data_definition in data_definitions) {
        data_destination = data_definition$destination
        data_writer = data_definition$writer
        destination_key = str_c(data_destination, "_", data_writer)
        if(is.null(destination_lists[[destination_key]])) {
            destination_lists[[destination_key]] = list()
        }
    }

    for(data_key in data_keys) {
        data_definition = data_definitions[[data_key]]
        data_source = data_definition$source
        reader_plugin = data_definition$reader
        data_destination = data_definition$destination
        writer_plugin = data_definition$writer
        destination_key = str_c(data_destination, "_", writer_plugin)

        if(!reader_plugin %in% reader_keys) {
            stop(str_interp("Invalid reader plugin '${reader_plugin}' for '${data_key}' data definition."))
        }

        if(!writer_plugin %in% writer_keys) {
            stop(str_interp("Invalid writer plugin '${writer_plugin}' for '${data_key}' data definition."))
        }

        if(!file.exists(data_source)) {
            stop(str_interp("Data source '${data_source}' does not exist for '${data_key}' data definition.", ))
        }

        reader_func = reader_plugins[[reader_plugin]]
        df = reader_func(data_source)
        processed_df = process_df(df, data_definition$pipeline)
        destination_lists[[destination_key]][[data_key]] = processed_df
    }
    destination_lists
}

process_df = function(df, pipeline) {
    if(length(pipeline) == 0) {
        return(df)
    }

    # print(pipeline)
    df
}

save_dfs = function(processed_dfs, out_dir, writer_plugins, merge_by) {
    writer_keys = names(processed_dfs)

    for(writer_key in writer_keys) {
        split_key = stringi::stri_reverse(writer_key) %>%
            str_split("_", n = 2, simplify = T) %>%
            map_chr(stringi::stri_reverse)

        dfs = processed_dfs[[writer_key]]
        if(merge_by == "col") {
            merged_dfs = bind_cols(dfs)
        } else {
            merged_dfs = bind_rows(dfs)
        }

        writer_plugin = split_key[1]
        data_destination = split_key[2]
        out_file = file.path(out_dir, data_destination)
        writer_func = writer_plugins[[writer_plugin]]
        writer_func(merged_dfs, out_file)
        message(str_c("Created file: ", out_file))
    }
    
}
