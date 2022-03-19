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
        data_definition = data_definitions[[data_key]]
        data_source = data_definition$source
        data_reader = data_definition$reader
        data_destination = data_definition$destination
        data_writer = data_definition$writer$name
        data_merge_by = data_definition$merge_by$name
        destination_key = str_interp("${data_destination}_${data_writer}_${data_merge_by}")

        if(!file.exists(data_source)) {
            stop(str_interp("Data source '${data_source}' does not exist for '${data_key}' data definition.", ))
        }
        
        df = do.call(data_reader$name, c(data_source, data_reader$args))
        processed_df = process_df(df, data_definition$pipeline)
        destination_lists[[destination_key]][["dfs"]][[data_key]] = processed_df
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

save_dfs = function(processed_items, out_dir, config) {
    for(processed_item in processed_items) {
        data_destination = processed_item$destination
        data_writer = processed_item$writer
        merge_by = processed_item$merge_by
        dfs = unname(processed_item$dfs)
        out_file = file.path(out_dir, data_destination)
        merged_dfs = do.call(merge_by$name, list(dfs, merge_by$args))
        do.call(data_writer$name, c(list(merged_dfs), out_file, data_writer$args))
        message(str_c("Created file: ", out_file))
    }

    
    # for(writer_key in writer_keys) {
    #     split_key = stringi::stri_reverse(writer_key) %>%
    #         str_split("_", n = 2, simplify = T) %>%
    #         map_chr(stringi::stri_reverse)

    #     dfs = processed_dfs[[writer_key]]
    #     if(merge_by == "col") {
    #         merged_dfs = bind_cols(dfs)
    #     } else {
    #         merged_dfs = bind_rows(dfs)
    #     }

    #     writer_plugin = split_key[1]
    #     data_destination = split_key[2]
    #     out_file = file.path(out_dir, data_destination)
    #     writer_func = writer_plugins[[writer_plugin]]
    #     writer_func(merged_dfs, out_file)
    #     message(str_c("Created file: ", out_file))
    # }
    
}
