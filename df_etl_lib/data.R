##################################################
# Library
##################################################

get_subdir_prefix = function() {
    current_date = Sys.Date() %>% format("%Y_%m_%d")
    current_time = Sys.time() %>% format("%H_%M")
    out_subdir = str_c(current_date, "_", current_time)
}

create_out_dir = function(sub_dir, main_dir = "out", showWarnings = F) {
    out_dir = file.path(main_dir, sub_dir)
    dir.create(out_dir, showWarnings = showWarnings)
    out_dir
}

clear_out_dir = function(delete_out, main_dir = "out") {
    if(!delete_out) {
        return()
    }

    list.dirs(main_dir, recursive = F) %>%
        (function(out_dir) {
            if(length(out_dir) == 0) {
                return()
            }

            delete_res = unlink(out_dir, recursive = T)
            if(delete_res == 0) {
                message(str_c("Deleted directory: ", out_dir, "\n"))
            } else {
                message(str_c("Failed to delete directory: ", out_dir, "\n"))
            }
        })(.)
}


perform_eda = function(merged_df_list, output_dir) {
    library(DataExplorer)

    message("Performing EDA...")
    for(merged_df_data in merged_df_list) {
        destination = merged_df_data$destination
        df = merged_df_data$df
        output_file = str_interp("${destination}.html")
        create_report(df, output_file = output_file, output_dir = file.path(getwd(), output_dir), quiet = T)
        output_file = file.path(output_dir, output_file)
        message(str_interp("Created ${output_file}"))
    }
}
