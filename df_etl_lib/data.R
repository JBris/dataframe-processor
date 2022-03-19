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

clear_out_dir = function(args, main_dir = "out") {
    if(!args$delete_out) {
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