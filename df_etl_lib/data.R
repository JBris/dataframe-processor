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