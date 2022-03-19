##################################################
# Library
##################################################

READER_PLUGINS = list(
    csv = readr::read_csv,
    tsv = readr::read_tsv
)

WRITER_PLUGINS = list(
    csv = readr::write_csv,
    tsv = readr::write_tsv
)

MERGE_BY_OPTIONS = list("col", "row")

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
            delete_res = unlink(out_dir, recursive = T)
            if(delete_res == 0) {
                message(str_c("Deleted directory: ", out_dir, "\n"))
            }
        })(.)
}