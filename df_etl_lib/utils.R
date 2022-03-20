##################################################
# Library
##################################################

call_prehook = function(prehook) {
    if(!is.character(prehook)) {
        return()
    }

    if(!file.exists(prehook)) {
        stop(str_c("Prehook file does not exist: ", prehook))
    }

    source(prehook)
}
