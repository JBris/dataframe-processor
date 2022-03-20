##################################################
# Library
##################################################

call_hook = function(hook, hook_type) {
    if(!is.character(hook)) {
        return()
    }

    if(!file.exists(hook)) {
        stop(str_interp("${hook_type} file does not exist: ${hook}"))
    }

    source(hook)
}
