##################################################
# Library
##################################################

load_env_vars = function(env_file) {
    if(is.null(env_file)) {
        return()
    }

    if(!file.exists(env_file)) {
        stop(str_interp("Environment variable file does not exist: ${env_file}"))
    }

    readRenviron(env_file)
}

call_hook = function(hook, hook_type) {
    if(!is.character(hook)) {
        return()
    }

    if(!file.exists(hook)) {
        stop(str_interp("${hook_type} file does not exist: ${hook}"))
    }

    source(hook)
}
