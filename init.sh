#!/usr/bin/env bash

###################################################################
# Functions
###################################################################

usage() {
    echo """
    Usage: ${0} -c -e 

    Options:
        c   Recreate the config.yaml file from config.yaml.example.
        e   Recreate the .env file from .env.example.
    """
}

help() {
    if [ "${1}" = "-h" ] || [ "${1}" = "-help" ]; then
        usage
        exit 1
    fi
}

create_file () {
    if [ ! -f "${1}" ]; then 
        cp "${2}" "${1}" 
        echo "Created ${1}"
    fi
}

###################################################################
# Main
###################################################################

help "${1}"

create_file config.yaml config.yaml.example
create_file .env .env.example

while getopts "ce" opt; do
    case $opt in
        c)
            create_file config.yaml config.yaml.example
            ;;
        e) 
            create_file .env .env.example
            ;;
        *) 
            echo 
            ;;
  esac
done

shift $(($OPTIND - 1))
echo "Done"
