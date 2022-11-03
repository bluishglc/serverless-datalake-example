#!/usr/bin/env bash

parseIni() {
    ini="$(<$1)"                # read the file
    oldifs=$IFS
    IFS=$'\n' && ini=( ${ini} ) # convert to line-array
    ini=( ${ini[*]//;*/} )      # remove comments with ;
    ini=( ${ini[*]/\    =/=} )  # remove tabs before =
    ini=( ${ini[*]/=\   /=} )   # remove tabs be =
    ini=( ${ini[*]/\ =\ /=} )   # remove anything with a space around =
    ini+=("[CFG_END]") # dummy section to terminate
    ini_sections=()
    sections=()
    section=CFG_NULL
    vals=""
    for line in "${ini[@]}"; do
      #echo $line
      if [ "${line:0:1}" == "[" ] ; then
        #echo "section mark"
        # close previous section
        eval "ini_${section}+=(\"$vals\")"
        #eval echo "ini_$section[@]"
        if [ "$line" == "[CFG_END]" ]; then
          break
        fi
        # new section
        section=${line#[}
        section=${section%]}
        #echo "section: $section"
        secs="${sections[*]}"
        if [ "$secs" == "${secs/$section//}" ] ; then
          sections+=($section)
          eval "ini_${section}=()"
        fi
        vals=""
        continue
      fi
#      key="${section}_${line%%=*}"
      key=${line%%=*}
      value=${line#*=}
      value=${value//\"/\\\"}
      if [ "$vals" != "" ] ; then
        vals+=" "
      fi
      vals+="$key='$value'"
    done
    eval "ini_sections=(\${sections[@]})"
    IFS=$oldifs
}

# read number of keys (subsections) in a given section
parseIniSectionKeys() {
  eval "keys=(\${!ini_$1[@]})"
}

resetIniItems() {
    srcAction=""
    srcTable=""
    action=""
    sqlFiles=""
    sqlParams=""
    sinkAction=""
    sinkTable=""
    hudiRecordKeyField=""
    hudiPrecombineField=""
    hudiPartitionPathField=""
}

# read in settings for a specific key in a given section
parseIniSection () {
    resetIniItems
    section=$1
    key=$2
    if [ "$key" == "" ] ; then
    key=0
    fi
    eval "vals="
    eval "vals=\${ini_$section[$key]}"
    eval $vals
}

# read and merge all settings for all keys of a given section
parseIniSectionMerge() {
    parseIniSectionKeys $1
    for key in "${keys[@]}"; do
        parseIniSection $section $key
    done
}

