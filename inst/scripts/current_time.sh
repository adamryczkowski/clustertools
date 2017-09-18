#!/bin/bash
#Returns space-delimited list of processes that are spawned by pid in argument+
#+ that PID
pidtree() (
    local out=""
    [ -n "$ZSH_VERSION"  ] && setopt shwordsplit
    declare -A CHILDS
    while read P PP;do
        CHILDS[$PP]+=" $P"
    done < <(ps -e -o pid= -o ppid=)

    walk() {
        out="$out $1"
        for i in ${CHILDS[$1]};do
            walk $i
        done
    }

    for i in "$@";do
        walk $i
    done
	echo $out
)

ps -p "$(pidtree $1)" -o time | awk -F '[ :]' '
  {sum += $NF + 60 * ($(NF-1) + 60 * $(NF-2))}
  END {print sum}'
