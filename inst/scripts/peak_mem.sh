#!/usr/bin/env bash



if [[ -z "$1" ]]; then
	exit 1
fi
mypid="$1"

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

dir_resolve()
{
	cd "$1" 2>/dev/null || return $?  # cd to desired directory; if fail, quell any error messages but return exit status
	echo "`pwd -P`" # output full, link-resolved path
}
exec_mypath=${0%/*}
exec_mypath=$(dir_resolve $exec_mypath)
cd $exec_mypath


sizes() { /bin/ps -o rss= -p "$(pidtree $mypid)" | /usr/bin/awk '{sum+=$1} END {print sum}';}
peak=0

# trap ctrl-c and call ctrl_c()
trap ctrl_c INT

function ctrl_c() {
	if [ -p script_control ]; then
		rm script_control
	fi

	if [ -p script_output ]; then
		rm script_output
	fi
}

if [ ! -p script_control ]; then
	if [ -f script_control ]; then
		rm script_control
	fi
	mkfifo script_control
fi
exec 7<>script_control

if [ ! -p script_output ]; then
	if [ -f script_output ]; then
		rm script_output
	fi
	mkfifo script_output
fi
exec 8<>script_output

while sizes=$(sizes $mypid)
do
    set -- $sizes
    sample=$((${@/#/+}))
	if [[ "$sample" == "0" ]]; then
		break #If the process vanishes, we also exit
	fi
    let peak="sample > peak ? sample : peak"
	read -r -t 0.2 -u 7 RETVAL
	if [[ -n $RETVAL ]]; then
		case "$RETVAL" in
		get_peak_mem)
		    echo "$peak">&8
		    ;;
		get_current_mem)
			echo "$sample">&8
			;;
		stop)
		    break
		    ;;
		reset)
		    peak=0
		    ;;
		*)
		    echo $"Usage: send command by fifo 'script_fifo' one of the commands: {get_peak_mem|get_current_mem|stop|reset}"
		    exit 1
		esac
	fi
done

ctrl_c
