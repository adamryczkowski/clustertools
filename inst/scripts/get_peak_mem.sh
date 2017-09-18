#!/usr/bin/env bash
dir_resolve()
{
	cd "$1" 2>/dev/null || return $?  # cd to desired directory; if fail, quell any error messages but return exit status
	echo "`pwd -P`" # output full, link-resolved path
}
exec_mypath=${0%/*}
exec_mypath=$(dir_resolve $exec_mypath)
cd $exec_mypath

echo "get_peak_mem">>script_control
exec 8<>script_output

read -r -t 0.2 -u 8 MYMEM
if [[ -z $MYMEM ]]; then
	echo -1
else
	echo $MYMEM
fi
