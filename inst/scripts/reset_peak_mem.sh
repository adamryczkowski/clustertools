#!/usr/bin/env bash
dir_resolve()
{
	cd "$1" 2>/dev/null || return $?  # cd to desired directory; if fail, quell any error messages but return exit status
	echo "`pwd -P`" # output full, link-resolved path
}
exec_mypath=${0%/*}
exec_mypath=$(dir_resolve $exec_mypath)
cd $exec_mypath

echo "reset">>script_control

