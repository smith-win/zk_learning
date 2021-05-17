#!/bin/bash


## https://opensource.com/article/19/10/programming-bash-logical-operators-shell-expansions
## -x includes the file if it is executable
## 

## Find executable
EXEC_FILE=""

for f in target/release/*
do

	if [[ -f $f ]]  && [[ -x $f ]] 
	then
		echo [${f}] is a file and executable
	fi
done

