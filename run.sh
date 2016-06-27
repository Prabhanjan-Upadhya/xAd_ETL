#! /bin/sh

if [[ $# -eq 0 ]]
then
./test.py
elif [[ $1 == "-h" ]]
then
echo "./run.sh <parallelism factor>"
elif [[ $1 == "-clean" ]]
then
rm -rf logs/ out/ *pyc
else
./test.py -p $1
fi
