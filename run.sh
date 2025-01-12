#!/bin/bash

# Check that at least one argument is provided
if [ $# -lt 1 ]
then
    echo "Usage: $0 [yarn|local]"
    exit 1
fi

# Check that the argument is valid
if [ "$1" != "yarn" ] && [ "$1" != "local" ]
then
    echo "Usage: $0 [yarn|local]"
    exit 1
fi

# Check that spark-submit is available
if ! command -v spark-submit 2>&1 >/dev/null
then
    echo "spark-submit could not be found"
    exit 1
fi

# Check that the files are available
if [ ! -f cite75_99.txt ]
then
    # Check if file parts are available
    if [ ! -f cite75_9900 ] || [ ! -f cite75_9901 ] || [ ! -f cite75_9902 ]
    then
        echo "The file cite75_99.txt is not available"
        exit 1
    else
        echo "The file cite75_99.txt is not available, but the parts are. Merging them"
        cat cite75_9900 cite75_9901 cite75_9902 > cite75_99.txt
    fi

fi

if [ ! -f apat63_99.txt ]
then
    if [ ! -f apat63_9900 ] || [ ! -f apat63_9901 ] || [ ! -f apat63_9902 ]
    then
        echo "The file apat63_99.txt is not available"
        exit 1
    else
        echo "The file apat63_99.txt is not available, but the parts are. Merging them"
        cat apat63_9900 apat63_9901 apat63_9902 > apat63_99.txt
    fi
fi

if [ ! -f country_codes.txt ]
then
    echo "The file country_codes.txt is not available"
    exit 1
fi

# If the flag is yarn, the flags variable should be: --master yarn --num-executors 8 --driver-memory 4g --queue urgent
# else the flags variable should be: --master local[*] --driver-memory 4g

if [ "$1" == "yarn" ]
then
    flag="--master yarn --num-executors 8 --driver-memory 4g --queue urgent"
    hdfs dfs -rm -r dfCitas.parquet dfInfo.parquet p2out p3out p4out p5out
    hdfs dfs -put cite75_99.txt apat63_99.txt
else
    flag="--master local[*] --driver-memory 4g"
    rm -rf dfCitas.parquet dfInfo.parquet p2out p3out p4out p5out
fi

# Run the exercises, beware, you will need a lot of ram!
#Hide the output of the spark-submit command
echo "Running the exercise 1"
spark-submit $flag p1.py cite75_99.txt apat63_99.txt dfCitas.parquet dfInfo.parquet
echo "Running the exercise 2"
spark-submit $flag p2.py dfCitas.parquet/ dfInfo.parquet/ ./country_codes.txt p2out
echo "Running the exercise 3"
spark-submit $flag p3.py apat63_99.txt p3out
echo "Running the exercise 4"
spark-submit $flag p4.py dfCitas.parquet/ dfInfo.parquet/ ES,FR p4out
echo "Running the exercise 5"
spark-submit $flag p5.py dfInfo.parquet/ p5out
echo "All exercises have been run"