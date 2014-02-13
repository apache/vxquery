# Run all the queries and save a log. 
# First argument: Supply the folder which houses all the queries (recursive).
# Second argument: adds options to the VXQuery CLI.
#
# run_benchmark.sh ./noaa-ghcn-daily/benchmarks/local_speed_up/queries/
# run_benchmark.sh ./noaa-ghcn-daily/benchmarks/local_speed_up/queries/ "-client-net-ip-address 169.235.27.138"
#

if [ -z "${1}" ]
then
    echo "Please supply a directory for query files to be found."
    exit
fi

for j in $(find ${1} -name '*.xq')
do
	echo "Running query: ${j}"
	time sh ./vxquery-cli/target/appassembler/bin/vxq ${j} ${2} -timing -showquery -frame-size 1000000 -repeatexec 10 > ${j}.log 2>&1
done

