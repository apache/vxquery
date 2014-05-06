#!/bin/bash
LOG_FILE=logs/top.log

# Reset counters
iostat >> /dev/null
sar -n DEV 1 1  >> /dev/null

# Save IO, CPU and Network snapshot to a log file.
while (sleep 10)
do
	echo "---------------------------------------------" >> ${LOG_FILE}
	date >> ${LOG_FILE}
	echo >> ${LOG_FILE}
	iostat >> ${LOG_FILE}
	top -n 1 -b | head -11 | tail -6 >> ${LOG_FILE}
	sar -n DEV 1 1 >> ${LOG_FILE}
done;