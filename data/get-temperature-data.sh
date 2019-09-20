#!/bin/bash

FILE="temp-sg.txt"
SLEEP=120

while true; do
	curl -X GET "https://api.data.gov.sg/v1/environment/air-temperature" -H  "accept: application/json" >> $FILE
	echo "" >> $FILE
	sleep $SLEEP
done

