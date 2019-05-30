#!/bin/bash
# sample run: ./csv.sh -hc=src/main/resources/hazelcast-client.xml -mn=employees -if=name -v src/main/resources/data.csv
java -jar target/csvToIMDG-1.0-SNAPSHOT-jar-with-dependencies.jar $1 $2 $3 $4 $5 $6 $7 $8
