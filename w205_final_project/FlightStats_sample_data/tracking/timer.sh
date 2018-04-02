#!/bin/bash

rm -r /tmp/streaming
mkdir /tmp/streaming

for f in *.json
do
	cp -v "$f" /tmp/streaming
	sleep 10s
done

