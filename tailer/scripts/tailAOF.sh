#!/bin/bash

while read line; do
	if [[ $line == "ZADD"* ]]
	then
		read garbage;
		read id;
		id2=`echo $id|tr -d '\r'`
		read garbage2;
		read score;
		score2=`echo $score|tr -d '\r'`
		json=`printf '{"id":"%s","score":%i}\n' "$id2" "$score2"`;
		amqp-publish -e $1 -r $2 -b \'$json\' --url $3;
	fi
done

