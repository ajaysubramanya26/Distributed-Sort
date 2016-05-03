#! /bin/bash

BUCKET="aws s3 cp s3://${1}/"
#masterScript
echo "#! /bin/bash" > masterScript.txt
echo "set -e -x" >> masterScript.txt
echo "${BUCKET}job.config ." >> masterScript.txt
echo "${BUCKET}log4j.properties ." >> masterScript.txt
echo "${BUCKET}netty_server.jar ." >> masterScript.txt
echo -n "java -jar netty_server.jar" >> masterScript.txt


#slaveScript
echo "#! /bin/bash" > slaveScript.txt
echo "set -e -x" >> slaveScript.txt
echo "${BUCKET}job.config ." >> slaveScript.txt
echo "${BUCKET}log4j.properties ." >> slaveScript.txt
echo "${BUCKET}netty_client.jar ." >> slaveScript.txt
echo -n "java -Xmx8000M -jar netty_client.jar" >> slaveScript.txt
