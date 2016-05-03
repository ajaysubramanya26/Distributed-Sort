#! /bin/bash

cd ../netty-server/
cp params job.config
echo -n "FILES_TO_READ=" >> job.config
BUCKET_PATH=`grep "BUCKET_NAME=" job.config | cut -d'=' -f2`
OUTPUT_EXISTS=`aws s3 ls s3://${BUCKET_PATH}/OutputA9 | wc -l`

if [ $OUTPUT_EXISTS -eq 0 ]; then
	mvn clean package
	aws s3 rm s3://${BUCKET_PATH}/netty_server.jar || true
	aws s3 rm s3://${BUCKET_PATH}/job.config || true
	aws s3 rm s3://${BUCKET_PATH}/log4j.properties || true
	aws s3 cp target/netty_server.jar s3://${BUCKET_PATH}/ 
	aws s3 cp job.config s3://${BUCKET_PATH}/ 
	aws s3 cp log4j.properties s3://${BUCKET_PATH}/ 

	cd ../netty-client/
	mvn clean package
	aws s3 rm s3://${BUCKET_PATH}/netty_client.jar || true
	aws s3 cp target/netty_client.jar s3://${BUCKET_PATH}/

	cd ../Setup/
	./masterSlaveScripts.sh ${BUCKET_PATH}

	java -jar target/setup.jar bootstrap.txt  masterScript.txt  slaveScript.txt ${1} ${BUCKET_PATH}
else
	echo "Error :: S3 Output folder already exists"
fi
