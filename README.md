# Distributed Sort

## Introduction 

This readme encompasses the prerequisites, instructions to run the code, 
design choice, performance, results and effort. The goal of the project 
is to mirror shuffle phase of Mapreduce operation using distributed sort 
for climate data set.

## Prerequisites

* Update the script

* We are using Apache Maven 3.3.9 for dependency management and build automation

* Please ```chmod +x buildAndCopyToAWS.sh``` and ```chmod +x masterSlaveScripts.sh```

* AWS CLI

* AWS Instance profile #link http://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-ec2_instance-profiles.html 
  the above instance profile should have full access to s3. the reason we use 
  this is to not pass credentials around while also granting full access to s3

* Update /Setup/bootstrap.txt with your details.

* Update /netty-server/params with your details.

## Instructions to run Distributed sort

* ```make setup3``` - for 2 node setup

* ```make setup9``` - for 8 node setup


## Design
The project consists of virtually three parts
* Setup ­ Resource manager	* Spawns as many nodes as requested by the user.	* Polls the machines for their state.	* Pushes the scripts needed to run master and slave instance on the
	  machines.*  Master ­ Handles the slaves	* Once started by the script “Setup” places on the ec2 instance, 
	  it wait for a slave to connect.	* When the master receives a ready message from the client, it sends 
	  it the metadata about the sort data. This is done intelligently 
	  without overloading any slave with too much data.	* When the master receives all the metadata about the sorted data 
	  from all slaves, it decides what chunks of data needs to be transmitted 
	  between slaves.	* The master is notified that all slaves are done at which point the master 
	  place a file by the name _SUCCESS to the output bucket on S3.* Slaves ­ Does the work master assigned	* Once started by the boot­up script , sends a ready message to themaster.	* When the slaves receives the data that it needs to sort it does the sortlocally and send metadata about the sorted data to the master.
	* When it receives the instruction to send the data to other slaves from 
	  the master, it sends the data and wait to receive data.	* Once all the slaves are done sending and receiving data the final sort 
	  is done and results are written to S3.

## Bells and Whistles
* Even File Distribution:The files are distributed evenly across the slaves based on the total size.* Data Locality:The master makes sure that every slave sorts more data locally and sends 
less data over the network. Based on which buckets data the slave has the 
highest, the master assigns the buckets to the slaves to sort.

## Caveats
* Spill to disk logic is not implemented:  All the data sorting is done in memory. So this results in high memory 
  requirement of machines.* Retrying mechanism not implemented:Once a task or a slave fails, the master will terminate and exit.* Timeout mechanism not implemented:  If any slave dies for some reason or fails to send a message, the server 
  will keep waiting for that message from that slave forever.* Logging is thoroughly implemented. But final step of uploading to s3 
  could not be done to lack of time. Can we seen when ssh to machine.


## Configuration and Performance 
* 9 m3 xlarge machines. 
* 1 master and 8 slaves nodes.* Total time with provisioning and writing output to s3 and terminating 
  is around 8~10 mins.## Results
* Top 10 sorted dry\_bulb_temp values in the entire climate dataset.
	14836,19990312,155,­128.4 24011,19981231,555,­128.0 26616,19991209,453,
	­123.0 24243,19960820,656,­117.4 13773,19991102,1355,­113.6 25501,19990704,
	553,­108.4 14836,19990312,255,­99.6 12921,19980302,1156,­88.6 93226,19980111,
	56,­79.0 13911,20061229,755,­78.0
* Output part files can be found under your given Bucket Name as “OutputA9”.