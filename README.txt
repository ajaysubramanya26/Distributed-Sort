################################################################################
# README-A9 DISTRIBUTED SORT
# Smitha Bangalore Naresh, Prasad Memane, Swapnil Mahajan, Ajay Subramanya

# bangalorenaresh{dot}s{at}husky{dot}neu{dot}edu
# memane{dot}p{at}husky{dot}neu{dot}edu
# mahajan{dot}sw{at}husky{dot}neu{dot}edu
# subramanya{dot}a{at}husky{dot}neu{dot}edu
################################################################################

################################  PREREQUISITES ################################

* Update the script

* We are using Apache Maven 3.3.9 for dependency management and build automation

* Please `chmod +x buildAndCopyToAWS.sh` and `chmod +x masterSlaveScripts.sh`

* AWS CLI

* AWS Instance profile #link http://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-ec2_instance-profiles.html 
  the above instance profile should have full access to s3. the reason we use 
  this is to not pass credentials around while also granting full access to s3

* Update /Setup/bootstrap.txt with your details.

* Update /netty-server/params with your details.

################################## RUN SCRIPT ##################################

* 'make setup3' - for 2 node setup

* 'make setup9' - for 8 node setup

################################### THE END ####################################
