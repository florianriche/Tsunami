# Tsunami

USER GUIDE

1-AWS cluster initialization
follow documentation from "Using OpsCenter to create a cluster on Amazon EC2"
http://www.datastax.com/documentation/datastax_enterprise/4.6/datastax_enterprise/install/installAMIOpsc.html

create the key pair 
chmod 400 <my-key-pair>.pem


2-Launch the AMI (us-east-1	ami-f9a2b690)
follow documentation "Installing on Amazon EC2> Launch the AMI"
http://www.datastax.com/documentation/datastax_enterprise/4.6/datastax_enterprise/install/installAMIlaunch.html

In Advandced details, set parameters:


--version enterprise
--analytics nodes 6
--totalnodes 6
--username datastaxusername
--password datastaxpassword

3-Connect to Spark Master :


ssh -i <my-key-pair>.pem ubuntu@<ip-master> then launch "dse spark"

4-Connect to Cassandra Master:


ssh -i <my-key-pair>.pem ubuntu@<ip-master> then launch "cqlsh"

5-On Spark, import data from S3, preprocess the data and save to Cassandra:


execute on spark terminal, launch the file sparkCSV.scala

6-Install Python Librairies on the AMIs:


sh config_python.sh

7- Create keyspaces and tables on Cassandra


- CREATE KEYSPACE test WITH replication = {   'class': 'SimpleStrategy',   'replication_factor': 2 };
- create table cassandraresult (tel text,lat text,longi text, PRIMARY KEY (tel, lat));
- create table test_spark_bigText(t timestamp, id_ville text, tels text, primary key ((t,id_ville)));

8-Launch the python file requetage.py


