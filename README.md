# Tsunami

#USER GUIDE

##1-AWS cluster initialization
follow documentation from [Using OpsCenter to create a cluster on Amazon EC2](
http://www.datastax.com/documentation/datastax_enterprise/4.6/datastax_enterprise/install/installAMIOpsc.html)

create the key pair 
```javascript
chmod 400 <my-key-pair>.pem
```

##2-Launch the AMI (us-east-1	ami-f9a2b690)
follow documentation [Installing on Amazon EC2> Launch the AMI](
http://www.datastax.com/documentation/datastax_enterprise/4.6/datastax_enterprise/install/installAMIlaunch.html)

In Advandced details, set parameters:

```javascript
--version enterprise
--analytics nodes 6
--totalnodes 6
--username datastaxusername
--password datastaxpassword
````

##3-Connect to Spark Master :
```javascript
ssh -i <my-key-pair>.pem ubuntu@<ip-master> then launch "dse spark"
```
##4-Connect to Cassandra Master:

```javascript
ssh -i <my-key-pair>.pem ubuntu@<ip-master> then launch "cqlsh"
````

##5-On Spark, import data from S3, preprocess the data and save to Cassandra:

On spark terminal, copy/paste the file sparkCSV.scala

##6-Install Python Librairies on the AMIs:

```javascript
sh config_python.sh
```
and then git clone this repository 

##7- Create keyspaces and tables on Cassandra

```javascript
CREATE KEYSPACE test WITH replication = {   'class': 'SimpleStrategy',   'replication_factor': 2 };
create table cassandraresult (tel text,lat text,longi text, PRIMARY KEY (tel, lat));
create table test_spark_bigText(t timestamp, id_ville text, tels text, primary key ((t,id_ville)));
````
##8-Set parameters in Requetage.py
Insert the IP adresses of the 5 workers nodes in the table IPaddressesTables (line 124)
```javascript
IPaddressesTables=['172.31.53.38','172.31.53.39','172.31.53.40','172.31.53.41', '172.31.53.41']
```
##9-Launch the python file requetage.py


