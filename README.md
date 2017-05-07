# enron-emails-analysis

Java Spark batch application that analyses the contents of the Enron e-mail data  that can be found at [https://aws.amazon.com/datasets/enron-email-data/]

## Hardware

The code has been run against the full PST dataset on an AWS EC2 cluster.

### Specifications
* EBS volume: Cold HDD 500 GB
* EC2 ??? cluster configured for Apache Spark

The instructions below assume that you have set up an EC2 cluster 
with the above specifications and have the AWS CLI installed.

## Build

## Deploy

## Test

Note that some of the integration tests make use of sample Enron files to carry out the test.
The following files must be present on the given file path(s) in order for the tests to work:
  
  /data/edrm-enron-v1/EDRM-Enron-PST-031.zip  
  /data/edrm-enron-v2/edrm-enron-v2_meyers-a_pst.zip
  /data/edrm-enron-v2/edrm-enron-v2_panus-s_pst.zip

## Run
