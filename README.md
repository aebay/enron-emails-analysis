# enron-emails-analysis

**UNDER CONSTRUCTION**

* Test on EDRM-Enron-PST-031.zip is generating heap size errors
* Run configuration pending.

Java Spark batch application that analyses the contents of the Enron e-mail data  that can be found at [https://aws.amazon.com/datasets/enron-email-data/]

## Assumptions

* Enron email addresses that are in the form `/O=ENRON/OU=NA/CN=RECIPIENTS/CN=DL-PortlandRealTimeShift` are reconstructed using the string portion that follows the last `CN=` and `@enron.com` appended.
* The total words in all of the emails will not exceed the maximum system value of Long.
* The `edrm-enron-v1` and `edrm-enron-v2` both contain unique email archives.

## Known issues

* Some email metadata only contains the recipient display name.  In these situations no email address is added to the analysis.

## Hardware

TBD

### Specifications

TBD

* EBS volume: Cold HDD 500 GB
* EC2 ??? cluster configured for Apache Spark

The instructions below assume that you have set up an EC2 cluster 
with the above specifications and have the AWS CLI installed.

## Test

### Local

Local testing and development was carried out in IntelliJ.  These instructions assume  
that you are using IntelliJ and have Maven and the Java 8 JDK installed.

#### Data

Note: the default directories in these instructions can be changed by editing the respective application.properties file  
in `resources` for either the main application or the tests.

1. Download the following files from the Enron volume:
```
./edrm-enron-v1/EDRM-Enron-PST-031.zip  
./edrm-enron-v2/edrm-enron-v2_meyers-a_pst.zip
./edrm-enron-v2/edrm-enron-v2_panus-s_pst.zip
```

2. Store the downloaded files in `/data`.

#### Run

1. Run the `org.uk.aeb.driver.Main` class.  It will fail, this is ok.
2. Go to _Run->Edit Configurations..._.
3. Click on `Application->Main` in the left-hand panel to bring up the run configWrapper for the driver class.
4. Click on `VM options`.
5. Enter the following into the box, replacing `<PROJECT_PATH>` with the full path to your project:
```
-DconfDir=file:///<PROJECT_PATH>/enron-emails-analysis/src/main/resources
```
6. Run the `org.uk.aeb.driver.Main` class.

Note that some of the integration tests make use of sample Enron files to carry out the test.
The following files must be present on the given file path(s) in order for the tests to work:
  
  /data/edrm-enron-v1/EDRM-Enron-PST-031.zip  
  /data/edrm-enron-v2/edrm-enron-v2_meyers-a_pst.zip
  /data/edrm-enron-v2/edrm-enron-v2_panus-s_pst.zip

## Build

To build the project you must have Maven installed and configured on your path.

1. Open a command/terminal window and navigate to the root directory of the project.
2. Execute `mvn clean package -DskipTests=true -DargLine=Xms512m`.

## Deploy

1. Navigate to the `target\` directory of the project.
2. Open a command/terminal window and copy `enron-emails-analysis-<PROJECT_VERSION>.jar` and `enron-emails-analysis-<PROJECT_VERSION>.zip` to the cluster.
3. Unzip the configuration files.

## Run

./bin/spark-submit \
  --class org.uk.aeb.driver.Main \
  --master local[*] \
  --conf spark.kryoserializer.buffer.max=1g \
  --conf spark.driver.extraJavaOptions='-DconfDir=<CONFIGURATION_FILE_PATH>' \ 
  <JAR_PATH_NAME>
  
  
TBD