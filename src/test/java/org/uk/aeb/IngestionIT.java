package org.uk.aeb;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.uk.aeb.processors.ingestion.Ingestion.*;
import static org.uk.aeb.utilities.HdfsUtils.*;

/**
 * Created by AEB on 21/05/17.
 *
 * Covers testing of ingestion of files from HDFS.
 */
public class IngestionIT {

    private final static Logger logger = Logger.getLogger("org.uk.aeb.IngestionIT");

    private static final String CONFIG_PATH = "";
    private static final String APPLICATION_CONFIG_FILE = "application.properties";
    private static final String SPARK_CONFIG_FILE = "spark.properties";
    private static final String HADOOP_CONFIG_FILE = "hadoop.properties";

    private static final String TXT_FILE_NAME = "/edrm-enron-v2_meyers_a_pst.txt.zip";
    private static final String XLS_FILE_NAME = "/edrm-enron-v2_meyers_a_pst.xls.zip";

    private static String TEST_DIRECTORY_1;
    private static String TEST_DIRECTORY_2;
    private static String SOURCE_DIRECTORY_1;
    private static String SOURCE_DIRECTORY_2;

    private static String ZIP_FILE_NAME_1;
    private static String ZIP_FILE_NAME_2;
    private static String ZIP_FILE_NAME_3;

    private static JavaSparkContext sparkContext;

    private static Configuration configuration;

    @BeforeClass
    public static void setUp() throws Exception {

        // configuration
        Config sparkConfig = ConfigFactory.load( CONFIG_PATH + SPARK_CONFIG_FILE );
        Config applicationConfig = ConfigFactory.load( CONFIG_PATH + APPLICATION_CONFIG_FILE );
        Config hadoopConfig = ConfigFactory.load( CONFIG_PATH + HADOOP_CONFIG_FILE );

        TEST_DIRECTORY_1 = applicationConfig.getString( "test.root.directory.one.path" );
        TEST_DIRECTORY_2 = applicationConfig.getString( "test.root.directory.two.path" );
        SOURCE_DIRECTORY_1 = applicationConfig.getString( "test.source.directory.one.path" );
        SOURCE_DIRECTORY_2 = applicationConfig.getString( "test.source.directory.two.path" );

        ZIP_FILE_NAME_1 = applicationConfig.getString( "test.source.directory.one.file" );
        String[] sourceDirectoryTwoFiles = applicationConfig.getString( "test.source.directory.two.file" ).split( "," );
        ZIP_FILE_NAME_2 = sourceDirectoryTwoFiles[0];
        ZIP_FILE_NAME_3 = sourceDirectoryTwoFiles[1];

        configuration = new Configuration();
        configuration.addResource( hadoopConfig.getString( "core.site.pathname" ) );
        configuration.addResource( hadoopConfig.getString( "hdfs.site.pathname" ) );

        SparkConf sparkConf = new SparkConf()
                .setMaster( sparkConfig.getString( "spark.master" ) )
                .setAppName( "Ingestion HDFS IT" )
                .set( "spark.serializer", sparkConfig.getString( "spark.serializer" ) )
                .set( "spark.kryo.registrationRequired", sparkConfig.getString( "kryo.registration" ) )
                .set( "spark.kryoserializer.buffer.max", sparkConfig.getString( "kryo.buffer.max" ) ); // this doesn't actually do anything when hardcoded, but is here for reference when running spark-submit

        sparkContext = new JavaSparkContext( sparkConf );

        // create test directories
        createHdfsDirectory( TEST_DIRECTORY_1, configuration );
        createHdfsDirectory( TEST_DIRECTORY_2, configuration );

        // copy test data to test directories
        copyFileToHdfs( SOURCE_DIRECTORY_1 + ZIP_FILE_NAME_1, TEST_DIRECTORY_1 + ZIP_FILE_NAME_1, configuration );
        copyFileToHdfs( SOURCE_DIRECTORY_2 + ZIP_FILE_NAME_2, TEST_DIRECTORY_2 + ZIP_FILE_NAME_2, configuration );
        copyFileToHdfs( SOURCE_DIRECTORY_2 + ZIP_FILE_NAME_3, TEST_DIRECTORY_2 + ZIP_FILE_NAME_3, configuration );

        // create dummy files for filter test
        writeFileToHdfs( TEST_DIRECTORY_1 + TXT_FILE_NAME, configuration );
        writeFileToHdfs( TEST_DIRECTORY_1 + XLS_FILE_NAME, configuration );
        writeFileToHdfs( TEST_DIRECTORY_2 + TXT_FILE_NAME, configuration );
        writeFileToHdfs( TEST_DIRECTORY_2 + XLS_FILE_NAME, configuration );

    }

    @AfterClass
    public static void tearDown() throws Exception {

        sparkContext.stop();

        // delete test data
        deleteHdfsDirectory( TEST_DIRECTORY_1, configuration );
        deleteHdfsDirectory( TEST_DIRECTORY_2, configuration );

    }

    @Test
    public void testFilteringOfNonRelevantFiles() throws Exception {

        // data
        List<String> filePaths = Arrays.asList( new String[]{TEST_DIRECTORY_1, TEST_DIRECTORY_2} );

        // expected results
        List<String> expectedResults = Arrays.asList( new String[]{
                TEST_DIRECTORY_1 + ZIP_FILE_NAME_1,
                TEST_DIRECTORY_2 + ZIP_FILE_NAME_2,
                TEST_DIRECTORY_2 + ZIP_FILE_NAME_3 }
        );

        // actual results
        List<String> actualResults = getHdfsPathNames( filePaths, configuration );

        assertEquals( expectedResults.toString(), actualResults.toString() );

    }

    @Test
    public void testReadingMultipleFilesFromMultipleDirectories() throws Exception {

        // data
        List<String> pathNames = Arrays.asList( new String[]{
                TEST_DIRECTORY_1 + ZIP_FILE_NAME_1,
                TEST_DIRECTORY_2 + ZIP_FILE_NAME_2,
                TEST_DIRECTORY_2 + ZIP_FILE_NAME_3 }
        );

        // expected results
        List< String > expectedResults = Arrays.asList( new String[]{
                TEST_DIRECTORY_1 + ZIP_FILE_NAME_1,
                TEST_DIRECTORY_2 + ZIP_FILE_NAME_2,
                TEST_DIRECTORY_2 + ZIP_FILE_NAME_3 }
        );

        // actual results
        JavaPairRDD< String, PortableDataStream > actualResults = readPstZipFiles( sparkContext, pathNames );

        assertEquals( expectedResults.toString(), actualResults.keys().collect().toString() );

    }

    @Test
    public void testRetrievePSTFilesFromZip() throws Exception {

        // data
        List<String> filePaths = Arrays.asList( new String[]{TEST_DIRECTORY_1} );

        // expected results
        List< String > expectedResults = Arrays.asList( new String[]{
                "vkaminski_002",
                "vladi_pimenov_000",
                "vkaminski_003" }
        );

        // actual results
        List< String > pathNames = getHdfsPathNames( filePaths, configuration );
        JavaPairRDD< String, PortableDataStream > zipPstFiles = readPstZipFiles( sparkContext, pathNames );
        JavaRDD< String > actualResults = unzipFiles( zipPstFiles )
                .map( pst -> pst.getMessageStore().getDisplayName() );

        assertEquals( expectedResults.toString(), actualResults.collect().toString() );

    }

}