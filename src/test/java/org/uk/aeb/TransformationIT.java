package org.uk.aeb;

import com.pff.PSTFile;
import com.pff.PSTFolder;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;

import org.junit.BeforeClass;
import org.junit.Test;

import org.uk.aeb.models.PstWrapper;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.uk.aeb.processors.ingestion.Ingestion.*;
import static org.uk.aeb.utilities.HdfsUtils.*;

/**
  * Created by AEB on 06/05/17.
  */
public class TransformationIT {

    private static final String CONFIG_PATH = "";
    private static final String APPLICATION_CONFIG_FILE = "application.properties";
    private static final String SPARK_CONFIG_FILE = "spark.properties";
    private static final String HADOOP_CONFIG_FILE = "hadoop.properties";

    private static String TEST_DIRECTORY_1;
    private static String SOURCE_DIRECTORY_1;
    private static String ZIP_FILE_NAME_1;

    private static JavaSparkContext sparkContext;

    private static PstWrapper pstWrapper;

    private static Configuration configuration;

    @BeforeClass
    public static void setUp() throws Exception {

        // configuration
        Config sparkConfig = ConfigFactory.load( CONFIG_PATH + SPARK_CONFIG_FILE );
        Config applicationConfig = ConfigFactory.load( CONFIG_PATH + APPLICATION_CONFIG_FILE );
        Config hadoopConfig = ConfigFactory.load( CONFIG_PATH + HADOOP_CONFIG_FILE );

        TEST_DIRECTORY_1 = applicationConfig.getString( "test.root.directory.two.path" );
        SOURCE_DIRECTORY_1 = applicationConfig.getString( "test.source.directory.two.path" );
        ZIP_FILE_NAME_1 = applicationConfig.getString( "test.source.directory.two.file" ).split(",")[0];

        configuration = new Configuration();
        configuration.addResource( hadoopConfig.getString( "core.site.pathname" ) );
        configuration.addResource( hadoopConfig.getString( "hdfs.site.pathname" ) );

        SparkConf sparkConf = new SparkConf()
                .setMaster( sparkConfig.getString( "spark.master" ) )
                .setAppName( "Extraction IT" )
                .set( "spark.serializer", sparkConfig.getString( "spark.serializer" ) )
                .set( "spark.kryo.registrationRequired", sparkConfig.getString( "kryo.registration" ) )
                .set( "spark.kryoserializer.buffer.max", sparkConfig.getString( "kryo.buffer.max" ) ); // this doesn't actually do anything when hardcoded, but is here for reference when running spark-submit

        sparkContext = new JavaSparkContext( sparkConf );

        // create test directories
        createHdfsDirectory( TEST_DIRECTORY_1, configuration );

        // copy test data to test directories
        copyFileToHdfs( SOURCE_DIRECTORY_1 + ZIP_FILE_NAME_1, TEST_DIRECTORY_1 + ZIP_FILE_NAME_1, configuration );

        // create a PSTFile object to use in the test
        List<String> filePaths = Arrays.asList( new String[]{TEST_DIRECTORY_1} );
        List< String > pathNames = getHdfsPathNames( filePaths, configuration );
        JavaPairRDD< String, PortableDataStream > zipPstFiles = readPstZipFiles( sparkContext, pathNames );
        PSTFile pstFile = unzipFiles( zipPstFiles ).first();

        // instantiate the PstWrapper class
        PSTFolder rootFolder = pstFile.getRootFolder();
        pstWrapper = new PstWrapper( rootFolder );

        // clean up unneeded objects and processes
        sparkContext.stop();
        deleteHdfsDirectory( TEST_DIRECTORY_1, configuration );

    }

    @Test
    public void testExtractEmailBody() throws Exception {

        // expected results
        List<String> expectedResults = Arrays.asList( new String[]{
                "Bill:\r\n" +
                        "\r\n" +
                        "Please note the following due to the past two days schedules have been wrong in the EPE Schedules in Excel:\r\n" +
                        "\r\n" +
                        "Tag number 6181 has been cancelled (50mw to the CISO).\r\n" +
                        "\r\n" +
                        "Lending is wrong in the EPE schedules (it is 75mw from PSCO instead of 50mw).\r\n" +
                        "\r\n" +
                        "SPS is wrong for HE 08 (it is 130mw instead of 100mw).\r\n" +
                        "\r\n" +
                        "I thought you might like to since this is the only income we have currently for real-time and a major screw-up could hurt our relationship.\r\n" +
                        "\r\n" +
                        "Regards,\r\n" +
                        "\r\n" +
                        "Bert Meyers\r\n" +
                        "\r\n" +
                        "***********\r\n" +
                        "EDRM Enron Email Data Set has been produced in EML, PST and NSF format by ZL Technologies, Inc. This Data Set is licensed under a Creative Commons Attribution 3.0 United States License <http://creativecommons.org/licenses/by/3.0/us/> . To provide attribution, please cite to \"ZL Technologies, Inc. (http://www.zlti.com).\"\r\n" +
                        "***********\r\n"
        }  );

        // actual results
        List<String> actualResults = pstWrapper.getEmailBodies();

        assertEquals( expectedResults.get( 0 ), actualResults.get( 0 ) );

    }

    @Test
    public void testExtractEmailToFieldRecipientNames() throws Exception {

        // expected results
        List<String> expectedResults = Arrays.asList( new String[]{
                "bwillia5@enron.com",
                "rslinger@enron.com",
                "thomas.rosendahl@ubspainewebber.com"
        }  );

        // actual results
        List<String> actualResults = pstWrapper.getToRecipients();

        assertEquals( expectedResults.get( 0 ), actualResults.get( 0 ) );
        assertEquals( expectedResults.get( 1 ), actualResults.get( 1 ) );
        assertEquals( expectedResults.get( 2 ), actualResults.get( 2 ) );

    }

    @Test
    public void testExtractEmailCCFieldRecipientNames() throws Exception {

        // expected results
        List<String> expectedResults = Arrays.asList( new String[]{
                "bwillia5@enron.com", // i.e. empty
                "bwillia5@enron.com",
                "david.steiner@enron.com"
        }  );

        // actual results
        List<String> actualResults = pstWrapper.getCcRecipients();

        assertEquals( expectedResults.get( 0 ), actualResults.get( 0 ) );
        assertEquals( expectedResults.get( 1 ), actualResults.get( 1 ) );
        assertEquals( expectedResults.get( 2 ), actualResults.get( 2 ) );

    }

}