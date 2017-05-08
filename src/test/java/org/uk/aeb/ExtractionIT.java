package org.uk.aeb;

import com.pff.PSTFile;
import com.pff.PSTFolder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.uk.aeb.processors.transformation.PstWrapper;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.uk.aeb.processors.ingestion.Ingestion.getPathNames;
import static org.uk.aeb.processors.ingestion.Ingestion.readPstZipFiles;
import static org.uk.aeb.processors.ingestion.Ingestion.unzipFiles;
import static org.uk.aeb.utilities.FileUtils.copyFile;
import static org.uk.aeb.utilities.FileUtils.createDirectory;
import static org.uk.aeb.utilities.FileUtils.deletePath;

/**
  * Created by AEB on 06/05/17.
  */
public class ExtractionIT {

    private static final String TEST_DIRECTORY_1 = "/tmp/edrm-enron-v1";
    private static final String SOURCE_DIRECTORY_1 = "/data/edrm-enron-v1";
    private static final String ZIP_FILE_NAME_1 = "/EDRM-Enron-PST-031.zip";

    private static JavaSparkContext sparkContext;

    private static PSTFile pstFile;

    @BeforeClass
    public static void setUp() throws Exception {

        SparkConf sparkConf = new SparkConf()
            .setAppName( "extractionTest" )
            .setMaster( "local[*]" );

        sparkContext = new JavaSparkContext( sparkConf );

        // create test directory and copy test file to it
        createDirectory( TEST_DIRECTORY_1 );
        copyFile( SOURCE_DIRECTORY_1 + ZIP_FILE_NAME_1, TEST_DIRECTORY_1 + ZIP_FILE_NAME_1 );

        // create a PSTFile object to use in the test
        List<String> filePaths = Arrays.asList( new String[]{TEST_DIRECTORY_1} );
        List< String > pathNames = getPathNames( filePaths );
        JavaPairRDD< String, PortableDataStream > zipPstFiles = readPstZipFiles( sparkContext, pathNames );
        pstFile = unzipFiles( zipPstFiles ).first();

    }

    @AfterClass
    public static void tearDown() throws Exception {

        sparkContext.stop();

        // delete test data
        deletePath( TEST_DIRECTORY_1 );

    }

    @Test
    public void testExtractEmailBody() throws Exception {

        // get the folder
        PSTFolder rootFolder = pstFile.getRootFolder();

        PstWrapper.processFolder(rootFolder);

    }

    @Test
    public void testExtractEmailToFieldRecipientNames() throws Exception {

        // TODO

    }

    @Test
    public void testExtractEmailCCFieldRecipientNames() throws Exception {

        // TODO

    }

}
