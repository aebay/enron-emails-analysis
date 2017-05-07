package org.uk.aeb;

import org.apache.log4j.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.input.PortableDataStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.nio.file.*;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;

import static org.junit.Assert.assertEquals;
import static org.uk.aeb.processors.ingestion.Ingestion.getPathNames;
import static org.uk.aeb.processors.ingestion.Ingestion.readPstZipFiles;
import static org.uk.aeb.processors.ingestion.Ingestion.unzipFiles;

/**
 * Created by AEB on 06/05/17.
 *
 * Covers testing of ingestion of files from the file system.
 */
public class IngestionIT {

    static Logger logger = Logger.getLogger("org.uk.aeb.IngestionIT");

    private static final String TEST_DIRECTORY_1 = "/tmp/edrm-enron-v1";
    private static final String TEST_DIRECTORY_2 = "/tmp/edrm-enron-v2";
    private static final String SOURCE_DIRECTORY_1 = "/data/edrm-enron-v1";
    private static final String SOURCE_DIRECTORY_2 = "/data/edrm-enron-v2";

    private static final String TXT_FILE_NAME = "/edrm-enron-v2_meyers_a_pst.txt.zip";
    private static final String XLS_FILE_NAME = "/edrm-enron-v2_meyers_a_pst.xls.zip";
    private static final String ZIP_FILE_NAME_1 = "/EDRM-Enron-PST-031.zip";
    private static final String ZIP_FILE_NAME_2 = "/edrm-enron-v2_meyers-a_pst.zip";
    private static final String ZIP_FILE_NAME_3 = "/edrm-enron-v2_panus-s_pst.zip";

    private static JavaSparkContext sparkContext;

    @BeforeClass
    public static void setUp() throws Exception {

        SparkConf sparkConf = new SparkConf()
            .setAppName( "extractionTest" )
            .setMaster( "local[*]" );

        sparkContext = new JavaSparkContext( sparkConf );

        // create test directories
        createDirectory( TEST_DIRECTORY_1 );
        createDirectory( TEST_DIRECTORY_2 );

        // copy test data to test directories
        copyFile( SOURCE_DIRECTORY_1 + ZIP_FILE_NAME_1, TEST_DIRECTORY_1 + ZIP_FILE_NAME_1 );
        copyFile( SOURCE_DIRECTORY_2 + ZIP_FILE_NAME_2, TEST_DIRECTORY_2 + ZIP_FILE_NAME_2 );
        copyFile( SOURCE_DIRECTORY_2 + ZIP_FILE_NAME_3, TEST_DIRECTORY_2 + ZIP_FILE_NAME_3 );

        // create dummy files for filter test
        writeFile( TEST_DIRECTORY_1 + TXT_FILE_NAME );
        writeFile( TEST_DIRECTORY_1 + XLS_FILE_NAME );
        writeFile( TEST_DIRECTORY_2 + TXT_FILE_NAME );
        writeFile( TEST_DIRECTORY_2 + XLS_FILE_NAME );

    }

    @AfterClass
    public static void tearDown() throws Exception {

        sparkContext.stop();

        // delete test data
        deletePath( TEST_DIRECTORY_1 );
        deletePath( TEST_DIRECTORY_2 );

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
        List<String> actualResults = getPathNames( filePaths );

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
                "file:" + TEST_DIRECTORY_1 + ZIP_FILE_NAME_1,
                "file:" + TEST_DIRECTORY_2 + ZIP_FILE_NAME_2,
                "file:" + TEST_DIRECTORY_2 + ZIP_FILE_NAME_3 }
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
        List< String > pathNames = getPathNames( filePaths );
        JavaPairRDD< String, PortableDataStream > zipPstFiles = readPstZipFiles( sparkContext, pathNames );
        JavaRDD< String > actualResults = unzipFiles( zipPstFiles )
                .map( pst -> pst.getMessageStore().getDisplayName() );

        assertEquals( expectedResults.toString(), actualResults.collect().toString() );

    }

    /**
    * Writes a file, containing dummy data, to disk.
    *
    * @param pathName
    */
    private static void writeFile( final String pathName ) {

        BufferedWriter bufferedWriter = null;
        FileWriter fileWriter = null;

        try {

            String content = "Generic text string\n";

            fileWriter = new FileWriter( pathName );
            bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(content);

            logger.debug( pathName + " written to disk successfully." );

        } catch (IOException e) {

            e.printStackTrace();

        } finally {

            try {

                if (bufferedWriter != null)
                    bufferedWriter.close();

                if (fileWriter != null)
                    fileWriter.close();

            } catch (IOException e) {

                logger.debug( e );

            }

        }

    }

    /**
    * Deletes a file or directory (and it's contents) from disk.
    *
    * @param pathName
    */
    private static void deletePath( final String pathName ) {

        try {

            File file = new File( pathName );
            FileUtils.deleteDirectory( file );

            logger.debug( pathName + " and contents deleted successfully" );

        } catch( IOException e ) {
            logger.debug( e );
        }

    }

    /**
    * Copy a file from one disk location to another.
    *
    * @param sourcePathName
    * @param destinationPathName
    */
    private static void copyFile( final String sourcePathName, final String destinationPathName ) {

        try {

            File sourceFile = new File( sourcePathName );
            File destinationFile = new File( destinationPathName );
            FileUtils.copyFile( sourceFile, destinationFile );

            logger.debug( sourcePathName + " copied to " + destinationPathName + " successfully" );

        } catch( IOException e ){
            logger.debug( e );
        }

    }

    /**
     * Create a directory
     *
     * @param pathName
     */
    private static void createDirectory( final String pathName ) {

        try {

            Path path = Paths.get(pathName);
            Files.createDirectory(path);
            logger.debug( pathName + " created successfully" );

        } catch( IOException e ) {
            logger.debug( e );
        }

    }

}