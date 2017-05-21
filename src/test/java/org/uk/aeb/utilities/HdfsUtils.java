package org.uk.aeb.utilities;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;

/**
 * Created by AEB on 21/05/17.
 */
final public class HdfsUtils {

    static Logger logger = Logger.getLogger("org.uk.aeb.utilities.HdfsUtils");

    /**
     * Writes a file, containing dummy data, to HDFS.
     *
     * @param pathName
     */
    public static void writeFileToHdfs( final String pathName, final Configuration configuration ) {

        FileSystem fileSystem = null;
        BufferedWriter bufferedWriter = null;

        try {

            fileSystem = FileSystem.get( URI.create("hdfs://localhost"), configuration );
            FSDataOutputStream outputStream = fileSystem.create( new Path( pathName ) );
            bufferedWriter = new BufferedWriter( new OutputStreamWriter( outputStream, "UTF-8" ) );

            String content = "Generic text string\n";

            bufferedWriter.write( content );
            bufferedWriter.close();

        } catch( IOException e ) {
            logger.debug( e );
        } finally {

            try {
                if ( bufferedWriter != null ) {
                    bufferedWriter.close();
                }

                if ( fileSystem != null ) {
                    fileSystem.close();
                }

            } catch( IOException e ) {
                logger.debug( e );
            }

        }

    }

    /**
     * Deletes a file or directory (and its contents) from HDFS.
     *
     * @param pathName
     */
    public static void deleteHdfsDirectory( final String pathName, final Configuration configuration ) {

        FileSystem fileSystem = null;

        try {

            fileSystem = FileSystem.get( URI.create("hdfs://localhost"), configuration );
            fileSystem.delete( new Path( pathName ), true);
            logger.debug( pathName + " and contents deleted successfully" );

        } catch( IOException e ) {
            logger.debug( e );
        } finally {

            try {

                if ( fileSystem != null ) {
                    fileSystem.close();
                }

            } catch( IOException e ) {
                logger.debug( e );
            }

        }

    }

    /**
     * Copy a file from local to HDFS.
     *
     * @param sourcePathName
     * @param destinationPathName
     */
    public static void copyFileToHdfs( final String sourcePathName,
                                       final String destinationPathName,
                                       final Configuration configuration ) {

        FileSystem fileSystem = null;

        try {

            fileSystem = FileSystem.get( URI.create("hdfs://localhost"), configuration );
            fileSystem.copyFromLocalFile( new Path( sourcePathName ), new Path( destinationPathName ) );
            logger.debug( sourcePathName + " copied to " + destinationPathName + " successfully" );

        } catch( IOException e ){
            logger.debug( e );
        } finally {

            try {

                if ( fileSystem != null ) {
                    fileSystem.close();
                }

            } catch( IOException e ) {
                logger.debug( e );
            }

        }

    }

    /**
     * Create an HDFS directory
     *
     * @param pathName
     */
    public static void createHdfsDirectory( final String pathName, final Configuration configuration ) {

        FileSystem fileSystem = null;

        try {

            fileSystem = FileSystem.get( URI.create("hdfs://localhost"), configuration );
            Path path = new Path( pathName );
            fileSystem.mkdirs( path );
            logger.debug( pathName + " created successfully" );

        } catch( IOException e ) {
            logger.debug( e );
        } finally {

            try {

                if ( fileSystem != null ) {
                    fileSystem.close();
                }

            } catch( IOException e ) {
                logger.debug( e );
            }

        }

    }

}
