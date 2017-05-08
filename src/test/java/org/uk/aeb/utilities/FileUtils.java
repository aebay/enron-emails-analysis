package org.uk.aeb.utilities;

import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by AEB on 08/05/17.
 */
final public class FileUtils {

    static Logger logger = Logger.getLogger("org.uk.aeb.utilities.FileUtils");

    /**
     * Writes a file, containing dummy data, to disk.
     *
     * @param pathName
     */
    public static void writeFile( final String pathName ) {

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
    public static void deletePath( final String pathName ) {

        try {

            File file = new File( pathName );
            org.apache.commons.io.FileUtils.deleteDirectory( file );

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
    public static void copyFile( final String sourcePathName, final String destinationPathName ) {

        try {

            File sourceFile = new File( sourcePathName );
            File destinationFile = new File( destinationPathName );
            org.apache.commons.io.FileUtils.copyFile( sourceFile, destinationFile );

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
    public static void createDirectory( final String pathName ) {

        try {

            Path path = Paths.get(pathName);
            Files.createDirectory(path);
            logger.debug( pathName + " created successfully" );

        } catch( IOException e ) {
            logger.debug( e );
        }

    }

}
