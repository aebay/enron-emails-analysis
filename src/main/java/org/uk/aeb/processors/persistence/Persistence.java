package org.uk.aeb.processors.persistence;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import java.util.List;

/**
 * Created by AEB on 13/05/17.
 *
 * Persists the information to disk.
 */
final public class Persistence {

    final private static Logger logger = LogManager.getLogger( "org.uk.aeb.processors.persistence" );

    /**
     * Persists the results to disk.
     *
     * @param averageWordsPerEmail
     * @param emailAddresses
     * @param outputPathName
     */
    public static void run( final Double averageWordsPerEmail,
                            final List< Tuple2<String,Long> > emailAddresses,
                            final String outputPathName ) {

        BufferedWriter bufferedWriter = null;
        FileWriter fileWriter = null;

        try {

            fileWriter = new FileWriter( outputPathName );
            bufferedWriter = new BufferedWriter(fileWriter);

            // write data
            bufferedWriter.write( averageWordsPerEmail.toString() + "\n" );
            for ( Tuple2<String,Long> emailAddress : emailAddresses )
                bufferedWriter.write( emailAddress + "\n" );

        } catch (IOException e) {

            logger.error( e );

        } finally {

            try {

                if ( bufferedWriter != null )
                    bufferedWriter.close();

                if ( fileWriter != null )
                    fileWriter.close();

            } catch ( IOException e ) {

                logger.error( e );

            }

        }

    }

}