package org.uk.aeb.processors.logic;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.uk.aeb.models.PstWrapper;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by AEB on 09/05/17.
 *
 * Calculates the average length, words, of an e-mail from the set passed in.
 */
final public class AverageEmailLength {

    /**
     * <p>
     *   Extracts the lists of email bodies from the wrapper objects,
     *   flattens the lists and calculates the average number of words
     *   per list.
     * </p>
     *
     * @param pstWrapper
     * @return
     */
    public static Double run(final JavaRDD<PstWrapper> pstWrapper ) {

        JavaRDD< String > emailBodies = extractEmailBodies( pstWrapper );

        JavaPairRDD< String, Long > wordsPerEmail =



    }

    /**
     * <p>
     *     Extract the lists of email bodies and flatten these into a
     *     single string RDD.
     * </p>
     *
     * @param pstWrapper
     * @return
     */
    public static JavaRDD< String > extractEmailBodies( final JavaRDD<PstWrapper> pstWrapper ) {

        JavaRDD< String > emailBodies = pstWrapper.flatMap( pst -> pst.getEmailBodies() );

        return emailBodies;

    }

    /**
     * Calculate the words per email.
     *
     * @param emailBodies
     * @return
     */
    public static JavaPairRDD< String, Long > wordsPerEmail( final JavaRDD< String > emailBodies ) {

        JavaPairRDD< String, Long > wordsPerEmail = emailBodies.map( email -> {

            List<String> emailWords = Arrays.asList( email.split( " " ) );

            String emailHash // to save space

            return new Tuple2<>();
        } );

    }

}
