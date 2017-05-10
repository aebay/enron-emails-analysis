package org.uk.aeb.processors.logic;

import org.apache.spark.api.java.JavaRDD;
import org.uk.aeb.models.PstWrapper;

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

        JavaRDD< String > cleanedEmailBodies = cleanEmailBodies( emailBodies );

        JavaRDD< Integer > wordsPerEmail = countWordsPerEmail( cleanedEmailBodies ).cache();

        return averageWordsPerEmail( wordsPerEmail );

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
     * Replace delimiters with spaces and remove duplicated whitespace.
     *
     * @param emailBodies
     * @return
     */
    public static JavaRDD< String > cleanEmailBodies( final JavaRDD<String> emailBodies ) {

        JavaRDD< String > cleanedEmailBodies = emailBodies.map( email -> {

            String cleanedEmail =
                    email.replace( "\n", " " )
                            .replace( "\r", " " )
                            .replace( "\t", " " )
                            .replaceAll("\\s+", " "); // to remove duplicated whitespace

            return cleanedEmail;

        } );

        return cleanedEmailBodies;

    }

    /**
     * Calculate the words per email.
     *
     * @param emailBodies
     * @return
     */
    public static JavaRDD< Integer > countWordsPerEmail( final JavaRDD< String > emailBodies ) {

        JavaRDD< Integer > wordsPerEmail = emailBodies
                .map( email -> email.split( " " ).length );

        return wordsPerEmail;

    }

    /**
     * <p>
     *  Calculates the mean average number of words per email.
     *
     *  Words per email are normalised by the total number of emails before being
     *  aggregated to avoid an overflow if something like long had been used.
     * </p>
     *
     * @param wordsPerEmail
     * @return
     */
    public static Double averageWordsPerEmail( JavaRDD< Integer > wordsPerEmail ) {

        // get the count of records
        final Long numberOfEmails = wordsPerEmail.count();

        // normalise the words per e-mail
        JavaRDD< Double > normalisedCount =
                wordsPerEmail.map( count -> count.doubleValue() / numberOfEmails );

        // add the normalised totals together to calculate the average
        Double averageWords = normalisedCount.reduce( (a, b) -> a + b );

        return averageWords;

    }

}
