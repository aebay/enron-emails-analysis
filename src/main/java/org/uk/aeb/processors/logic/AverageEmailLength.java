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
     * @param pstWrappers
     * @return
     */
    public static Double calculate(final JavaRDD<PstWrapper> pstWrappers ) {

        JavaRDD< String > emailBodies = extractEmailBodies( pstWrappers );

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
     * <p>
     *     Calculate the words per email.
     *
     *     Assumption: number of words in an email will not exceed the maximum
     *     Integer limit (2147483647)
     * </p>
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
     *  It's assumed that the total number of words in the entire data source will
     *  not exceed the long limit (9223372036854775807L).
     * </p>
     *
     * @param wordsPerEmail
     * @return
     */
    public static Double averageWordsPerEmail( JavaRDD< Integer > wordsPerEmail ) {

        // get the count of records
        final Long numberOfEmails = wordsPerEmail.count();

        // aggregate the number of words per email
        Long totalEmailWords = wordsPerEmail
                .map( count -> count.longValue() )
                .reduce( (a,b) -> a + b );

        // calculate the average
        Double averageWordsPerEmail = totalEmailWords.doubleValue() / numberOfEmails;

        return averageWordsPerEmail;

    }

}
