package org.uk.aeb.processors.logic;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.uk.aeb.models.PstWrapper;
import scala.Tuple2;

import java.util.List;

/**
 * Created by AEB on 13/05/17.
 *
 * <p>
 *   Calculates the top N recipient email addresses where "To" has a
 *   weighting of 1 and "Cc" a weighting of 0.5.
 * </p>
 */
final public class TopNRecipientEmailAddresses {

    /**
     * Performs a word count on the email addresses.
     *
     * @param pstWrappers
     * @param listLength
     * @return
     */
    public static List< Tuple2<String, Long> > calculate( final JavaRDD<PstWrapper> pstWrappers, final Integer listLength ) {

        // word count on "To" recipients
        JavaPairRDD< String, Long > toRecipientWordCount = wordCount( pstWrappers.flatMap( wrapper -> wrapper.getToRecipients() ) );

        // word count on "Cc" recipients ( /2 to weight cc emails by 50% )
        JavaPairRDD< String, Long > ccRecipientWordCount = wordCount( pstWrappers.flatMap( wrapper -> wrapper.getCcRecipients() ) )
                .mapToPair( kv -> new Tuple2<>( kv._1, kv._2 / 2 ) );

        // combine recipients
        JavaPairRDD< String, Long > recipientWordCount = combineEmailLists( toRecipientWordCount, ccRecipientWordCount );

        // retrieve top N email addresses by count
        List< Tuple2<String, Long > > topNEmailAddresses = orderEmailList( recipientWordCount, listLength );

        return topNEmailAddresses;

    }

    /**
     * Performs word count on the email addresses.
     *
     * @param emailAddresses
     * @return
     */
    public static JavaPairRDD< String, Long > wordCount( final JavaRDD<String> emailAddresses ) {

        JavaPairRDD< String, Long > wordCount = emailAddresses
                .mapToPair( recipient -> new Tuple2<>( recipient, 1L ) )
                .reduceByKey( (a,b) -> a + b );

        return wordCount;

    }

    /**
     * Combines and ranks the email addresses by count in descending order.
     *
     * @param toRecipientWordCount
     * @param ccRecipientWordCount
     * @return
     */
    public static JavaPairRDD< String, Long > combineEmailLists( final JavaPairRDD< String, Long > toRecipientWordCount,
                                                                 final JavaPairRDD< String, Long > ccRecipientWordCount ) {

        // combine the two types of recipient
        JavaPairRDD< String, Long > recipientWordCount = toRecipientWordCount
                .union( ccRecipientWordCount )
                .reduceByKey( (a,b) -> a + b );

        return recipientWordCount;

    }

    /**
     * <p>
     *   Returns a list of emails in address-count pairs in descending order for
     *   the length specified.
     * </p>
     *
     * @param recipientWordCount
     * @param listLength
     * @return
     */
    public static List< Tuple2<String, Long > > orderEmailList( final JavaPairRDD< String, Long > recipientWordCount,
                                                                final Integer listLength ) {

        return recipientWordCount.takeOrdered( listLength, new RecipientComparator() );

    }

}
