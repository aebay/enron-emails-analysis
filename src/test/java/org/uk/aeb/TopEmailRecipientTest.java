package org.uk.aeb;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.uk.aeb.processors.logic.TopNRecipientEmailAddresses;

import scala.Tuple2;

import java.util.*;

import static org.junit.Assert.assertEquals;

/**
  * Created by AEB on 06/05/17.
  */
public class TopEmailRecipientTest {

    private static JavaSparkContext sparkContext;

    @BeforeClass
    public static void setUp() throws Exception {

        SparkConf sparkConf = new SparkConf()
                .setAppName( "extractionTest" )
                .setMaster( "local[*]" )
                .set( "spark.kryo.registrationRequired", "false" )
                .set( "spark.kryoserializer.buffer.max", "128m" ); // this doesn't actually do anything when hardcoded, but is here for reference when running spark-submit

        sparkContext = new JavaSparkContext( sparkConf );

    }

    @AfterClass
    public static void tearDown() throws Exception {

        sparkContext.stop();

    }

    @Test
    public void testWordCount() throws Exception {

        // data
        final long COUNT_ALPHA = 12363324L;
        final long COUNT_BETA = 321234L;
        final String WORD_ALPHA = "alpha";
        final String WORD_BETA = "beta";

        List< String > wordList = new ArrayList<>();
        for ( long i = 0; i < COUNT_ALPHA; i++ ) wordList.add( WORD_ALPHA );
        for ( long i = 0; i < COUNT_BETA; i++ ) wordList.add( WORD_BETA );

        // expected results
        Map< String, Long > expectedResults = new HashMap<>();
        expectedResults.put( WORD_ALPHA, COUNT_ALPHA );
        expectedResults.put( WORD_BETA, COUNT_BETA );

        // actual results
        JavaRDD< String > wordRdd = sparkContext.parallelize( wordList );
        JavaPairRDD< String, Long > wordCountRdd = TopNRecipientEmailAddresses.wordCount( wordRdd );
        Map< String, Long > actualResults = wordCountRdd.collectAsMap();

        assertEquals( expectedResults.get( WORD_ALPHA ), actualResults.get( WORD_ALPHA ) );
        assertEquals( expectedResults.get( WORD_BETA ), actualResults.get( WORD_BETA ) );

    }

    @Test
    public void testCombineEmailLists() throws Exception {

        // data
        List< Tuple2<String, Long > > toRecipientWordCounts = new ArrayList<>();
        toRecipientWordCounts.add( new Tuple2<>( "alpha", 34L ) );
        toRecipientWordCounts.add( new Tuple2<>( "beta", 123L ) );
        toRecipientWordCounts.add( new Tuple2<>( "delta", 67893424L ) );
        toRecipientWordCounts.add( new Tuple2<>( "gamma", 234234L ) );
        toRecipientWordCounts.add( new Tuple2<>( "epsilon", 454353L ) );

        List< Tuple2<String, Long > > ccRecipientWordCounts = new ArrayList<>();
        ccRecipientWordCounts.add( new Tuple2<>( "alpha", 675646439078L ) );
        ccRecipientWordCounts.add( new Tuple2<>( "beta", 1L ) );
        ccRecipientWordCounts.add( new Tuple2<>( "delta", 9452365L ) );
        ccRecipientWordCounts.add( new Tuple2<>( "gamma", 53L ) );
        ccRecipientWordCounts.add( new Tuple2<>( "nu", 12345779L ) );

        // expected results
        Map< String, Long > expectedResults = new HashMap<>();
        expectedResults.put( "alpha", 675646439112L );
        expectedResults.put( "beta", 124L );
        expectedResults.put( "delta", 77345789L );
        expectedResults.put( "gamma", 234287L );
        expectedResults.put( "epsilon", 454353L );
        expectedResults.put( "nu", 12345779L );

        // actual results
        JavaPairRDD< String, Long > toRecipientWordCount = sparkContext.parallelizePairs( toRecipientWordCounts );
        JavaPairRDD< String, Long > ccRecipientWordCount = sparkContext.parallelizePairs( ccRecipientWordCounts );
        JavaPairRDD< String, Long > combinedRecipientWordCount =
                TopNRecipientEmailAddresses.combineEmailLists( toRecipientWordCount, ccRecipientWordCount );
        Map< String, Long > actualResults = combinedRecipientWordCount.collectAsMap();

        assertEquals( expectedResults.get( "alpha" ), actualResults.get( "alpha" ) );
        assertEquals( expectedResults.get( "beta" ), actualResults.get( "beta" ) );
        assertEquals( expectedResults.get( "delta" ), actualResults.get( "delta" ) );
        assertEquals( expectedResults.get( "gamma" ), actualResults.get( "gamma" ) );
        assertEquals( expectedResults.get( "epsilon" ), actualResults.get( "epsilon" ) );
        assertEquals( expectedResults.get( "nu" ), actualResults.get( "nu" ) );

    }

    @Test
    public void testTopNEmailAddresses() throws Exception {

        // data
        final Integer LIST_LENGTH = 5;

        List< Tuple2< String, Long > > recipientWordCount = Arrays.asList(
                new Tuple2<>( "alpha", 3L ),
                new Tuple2<>( "beta", 345345L ),
                new Tuple2<>( "delta", 9745343242L ),
                new Tuple2<>( "gamma", 234L ),
                new Tuple2<>( "epsilon", 90898765534L ),
                new Tuple2<>( "zeta", 3423432L ),
                new Tuple2<>( "eta", 34321L ),
                new Tuple2<>( "theta", 54L ),
                new Tuple2<>( "iota", 7657L ),
                new Tuple2<>( "kappa", 74353534L )
        );

        // expected results
        List< Tuple2< String, Long > > expectedResults = Arrays.asList(
                new Tuple2<>( "epsilon", 90898765534L ),
                new Tuple2<>( "delta",   9745343242L ),
                new Tuple2<>( "kappa",   74353534L ),
                new Tuple2<>( "zeta",    3423432L ),
                new Tuple2<>( "beta",    345345L )
        );

        // actual results
        JavaPairRDD< String, Long > recipientWordCountRdd = sparkContext.parallelizePairs( recipientWordCount );
        List< Tuple2< String, Long > > actualResults =
                TopNRecipientEmailAddresses.orderEmailList( recipientWordCountRdd, LIST_LENGTH );

        // since both lists are ordered, strings should match
        assertEquals( expectedResults.toString(), actualResults.toString() );

    }

}