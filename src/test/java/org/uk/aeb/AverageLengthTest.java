package org.uk.aeb;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.uk.aeb.processors.logic.AverageEmailLength;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by AEB on 06/05/17.
 */
public class AverageLengthTest {

  private static JavaSparkContext sparkContext;

  @BeforeClass
  public static void setUp() throws Exception {

    SparkConf sparkConf = new SparkConf()
            .setAppName( "extractionTest" )
            .setMaster( "local[*]" );

    sparkContext = new JavaSparkContext( sparkConf );

  }

  @AfterClass
  public static void tearDown() throws Exception {

    sparkContext.stop();

  }

  @Test
  public void testCleanEmailBody() throws Exception {

      // data
      String emailBody1 = "abc\r 123 \n 45 \t 2";
      String emailBody2 = "def\r 456 \n\t78#'";
      List<String> emailBodies = Arrays.asList( emailBody1, emailBody2 );

      // expected results
      List<String> expectedResults = Arrays.asList(
              "abc  123   45   2",
              "def  456   78#'"
      );

      // actual results
      List<String> actualResults = AverageEmailLength
              .cleanEmailBodies( sparkContext.parallelize( emailBodies ) )
              .collect();

      assertEquals( expectedResults.get(0), actualResults.get(0) );
      assertEquals( expectedResults.get(1), actualResults.get(1) );

  }

  @Test
  public void testLengthOfEmailBody() throws Exception {

      // data
      String emailBody1 = "abc def 1 2 3   asdf  jkl;";
      String emailBody2 = "xyzz 453   sa lp;";
      List<String> emailBodies = Arrays.asList( emailBody1, emailBody2 );

      // expected results
      List<Integer> expectedResults = Arrays.asList( 7, 4 );

      // actual results
      JavaRDD< String > cleanedEmails = AverageEmailLength
              .cleanEmailBodies( sparkContext.parallelize( emailBodies ) );
      List<Integer> actualResults = AverageEmailLength
              .countWordsPerEmail( cleanedEmails )
              .collect();

      assertEquals( expectedResults.get(0), actualResults.get(0) );
      assertEquals( expectedResults.get(1), actualResults.get(1) );

  }

  @Test
  public void testAverageLengthOfEmailBody() throws Exception {

      // data
      List< Integer > wordCountList = Arrays.asList( 10, 54, 105, 6032, 256 );

      // expected results
      Double expectedResult = 1291.4;

      // actual results
      Double actualResult = AverageEmailLength
              .averageWordsPerEmail( sparkContext.parallelize( wordCountList ) );

      assertEquals( expectedResult, actualResult );

  }

}