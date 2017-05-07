package org.uk.aeb;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
  * Created by AEB on 06/05/17.
  */
public class IngestionIT {

  private static final String ZIP_FILE_PATH = "/data/edrm-enron-v2/edrm-enron-v2_meyers_a_pst.zip";
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
  public void testFilteringOfNonRelevantFiles() throws Exception {

    // TODO

  }

  @Test
  public void testReadingFilesFromMultipleDirectories() throws Exception {

    // TODO

  }

  @Test
  public void testReadingMultipleFilesFromMultipleDirectories() throws Exception {

    // TODO

  }

}