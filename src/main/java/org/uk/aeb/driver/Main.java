package org.uk.aeb.driver;

import com.typesafe.config.Config;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.uk.aeb.processors.Executor;

import java.io.IOException;

/**
 * Created by AEB on 07/05/17.
 *
 * <p>
 *   Driver class for the application.  Loads configuration files,
 *   defines the Spark context and launches the executor.
 * </p>
 *
 */
public class Main {

    private final static Logger logger = Logger.getLogger( "org.uk.aeb.driver" );

    /**
     * Driver for the application.
     *
     * @param args
     */
    public static void main(String[] args) {

        try {

            Configurations configurations = new Configurations();

            Config sparkConfig = configurations.getSparkConfig();
            SparkConf sparkConf = new SparkConf()
                    .setMaster( sparkConfig.getString( "spark.master" ) )
                    .setAppName( sparkConfig.getString( "app.name" ) );

            JavaSparkContext sparkContext = new JavaSparkContext( sparkConf );

            Executor.run( sparkContext, configurations.getApplicationConfig() );

        } catch( IOException e ) {

            logger.error( e );
            System.exit(1);

        }

    }

}
