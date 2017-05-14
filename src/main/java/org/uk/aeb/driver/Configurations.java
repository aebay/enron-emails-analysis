package org.uk.aeb.driver;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.io.IOException;

/**
 * Created by AEB on 11/05/17.
 *
 * <p>
 *     Handles configuration path and loads configuration files
 *     into application.
 * </p>
 */
public class Configurations {

    final private static String CONF_DIR = "confDir";
    final private static String SPARK_FILE = "/spark.properties";
    final private static String APPLICATION_FILE = "/application.properties";

    private String path;

    private Config sparkConfig;
    private Config applicationConfig;

    /**
     * Constructor
     */
    public Configurations() throws IOException {
        setPath();
        loadConfigurations();
    }

    /**
     *
     * @return
     */
    public Config getSparkConfig() {
        return sparkConfig;
    }

    /**
     *
     * @return
     */
    public Config getApplicationConfig() {
        return applicationConfig;
    }

    /**
     * Sets the configuration directory path.
     */
    private void setPath() throws IOException {

        path = System.getProperty( CONF_DIR );

        if ( path.isEmpty() || path == null ) throw new IOException();

    }

    /**
     * Load the configuration files
     */
    private void loadConfigurations() {

        final File sparkPath = new File( path.concat( SPARK_FILE ) );
        final File applicationPath = new File( path.concat( APPLICATION_FILE ) );

        sparkConfig = ConfigFactory.parseFile( sparkPath );
        applicationConfig = ConfigFactory.parseFile( applicationPath );

    }
}
