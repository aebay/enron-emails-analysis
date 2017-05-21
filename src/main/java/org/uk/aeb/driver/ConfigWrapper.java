package org.uk.aeb.driver;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.conf.Configuration;

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
public class ConfigWrapper {

    final private static String CONF_DIR = "confDir";
    final private static String SPARK_FILE = "/spark.properties";
    final private static String APPLICATION_FILE = "/application.properties";
    final private static String HADOOP_FILE = "/hadoop.properties";

    private String path;

    private Config sparkConfig;
    private Config applicationConfig;
    private Config hadoopConfig;

    private Configuration configuration;

    /**
     * Constructor
     */
    public ConfigWrapper() throws IOException {
        setPath();
        loadConfigurations();
        setFsConfiguration();
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
    public Configuration getConfiguration() { return configuration; }

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
        final File hadoopPath = new File( path.concat( HADOOP_FILE ) );

        sparkConfig = ConfigFactory.parseFile( sparkPath );
        applicationConfig = ConfigFactory.parseFile( applicationPath );
        hadoopConfig = ConfigFactory.parseFile( hadoopPath );

    }

    private void setFsConfiguration() {

        configuration = new Configuration();
        configuration.addResource( hadoopConfig.getString( "core.site.pathname" ) );
        configuration.addResource( hadoopConfig.getString( "hdfs.site.pathname" ) );

    }
}
