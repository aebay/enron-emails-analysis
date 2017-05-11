package org.uk.aeb.driver;

import jdk.nashorn.internal.runtime.regexp.joni.Config;

/**
 * Created by AEB on 11/05/17.
 *
 * <p>
 *     Handles configuration path and loads configuration files
 *     into application.
 * </p>
 */
final public class Configurations {

    final private static String SPARK_FILE = "spark.properties";
    final private static String APPLICATION_FILE = "application.properties";

    private Config sparkConfig;

    /**
     * Constructor
     */
    public Configurations() {

        loadPath();

        this.sparkConfig = sparkConfig;
    }
}
