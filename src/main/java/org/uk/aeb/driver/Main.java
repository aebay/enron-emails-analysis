package org.uk.aeb.driver;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.IOException;

/**
 * Created by AEB on 07/05/17.
 */
public class Main {

    /**
     * Driver for the application.
     *
     * @param args
     */
    public static void main(String[] args) {

        try {

            // load configuration file
            String confPath = System.getProperty( "confPath" );
            if ( confPath.isEmpty() || confPath == null ) throw new IOException();

        } catch( IOException ) {

        }

        Config sparkConfig = ConfigFactory.load();

        // set spark context

            // execute batch job



    }

}
