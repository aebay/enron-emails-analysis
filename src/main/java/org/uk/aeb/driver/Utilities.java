package org.uk.aeb.driver;

import java.util.Arrays;
import java.util.List;

/**
 * Created by AEB on 13/05/17.
 *
 * Contains functions used for configuration file processing.
 */
final public class Utilities {

    /**
     * Splits a list string representation into a string list.
     *
     * @param list
     * @param delimiter
     * @return
     */
    public static List<String> parseList( final String list, final String delimiter ) {

        List<String> parsedList = Arrays.asList( list.split( delimiter ) );

        return parsedList;

    }

}
