package org.uk.aeb.processors;

import com.pff.PSTFile;

import com.typesafe.config.Config;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.uk.aeb.driver.Utilities;
import org.uk.aeb.models.PstWrapper;
import org.uk.aeb.processors.ingestion.Ingestion;
import org.uk.aeb.processors.logic.AverageEmailLength;
import org.uk.aeb.processors.logic.TopNRecipientEmailAddresses;
import org.uk.aeb.processors.persistence.Persistence;
import org.uk.aeb.processors.transformation.Transformation;
import scala.Tuple2;

import java.util.List;

/**
 * Created by AEB on 11/05/17.
 *
 * Runs the processor modules.
 */
final public class Executor {

    /**
     * Runs application processes.
     *
     * @param sparkContext
     * @param applicationConfig
     */
    public static void run( final JavaSparkContext sparkContext, final Config applicationConfig ) {

        // ingestion
        List<String> inputPathList = Utilities.parseList( applicationConfig.getString( "input.source.directories" ), "|" );
        JavaRDD<PSTFile> pstFiles = Ingestion.run( sparkContext, inputPathList );

        // transformation
        JavaRDD<PstWrapper> pstWrappers = Transformation.run( pstFiles ).cache();

        // logic
        Integer topEmailNumber = applicationConfig.getInt( "top.email.number" );
        Double averageEmailLength = AverageEmailLength.calculate( pstWrappers );
        List< Tuple2<String, Long> > topNRecipientEmailAddresses =
                TopNRecipientEmailAddresses.calculate( pstWrappers, topEmailNumber );

        // persistence
        String outputPathName = applicationConfig.getString( "output.path.name" );
        Persistence.run( averageEmailLength, topNRecipientEmailAddresses, outputPathName );

    }

}
