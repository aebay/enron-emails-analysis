package org.uk.aeb.processors.transformation;

import com.pff.PSTFile;
import org.apache.spark.api.java.JavaRDD;
import org.uk.aeb.models.PstWrapper;

/**
 * Created by AEB on 09/05/17.
 *
 * <p>
 *     Module that handles the transformation of PSTFile objects to
 *     an object that is easier to handle for the analysis (PstWrapper).
 * </p>
 */
final public class Transformation {

    /**
     * <p>
     *  Converts PSTFile objects to an object that retrieves the necessary
     *  variables for the analysis downstream.
     * </p>
     *
     * @param pstFiles
     * @return
     */
    public static JavaRDD<PstWrapper> run(final JavaRDD<PSTFile> pstFiles ) {

        JavaRDD<PstWrapper> pstWrapper = pstFiles.map( pst -> new PstWrapper( pst.getRootFolder() ) );

        return pstWrapper;

    }

}
