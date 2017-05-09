package org.uk.aeb.processors.ingestion;

import com.pff.PSTFile;
import org.apache.commons.io.IOUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Created by AEB on 07/05/17.
 *
 * Module that handles the ingestion of files
 */
final public class Ingestion {

    /**
     * <p>
     *     Ingests files from the paths provided, decompresses these and returns
     *     a JavaRDD of these in PST file form.
     * </p>
     *
     * @param sparkContext
     * @param filePaths: list of file paths that contain the files to be ingested
     */
    public static JavaRDD< PSTFile > run( final JavaSparkContext sparkContext,
                                          final List<String> filePaths ) {

        List< String > pathNames = getPathNames( filePaths );

        JavaPairRDD< String, PortableDataStream > zipPstFiles = readPstZipFiles( sparkContext, pathNames );

        JavaRDD< PSTFile > pstFiles = unzipFiles( zipPstFiles );

        return pstFiles;
    }

    /**
     * Unzips the PST file archives and converts them to PSTFile format.
     *
     * @param zipPstFiles
     * @return
     */
    public static JavaRDD<PSTFile> unzipFiles( final JavaPairRDD< String, PortableDataStream > zipPstFiles ) {

        JavaRDD< ZipInputStream > zipInputStream = zipPstFiles.map( kv -> new ZipInputStream(kv._2.open()) );

        JavaRDD< PSTFile > pstFiles = zipInputStream.flatMap( zis -> {

            List<PSTFile> pstFileList = new ArrayList<>();

            ZipEntry zipEntry = zis.getNextEntry(); // get ZIP file list entry

            while( zipEntry != null ) {

                PSTFile pstFile = new PSTFile( IOUtils.toByteArray( zis ) );
                pstFileList.add( pstFile );

                zipEntry = zis.getNextEntry();
            }

            return pstFileList;

        } );

        return pstFiles;

    }

    /**
     * Reads in all PST files from the specified file paths
     *
     * @param sparkContext
     * @param pathNames
     * @return
     */
    public static JavaPairRDD<String, PortableDataStream> readPstZipFiles( final JavaSparkContext sparkContext,
                                                                           final List<String> pathNames ) {

        StringBuilder pathNameList = new StringBuilder();
        for ( String pathName : pathNames ) {
            pathNameList.append( ",").append( pathName );
        }

        JavaPairRDD< String, PortableDataStream > zipPstFiles = sparkContext.binaryFiles( pathNameList.substring(1) );

        return zipPstFiles;

    }

    /**
     * <p>
     *     Searches all directory file paths passed in and returns a list of files
     *     that contain PST archive data.
     * </p>
     *
     * @param filePaths
     * @return
     */
    public static List<String> getPathNames(final List<String> filePaths ) {

        List<String> pathNames = new ArrayList<>();

        for ( String filePath : filePaths ) {

            // get list of files in the target directory
            File file = new File( filePath );
            List<String> fileNames = Arrays.asList( file.list() );

            // filter file names to eliminate irrelevants
            for ( String fileName : fileNames ) {

                if ( ( fileName.contains( "pst" ) || fileName.contains( "PST" ) ) &&
                        fileName.contains( ".zip" ) &&
                        !fileName.contains( ".txt" ) &&
                        !fileName.contains( ".xls" ) ) {
                    pathNames.add( filePath + "/" + fileName );
                }

            }

        }

        return pathNames;

    }

}
