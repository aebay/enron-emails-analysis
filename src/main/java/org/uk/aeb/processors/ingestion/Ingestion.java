package org.uk.aeb.processors.ingestion;

import com.pff.PSTFile;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;

import java.io.File;
import java.io.IOException;
import java.net.URI;
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
                                          final List<String> filePaths,
                                          final Configuration configuration ) {

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

    /**
     * Retrieves path names from HDFS
     *
     * @param filePathList
     * @param configuration
     * @return
     * @throws IOException
     */
    public static List<String> getHdfsPathNames( final List<String> filePathList,
                                                 final Configuration configuration ) throws IOException {

        final FileSystem fileSystem = FileSystem.get( URI.create( "hdfs://localhost"), configuration );

        List<String> pathNames = new ArrayList<>();

        // get list of files on path
        Path[] filePaths = new Path[ filePathList.size() ];
        for ( int i = 0; i < filePathList.size(); i++ ) filePaths[i] = new Path( filePathList.get(i) );
        FileStatus[] fileStatus = fileSystem.listStatus( filePaths );
        Path[] listedPathNames = FileUtil.stat2Paths( fileStatus );

        // filter out irrelevant files
        for ( Path filePathName : listedPathNames ) {

            String filePathNameString = filePathName.toString();
            if ((filePathNameString.contains("pst") || filePathNameString.contains("PST")) &&
                    filePathNameString.contains(".zip") &&
                    !filePathNameString.contains(".txt") &&
                    !filePathNameString.contains(".xls")) {
                pathNames.add(filePathNameString);

            }

        }

        return pathNames;

    }

}
