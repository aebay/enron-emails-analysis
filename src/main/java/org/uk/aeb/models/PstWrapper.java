package org.uk.aeb.models;

import com.pff.PSTException;
import com.pff.PSTFolder;
import com.pff.PSTMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by AEB on 08/05/17.
 *
 * Code adapted from http://www.devjavasource.com/java/java-program-to-read-pst-and-ost-files/
 *
 * There are only getters, since the e-mail contents are generated on initialisation.
 */
public class PstWrapper {

    private PSTFolder rootFolder;
    private int depth;

    private List<String> emailBodies;
    private List<String> toRecipients;
    private List<String> ccRecipients;

    /**
     * Constructor
     *
     * @param rootFolder
     */
    public PstWrapper(final PSTFolder rootFolder ) throws PSTException, IOException {
        this.rootFolder = rootFolder;
        this.depth = -1;
        this.emailBodies = new ArrayList<>();
        this.toRecipients = new ArrayList<>();
        this.ccRecipients = new ArrayList<>();

        processFolder( rootFolder );
    }

    /**
     * Returns a list of email bodies in continuous strings.
     *
     * @return
     */
    public List<String> getEmailBodies() {
        return emailBodies;
    }

    /**
     * Returns a list of "To" recipients in continuous strings.
     *
     * @return
     */
    public List<String> getToRecipients() {
        return toRecipients;
    }

    /**
     * Returns a list of "Cc" recipients in continuous strings.
     *
     * @return
     */
    public List<String> getCcRecipients() {
        return ccRecipients;
    }


    /**
     * <p>
     *     Recursive function that extracts any and all e-mail content by traversing
     *     through the folder structure.
     * </p>
     *
     * @param folder
     * @throws PSTException
     * @throws java.io.IOException
     */
    public void processFolder(PSTFolder folder) throws PSTException, java.io.IOException
    {
        depth++;

        // recursively iterate through the folders...
        if ( folder.hasSubfolders() ) {
            List< PSTFolder > childFolders = folder.getSubFolders();
            for (PSTFolder childFolder : childFolders) {
                processFolder( childFolder );
            }
        }

        // extract information if any emails are present
        if (folder.getContentCount() > 0) {
            depth++;
            PSTMessage email = (PSTMessage)folder.getNextChild();
            while (email != null) {

                emailBodies.add( email.getBody() );
                toRecipients.add( email.getDisplayTo() );
                ccRecipients.add( email.getDisplayCC() );

                email = (PSTMessage)folder.getNextChild();

            }
            depth--;
        }
        depth--;
    }

}
