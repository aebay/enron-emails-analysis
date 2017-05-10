package org.uk.aeb.models;

import com.pff.PSTException;
import com.pff.PSTMessage;
import com.pff.PSTRecipient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by AEB on 10/05/17.
 *
 * Class to hold lists of recipients by type (To and Cc supported).
 *
 * <p>
 *   Issue with some email addresses being split across 2 recipient
 *   classes.
 * </p>
 *
 * Easier to read in full display list and parse list.
 */
@Deprecated
public class Recipients {

    private List<String> to;
    private List<String> cc;

    /**
     * Constructor
     *
     * @param message
     */
    public Recipients( final PSTMessage message ) throws PSTException, IOException {

        this.to = new ArrayList<>();
        this.cc = new ArrayList<>();

        sortRecipients( message );

    }

    /**
     *
     * @return
     */
    public List<String> getTo() { return to; }

    /**
     *
     * @return
     */
    public List<String> getCc() { return cc; }

    /**
     * Determines type of recipient and adds email address to the correct list.
     *
     * @param message
     */
    private void sortRecipients( final PSTMessage message ) throws PSTException, IOException {

        final int numRecipients = message.getNumberOfRecipients();

        for ( int i = 0; i < numRecipients; i++ ) {

            PSTRecipient recipient = message.getRecipient( i );

            if ( recipient.getRecipientType() == 1 ) to.add( recipient.getDisplayName() );
            if ( recipient.getRecipientType() == 2 ) cc.add( recipient.getDisplayName() );

        }

    }

}
