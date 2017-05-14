package org.uk.aeb.models;

import com.pff.PSTException;
import com.pff.PSTMessage;
import com.pff.PSTRecipient;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by AEB on 10/05/17.
 *
 * Class to hold lists of recipients by type (To and Cc supported).
 *
 * <p>
 *   Some internal Enron e-mail addresses are not provided in raw form.
 *   These have been reconstructed using the email short code.
 * </p>
 */
public class Recipients {

    final private static Logger logger = LogManager.getLogger( "org.uk.aeb.models.Recipients");

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

        processRecipients( message );

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
     * Parses email, determines type of recipient and adds email address to the correct list.
     *
     * @param message
     */
    private void processRecipients( final PSTMessage message ) throws PSTException, IOException {

        int numRecipients = 0;

        String originalEmailAddress;
        String emailAddress;

        PSTRecipient recipient;

        try {
            numRecipients = message.getNumberOfRecipients();
        } catch ( NullPointerException e  ) {
            logger.warn( "No message recipients" );
            logger.warn( e );
        }

        for ( int i = 0; i < numRecipients; i++ ) {

            recipient = message.getRecipient(i);

            // format email address
            originalEmailAddress = recipient.getEmailAddress();

            if ( originalEmailAddress.contains("@") ) emailAddress = cleanEmailAddress( originalEmailAddress );
            else if ( originalEmailAddress.contains( "DL-" ) ) emailAddress = cleanCnEmailAddress( originalEmailAddress, "DL-", 0 );
            else {

                i++;
                recipient = message.getRecipient(i);
                originalEmailAddress = recipient.getEmailAddress();

                emailAddress = cleanCnEmailAddress( originalEmailAddress, "cn=", 3 );

            }

            // bin email address by sender type
            if (recipient.getRecipientType() == 1) to.add( emailAddress ); // "To" recipient
            if (recipient.getRecipientType() == 2) cc.add( emailAddress ); // "Cc" recipient

        }

    }

    /**
     * Cleans and normalises a raw email address.
     *
     * <p>
     *   By default, all @enron.com addresses are converted to lower case
     *   as both cases are used for named person Enron recipient emails
     *   but Outlook emails are case insensitive.
     * </p>
     *
     * @param rawEmailAddress
     * @return
     */
    private String cleanEmailAddress( final String rawEmailAddress ) {

        // clean
        String cleanedEmailAddress = rawEmailAddress.replace( "'", "" );

        // normalise
        String lowerCaseEmailAddress = rawEmailAddress.toLowerCase();

        if ( lowerCaseEmailAddress.contains( "@enron.com" ) ) return lowerCaseEmailAddress;
        else return cleanedEmailAddress;

    }

    /**
     * <p>
     *     Special case to handle email addresses in the following form:
     *     /O=ENRON/OU=NA/CN=RECIPIENTS/CN=DL-PortlandRealTimeShift
     * </p>
     *
     * @param rawEmailAddress
     * @param emailCharacterSequenceMatch : sequence of characters to match on
     * @param emailMatchOffset            : offset from match start index where the email substring begins
     * @return
     */
    private String cleanCnEmailAddress( final String rawEmailAddress,
                                        final String emailCharacterSequenceMatch,
                                        final Integer emailMatchOffset ) {

        Integer emailNameStartIndex;

        String emailAddress;

        String lowerCaseEmailAddress = rawEmailAddress.toLowerCase();
        String lowerCaseCharacterMatch = emailCharacterSequenceMatch.toLowerCase();

        // case where the email address is split over two recipient objects
        if ( lowerCaseEmailAddress.contains( "@" ) && !lowerCaseEmailAddress.contains( lowerCaseCharacterMatch ) ) {
            emailAddress = lowerCaseEmailAddress;
        }
        else {

            emailNameStartIndex = lowerCaseEmailAddress.lastIndexOf( lowerCaseCharacterMatch );

            emailAddress = lowerCaseEmailAddress.substring( emailNameStartIndex + emailMatchOffset )
                    .toLowerCase()
                    .concat( "@enron.com" );

        }

        return emailAddress;

    }

}
