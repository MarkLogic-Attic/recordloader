/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.recordloader;

import com.marklogic.ps.Utilities;
import com.marklogic.recordloader.xcc.XccConfiguration;
import com.marklogic.xcc.exceptions.XQueryException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class FileLoader extends AbstractLoader {

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.AbstractLoader#process()
     */
    @SuppressWarnings("unused")
    public void process() throws LoaderException {
        super.process();

        // handle input as a single document, without parsing
        logger.fine("setting currentId = " + currentRecordPath);

        // loop for retries, for XDMP-TOOMANYSTANDS
        long retryMillis = config.getTooManyStandsRetryMillis();
        while (true) {
            int size = 0;
            try {
                // we need the content object, hence the URI, before we can
                // check
                // its existence
                currentUri = composeUri(currentRecordPath);
                content = contentFactory.newContent(currentUri);
                boolean skippingRecord = checkIdAndUri(currentRecordPath);

                // grab the entire document, as bytes to support binaries
                // do not pass the stream directly, so that XCC can retry
                byte[] bytes = Utilities.read(input);
                size = bytes.length;
                if (null == bytes || 0 == size) {
                    throw new LoaderException("empty document: "
                            + currentRecordPath);
                }

                if (!skippingRecord) {
                    logger.finest("bytes = " + bytes.length);
                    content.setBytes(bytes);
                    content.setFormat(format);
                    insert();
                }
                break;
            } catch (Exception e) {
                if (config.isFatalErrors()) {
                    throw new FatalException(e);
                }
                if (retryMillis > 0
                        && e instanceof XQueryException
                        && XccConfiguration.CODE_TOOMANYSTANDS
                                .equals(((XQueryException) e).getCode())) {
                    logger.warning(e.getLocalizedMessage()
                            + " - sleeping " + retryMillis);
                    // sleep and retry
                    try {
                        Thread.sleep(retryMillis);
                    } catch (InterruptedException e1) {
                        // nothing useful we can do
                        e1.printStackTrace();
                    }
                    continue;
                }
                event.stop(true);
                logger.logException(e);
                break;
            } finally {
                updateMonitor(size);
                cleanupInput();
            }
        }
    }

}