/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.recordloader;

import com.marklogic.ps.Utilities;

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

        int size = 0;
        try {
            // we need the content object, hence the URI, before we can check
            // its existence
            currentUri = composeUri(currentRecordPath);
            content = contentFactory.newContent(currentUri);
            boolean skippingRecord = checkIdAndUri(currentRecordPath);

            // grab the entire document
            // do not pass the stream directly, so that XCC can retry
            byte[] bytes = Utilities.read(input);
            size = bytes.length;
            if (null == bytes || 0 == size) {
                throw new LoaderException("empty document: "
                        + currentRecordPath);
            }

            if (!skippingRecord) {
                content.setBytes(bytes);
                insert();
            }
        } catch (Exception e) {
            if (config.isFatalErrors()) {
                throw new FatalException(e);
            }
            event.stop(true);
            logger.logException(e);
        } finally {
            updateMonitor(size);
            cleanupInput();
        }
    }

}