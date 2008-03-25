/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.recordloader;

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
        if (null == input) {
            throw new NullPointerException("caller must set input");
        }

        StringBuffer sb = new StringBuffer();
        try {
            // handle input as a single document, without parsing
            logger.fine("setting currentId = " + currentRecordPath);

            // we need the content object, hence the URI, before we can check
            // its existence
            currentUri = composeUri(currentRecordPath);
            content = contentFactory.newContent(currentUri);
            boolean skippingRecord = checkIdAndUri(currentRecordPath);

            // grab the entire document
            // uses a reader, so charset translation should be ok
            // TODO can we pass the reader directly to the Content?
            int size;
            char[] buf = new char[32 * 1024];
            while ((size = input.read(buf)) > 0) {
                sb.append(buf, 0, size);
            }

            if (0 == sb.length()) {
                throw new LoaderException("empty document: "
                        + currentRecordPath);
            }

            if (!skippingRecord) {
                content.setXml(sb.toString());
                insert();
            }
        } catch (Exception e) {
            if (config.isFatalErrors()) {
                throw new FatalException(e);
            }
            event.stop(true);
            logger.logException(e);
        } finally {
            updateMonitor(sb.length());
            cleanup();
        }
    }

}
