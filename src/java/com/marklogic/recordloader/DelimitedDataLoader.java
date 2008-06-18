/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.recordloader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.marklogic.ps.Utilities;
import com.marklogic.ps.timing.TimedEvent;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class DelimitedDataLoader extends AbstractLoader {

    DelimitedDataConfiguration config;

    private String recordName;

    private String idName;

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

        config = (DelimitedDataConfiguration) super.config;
        String fieldDelimiter = config.getFieldDelimiter();
        boolean downcaseLabels = config.isDowncaseLabels();

        BufferedReader br = new BufferedReader(new InputStreamReader(
                input, decoder));
        StringBuffer xml = null;
        String line;
        String id;

        int labelIndex = 0;
        idName = config.getIdNodeName();
        recordName = config.getRecordName();
        startId = config.getStartId();
        boolean isFatalErrors = config.isFatalErrors();
        if (downcaseLabels) {
            recordName = recordName.toLowerCase();
        }

        try {
            // first line contains the labels
            line = br.readLine();
            String[] labels = line.split(fieldDelimiter);
            // match the configured idName with the input labels
            for (int i = 0; i < labels.length; i++) {
                if (downcaseLabels) {
                    labels[i] = labels[i].toLowerCase();
                }
                if (idName.equals(labels[i])) {
                    labelIndex = i;
                    // do not exit loop - must downcase remaining labels
                }
            }

            String fields[];
            int lineNumber = 0;
            while (null != (line = br.readLine())) {
                event = new TimedEvent();
                lineNumber++;
                // TODO this is too simplistic for CSV with quoted values
                fields = line.split(fieldDelimiter);

                // sanity check
                if (fields.length != labels.length) {
                    String msg = "document mismatch at "
                            + currentRecordPath + ":" + lineNumber + ": "
                            + line;
                    if (isFatalErrors) {
                        throw new LoaderException(msg);
                    }
                    logger.warning(msg);
                }

                id = fields[labelIndex];
                currentUri = composeUri(id);
                content = contentFactory.newContent(currentUri);
                boolean skippingRecord = checkIdAndUri(currentRecordPath);

                // build the xml
                xml = new StringBuffer("<" + recordName + ">");
                for (int i = 0; i < labels.length; i++) {
                    xml.append("<" + labels[i] + ">"
                            + Utilities.escapeXml(fields[i]) + "</"
                            + labels[i] + ">");
                }
                xml.append("</" + recordName + ">");

                if (!skippingRecord) {
                    // write the xml
                    content.setBytes(xml.toString().getBytes());
                    insert();
                }
                event.stop();
                updateMonitor(xml.length());
                cleanup();
            }
        } catch (Exception e) {
            if (isFatalErrors) {
                throw new FatalException(e);
            }
            event.stop(true);
            updateMonitor(xml.length());
            logger.logException(e);
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                // no point in doing anything...
                logger.logException(e);
            }
            cleanup();
        }
    }

}
