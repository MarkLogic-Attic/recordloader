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

    private String fieldDelimiter;

    private int lineNumber;

    private boolean isFatalErrors;

    private String fields[];

    private String[] labels;

    private int labelIndex;

    private String charsetName;

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.AbstractLoader#process()
     */
    @SuppressWarnings("unused")
    public void process() throws LoaderException {
        super.process();

        logger.fine("starting with decoder = " + decoder);
        if (null != decoder) {
            charsetName = decoder.charset().name();
            logger.fine("using " + charsetName);
        }

        if (!(super.config instanceof DelimitedDataConfiguration)) {
            throw new FatalException(
                    Configuration.CONFIGURATION_CLASSNAME_KEY
                            + " must be set to "
                            + DelimitedDataConfiguration.class.getName());
        }
        config = (DelimitedDataConfiguration) super.config;
        fieldDelimiter = config.getFieldDelimiter();
        idName = config.getIdNodeName();
        recordName = config.getRecordName();
        isFatalErrors = config.isFatalErrors();

        boolean downcaseLabels = config.isDowncaseLabels();

        BufferedReader br = new BufferedReader(new InputStreamReader(
                input, decoder));
        String line;
        String id;

        lineNumber = 0;
        labelIndex = 0;
        if (downcaseLabels) {
            recordName = recordName.toLowerCase();
        }

        try {
            // first line contains the labels
            line = br.readLine();
            lineNumber++;
            labels = line.split(fieldDelimiter);
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

            while (null != (line = br.readLine())) {
                String xml = null;
                // line-by-line, so we can move on after errors
                try {
                    xml = handleRecord(line);
                    event.stop();
                } catch (Exception e) {
                    if (isFatalErrors) {
                        throw new FatalException(e);
                    }
                    event.stop(true);
                    logger.logException(e);
                } finally {
                    updateMonitor((null != xml) ? xml.length() : 0);
                    cleanupRecord();
                }
            }
        } catch (Exception e) {
            if (isFatalErrors) {
                throw new FatalException(e);
            }
            event.stop(true);
            logger.logException(e);
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                // no point in doing anything...
                logger.logException(e);
            }
            cleanupInput();
        }
    }

    /**
     * @param line
     * @return
     * @throws LoaderException
     * @throws IOException
     */
    private String handleRecord(String line) throws LoaderException,
            IOException {
        String xml;
        String id;
        event = new TimedEvent();
        lineNumber++;
        // TODO this is too simplistic for CSV with quoted values
        fields = line.split(fieldDelimiter);

        // sanity check
        if (fields.length != labels.length) {
            String msg = "document mismatch at " + currentRecordPath
                    + ":" + lineNumber + ": " + line;
            if (isFatalErrors) {
                throw new LoaderException(msg);
            }
            logger.warning(msg);
        }

        id = fields[labelIndex];
        currentUri = composeUri(id);
        content = contentFactory.newContent(currentUri);
        boolean skippingRecord = checkIdAndUri(currentRecordPath);

        xml = getXml(labels, fields);

        if (!skippingRecord) {
            // write the xml
            // NB - getBytes will return the default-encoding bytes
            content.setBytes(null == decoder ? xml.getBytes() : xml
                    .getBytes(charsetName));
            insert();
        }
        return xml;
    }

    /**
     * @param labels
     * @param fields
     * @return
     */
    private String getXml(String[] labels, String[] fields) {
        // build the xml
        StringBuilder xml = new StringBuilder("<" + recordName + ">");
        for (int i = 0; i < labels.length; i++) {
            xml.append("<" + labels[i] + ">"
                    + Utilities.escapeXml(fields[i]) + "</" + labels[i]
                    + ">");
        }
        xml.append("</" + recordName + ">");
        return xml.toString();
    }

}
