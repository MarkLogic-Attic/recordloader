/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.recordloader;

import com.marklogic.ps.Utilities;
import com.marklogic.recordloader.xcc.XccConfiguration;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class DelimitedDataConfiguration extends XccConfiguration {

    public static final String FIELD_DELIMITER_KEY = "FIELD_DELIMITER";

    public static final String FIELD_DELIMITER_DEFAULT = "\t";

    private static final String DOWNCASE_LABELS_KEY = "DOWNCASE_LABELS";

    private static final String DOWNCASE_LABELS_DEFAULT = "true";

    /**
     * @return
     */
    public String getFieldDelimiter() {
        return properties.getProperty(FIELD_DELIMITER_KEY,
                FIELD_DELIMITER_DEFAULT);
    }

    /**
     * @return
     */
    public boolean isDowncaseLabels() {
        return Utilities.stringToBoolean(properties.getProperty(
                DOWNCASE_LABELS_KEY, DOWNCASE_LABELS_DEFAULT));
    }

}
