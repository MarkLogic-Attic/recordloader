/**
 * Copyright (c) 2008-2009 Mark Logic Corporation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * The use of the Apache License does not indicate that this project is
 * affiliated with the Apache Software Foundation.
 */
package com.marklogic.recordloader.xcc;

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

    /**
     * @return
     */
    public String getLoaderClassName() {
        return DelimitedDataLoader.class.getCanonicalName();
    }

    public String getRecordName() {
        return (null == recordName) ? "document-root" : recordName;
    }
}
