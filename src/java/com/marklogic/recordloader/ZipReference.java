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
package com.marklogic.recordloader;

import java.io.File;
import java.io.IOException;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

import com.marklogic.ps.SimpleLogger;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class ZipReference extends ZipFile {

    protected volatile int references = 0;

    protected SimpleLogger logger;

    protected Object mutex = new Object();

    /**
     * @param _file
     * @param _logger
     * @throws IOException
     * @throws ZipException
     */
    public ZipReference(File _file, SimpleLogger _logger)
            throws ZipException, IOException {
        super(_file);
        logger = _logger;
    }

    /**
     * 
     */
    public void addReference() {
        synchronized (mutex) {
            references++;
        }
    }

    /**
     * 
     */
    public void closeReference() {
        synchronized (mutex) {
            references--;

            if (0 > references) {
                throw new FatalException("refcount error: " + references
                        + " for " + getName());
            }

            if (0 != references) {
                return;
            }

            // free the resources for the input zip package
            try {
                logger.info("closing " + getName());
                close();
            } catch (IOException e) {
                // should not happen - tell the user and proceed
                e.printStackTrace();
            }
        }
    }

}
