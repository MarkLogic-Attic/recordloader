/**
 * Copyright (c) 2008-2010 Mark Logic Corporation. All rights reserved.
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

import com.marklogic.ps.Utilities;

/**
 * @author Michael Blakeley, Mark Logic Corporation
 * 
 */
public class TranscodingFileLoader extends AbstractLoader {

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.recordloader.AbstractLoader#process()
     */
    public void process() throws LoaderException {
        super.process();

        // handle input as a single document, without parsing
        logger.fine("setting currentId = " + currentRecordPath);

        int size = 0;
        try {
            // we need the content object, hence the URI, before we can
            // check its existence
            currentUri = composeUri(currentRecordPath);
            content = contentFactory.newContent(currentUri);
            boolean skippingRecord = checkIdAndUri(currentRecordPath);

            // grab the entire document in the desired encoding
            byte[] bytes = Utilities.read(input, decoder).getBytes(
                    config.getOutputEncoding());
            if (null == bytes) {
                throw new LoaderException("null document: "
                        + currentRecordPath);
            }
            size = bytes.length;
            if (0 == size) {
                throw new LoaderException("empty document: "
                        + currentRecordPath);
            }

            if (!skippingRecord) {
                logger.finest("bytes = " + size);
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
            cleanupInput(event.isError());
        }
    }

}