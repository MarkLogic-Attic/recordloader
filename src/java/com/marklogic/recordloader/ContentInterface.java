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

import java.io.InputStream;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 * This interface defines a simple mechanism for content inserts.
 */
public interface ContentInterface {
    /**
     * Used to check for document URI uniqueness, for resumable loads.
     * 
     * @param _uri
     * @return
     * @throws LoaderException
     */
    public boolean checkDocumentUri(String _uri) throws LoaderException;

    /**
     * Every document must have a URI
     * 
     * @param _uri
     */
    public void setUri(String _uri);

    /**
     * Commit the new document to the database
     * 
     * @throws LoaderException
     */
    public void insert() throws LoaderException;

    /**
     * For monolithic document inserts, provide the XML content as a string
     * 
     * @param _bytes
     * @throws LoaderException 
     */
    public void setBytes(byte[] _bytes) throws LoaderException;

    /**
     * Perform any necessary cleanup work
     */
    public void close();

    /**
     * @param _is
     * @throws LoaderException 
     */
    public void setInputStream(InputStream _is) throws LoaderException;
}
