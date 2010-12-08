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

import java.net.URI;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public interface ContentFactory {

    /**
     * All other properties are up to the implementation
     * 
     * @param _configuration
     * @throws LoaderException
     */
    public void setConfiguration(Configuration _configuration)
            throws LoaderException;

    /**
     * @param _name
     * @throws LoaderException 
     */
    public void setFileBasename(String _name) throws LoaderException;

    /**
     * @param _uri
     * @return
     * @throws LoaderException 
     */
    public ContentInterface newContent(String _uri) throws LoaderException;

    /**
     * @param _uri
     * @throws LoaderException 
     */
    public void setConnectionUri(URI _uri) throws LoaderException;

    /**
     * @return
     */
    public String getVersionString();

    /**
     * 
     */
    public void close();

}
