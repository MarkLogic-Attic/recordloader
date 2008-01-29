/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
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
