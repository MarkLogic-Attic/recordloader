/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
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
