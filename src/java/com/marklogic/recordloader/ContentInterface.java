/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.recordloader;


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
     * @param _xml
     * @throws LoaderException 
     */
    public void setXml(String _xml) throws LoaderException;

    /**
     * Perform any necessary cleanup work
     */
    public void close();

    /**
     * @param _producer
     * @throws LoaderException 
     */
    public void setProducer(Producer _producer) throws LoaderException;
}
