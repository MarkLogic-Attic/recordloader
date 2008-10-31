/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.recordloader;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.CharsetDecoder;
import java.util.concurrent.Callable;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public interface LoaderInterface extends Callable<Object> {

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.Callable#call()
     * 
     * NB - always returns null
     */
    public abstract Object call() throws Exception;

    /**
     * @throws LoaderException
     * 
     * Do not call this method directly: it will be called by call()
     */
    abstract void process() throws LoaderException;

    /**
     * @param _config
     * @throws LoaderException
     */
    public abstract void setConfiguration(Configuration _config)
            throws LoaderException;

    /**
     * @param _monitor
     * @throws LoaderException
     */
    public abstract void setMonitor(Monitor _monitor)
            throws LoaderException;

    /**
     * @param _is
     * @throws LoaderException
     */
    public abstract void setInput(InputStream _is, CharsetDecoder _decoder)
            throws LoaderException;

    /**
     * @param _file
     */
    public abstract void setInput(File _file, CharsetDecoder _decoder)
            throws LoaderException;

    /**
     * @param _path
     */
    public abstract void setFileBasename(String _name)
            throws LoaderException;

    /**
     * @param _path
     */
    public abstract void setRecordPath(String _path)
            throws LoaderException;

    /**
     * @param uri
     */
    public abstract void setConnectionUri(URI uri) throws LoaderException;

}