/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.recordloader;


/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 *
 */
public class LoaderException extends Exception {

    /**
     * @param cause
     */
    public LoaderException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public LoaderException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message
     */
    public LoaderException(String message) {
        super(message);
    }

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

}
