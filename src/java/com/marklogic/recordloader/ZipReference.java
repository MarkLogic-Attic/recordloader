/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
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

    protected int references = 0;

    private SimpleLogger logger;

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
        // TODO synchronized? mutex?
        references++;
        //logger.info(getName() + ": " + references);
    }

    /**
     * 
     */
    public void closeReference() {
        // TODO synchronized? mutex?
        references--;
        //logger.info(getName() + ": " + references);

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
