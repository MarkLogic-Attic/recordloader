/**
 * Copyright (c) 2006 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.recordloader;

import org.xmlpull.v1.XmlPullParser;

import com.marklogic.ps.SimpleLogger;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class ProducerThreadFactory {

    private Loader loader;

    private Configuration config;

    private SimpleLogger logger;

    private XmlPullParser xpp;

    /**
     * @param _loader
     * @param _config
     */
    public ProducerThreadFactory(Loader _loader, Configuration _config) {
        loader = _loader;
        config = _config;
        
        logger = config.getLogger();
        xpp = loader.getParser();
    }

    /**
     * @return
     */
    public ProducerThread newProducerThread() {
        return new ProducerThread(loader, config, logger, xpp);
    }

}
