/**
 * Copyright (c) 2006 Mark Logic Corporation. All rights reserved.
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
