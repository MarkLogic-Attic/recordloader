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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.CharsetDecoder;

import org.xmlpull.v1.XmlPullParserException;

import com.marklogic.xcc.exceptions.XccException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class LoaderFactory {

    private Monitor monitor;

    private CharsetDecoder decoder;

    private Configuration config;

    private long count = 0;

    /**
     * @param _monitor
     * @param _decoder
     * @param _config
     */
    public LoaderFactory(Monitor _monitor,
            CharsetDecoder _decoder, Configuration _config) {
        monitor = _monitor;
        decoder = _decoder;
        config = _config;
    }

    private Loader getLoader() throws XccException,
            XmlPullParserException {
        // if multiple connString are available, we round-robin
        int x = (int) (count++ % config.getConnectionStrings().length);
        return new Loader(monitor, config.getConnectionStrings()[x],
                config);
    }

    /**
     * @param stream
     * @param _name
     * @return
     * @throws XccException
     * @throws XmlPullParserException
     */
    public Loader newLoader(InputStream stream, String _name)
            throws XccException, XmlPullParserException {
        Loader loader = getLoader();
        loader.setInput(new BufferedReader(new InputStreamReader(stream,
                decoder)));
        if (_name != null) {
            loader.setFileBasename(stripExtension(_name));
        }
        return loader;
    }

    /**
     * @param file
     * @return
     * @throws XccException
     * @throws XmlPullParserException
     * @throws FileNotFoundException
     */
    public Loader newLoader(File file) throws XccException,
            XmlPullParserException, FileNotFoundException {
        Loader loader = getLoader();
        loader.setInput(file);
        loader.setFileBasename(stripExtension(file.getName()));
        return loader;
    }

    /**
     * @param name
     * @return
     */
    private static String stripExtension(String name) {
        if (name == null || name.length() < 3) {
            return name;
        }

        int i = name.lastIndexOf('.');
        if (i < 1) {
            return name;
        }

        return name.substring(0, i);
    }

}
