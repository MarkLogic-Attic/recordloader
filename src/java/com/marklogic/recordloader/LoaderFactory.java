/**
 * Copyright (c) 2006-2010 Mark Logic Corporation. All rights reserved.
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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.logging.Logger;

import org.xmlpull.v1.XmlPullParserException;

import com.marklogic.ps.RecordLoader;
import com.marklogic.ps.SimpleLogger;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class LoaderFactory {

    private Monitor monitor;

    private Configuration config;

    private long count = 0;

    private SimpleLogger logger;

    private Constructor<? extends LoaderInterface> loaderConstructor;

    /**
     * @param _monitor
     * @param _config
     * @throws NoSuchMethodException
     * @throws SecurityException
     * @throws ClassNotFoundException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    public LoaderFactory(Monitor _monitor, Configuration _config)
            throws SecurityException, NoSuchMethodException,
            ClassNotFoundException, IllegalArgumentException,
            IllegalAccessException, InvocationTargetException {
        monitor = _monitor;
        config = _config;

        logger = config.getLogger();

        // *after* config init, figure out which loader to use:
        // this should only be called once, in a single-threaded context
        String loaderClassName = config.getLoaderClassName();
        if (config.isFirstLoop()) {
            logger.info("Loader is " + loaderClassName);
        }
        Class<? extends LoaderInterface> loaderClass;
        loaderClass = Class.forName(loaderClassName, true,
                RecordLoader.getClassLoader()).asSubclass(
                LoaderInterface.class);
        loaderConstructor = loaderClass.getConstructor(new Class[] {});

        /*
         * AbstractLoader subclasses can define a static method
         * checkEnvironment(), and this is where we call it. If anything goes
         * wrong, it will throw a run-time exception.
         */
        if (config.isFirstLoop()) {
            Method check = loaderClass.getMethod("checkEnvironment",
                    Logger.class);
            check.invoke(null, logger);
            if (TranscodingLoader.class.isAssignableFrom(loaderClass)) {
                logger.info("transcoding loader");
                config.setLoaderTranscoding(true);
            }
        }
    }

    private LoaderInterface getLoader() throws LoaderException {
        // if multiple connString are available, we round-robin
        int x = (int) (count++ % config.getConnectionStrings().length);
        try {
            LoaderInterface loader = loaderConstructor.newInstance();
            loader.setConfiguration(config);
            loader.setMonitor(monitor);
            loader.setConnectionUri(config.getConnectionStrings()[x]);
            logger.finer("loader " + loader);
            return loader;
        } catch (IllegalArgumentException e) {
            throw new LoaderException(e);
        } catch (InstantiationException e) {
            throw new LoaderException(e);
        } catch (IllegalAccessException e) {
            throw new LoaderException(e);
        } catch (InvocationTargetException e) {
            throw new LoaderException(e);
        }
    }

    /**
     * @param _in
     * @return
     * @throws LoaderException
     */
    public LoaderInterface newLoader(InputStream _in)
            throws LoaderException {
        return newLoader(_in, null, null);
    }

    /**
     * @param _stream
     * @param _name
     * @return
     * @throws LoaderException
     */
    public LoaderInterface newLoader(InputStream _stream, String _name,
            String _path) throws LoaderException {
        LoaderInterface loader = getLoader();
        BufferedInputStream br = new BufferedInputStream(_stream);
        // decoders are not thread-safe - new one every time
        loader.setInput(br, config.getDecoder());
        setup(loader, _name, _path);
        return loader;
    }

    /**
     * @param _file
     * @return
     * @throws LoaderException
     * @throws XmlPullParserException
     * @throws FileNotFoundException
     */
    public LoaderInterface newLoader(File _file) throws LoaderException {
        // some duplicate code: we want to defer opening the file,
        // to limit the number of open file descriptors
        LoaderInterface loader = getLoader();
        // decoder is not thread-safe - new one every time
        loader.setInput(_file, config.getDecoder());
        setup(loader, _file.getName(), _file.getPath());
        return loader;
    }

    /**
     * @param _loader
     * @param _name
     * @param _path
     * @throws LoaderException
     */
    private void setup(LoaderInterface _loader, String _name, String _path)
            throws LoaderException {
        if (null != _name) {
            _loader.setFileBasename(_name);
        }
        _loader.setRecordPath(_path);
    }

    /**
     * @param _bytes
     * @return
     * @throws LoaderException
     */
    public LoaderInterface newLoader(byte[] _bytes)
            throws LoaderException {
        return newLoader(new ByteArrayInputStream(_bytes));
    }

}
