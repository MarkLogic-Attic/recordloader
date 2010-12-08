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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.xmlpull.v1.XmlPullParser;

import com.marklogic.ps.RecordLoader;
import com.marklogic.ps.SimpleLogger;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class ProducerFactory {

    private Configuration config;

    private XmlPullParser xpp;

    private SimpleLogger logger;

    private Constructor<? extends Producer> producerConstructor;

    protected static boolean isFirstInit = true;

    protected static Object staticMutex = new Object();

    /**
     * @param _config
     * @param _xpp
     * @throws LoaderException
     */
    public ProducerFactory(Configuration _config, XmlPullParser _xpp)
            throws LoaderException {
        config = _config;
        xpp = _xpp;

        logger = config.getLogger();

        // this should only be called once, in a single-threaded context
        String producerClassName = config.getProducerClassName();
        if (isFirstInit) {
            synchronized (staticMutex) {
                if (isFirstInit) {
                    logger.info("Producer is " + producerClassName);
                    isFirstInit = false;
                }
            }
        }
        Class<? extends Producer> producerClass;
        try {
            producerClass = Class.forName(producerClassName, true,
                    RecordLoader.getClassLoader()).asSubclass(
                    Producer.class);
            producerConstructor = producerClass
                    .getConstructor(new Class[] { Configuration.class,
                            XmlPullParser.class });
        } catch (ClassNotFoundException e) {
            throw new LoaderException(e);
        } catch (SecurityException e) {
            throw new LoaderException(e);
        } catch (NoSuchMethodException e) {
            throw new LoaderException(e);
        }
    }

    /**
     * @return
     * @throws LoaderException
     */
    public Producer newProducer() throws LoaderException {
        try {
            return producerConstructor.newInstance(config, xpp);
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

}
