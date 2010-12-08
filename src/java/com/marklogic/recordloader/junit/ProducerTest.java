/**
 * Copyright (c) 2006-2009 Mark Logic Corporation. All rights reserved.
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
package com.marklogic.recordloader.junit;

import java.io.StringReader;
import java.util.Properties;

import junit.framework.TestCase;

import org.xmlpull.v1.XmlPullParser;

import com.marklogic.ps.SimpleLogger;
import com.marklogic.recordloader.Configuration;
import com.marklogic.recordloader.Producer;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class ProducerTest extends TestCase {

    // just large enough to expose bugs
    private static final int READ_SIZE = 8;

    SimpleLogger logger = SimpleLogger.getSimpleLogger();

    public void testExternalId() throws Exception {
        Configuration config = new Configuration();
        config.setLogger(logger);

        config.setIdNodeName("#FILENAME");
        config.setRecordNamespace("");
        String recordName = "record";
        config.setRecordName(recordName);

        XmlPullParser xpp = config.getXppFactory().newPullParser();
        xpp.setFeature(XmlPullParser.FEATURE_PROCESS_NAMESPACES, true);

        String testXml = "<root><record>hello world</record></root>";
        xpp.setInput(new StringReader(testXml));

        int eventType;
        while (true) {
            eventType = xpp.nextToken();
            if (eventType == XmlPullParser.START_TAG) {
                if (xpp.getName().equals(recordName)) {
                    break;
                }
            }
        }

        Producer producer = new Producer(config, xpp);
        String id = "test1";
        producer.setCurrentId(id);

        StringBuilder outputXml = new StringBuilder();
        byte[] buf = new byte[READ_SIZE];
        int len;
        while ((len = producer.read(buf)) > -1) {
            outputXml.append(new String(buf, 0, len));
        }
        String expectedXml = "<record>hello world</record>".trim();
        // logger.info("expected = " + expectedXml);
        String actual = outputXml.toString().trim();
        // logger.info("actual = " + actual);
        assertEquals(expectedXml, actual);
        assertEquals(id, producer.getCurrentId());
    }

    public void testPrefixes() throws Exception {
        Configuration config = new Configuration();
        config.setLogger(logger);

        config.setIdNodeName("@id");
        config.setRecordNamespace("test");
        String recordName = "record";
        config.setRecordName(recordName);

        XmlPullParser xpp = config.getXppFactory().newPullParser();
        xpp.setFeature(XmlPullParser.FEATURE_PROCESS_NAMESPACES, true);

        String testXml = "<root xmlns:t=\"test\">\n"
                + "<t:record id=\"1\"><description xmlns:t=\"t2\">record 1</description></t:record>\n"
                + "</root>";
        xpp.setInput(new StringReader(testXml));

        int eventType;
        while (true) {
            eventType = xpp.nextToken();
            if (eventType == XmlPullParser.START_TAG) {
                if (xpp.getName().equals(recordName)) {
                    break;
                }
            }
        }

        Producer producer = new Producer(config, xpp);

        StringBuilder outputXml = new StringBuilder();
        byte[] buf = new byte[READ_SIZE];
        int len;
        while ((len = producer.read(buf)) > -1) {
            outputXml.append(new String(buf, 0, len));
        }
        String expectedXml = "<t:record id=\"1\" xmlns:t=\"test\"><description xmlns:t=\"t2\">record 1</description></t:record>\n"
                .trim();
        // logger.info("expected = " + expectedXml);
        String actual = outputXml.toString().trim();
        // logger.info("actual = " + actual);
        assertEquals(expectedXml, actual);
    }

    public void testPrefixes2() throws Exception {
        Configuration config = new Configuration();
        config.setLogger(logger);

        config.setIdNodeName("#FILE");
        config.setRecordNamespace("http://www.test.com/glp/comm");
        String recordName = "body";
        config.setRecordName(recordName);

        XmlPullParser xpp = config.getXppFactory().newPullParser();
        xpp.setFeature(XmlPullParser.FEATURE_PROCESS_NAMESPACES, true);

        String testXml = "<COMMENTARYDOC "
                + " xmlns:comm=  \"http://www.test.com/glp/comm\"  >"
                + "<comm:body >" + "<level leveltype= \"comm12\" >"
                + "<heading align= \"left\" searchtype= \"COMMENTARY\" >"
                + "<title >" + "<emph typestyle= \"bf\" > FORMS </emph >"
                + "</title >" + "</heading >" + "</level >"
                + "</comm:body >" + "</COMMENTARYDOC >\n";
        xpp.setInput(new StringReader(testXml));

        int eventType;
        while (true) {
            eventType = xpp.nextToken();
            if (eventType == XmlPullParser.START_TAG) {
                if (xpp.getName().equals(recordName)) {
                    break;
                }
            }
        }

        Producer producer = new Producer(config, xpp);
        producer.setCurrentId("test");

        StringBuilder outputXml = new StringBuilder();
        byte[] buf = new byte[READ_SIZE];
        int len;
        while ((len = producer.read(buf)) > -1) {
            outputXml.append(new String(buf, 0, len));
        }
        String expectedXml = "<comm:body"
                + "  xmlns:comm=\"http://www.test.com/glp/comm\">"
                + "<level leveltype= \"comm12\" >"
                + "<heading align= \"left\" searchtype= \"COMMENTARY\" >"
                + "<title ><emph typestyle= \"bf\" > FORMS </emph ></title >"
                + "</heading ></level >" + "</comm:body >".trim();
        // logger.info("expected = " + expectedXml);
        String actual = outputXml.toString().trim();
        // logger.info("actual = " + actual);
        assertEquals(expectedXml, actual);
    }

    public void testAttributesWithEntities() throws Exception {
        Configuration config = new Configuration();
        Properties props = new Properties();
        props.setProperty("LOG_LEVEL", "FINEST");
        logger.configureLogger(props);
        config.setLogger(logger);

        config.setIdNodeName("#FILE");
        String recordName = "record";
        config.setRecordNamespace("");
        config.setRecordName(recordName);

        XmlPullParser xpp = config.getXppFactory().newPullParser();
        xpp.setFeature(XmlPullParser.FEATURE_PROCESS_NAMESPACES, true);
        String beginXml = "<root>\n";
        String endXml = "</root>\n";
        String testXml = beginXml + "<record><ISBN>\n"
                + "<Number>0-521-79351-3</Number>\n"
                + "<BindingType>hardcover</BindingType>\n" + "</ISBN>\n"
                + "<ISBN Qualifier=\"Volume 1&amp;2 hardcover, set\">\n"
                + "<Number>0-521-79351-3</Number>\n"
                + "<BindingType>hardcover</BindingType>\n"
                + "</ISBN></record>\n" + endXml;

        xpp.setInput(new StringReader(testXml));
        int eventType;
        while (true) {
            eventType = xpp.nextToken();
            if (eventType == XmlPullParser.START_TAG) {
                if (xpp.getName().equals(recordName)) {
                    break;
                }
            }
        }

        Producer producer = new Producer(config, xpp);
        producer.setCurrentId("test");

        StringBuilder outputXml = new StringBuilder();
        byte[] buf = new byte[READ_SIZE];
        int len;
        while ((len = producer.read(buf)) > -1) {
            outputXml.append(new String(buf, 0, len));
        }
        String actual = outputXml.toString().trim();
        logger.info("actual = " + actual);
        String expectedXml = testXml.trim().substring(beginXml.length());
        expectedXml = expectedXml.substring(0, expectedXml.length()
                - endXml.length());
        logger.info("expected = " + expectedXml);
        assertEquals(expectedXml, actual);
    }
}