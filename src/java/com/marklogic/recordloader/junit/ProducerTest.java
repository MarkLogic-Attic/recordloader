/**
 * Copyright (c) 2006 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.recordloader.junit;

import java.io.StringReader;

import org.xmlpull.v1.XmlPullParser;

import com.marklogic.ps.SimpleLogger;
import com.marklogic.recordloader.Configuration;
import com.marklogic.recordloader.Producer;

import junit.framework.TestCase;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class ProducerTest extends TestCase {

    SimpleLogger logger = SimpleLogger.getSimpleLogger();

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
            eventType = xpp.next();
            if (eventType == XmlPullParser.START_TAG) {
                if (xpp.getName().equals(recordName)) {
                    break;
                }
            }
        }

        Producer producer = new Producer(config, xpp);

        StringBuffer outputXml = new StringBuffer();
        byte[] buf = new byte[1024];
        int len;
        while ((len = producer.read(buf)) > -1) {
            outputXml.append(new String(buf, 0, len));
        }
        String expectedXml = "<t:record id=\"1\" xmlns:t=\"test\"><description xmlns:t=\"t2\">record 1</description></t:record>\n"
                .trim();
        // logger.info("expected = " + expectedXml);
        String actual = outputXml.toString().trim();
        // logger.info("actual   = " + actual);
        assertEquals(expectedXml, actual);
    }

    public void testPrefixes2() throws Exception {
        Configuration config = new Configuration();
        config.setLogger(logger);

        config.setIdNodeName("#FILE");
        //config.setRecordNamespace("");
        //String recordName = "COMMENTARYDOC";
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
            eventType = xpp.next();
            if (eventType == XmlPullParser.START_TAG) {
                if (xpp.getName().equals(recordName)) {
                    break;
                }
            }
        }

        Producer producer = new Producer(config, xpp);
        producer.setCurrentId("test");

        StringBuffer outputXml = new StringBuffer();
        byte[] buf = new byte[1024];
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
//        logger.info("expected = " + expectedXml);
        String actual = outputXml.toString().trim();
//        logger.info("actual   = " + actual);
        assertEquals(expectedXml, actual);
    }

}
