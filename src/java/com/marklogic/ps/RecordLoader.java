/*
 * Copyright (c)2005-2006 Mark Logic Corporation
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

package com.marklogic.ps;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

import org.xmlpull.v1.XmlPullParserException;

import com.marklogic.recordloader.Configuration;
import com.marklogic.recordloader.Loader;
import com.marklogic.recordloader.LoaderFactory;
import com.marklogic.recordloader.Monitor;
import com.marklogic.xcc.exceptions.XccException;

/**
 * @author Michael Blakeley, <michael.blakeley@marklogic.com>
 * 
 */

public class RecordLoader {

	private static final String SIMPLE_NAME = RecordLoader.class
			.getSimpleName();

	public static final String VERSION = "2006-09-18.2";

	public static final String NAME = RecordLoader.class.getName();

	private static SimpleLogger logger = SimpleLogger.getSimpleLogger();

	// number of entries overflows at 2^16 = 65536
	// ref: http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4828461
	// (supposed to be fixed, but isn't)
	private static final int MAX_ENTRIES = 65536 - 1;

	public static void main(String[] args) throws FileNotFoundException,
			IOException, XccException, XmlPullParserException,
			URISyntaxException {
		// use system properties as a basis
		// this allows any number of properties at the command-line,
		// using -DPROPNAME=foo
		// as a result, we no longer need any args: default to stdin
		Configuration config = new Configuration();
		List<File> xmlFiles = new ArrayList<File>();
		List<File> zipFiles = new ArrayList<File>();
		Iterator iter = Arrays.asList(args).iterator();
		File file = null;
		String arg = null;
		while (iter.hasNext()) {
			arg = (String) iter.next();
			logger.info("processing argument: " + arg);
			file = new File(arg);
			if (!file.exists()) {
				logger.warning("skipping " + arg + ": file does not exist.");
				continue;
			}
			if (!file.canRead()) {
				logger.warning("skipping " + arg + ": file cannot be read.");
				continue;
			}
			if (arg.endsWith(".properties")) {
				// this will override existing properties
				config.load(new FileInputStream(file));
			} else if (arg.endsWith(".zip")) {
				// add to zip list
				zipFiles.add(file);
			} else {
				// add to xml list
				xmlFiles.add(file);
			}
		}

		// override with any system props
		config.load(System.getProperties());
		config.setLogger(logger);
		config.configure();

		logger.info(SIMPLE_NAME + " starting, version " + VERSION);

		CharsetDecoder inputDecoder = getDecoder(config.getInputEncoding(),
				config.getMalformedInputAction());

		String inputPath = config.getInputPath();
		if (inputPath != null) {
			String inputPattern = config.getInputPattern();
			logger.fine("finding matches for " + inputPattern + " in "
					+ inputPath);
			// find all the files
			FileFinder ff = new FileFinder(inputPath, inputPattern);
			ff.find();
			while (ff.size() > 0) {
				file = ff.remove();
				if (file.getName().endsWith(".zip")) {
					zipFiles.add(file);
				} else {
					xmlFiles.add(file);
				}
			}
		}

		logger.finer("zipFiles.size = " + zipFiles.size());
		logger.finer("xmlFiles.size = " + xmlFiles.size());

		// if START_ID was supplied, run single-threaded until found
		int threadCount = config.getThreadCount();
		String startId = null;
		if (config.hasStartId()) {
			startId = config.getStartId();
			logger.warning("will single-thread until start-id \"" + startId
					+ "\" is reached");
			threadCount = 1;
		}
		logger.info("thread count = " + threadCount);
		ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors
				.newFixedThreadPool(threadCount);
		// this seems to avoid intermittent errors on submit?
		pool.prestartAllCoreThreads();

		Monitor monitor = new Monitor();
		monitor.setLogger(logger);
		monitor.setPool(pool);
		monitor.setConfig(config);
		monitor.start();

		LoaderFactory factory = new LoaderFactory(monitor, inputDecoder, config);

		if (zipFiles.size() > 0 || xmlFiles.size() > 0) {
			handleFileInput(config, xmlFiles, zipFiles, inputDecoder, monitor,
					pool, factory);
		} else {
			handleStandardInput(config, inputDecoder, monitor, pool, factory);
		}

		pool.shutdown();

		while (monitor.isAlive()) {
			try {
				monitor.join();
			} catch (InterruptedException e) {
				logger.logException("interrupted", e);
			}
		}
	}

	private static CharsetDecoder getDecoder(String inputEncoding,
			String malformedInputAction) {
		CharsetDecoder inputDecoder;
		logger.info("using input encoding " + inputEncoding);
		// using an explicit decoder allows us to control the error reporting
		inputDecoder = Charset.forName(inputEncoding).newDecoder();
		if (malformedInputAction
				.equals(Configuration.INPUT_MALFORMED_ACTION_IGNORE)) {
			inputDecoder.onMalformedInput(CodingErrorAction.IGNORE);
		} else if (malformedInputAction
				.equals(Configuration.INPUT_MALFORMED_ACTION_REPLACE)) {
			inputDecoder.onMalformedInput(CodingErrorAction.REPLACE);
		} else {
			inputDecoder.onMalformedInput(CodingErrorAction.REPORT);
		}
		logger.info("using malformed input action "
				+ inputDecoder.unmappableCharacterAction().toString());
		inputDecoder.onUnmappableCharacter(CodingErrorAction.REPORT);
		return inputDecoder;
	}

	private static void handleFileInput(Configuration _config,
			List<File> _xmlFiles, List<File> _zipFiles,
			CharsetDecoder _inputDecoder, Monitor _monitor,
			ThreadPoolExecutor _pool, LoaderFactory _factory)
			throws IOException, ZipException, FileNotFoundException,
			XccException, XmlPullParserException {
		String zipInputPattern = _config.getZipInputPattern();
		Iterator<File> iter;
		File file;
		ZipFile zipFile;
		ZipEntry ze;
		String entryName;

		logger.info("populating queue");

		// queue any zip-entries first
		// NOTE this technique will intentionally leak zipfile objects!
		iter = _zipFiles.iterator();
		int size;
		if (iter.hasNext()) {
			Enumeration<? extends ZipEntry> entries;
			while (iter.hasNext()) {
				file = iter.next();
				zipFile = new ZipFile(file);
				// to avoid closing zipinputstreams randomly,
				// we have to "leak" them temporarily
				// tell the monitor about them, for later cleanup
				_monitor.add(zipFile);
				entries = zipFile.entries();
				size = zipFile.size();
				logger.fine("queuing entries from zip file "
						+ file.getCanonicalPath());
				if (size >= MAX_ENTRIES) {
					logger.warning("too many entries in input-package: " + size
							+ " >= " + MAX_ENTRIES + "("
							+ file.getCanonicalPath() + ")");
				}
				int count = 0;
				while (entries.hasMoreElements()) {
					ze = entries.nextElement();
					logger.fine("found zip entry " + ze);
					if (ze.isDirectory()) {
						// skip it
						continue;
					}
					entryName = ze.getName();
					if (zipInputPattern != null
							&& !entryName.matches(zipInputPattern)) {
						// skip it
						logger.finer("skipping " + entryName);
						continue;
					}
					submitLoader(_monitor, _pool, _factory.newLoader(zipFile
							.getInputStream(ze), file.getName(), entryName));
					count++;
				}
				logger.fine("queued " + count + " entries from zip file "
						+ file.getCanonicalPath());
			}
		}

		// queue any xml files
		iter = _xmlFiles.iterator();
		while (iter.hasNext()) {
			file = iter.next();
			logger.fine("queuing file " + file.getCanonicalPath());
			submitLoader(_monitor, _pool, _factory.newLoader(file));
		}

		// wait for all threads to complete their work
		logger.info("all files queued");
	}

	private static void handleStandardInput(Configuration _config,
			CharsetDecoder _inputDecoder, Monitor _monitor,
			ThreadPoolExecutor _pool, LoaderFactory _factory)
			throws XccException, XmlPullParserException {
		// use stdin by default
		// NOTE: will not use threads
		logger.info("Reading from standard input...");
		if (_config.getThreadCount() > 1) {
			logger.warning("Will not use multiple threads!");
			_pool.setMaximumPoolSize(1);
		}

		submitLoader(_monitor, _pool, _factory.newLoader(System.in));
	}

	@SuppressWarnings("unchecked")
	private static Future submitLoader(Monitor _monitor,
			ThreadPoolExecutor _pool, Loader _loader) {
		// TODO how to fix this line, without suppressing warnings?
		int retries = 0;
		int maxRetries = 3;
		while (true) {
			try {
				return _pool.submit(new FutureTask(_loader));
			} catch (RejectedExecutionException e) {
				logger.info("pool active=" + _pool.getActiveCount()
						+ ", completed=" + _pool.getCompletedTaskCount()
						+ ", total=" + _pool.getTaskCount() + ", pool-threads="
						+ _pool.getPoolSize() + ", core-threads="
						+ _pool.getCorePoolSize());
				retries++;
				if (retries < maxRetries) {
					logger.logException("retrying " + retries, e);
					try {
						Thread.sleep(500);
					} catch (InterruptedException e1) {
						logger.logException("interrupted", e);
					}
				} else {
					throw e;
				}
			}
		}
	}

}
