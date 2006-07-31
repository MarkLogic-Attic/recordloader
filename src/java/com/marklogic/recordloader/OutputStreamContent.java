/*
 * Copyright 2003-2006 Mark Logic Corporation. All Rights Reserved.
 */

package com.marklogic.recordloader;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;

/**
 * <p>
 * This is a specialized implementation of the {@link Content} interface which
 * allows you to write your content to an {@link OutputStream}.
 * </p>
 * <p>
 * Click here for the <a href="doc-files/OutputStreamContent.java.txt"> source
 * code for this class</a>
 * </p>
 * <p>
 * This class creates a piped pair of streams. The source ({@link InputStream})
 * is passed to the standard factory method
 * {@link ContentFactory#newUnBufferedContent(String, java.io.InputStream, com.marklogic.xcc.ContentCreateOptions)}.
 * The sink ({@link OutputStream}) can be retrieved with the
 * {@link #getOutputStream()} method.
 * </p>
 * <p>
 * <strong>NOTE:</strong> You must write to the {@link OutputStream} in a
 * different thread than the one in which you invoke
 * {@link com.marklogic.xcc.Session#insertContent} or your program may deadlock.
 * The {@link OutputStreamInserter} class shows an example of how to use this
 * class.
 * </p>
 * 
 * @see OutputStreamInserter
 */
public class OutputStreamContent implements Content {
    private final OutputStream sink;

    private final InputStream source;

    private final Content content;

    /**
     * Construct an instance with the usual URI and options parameters, the
     * actual content will be read from the other end of the pipe provided by
     * {@link #getOutputStream()}.
     * 
     * @param uri
     *            The URI by which the content (document) will be known in the
     *            contentbase.
     * @param options
     *            An instance of {@link ContentCreateOptions}.
     * @throws IOException
     */
    public OutputStreamContent(String uri, ContentCreateOptions options)
            throws IOException {
        PipedOutputStream sink = new PipedOutputStream();
        this.sink = sink;
        this.source = new PipedInputStream(sink);
        this.content = ContentFactory.newUnBufferedContent(uri, source,
                options);
    }

    // -----------------------------------------------------------
    // Implementation class-specific methods

    /**
     * Return the end of the pipe to which you will write your content (the
     * sink).
     * 
     * @return An instance of {@link OutputStream}. Be sure to close this
     *         object when you've written all the data.
     */
    public OutputStream getOutputStream() {
        return sink;
    }

    // -----------------------------------------------------------
    // Content interface delegation

    public String getUri() {
        return content.getUri();
    }

    /**
     * Passes the read end (source) of the pipe to the content insertion
     * framework.
     * 
     * @return An instance of {@link InputStream}.
     * @throws IOException
     *             Will never happen in this implementation.
     */
    public InputStream openDataStream() throws IOException {
        if (false)
            throw new IOException("dummy");

        return source;
    }

    public ContentCreateOptions getCreateOptions() {
        return content.getCreateOptions();
    }

    /**
     * The answer is "no".
     * 
     * @return Always returns false, this implementation is non-rewindable.
     */
    public boolean isRewindable() {
        return false;
    }

    /**
     * This streaming-only implementation is not rewindable.
     * 
     * @throws IOException
     *             Will always be thrown if called.
     */
    public void rewind() throws IOException {
        throw new IOException("Rewind is not supported");
    }

    /**
     * Unknown size.
     * 
     * @return Always returns -1 to indicate that the size is unknown.
     */
    public long size() {
        return -1;
    }

    public void close() {
        content.close();
    }
}
