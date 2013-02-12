package org.kauriproject.template.handling;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;

import org.kauriproject.template.source.Source;
import org.kauriproject.util.io.IOUtils;
import org.xml.sax.InputSource;

public class HandlingInput implements Closeable {

    private final Reader reader;

    HandlingInput(final InputStream is, final String encoding) throws UnsupportedEncodingException {
        this(encoding == null ? new InputStreamReader(is) : new InputStreamReader(is, encoding));
    }

    HandlingInput(final Reader reader) {
        this.reader = reader;
    }

    public static HandlingInput newInput(final Source source, final String encoding, final CharSequence valueStr)
            throws UnsupportedEncodingException, IOException {
        if (source == null) {
            // we read from the string, no encoding to be followed
            final Reader input = new StringReader(valueStr.toString());
            return new HandlingInput(input);
        }
        // else
        return newInput(source, encoding);
    }

    public static HandlingInput newInput(final Source source, String encoding) throws IOException,
            UnsupportedEncodingException {
        if (encoding == null) // if nothing specified, let source charset decide
            encoding = source.getEncoding();

        final InputStream input = source.getInputStream();
        return new HandlingInput(input, encoding);
    }

    public static HandlingInput newInput(final Source source) throws IOException,
            UnsupportedEncodingException {

        final InputStream input = source.getInputStream();
        return new HandlingInput(input, source.getEncoding());
    }

    InputSource getInputSource() {
        return new InputSource(reader);
    }

    public Reader getReader() {
        return reader;
    }

    public void close() {
        IOUtils.closeQuietly(reader);
    }
}