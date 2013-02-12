package org.lilyproject.process.test;

import org.apache.http.HttpResponse;

public class ResponseAndContent {

    private HttpResponse response;
    private byte[] data;

    public ResponseAndContent(HttpResponse response, byte[] data) {
        this.response = response;
        this.data = data;
    }

    public HttpResponse getResponse() {
        return response;
    }

    public byte[] getContent() {
        return data;
    }

    /** added during migration from restlet based test to httpclient based test */
    public String getLocationRef() {
        return response.getFirstHeader("Location").getValue();
    }
}
