/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
