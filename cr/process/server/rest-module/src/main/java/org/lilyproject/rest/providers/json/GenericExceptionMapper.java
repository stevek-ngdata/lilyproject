/*
 * Copyright 2010 Outerthought bvba
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
package org.lilyproject.rest.providers.json;

import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.rest.ResourceException;
import org.lilyproject.util.json.JsonFormat;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;

@Provider
public class GenericExceptionMapper implements ExceptionMapper<Throwable> {

    @Override
    public Response toResponse(Throwable throwable) {
        int status = 500;
        Throwable mainThrowable = throwable;

        if (throwable instanceof ResourceException) {
            ResourceException re = (ResourceException)throwable;

            status = re.getStatus();

            // If the exception only serves to annotate another exception with a status code...
            if (re.getSpecificMessage() == null && re.getCause() != null) {
                if (re.getCause() != null) {
                    mainThrowable = re.getCause();
                }
            } else {
                mainThrowable = throwable;
            }

        }

        final ObjectNode msgNode = JsonNodeFactory.instance.objectNode();
        msgNode.put("status", status);

        Response.Status statusObject = Response.Status.fromStatusCode(status);
        String description =  statusObject != null ? statusObject.toString() : null;

        if (description != null) {
            msgNode.put("description", description);
        }

        msgNode.put("causes", causesToJson(mainThrowable));

        msgNode.put("stackTrace", formatStackTrace(mainThrowable));

        StreamingOutput entity = new StreamingOutput() {
            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                JsonFormat.serialize(msgNode, output);
            }
        };

        return Response.status(status).type(MediaType.APPLICATION_JSON_TYPE).entity(entity).build();
    }

    private ArrayNode causesToJson(Throwable throwable) {
        ArrayNode causesNode = JsonNodeFactory.instance.arrayNode();

        while (throwable != null) {
            ObjectNode causeNode = causesNode.addObject();
            causeNode.put("message", throwable.getMessage());
            causeNode.put("type", throwable.getClass().getName());

            throwable = throwable.getCause();
        }

        return causesNode;
    }

    private String formatStackTrace(Throwable throwable) {
        StringWriter writer = new StringWriter();
        PrintWriter printWriter = new PrintWriter(writer);
        throwable.printStackTrace(printWriter);
        return writer.toString();
    }
}
