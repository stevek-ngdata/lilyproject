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
package org.lilyproject.rest;

import org.lilyproject.repository.api.QName;
import org.lilyproject.tools.import_.json.WriteOptions;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import java.util.ArrayList;
import java.util.List;

import static javax.ws.rs.core.Response.Status.*;

public class ResourceClassUtil {
    public static QName parseQName(String name, MultivaluedMap<String, String> queryParams) {
        int pos = name.indexOf('$');
        if (pos == -1) {
            throw new ResourceException("Invalid qualified name: " + name, BAD_REQUEST.getStatusCode());
        }

        String prefix = name.substring(0, pos);
        String localName = name.substring(pos + 1);

        String uri = queryParams.getFirst("ns." + prefix);
        if (uri == null) {
            throw new ResourceException("Undefined prefix in qualified name: " + name, BAD_REQUEST.getStatusCode());
        }

        return new QName(uri, localName);
    }

    public static List<QName> parseFieldList(UriInfo uriInfo) {
        String fields = uriInfo.getQueryParameters().getFirst("fields");
        List<QName> fieldQNames = null;
        if (fields != null) {
            fieldQNames = new ArrayList<QName>();
            String[] fieldParts = fields.split(",");
            for (String field : fieldParts) {
                field = field.trim();
                if (field.length() == 0)
                    continue;

                fieldQNames.add(ResourceClassUtil.parseQName(field, uriInfo.getQueryParameters()));
            }
        }
        return fieldQNames;
    }

    public static Integer getIntegerParam(UriInfo uriInfo, String name, Integer defaultValue) {
        String value = uriInfo.getQueryParameters().getFirst(name);
        if (value == null) {
            return defaultValue;
        }

        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new ResourceException("Request parameter '" + name + "' does not contain an integer: '" + value +
                    "'.", BAD_REQUEST.getStatusCode());
        }
    }

    public static WriteOptions getWriteOptions(UriInfo uriInfo) {
        String includeSchema = uriInfo.getQueryParameters().getFirst("schema");
        if (includeSchema != null && includeSchema.equalsIgnoreCase("true")) {
            WriteOptions options = new WriteOptions();
            options.setIncludeSchema(true);
            return options;
        } else {
            return WriteOptions.INSTANCE;
        }
    }
}
