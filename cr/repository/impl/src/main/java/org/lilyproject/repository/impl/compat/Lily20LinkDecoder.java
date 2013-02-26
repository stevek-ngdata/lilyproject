/*
 * Copyright 2013 NGDATA nv
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
package org.lilyproject.repository.impl.compat;

import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.Link.LinkBuilder;
import org.lilyproject.repository.api.RecordId;

public class Lily20LinkDecoder {
    
    /**
     * Decodes links from Lily 2.0 and earlier (before table name was added to Link serialization).
     */
    public static Link decode(DataInput dataInput, IdGenerator idGenerator) {
        // The bytes format is as follows:
        // [byte representation of master record id, if not null][args: bytes of the string representation]

        int recordIdLength = dataInput.readInt();
        byte[] recordIdBytes = null;
        if (recordIdLength > 0) {
            recordIdBytes = dataInput.readBytes(recordIdLength);
        }
        String args = dataInput.readUTF();
        
        if (recordIdLength == 0 && args == null) {
            return new Link();
        }

        LinkBuilder builder = Link.newBuilder();
        if (recordIdLength > 0) {
            RecordId id = idGenerator.fromBytes(recordIdBytes);
            builder.recordId(id);
        }

        if (args != null && args.length() > 0) {
            argsFromString(args, builder, args /* does not matter, should never be invalid */);
        }

        return builder.create();
    }
    
    private static void argsFromString(String args, LinkBuilder builder, String link) {
        String[] variantStringParts = args.split(",");
        for (String part : variantStringParts) {
            int eqPos = part.indexOf('=');
            if (eqPos == -1) {
                String thing = part.trim();
                if (thing.equals("*")) {
                    // this is the default, but if users want to make explicit, allow them
                    builder.copyAll(true);
                } else if (thing.equals("!*")) {
                    builder.copyAll(false);
                } else if (thing.startsWith("+") && thing.length() > 1) {
                    builder.copy(thing.substring(1));
                } else if  (thing.startsWith("-") && thing.length() > 1) {
                    builder.remove(thing.substring(1));
                } else {
                    throw new IllegalArgumentException("Invalid link: " + link);
                }
            } else {
                String name = part.substring(0, eqPos).trim();
                String value = part.substring(eqPos + 1).trim();
                if (name.length() == 0 || value.length() == 0) {
                    throw new IllegalArgumentException("Invalid link: " + link);
                }
                builder.set(name, value);
            }
        }
    }

}
