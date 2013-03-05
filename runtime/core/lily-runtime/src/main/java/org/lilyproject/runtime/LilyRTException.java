/*
 * Copyright 2013 NGDATA nv
 * Copyright 2007 Outerthought bvba and Schaubroeck nv
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
package org.lilyproject.runtime;

import org.lilyproject.util.location.LocatedRuntimeException;
import org.lilyproject.util.location.Location;

public class LilyRTException extends LocatedRuntimeException {

    public LilyRTException(String message) {
        super(message);
    }

    public LilyRTException(String message, Throwable cause) throws LocatedRuntimeException {
        super(message, cause, null, false);
    }

    public LilyRTException(String message, Location location) {
        super(message, null, location, false);
    }

    public LilyRTException(String message, Throwable cause, Location location) throws LocatedRuntimeException {
        super(message, cause, location, false);
    }

    public LilyRTException(String message, Throwable cause, Location location, boolean rethrowLocated) throws LocatedRuntimeException {
        super(message, cause, location, rethrowLocated);
    }
}
