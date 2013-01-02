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
package org.lilyproject.rowlog.api;

/**
 * A processor notify observers is interested in getting notified when a message is available for processing.
 * <p/>
 * <p>The {@link RowLogConfigurationManager} is used to communicate the notifications from the {@link RowLog}
 * to the {@link RowLogProcessor}. The {@link RowLogProcessor} implements this interface and registers itself
 * on the {@link RowLogConfigurationManager}
 */
public interface ProcessorNotifyObserver {
    /**
     * Method to be called when a message has been posted on the rowlog that needs to be processed.
     */
    void notifyProcessor(String rowLogId, String subscriptionId);
}
