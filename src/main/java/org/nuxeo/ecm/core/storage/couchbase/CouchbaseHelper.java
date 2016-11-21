/*
 * (C) Copyright 2016 Nuxeo SA (http://nuxeo.com/) and others.
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
 *  
 * Contributors:
 *     Kevin Leturc
 */
package org.nuxeo.ecm.core.storage.couchbase;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Optional;

import com.couchbase.client.java.document.json.JsonObject;

/**
 * Couchbase helper class.
 *
 * @since 9.1
 */
class CouchbaseHelper {

    public static JsonObject serializeCalendar(Calendar cal) {
        return JsonObject.create().put("nx:type", "dateTime").put("timestamp", cal.getTimeInMillis());
    }

    public static Optional<Serializable> deserializeCalendar(JsonObject object) {
        if ("dateTime".equals(object.getString("nx:type"))) {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(object.getLong("timestamp").longValue());
            return Optional.of(cal);
        }
        return Optional.empty();
    }
}
