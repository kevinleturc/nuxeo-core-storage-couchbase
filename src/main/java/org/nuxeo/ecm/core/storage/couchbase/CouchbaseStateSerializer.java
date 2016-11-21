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
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map.Entry;

import org.nuxeo.ecm.core.storage.State;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

/**
 * Couchbase Serializer to convert {@link String} into {@link State}.
 *
 * @since 9.1
 */
class CouchbaseStateSerializer {

    private CouchbaseStateSerializer() {
        // nothing
    }

    public static JsonObject serialize(State state) {
        JsonObject jsonObject = JsonObject.create();
        for (Entry<String, Serializable> entry : state.entrySet()) {
            jsonObject.put(entry.getKey(), serialize(entry.getValue()));
        }
        return jsonObject;
    }

    private static Serializable serialize(Serializable value) {
        if (value instanceof List) {
            return serializeList((List<?>) value);
        } else if (value instanceof Object[]) {
            return serializeList(Arrays.asList((Object[]) value));
        } else if (value instanceof State) {
            return serialize((State) value);
        }
        return serializeScalar(value);
    }

    public static JsonArray serializeList(List<?> list) {
        JsonArray jsonArray = JsonArray.create();
        for (Object value : list) {
            jsonArray.add(serialize((Serializable) value));
        }
        return jsonArray;
    }

    public static Serializable serializeScalar(Object val) {
        if (val instanceof Calendar) {
            return CouchbaseHelper.serializeCalendar((Calendar) val);
        }
        return (Serializable) val;
    }

}
