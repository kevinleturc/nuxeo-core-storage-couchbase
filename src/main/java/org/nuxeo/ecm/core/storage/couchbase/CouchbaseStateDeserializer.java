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
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.nuxeo.ecm.core.storage.State;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

/**
 * Couchbase Deserializer to convert {@link String} into {@link State}.
 *
 * @since 9.1
 */
class CouchbaseStateDeserializer {

    private CouchbaseStateDeserializer() {
        // nothing
    }

    public static State deserialize(JsonDocument jsonDocument) {
        return deserialize(jsonDocument.content());
    }

    public static State deserialize(JsonObject jsonObject) {
        State state = new State();
        for (String key : jsonObject.getNames()) {
            state.put(key, deserialize(jsonObject.get(key)));
        }
        return state;
    }

    private static Serializable deserialize(Object value) {
        if (value instanceof JsonArray) {
            return deserializeList((JsonArray) value);
        } else if (value instanceof JsonObject) {
            JsonObject object = (JsonObject) value;
            return CouchbaseHelper.deserializeCalendar(object).orElseGet(() -> deserialize(object));
        }
        return deserializeScalar(value);
    }

    private static Serializable deserializeList(JsonArray value) {
        ArrayList<Serializable> list = StreamSupport.stream(value.spliterator(), false)
                                               .map(CouchbaseStateDeserializer::deserialize)
                                               .collect(Collectors.toCollection(ArrayList::new));
        if (!list.isEmpty()) {
            Serializable el = list.get(0);
            if (el instanceof State) {
                return list;
            }
            return list.toArray((Object[]) Array.newInstance(el.getClass(), list.size()));
        }
        return null;
    }

    private static Serializable deserializeScalar(Object val) {
        if (val instanceof Integer) {
            return Long.valueOf(((Integer) val).longValue());
        }
        return (Serializable) val;
    }
}
