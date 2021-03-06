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

import org.nuxeo.ecm.core.storage.dbs.DBSRepositoryService;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.model.ComponentInstance;
import org.nuxeo.runtime.model.DefaultComponent;

/**
 * Service holding the configuration for Couchbase repositories.
 *
 * @since 9.1
 */
public class CouchbaseRepositoryService extends DefaultComponent {

    private static final String XP_REPOSITORY = "repository";

    @Override
    public void registerContribution(Object contrib, String xpoint, ComponentInstance contributor) {
        if (XP_REPOSITORY.equals(xpoint)) {
            addContribution((CouchbaseRepositoryDescriptor) contrib);
        } else {
            throw new RuntimeException("Unknown extension point: " + xpoint);
        }
    }

    @Override
    public void unregisterContribution(Object contrib, String xpoint, ComponentInstance contributor) {
        if (XP_REPOSITORY.equals(xpoint)) {
            removeContribution((CouchbaseRepositoryDescriptor) contrib);
        } else {
            throw new RuntimeException("Unknown extension point: " + xpoint);
        }
    }

    protected void addContribution(CouchbaseRepositoryDescriptor descriptor) {
        Framework.getService(DBSRepositoryService.class).addContribution(descriptor, CouchbaseRepositoryFactory.class);
    }

    protected void removeContribution(CouchbaseRepositoryDescriptor descriptor) {
        Framework.getService(DBSRepositoryService.class).removeContribution(descriptor,
                CouchbaseRepositoryFactory.class);
    }

}
