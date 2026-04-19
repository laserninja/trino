/*
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
package io.trino.plugin.trino;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import jakarta.validation.constraints.NotNull;

public class TrinoConnectorConfig
{
    public enum CatalogMappingMode
    {
        FULL,
        SINGLE,
    }

    private CatalogMappingMode catalogMappingMode = CatalogMappingMode.FULL;
    private String remoteCatalog;

    @NotNull
    public CatalogMappingMode getCatalogMappingMode()
    {
        return catalogMappingMode;
    }

    @Config("trino.catalog-mapping-mode")
    @ConfigDescription("Catalog mapping mode: FULL exposes all remote catalogs as schemas; SINGLE exposes one remote catalog")
    public TrinoConnectorConfig setCatalogMappingMode(CatalogMappingMode catalogMappingMode)
    {
        this.catalogMappingMode = catalogMappingMode;
        return this;
    }

    public String getRemoteCatalog()
    {
        return remoteCatalog;
    }

    @Config("trino.remote-catalog")
    @ConfigDescription("Remote catalog name when using SINGLE catalog mapping mode")
    public TrinoConnectorConfig setRemoteCatalog(String remoteCatalog)
    {
        this.remoteCatalog = remoteCatalog;
        return this;
    }
}
