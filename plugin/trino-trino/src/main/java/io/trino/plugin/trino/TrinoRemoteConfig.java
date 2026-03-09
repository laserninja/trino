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
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.plugin.trino.authentication.TrinoRemoteAuthenticationType;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * To get custom properties in order to connect to the database.
 * User, password and URL parameters are provided by BaseJdbcClient; and are not required.
 * If there is another custom configuration it should be put here.
 */
public class TrinoRemoteConfig
{
    private TrinoRemoteAuthenticationType authenticationType;
    private boolean usePreparedStatement = true;
    private Duration connectionTimeout = new Duration(10, TimeUnit.SECONDS);
    private boolean sslEnabled = true;
    private String clientTags = "";
    private Duration networkTimeout = new Duration(2, MINUTES);
    private int maxSplitsPerScan = 1;
    private boolean impersonationEnabled;
    private DataSize targetResultSize = DataSize.of(1, MEGABYTE);
    private int metadataQueryTimeout = 30; // 30 seconds is the default metadata query timeout
    private boolean projectionPushDownEnabled = true;

    public TrinoRemoteConfig()
    {
        this.authenticationType = TrinoRemoteAuthenticationType.PASSWORD;
    }

    public TrinoRemoteAuthenticationType getAuthenticationType()
    {
        return authenticationType;
    }

    @Config("trino-remote.authentication.type")
    public TrinoRemoteConfig setAuthenticationType(TrinoRemoteAuthenticationType authenticationType)
    {
        this.authenticationType = authenticationType;
        return this;
    }

    public boolean isSslEnabled()
    {
        return sslEnabled;
    }

    @Config("trino-remote.ssl.enabled")
    public TrinoRemoteConfig setSslEnabled(boolean sslEnabled)
    {
        this.sslEnabled = sslEnabled;
        return this;
    }

    public Optional<String> getClientTags()
    {
        return Optional.ofNullable(clientTags);
    }

    @Config("trino-remote.clientTags")
    public TrinoRemoteConfig setClientTags(String clientTags)
    {
        this.clientTags = clientTags;
        return this;
    }

    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("trino-remote.connection-timeout")
    public TrinoRemoteConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public boolean isUsePreparedStatement()
    {
        return usePreparedStatement;
    }

    @Config("trino-remote.use-preparedstatement")
    public TrinoRemoteConfig setUsePreparedStatement(boolean usePreparedStatement)
    {
        this.usePreparedStatement = usePreparedStatement;
        return this;
    }

    public Duration getNetworkTimeout()
    {
        return networkTimeout;
    }

    @Config("trino-remote.network-timeout")
    public TrinoRemoteConfig setNetworkTimeout(Duration networkTimeout)
    {
        this.networkTimeout = networkTimeout;
        return this;
    }

    public boolean isImpersonationEnabled()
    {
        return this.impersonationEnabled;
    }

    @Config("trino-remote.impersonation.enabled")
    @ConfigDescription("Impersonate session user in remote trino cluster")
    public TrinoRemoteConfig setImpersonationEnabled(boolean impersonationEnabled)
    {
        this.impersonationEnabled = impersonationEnabled;
        return this;
    }

    public boolean isProjectionPushdownEnabled()
    {
        return projectionPushDownEnabled;
    }

    @Config("trino-remote.projection-pushdown-enabled")
    @ConfigDescription("Read only required fields from a row type")
    public TrinoRemoteConfig setProjectionPushdownEnabled(boolean projectionPushDownEnabled)
    {
        this.projectionPushDownEnabled = projectionPushDownEnabled;
        return this;
    }
}
