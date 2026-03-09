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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;

public final class TrinoRemoteSessionProperties
        implements SessionPropertiesProvider
{
    public static final String NETWORK_TIMEOUT = "network_timeout";
    public static final String MAX_SPLITS_PER_SCAN = "max_splits_per_scan";
    private static final String PROJECTION_PUSHDOWN_ENABLED = "projection_pushdown_enabled";
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public TrinoRemoteSessionProperties(TrinoRemoteConfig config)
    {
        sessionProperties = ImmutableList.of(
                durationProperty(
                        NETWORK_TIMEOUT,
                        "Duration to wait for a response from the remote Presto cluster",
                        config.getNetworkTimeout(),
                        false),
                booleanProperty(
                        PROJECTION_PUSHDOWN_ENABLED,
                        "Read only required fields from a row type",
                        config.isProjectionPushdownEnabled(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static Duration getNetworkTimeout(ConnectorSession session)
    {
        return session.getProperty(NETWORK_TIMEOUT, Duration.class);
    }

    public static boolean isProjectionPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(PROJECTION_PUSHDOWN_ENABLED, Boolean.class);
    }
}
