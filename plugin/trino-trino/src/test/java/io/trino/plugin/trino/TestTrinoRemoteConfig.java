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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.plugin.trino.authentication.TrinoRemoteAuthenticationType;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestTrinoRemoteConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(TrinoRemoteConfig.class)
                .setAuthenticationType(TrinoRemoteAuthenticationType.PASSWORD)
                .setSslEnabled(true)
                .setClientTags("")
                .setConnectionTimeout(new Duration(10, TimeUnit.SECONDS))
                .setImpersonationEnabled(false)
                .setNetworkTimeout(new Duration(2, TimeUnit.MINUTES))
                .setProjectionPushdownEnabled(true)
                .setUsePreparedStatement(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("trino-remote.ssl.enabled", "false")
                .put("trino-remote.authentication.type", "OAUTH2")
                .put("trino-remote.connection-timeout", "20s")
                .put("trino-remote.network-timeout", "5m")
                .put("trino-remote.projection-pushdown-enabled", "false")
                .put("trino-remote.clientTags", "[source_cluster=<cluster_identifier>]")
                .put("trino-remote.use-preparedstatement", "false")
                .put("trino-remote.impersonation.enabled", "true")
                .buildOrThrow();

        TrinoRemoteConfig expected = new TrinoRemoteConfig()
                .setConnectionTimeout(new Duration(20, TimeUnit.SECONDS))
                .setNetworkTimeout(new Duration(5, TimeUnit.MINUTES))
                .setProjectionPushdownEnabled(false)
                .setUsePreparedStatement(false)
                .setSslEnabled(false)
                .setClientTags("[source_cluster=<cluster_identifier>]")
                .setImpersonationEnabled(true)
                .setAuthenticationType(TrinoRemoteAuthenticationType.OAUTH2);

        assertFullMapping(properties, expected);
    }
}
