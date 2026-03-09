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

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcDynamicFilteringSplitManager;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.JdbcMetadataFactory;
import io.trino.plugin.jdbc.JdbcMetadataSessionProperties;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcWriteSessionProperties;
import io.trino.plugin.jdbc.MaxDomainCompactionThreshold;
import io.trino.plugin.jdbc.RemoteQueryCancellationModule;
import io.trino.plugin.jdbc.TypeHandlingJdbcConfig;
import io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.function.table.ConnectorTableFunction;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;

public class TrinoRemoteClientModule
        extends AbstractConfigurationAwareModule
{
    public static final int TRINO_CONNECTOR_DEFAULT_DOMAIN_COMPACTION_THRESHOLD = 1000;
    public static final int TRINO_CONNECTOR_MAX_DOMAIN_COMPACTION_THRESHOLD = 50000;

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(TrinoRemoteConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        install(conditionalModule(
                TrinoRemoteConfig.class,
                TrinoRemoteConfig::isSslEnabled,
                new SslModule(),
                new NoSslModule()));
        configBinder(binder).bindConfig(TypeHandlingJdbcConfig.class);
        bindSessionPropertiesProvider(binder, TypeHandlingJdbcSessionProperties.class);
        bindSessionPropertiesProvider(binder, JdbcMetadataSessionProperties.class);
        bindSessionPropertiesProvider(binder, JdbcWriteSessionProperties.class);
        bindSessionPropertiesProvider(binder, TrinoRemoteSessionProperties.class);
        newOptionalBinder(binder, JdbcMetadataFactory.class).setBinding().to(TrinoRemoteMetadataFactory.class).in(Scopes.SINGLETON);

        newOptionalBinder(binder, ConnectorSplitManager.class).setBinding().to(JdbcDynamicFilteringSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(TrinoRemoteClient.class).in(SINGLETON);

        install(new JdbcJoinPushdownSupportModule());
        install(new RemoteQueryCancellationModule());
        configBinder(binder).bindConfigDefaults(
                JdbcMetadataConfig.class,
                (config) -> config.setDomainCompactionThreshold(TRINO_CONNECTOR_DEFAULT_DOMAIN_COMPACTION_THRESHOLD));
        newOptionalBinder(binder, Key.get(Integer.TYPE, MaxDomainCompactionThreshold.class)).setBinding().toInstance(TRINO_CONNECTOR_MAX_DOMAIN_COMPACTION_THRESHOLD);
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(SINGLETON);
    }

    private class SslModule
            extends AbstractConfigurationAwareModule
    {
        protected void setup(Binder binder)
        {
            configBinder(binder).bindConfig(TrinoRemoteSslConfig.class);
        }
    }

    private class NoSslModule
            extends AbstractConfigurationAwareModule
    {
        protected void setup(Binder binder)
        {
            binder.bind(TrinoRemoteSslConfig.class).toInstance(new TrinoRemoteSslConfig());
        }
    }
}
