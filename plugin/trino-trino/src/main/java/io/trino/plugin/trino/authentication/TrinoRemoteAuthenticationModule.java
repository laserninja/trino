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
package io.trino.plugin.trino.authentication;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.jdbc.TrinoDriver;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ExtraCredentialsBasedIdentityCacheMappingModule;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;
import io.trino.plugin.jdbc.credential.DefaultCredentialPropertiesProvider;
import io.trino.plugin.trino.TrinoDriverConnectionFactory;
import io.trino.plugin.trino.TrinoRemoteConfig;
import io.trino.plugin.trino.TrinoRemoteSslConfig;

import java.io.File;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.trino.authentication.TrinoRemoteAuthenticationType.CERTIFICATE;
import static io.trino.plugin.trino.authentication.TrinoRemoteAuthenticationType.OAUTH2;
import static io.trino.plugin.trino.authentication.TrinoRemoteAuthenticationType.PASSWORD;
import static java.util.Objects.requireNonNull;

public class TrinoRemoteAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(conditionalModule(
                TrinoRemoteConfig.class,
                (config) -> { return config.getAuthenticationType() == PASSWORD && !config.isImpersonationEnabled(); },
                new PasswordModule()));
        install(conditionalModule(
                TrinoRemoteConfig.class,
                (config) -> { return config.getAuthenticationType() == PASSWORD && config.isImpersonationEnabled(); },
                new PasswordWithImpersonationModule()));
        install(conditionalModule(
                TrinoRemoteConfig.class,
                (config) -> {
                    return config.getAuthenticationType() == CERTIFICATE && config.isImpersonationEnabled();
                },
                new CertificateWithImpersonationModule()));
        install(conditionalModule(
                TrinoRemoteConfig.class,
                (config) -> {
                    return config.getAuthenticationType() == OAUTH2 && config.isImpersonationEnabled();
                },
                new AuthTokenWithImpersonationModule()));
    }

    private static class PasswordWithImpersonationModule
            extends AbstractConfigurationAwareModule
    {
        protected void setup(Binder binder)
        {
            install(new CredentialProviderModule());
            install(new AuthenticationBasedIdentityCacheMappingModule());
            configBinder(binder).bindConfig(TrinoRemoteCredentialConfig.class);
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(
                BaseJdbcConfig config,
                TrinoRemoteConfig trinoRemoteConfig,
                TrinoRemoteSslConfig sslConfig,
                CredentialProvider credentialProvider,
                OpenTelemetry openTelemetry)
        {
            Properties properties = new Properties();
            if (trinoRemoteConfig.isSslEnabled()) {
                setSslProperties(properties, sslConfig);
            }
            setOptionalProperty(properties, "clientTags", trinoRemoteConfig.getClientTags());

            return new TrinoDriverConnectionFactory(
                    new TrinoDriver(),
                    config.getConnectionUrl(),
                    properties,
                    new TrinoRemoteImpersonateCredentialPropertiesProvider(credentialProvider),
                    openTelemetry);
        }
    }

    private static class PasswordModule
            extends AbstractConfigurationAwareModule
    {
        protected void setup(Binder binder)
        {
            install(new CredentialProviderModule());
            install(new ExtraCredentialsBasedIdentityCacheMappingModule());
            configBinder(binder).bindConfig(TrinoRemoteCredentialConfig.class);
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(
                BaseJdbcConfig config,
                TrinoRemoteConfig trinoRemoteConfig,
                TrinoRemoteSslConfig sslConfig,
                CredentialProvider credentialProvider,
                OpenTelemetry openTelemetry)
        {
            CredentialPropertiesProvider<String, String> credentialPropertiesProvider = new DefaultCredentialPropertiesProvider(requireNonNull(credentialProvider, "credentialProvider is null"));
            Properties properties = new Properties();
            if (trinoRemoteConfig.isSslEnabled()) {
                setSslProperties(properties, sslConfig);
            }
            setOptionalProperty(properties, "clientTags", trinoRemoteConfig.getClientTags());

            return new TrinoDriverConnectionFactory(
                    new TrinoDriver(),
                    config.getConnectionUrl(),
                    properties,
                    credentialPropertiesProvider,
                    openTelemetry);
        }
    }

    private class CertificateWithImpersonationModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            binder.bind(CredentialProvider.class).to(IdentityPassThroughCredentialProvider.class).in(SINGLETON);
            install(new AuthenticationBasedIdentityCacheMappingModule());
            install(conditionalModule(
                    TrinoRemoteConfig.class,
                    TrinoRemoteConfig::isImpersonationEnabled,
                    (conditionalBinder) -> { configBinder(conditionalBinder).bindConfig(TrinoRemoteSslConfig.class);
                    }));
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(
                BaseJdbcConfig config,
                TrinoRemoteConfig trinoRemoteConfig,
                TrinoRemoteSslConfig sslConfig,
                CredentialProvider credentialProvider,
                OpenTelemetry openTelemetry)
        {
            requireNonNull(trinoRemoteConfig, "trinoRemoteConfig is null");
            requireNonNull(sslConfig, "sslConfig is null");
            checkState(trinoRemoteConfig.isImpersonationEnabled(), "User impersonation is required for CERTIFICATE based remote authentication");
            checkState(trinoRemoteConfig.isSslEnabled(), "SSL should be enabled for CERTIFICATE based authentication");
            checkState(sslConfig.getKeystoreFile().exists(), "Keystore file for CERTIFICATE authentication missing");
            checkState(!sslConfig.getKeyStorePassword().isEmpty(), "Keystore password for CERTIFICATE authentication missing");
            Properties properties = new Properties();
            setSslProperties(properties, sslConfig);
            setSslKeystoreProperties(properties, sslConfig);
            setOptionalProperty(properties, "clientTags", trinoRemoteConfig.getClientTags());

            return new TrinoDriverConnectionFactory(
                    new TrinoDriver(),
                    config.getConnectionUrl(),
                    properties,
                    new TrinoRemoteImpersonateCredentialPropertiesProvider(credentialProvider),
                    openTelemetry);
        }
    }

    private class AuthTokenWithImpersonationModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            binder.bind(CredentialProvider.class).to(IdentityPassThroughCredentialProvider.class).in(SINGLETON);
            install(new AuthenticationBasedIdentityCacheMappingModule());
            install(conditionalModule(
                    TrinoRemoteConfig.class,
                    TrinoRemoteConfig::isImpersonationEnabled,
                    (conditionalBinder) -> { configBinder(conditionalBinder).bindConfig(TrinoRemoteSslConfig.class);
                    }));
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(
                BaseJdbcConfig config,
                TrinoRemoteConfig trinoRemoteConfig,
                TrinoRemoteSslConfig sslConfig,
                CredentialProvider credentialProvider,
                OpenTelemetry openTelemetry)
        {
            requireNonNull(trinoRemoteConfig, "trinoRemoteConfig is null");
            requireNonNull(sslConfig, "sslConfig is null");
            checkState(trinoRemoteConfig.isImpersonationEnabled(), "Enable impersonation for CERTIFICATE based remote authentication");
            checkState(trinoRemoteConfig.isSslEnabled(), "SSL must be enabled when using CERTIFICATE based remote authentication");
            Properties properties = new Properties();
            setSslProperties(properties, sslConfig);
            setOptionalProperty(properties, "clientTags", trinoRemoteConfig.getClientTags());

            return new TrinoDriverConnectionFactory(
                    new TrinoDriver(),
                    config.getConnectionUrl(),
                    properties,
                    new TrinoRemoteImpersonateOAuth2CredentialPropertiesProvider(credentialProvider),
                    openTelemetry);
        }
    }

    private static void setOptionalProperty(Properties properties, String propertyKey, Optional<String> propertyValue)
    {
        propertyValue.ifPresent((value) -> {
            properties.setProperty(propertyKey, value);
        });
    }

    private static void setSslKeystoreProperties(Properties properties, TrinoRemoteSslConfig sslConfig)
    {
        properties.setProperty("SSLKeyStorePath", sslConfig.getKeystoreFile().getAbsolutePath());
        properties.setProperty("SSLKeyStorePassword", sslConfig.getKeyStorePassword());
    }

    private static void setSslProperties(Properties properties, TrinoRemoteSslConfig sslConfig)
    {
        properties.setProperty("SSL", "true");
        setOptionalProperty(properties, "SSLTrustStorePath", sslConfig.getTruststoreFile().map(File::getAbsolutePath));
        setOptionalProperty(properties, "SSLTrustStorePassword", sslConfig.getTruststorePassword());
        setOptionalProperty(properties, "SSLTrustStoreType", sslConfig.getTruststoreType());
    }
}
