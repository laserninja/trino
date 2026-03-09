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
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.util.Optional;

public class TrinoRemoteSslConfig
{
    private File truststoreFile;
    private String truststorePassword;
    private String truststoreType;
    private String keyStorePassword;
    private File keyStoreFile;

    @NotNull
    public Optional<File> getTruststoreFile()
    {
        return Optional.ofNullable(this.truststoreFile);
    }

    @Config("trino-remote.ssl.truststore.path")
    @ConfigDescription("File path to the location of the SSL truststore to use")
    public TrinoRemoteSslConfig setTruststoreFile(File truststoreFile)
    {
        this.truststoreFile = truststoreFile;
        return this;
    }

    @NotNull
    public Optional<String> getTruststorePassword()
    {
        return Optional.ofNullable(this.truststorePassword);
    }

    @Config("trino-remote.ssl.truststore.password")
    @ConfigSecuritySensitive
    @ConfigDescription("Password for the truststore")
    public TrinoRemoteSslConfig setTruststorePassword(String truststorePassword)
    {
        this.truststorePassword = truststorePassword;
        return this;
    }

    @NotNull
    public Optional<String> getTruststoreType()
    {
        return Optional.ofNullable(this.truststoreType);
    }

    @Config("trino-remote.ssl.truststore.type")
    @ConfigDescription("Type of truststore file used")
    public TrinoRemoteSslConfig setTruststoreType(String truststoreType)
    {
        this.truststoreType = truststoreType;
        return this;
    }

    public String getKeyStorePassword()
    {
        return keyStorePassword;
    }

    @Config("trino-remote.ssl.keystore.password")
    @ConfigDescription("Password for the keystore")
    @ConfigSecuritySensitive
    public TrinoRemoteSslConfig setKeyStorePassword(String keyStorePassword)
    {
        this.keyStorePassword = keyStorePassword;
        return this;
    }

    public File getKeystoreFile()
    {
        return this.keyStoreFile;
    }

    @Config("trino-remote.ssl.keystore.path")
    @ConfigDescription("File path to the location of the SSL keystore to use")
    public TrinoRemoteSslConfig setKeystoreFile(File keystoreFile)
    {
        this.keyStoreFile = keystoreFile;
        return this;
    }
}
