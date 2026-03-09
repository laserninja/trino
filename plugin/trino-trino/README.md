# Trino to Trino Plugin

This is a plugin for Trino that allow you to use another Trino cluster as a data source through JDBC interface.

[![Trino-Connectors Member](https://img.shields.io/badge/trino--connectors-member-green.svg)](http://presto-connectors.ml)

## Connection Configuration

Create new properties file inside etc/catalog dir:

        connector.name=trino
        connection-url=jdbc:trino://<host>:<port>/<catalog>?explicitPrepare=false
        trino-remote.ssl.enabled=true
        trino-remote.impersonation.enabled=true

        # PASSWORD based authentication
        trino-remote.authentication.type=PASSWORD
        connection-user=<user_id>
        connection-password=<password>

        join-pushdown.enabled=true
        trino-remote.clientTags=[<local_cluster_id>]

        unsupported-type-handling=CONVERT_TO_VARCHAR

## Remote Trino cluster changes

Add the below entries in the remote Trino cluster for secure access to data.
For password based authentication, make sure to add impersonation rules to allow 
remote Coordinator's access control file configuration - etc/rules.json
 
        "impersonation": [ 
         {
              "original_user": "principal_user_id",
              "user": "(.*)",
              "allow": true
         }
        ] 
