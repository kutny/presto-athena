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
package com.facebook.presto.plugin.athena;

import com.facebook.presto.plugin.jdbc.*;

import javax.inject.Inject;
import java.sql.SQLException;
import java.util.Properties;
import com.simba.athena.jdbc.Driver;
import io.airlift.log.Logger;

import static com.facebook.presto.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Implementation of AthenaClient. It describes table, schemas and columns behaviours.
 * It allows to change the QueryBuilder to a custom one as well.
 *
 * @author Jiri Koutny
 */
public class AthenaClient extends BaseJdbcClient {

    private static final Logger log = Logger.get(AthenaClient.class);

    @Inject
    public AthenaClient(JdbcConnectorId connectorId, BaseJdbcConfig config) throws SQLException {
        super(connectorId, config, "", connectionFactory(config));
    }

    private static ConnectionFactory connectionFactory(BaseJdbcConfig config) {
        log.info("Connection factory invoked");

        checkArgument(config.getConnectionUrl() != null, "Invalid JDBC URL for Athena connector");
        checkArgument(config.getConnectionUser() != null, "Invalid JDBC User for Athena connector");
        checkArgument(config.getConnectionPassword() != null, "Invalid JDBC Password for Athena connector");

        //Class.forName("com.simba.athena.jdbc.Driver");

        Properties connectionProperties = basicConnectionProperties(config);
        connectionProperties.setProperty("UID", config.getConnectionUser());
        connectionProperties.setProperty("PWD", config.getConnectionPassword());
        connectionProperties.setProperty("AwsRegion", "eu-west-1");
        connectionProperties.setProperty("S3OutputLocation", "s3://aws-athena-query-results-253287946333-eu-west-1/");

        log.info(config.getConnectionUrl());

        return new DriverConnectionFactory(new Driver(), config.getConnectionUrl(), connectionProperties);
    }
}
