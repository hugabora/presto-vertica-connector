package io.trino.plugin.vertica;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.jdbc.*;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import javax.inject.Inject;
import java.sql.*;
import java.util.Collection;
import java.util.Optional;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.util.Locale.ENGLISH;

public class VerticaClient extends BaseJdbcClient {

    @Inject
    public VerticaClient(BaseJdbcConfig config, ConnectionFactory connectionFactory) {
        super(config, "", connectionFactory);
    }

    @Override
    protected Collection<String> listSchemas(Connection connection) {
        try(ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM").toLowerCase(ENGLISH);
                if (!schemaName.equals("v_monitor") && !schemaName.equals("v_txtindex") && !schemaName.equals("v_catalog")) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        } catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e.getMessage());
        }
    }

   @Override
   protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName) throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        String escape = metadata.getSearchStringEscape();
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape).orElse(null),
                escapeNamePattern(tableName, escape).orElse(null),
                new String[]{"TABLE"});
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql) throws SQLException {
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(sql);
        return statement;
    }

    @Override
    protected String getTableSchemaName(ResultSet resultSet) throws SQLException {
        return resultSet.getString("TABLE_SCHEM").toLowerCase(ENGLISH);
    }

    @Override
    public Optional<ColumnMapping> toTrinoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle) {
        // TODO implement proper type mapping
        return legacyToPrestoType(session, connection, typeHandle);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type) {
        // TODO implement proper type mapping
        return legacyToWriteMapping(session, type);
    }

}
