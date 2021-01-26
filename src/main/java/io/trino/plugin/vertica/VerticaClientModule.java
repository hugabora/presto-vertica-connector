package io.trino.plugin.vertica;

import com.google.inject.Module;
import com.google.inject.*;
import com.vertica.jdbc.Driver;
import io.trino.plugin.jdbc.*;
import io.trino.plugin.jdbc.credential.CredentialProvider;

import java.sql.SQLException;
import java.util.Properties;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class VerticaClientModule implements Module {

    @Override
    public void configure(Binder binder) {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(VerticaClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        binder.install(new DecimalModule());
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider) throws SQLException  {
        Properties connectionProperties = new Properties();
        return new DriverConnectionFactory(
                new Driver(),
                config.getConnectionUrl(),
                connectionProperties,
                credentialProvider);
    }

}