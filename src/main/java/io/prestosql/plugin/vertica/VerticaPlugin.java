package io.prestosql.plugin.vertica;

import io.prestosql.plugin.jdbc.JdbcPlugin;

public class VerticaPlugin extends JdbcPlugin {

    public VerticaPlugin() {
        super("vertica", new VerticaClientModule());
    }

}
