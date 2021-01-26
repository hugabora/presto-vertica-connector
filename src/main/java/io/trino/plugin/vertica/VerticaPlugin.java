package io.trino.plugin.vertica;

import io.trino.plugin.jdbc.JdbcPlugin;

public class VerticaPlugin extends JdbcPlugin {

    public VerticaPlugin() {
        super("vertica", new VerticaClientModule());
    }

}
