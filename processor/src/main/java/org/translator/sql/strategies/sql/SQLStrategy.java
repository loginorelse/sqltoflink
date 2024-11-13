package org.translator.sql.strategies.sql;

import org.translator.sql.ProcessStrategy;

import java.util.HashSet;
import java.util.Set;

public abstract class SQLStrategy implements ProcessStrategy {
    // IMPORTS
    private static final String TABLE_STUFF =
            "import org.apache.flink.table.api.*;\n";

    private static final String TABLE_ENVIRONMENT =
            "import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;\n";

    protected final Set<String> imports;

    public SQLStrategy() {
        this.imports = new HashSet<>();
        imports.add(TABLE_STUFF);
        imports.add(TABLE_ENVIRONMENT);
    }
}
