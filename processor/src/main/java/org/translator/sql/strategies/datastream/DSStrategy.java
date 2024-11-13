package org.translator.sql.strategies.datastream;

import org.translator.sql.ProcessStrategy;

import java.util.HashSet;
import java.util.Set;

public abstract class DSStrategy implements ProcessStrategy {
    // IMPORTS
    protected final String DATA_STREAM =
            "import org.apache.flink.streaming.api.datastream.DataStream;\n";

    protected final String ROW =
            "import org.apache.flink.types.Row;\n";

    protected final Set<String> imports;

    public DSStrategy() {
        this.imports = new HashSet<>();
        imports.add(DATA_STREAM);
    }
}
