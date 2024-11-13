package org.translator.sql.strategies.datastream;

import org.translator.sql.Code;
import org.translator.sql.templates.DataStreamTemplate;

public class FilterDSStrategy extends DSStrategy {
    private final DataStreamTemplate template;

    private FilterDSStrategy(DataStreamTemplate template) {
        super();
        this.template = template;
        imports.add(ROW);
    }

    public static FilterDSStrategy from(DataStreamTemplate template) {
        return new FilterDSStrategy(template);
    }

    @Override
    public Code getCode() {
        String executable =
                "\t\tDataStream<Row> %s = tEnv.toChangelogStream(%s).filter(item -> item.getField(\"price\") != null \n\t\t\t && %s);\n\n"
                        .formatted(template.getName(), template.getFrom(), template.getOperation());

        return new Code(executable, imports);
    }
}
