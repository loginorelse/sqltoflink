package org.translator.sql.strategies.build;

import org.translator.sql.Code;
import org.translator.sql.templates.BuildOperatorTemplate;

public class SinkKafkaBuildStrategy extends KafkaBuildStrategy {
    // IMPORTS
    private static final String SINK_IMPORT =
            "import org.apache.flink.connector.kafka.sink.KafkaSink;\n";

    private SinkKafkaBuildStrategy(BuildOperatorTemplate template) {
        super(template);
        imports.add(SINK_IMPORT);
    }

    public static SinkKafkaBuildStrategy from(BuildOperatorTemplate template) {
        return new SinkKafkaBuildStrategy(template);
    }

    @Override
    public Code getCode() {
        String executable =
                "\t\tKafkaSink<%s> %s = KafkaSink.<%s>builder()\n%s"
                        .formatted(template.getParametrizedType(), template.getName(), template.getParametrizedType(),
                                construct(template.getDefinitions()));

        return new Code(executable, imports);
    }
}
