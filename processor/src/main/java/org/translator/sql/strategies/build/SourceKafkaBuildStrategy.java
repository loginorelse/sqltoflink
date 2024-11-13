package org.translator.sql.strategies.build;

import org.translator.sql.Code;
import org.translator.sql.templates.BuildOperatorTemplate;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;

public class SourceKafkaBuildStrategy extends KafkaBuildStrategy {
    // IMPORT
    private static final String KAFKA_SOURCE =
            "import org.apache.flink.connector.kafka.source.KafkaSource;\n";

    protected enum Parameter {
        STARTING_OFFSET, GROUP_ID, VALUE_ONLY_DESERIALIZER, BOUNDED, CLIENT_ID_PREFIX, KAFKA_SUBSCRIBER,
        PARTITIONS, PROPERTY, RACK_ID_SUPPLIER, TOPIC_PATTERN, UNBOUNDED;

        private static final HashSet<Parameter> knownTerms = new HashSet<>(Arrays.stream(Parameter.values()).toList());

        public static Optional<Parameter> from(String value) {
            if (knownTerms.contains(Parameter.valueOf(value.toUpperCase()))) {
                return Optional.of(Parameter.valueOf(value.toUpperCase()));
            } else {
                return Optional.empty();
            }
        }
    }

    private SourceKafkaBuildStrategy(BuildOperatorTemplate template) {
        super(template);
        imports.add(KAFKA_SOURCE);
    }

    public static SourceKafkaBuildStrategy from(BuildOperatorTemplate template) {
        return new SourceKafkaBuildStrategy(template);
    }

    @Override
    public Code getCode() {
        String executable = "\t\tKafkaSource<%s> %s = KafkaSource.<%s>builder()\n%s"
                .formatted(template.getParametrizedType(), template.getName(), template.getParametrizedType(),
                        construct(template.getDefinitions()));

        return new Code(executable, imports);
    }
}
