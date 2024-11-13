package org.translator.sql.strategies.build.utils;

import org.translator.sql.Code;
import org.translator.sql.strategies.build.BuildStrategy;
import org.translator.sql.entities.Definition;
import org.translator.sql.templates.BuildOperatorTemplate;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class SerializerBuildStrategy extends BuildStrategy {
    // IMPORTS
    private static final String KAFKA_RECORD_SER_SCHEMA =
            "import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;\n";

    private static final String STRING_VALUE_SER_SCHEMA =
            "import org.apache.flink.api.common.serialization.SimpleStringSchema;\n";

    protected enum Parameter {
        VALUE_SERIALIZATION_SCHEMA, TOPIC;

        private static final HashSet<Parameter> knownTerms = new HashSet<>(Arrays.stream(Parameter.values()).toList());

        public static Optional<Parameter> from(String value) {
            if (knownTerms.contains(Parameter.valueOf(value.toUpperCase()))) {
                return Optional.of(Parameter.valueOf(value.toUpperCase()));
            } else {
                return Optional.empty();
            }
        }
    }

    private final BuildOperatorTemplate template;

    private final Set<String> imports;

    private SerializerBuildStrategy(BuildOperatorTemplate template) {
        this.template = template;
        this.imports = new HashSet<>();
        imports.add(KAFKA_RECORD_SER_SCHEMA);
    }

    public static SerializerBuildStrategy from(BuildOperatorTemplate template) {
        return new SerializerBuildStrategy(template);
    }

    @Override
    public Code getCode() {
        String executable = "\t\tKafkaRecordSerializationSchema<%s> %s = KafkaRecordSerializationSchema.builder()\n%s"
                .formatted(template.getParametrizedType(), template.getName(), construct(template.getDefinitions()));

        return new Code(executable, imports);
    }

    @Override
    protected void buildUpCode(StringBuilder expression, Definition definition) {
        Parameter parameter = Parameter.from(definition.getTerm()).orElseThrow(NullPointerException::new);
        String content = definition.getContent();

        String appendable = switch (parameter) {
            case VALUE_SERIALIZATION_SCHEMA -> getValueSerializationSchema(content);
            case TOPIC -> getTopic(content);
        };

        expression.append(appendable);
    }

    private String getValueSerializationSchema(String valueSerializationSchema) {
        imports.add(STRING_VALUE_SER_SCHEMA);
        return "\t\t\t\t.setValueSerializationSchema(new %s())\n".formatted(valueSerializationSchema);
    }

    private String getTopic(String topic) {
        return "\t\t\t\t.setTopic(\"%s\")\n".formatted(topic);
    }
}
