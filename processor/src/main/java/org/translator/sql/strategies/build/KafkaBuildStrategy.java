package org.translator.sql.strategies.build;

import org.translator.sql.entities.Definition;
import org.translator.sql.templates.BuildOperatorTemplate;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public abstract class KafkaBuildStrategy extends BuildStrategy {
    // IMPORTS
    protected static final String KAFKA_RECORD_DESER_SCHEMA =
            "import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;\n";

    protected static final String STRING_DESER =
            "import org.apache.kafka.common.serialization.StringDeserializer;\n";

    private enum Parameter {
        BOOTSTRAP_SERVERS, TOPICS, DESERIALIZER, RECORD_SERIALIZER;

        private static final HashSet<Parameter> knownTerms = new HashSet<>(Arrays.stream(Parameter.values()).toList());

        public static Optional<Parameter> from(String value) {
            if (knownTerms.contains(Parameter.valueOf(value.toUpperCase()))) {
                return Optional.of(Parameter.valueOf(value.toUpperCase()));
            } else {
                return Optional.empty();
            }
        }
    }

    protected final BuildOperatorTemplate template;

    protected final Set<String> imports;

    protected KafkaBuildStrategy(BuildOperatorTemplate template) {
        this.template = template;
        this.imports = new HashSet<>();
    }

    @Override
    protected void buildUpCode(StringBuilder expression, Definition definition) {
        Parameter parameter = Parameter.from(definition.getTerm()).orElseThrow(NullPointerException::new);
        String content = definition.getContent();

        String appendable = switch (parameter) {
            case BOOTSTRAP_SERVERS -> getBootstrapServers(content);
            case DESERIALIZER -> getDeserializer(content);
            case RECORD_SERIALIZER -> getRecordSerializer(content);
            case TOPICS -> getTopics(content);
        };

        expression.append(appendable);
    }

    private String getRecordSerializer(String recordSerializer) {
        return "\t\t\t\t.setRecordSerializer(%s)\n".formatted(recordSerializer);
    }

    private String getBootstrapServers(String bootstrapServers) {
        return "\t\t\t\t.setBootstrapServers(\"%s\")\n".formatted(bootstrapServers);
    }

    private String getTopics(String topics) {
        return "\t\t\t\t.setTopics(%s)\n".formatted(parseParams(topics));
    }

    private String getDeserializer(String deserializer) {
        imports.add(STRING_DESER);
        imports.add(KAFKA_RECORD_DESER_SCHEMA);
        return "\t\t\t\t.setDeserializer(KafkaRecordDeserializationSchema.valueOnly(%s.class))\n".formatted(deserializer);
    }

    protected String parseParams(String params) {
        return Arrays.stream(params.split(" "))
                .reduce("", (acc, topic) -> acc.concat("\"" + topic + "\""))
                .stripTrailing()
                .replaceAll(" ", ",");
    }
}
