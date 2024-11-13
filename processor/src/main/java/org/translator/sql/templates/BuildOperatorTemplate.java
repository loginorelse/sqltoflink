package org.translator.sql.templates;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import org.translator.sql.entities.Definition;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Queue;

@Getter
@FieldDefaults(level= AccessLevel.PRIVATE)
public class BuildOperatorTemplate extends OperatorTemplate {
    public enum BuildOperatorType {
        KAFKA_SOURCE, KAFKA_SINK, RECORD_SERIALIZER;

        private static final HashSet<BuildOperatorType> knownTerms = new HashSet<>(Arrays.stream(BuildOperatorType.values()).toList());

        public static Optional<BuildOperatorType> from(String value) {
            if (knownTerms.contains(BuildOperatorType.valueOf(value.toUpperCase()))) {
                return Optional.of(BuildOperatorType.valueOf(value.toUpperCase()));
            } else {
                return Optional.empty();
            }
        }
    }

    final Queue<Definition> definitions;
    final BuildOperatorType type;
    final String parametrizedType;

    private BuildOperatorTemplate(Builder builder) {
        super(builder);
        this.definitions = builder.definitions;
        this.type = builder.type;
        this.parametrizedType = builder.parametrizedType;
    }

    public static class Builder extends OperatorTemplate.Builder<Builder> {
        private final Queue<Definition> definitions;
        private BuildOperatorType type;
        private String parametrizedType;

        @Override
        OperatorTemplate build() {
            return new BuildOperatorTemplate(this);
        }

        @Override
        protected Builder self() {
            return this;
        }

        public Builder(Queue<Definition> definitions) throws IllegalAccessException {
            for (int i = 0; i < OPERATOR_TEMPLATE_PARAM_COUNT; i++) {
                Definition definition = definitions.poll();
                Term term = Term.from(definition.getTerm()).orElseThrow(NullPointerException::new);
                switch (term) {
                    case TYPE -> type = BuildOperatorType.from(definition.getContent()).orElseThrow(NullPointerException::new);
                    case NAME -> setName(definition.getContent());
                    case PARAMETRIZED_TYPE -> parametrizedType = definition.getContent();
                    default -> throw new IllegalAccessException();
                }
            }
            this.definitions = definitions;
            setCategory(OperatorCategory.BUILD);
        }
    }
}
