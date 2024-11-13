package org.translator.sql.templates;

import lombok.Getter;
import org.translator.sql.entities.Definition;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Queue;

@Getter
public class DataStreamTemplate extends OperatorTemplate {
    public enum DataStreamOperatorType {
        FILTER, CREATE_TABLE, PRINT, READ_SOURCE, SINK;

        private static final HashSet<DataStreamOperatorType> knownTerms = new HashSet<>(Arrays.stream(DataStreamOperatorType.values()).toList());

        public static Optional<DataStreamOperatorType> from(String value) {
            if (knownTerms.contains(DataStreamOperatorType.valueOf(value.toUpperCase()))) {
                return Optional.of(DataStreamOperatorType.valueOf(value.toUpperCase()));
            } else {
                return Optional.empty();
            }
        }
    }

    final DataStreamOperatorType type;
    final String from;
    final String operation;
    final String parametrizedType;

    private DataStreamTemplate(Builder builder) {
        super(builder);
        this.type = builder.type;
        this.from = builder.from;
        this.operation = builder.operation;
        this.parametrizedType = builder.parametrizedType;
    }

    public static class Builder extends OperatorTemplate.Builder<Builder> {
        private String from;
        private String operation;
        private DataStreamOperatorType type;
        private String parametrizedType;

        public Builder(Queue<Definition> definitions) throws IllegalAccessException {
            while (!definitions.isEmpty()) {
                Definition definition = definitions.poll();
                Term term = Term.from(definition.getTerm()).orElseThrow(NullPointerException::new);
                switch (term) {
                    case TYPE -> type = DataStreamOperatorType.from(definition.getContent()).orElseThrow(NullPointerException::new);
                    case NAME -> setName(definition.getContent());
                    case PARAMETRIZED_TYPE -> parametrizedType = definition.getContent();
                    case FROM -> from = definition.getContent();
                    case OPERATION -> operation = definition.getContent();
                    default -> throw new IllegalAccessException();
                }
            }
            setCategory(OperatorCategory.DATASTREAM);
        }

        @Override
        OperatorTemplate build() {
            return new DataStreamTemplate(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
