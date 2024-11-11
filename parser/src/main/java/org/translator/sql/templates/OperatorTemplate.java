package org.translator.sql.templates;

import org.translator.sql.entities.Program;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;

public abstract class OperatorTemplate {
    protected enum Term {
        TYPE, OPERATION, NAME, FROM, VALUE, PARAMETRIZED_TYPE;

        private static final HashSet<Term> knownTerms = new HashSet<>(Arrays.stream(Term.values()).toList());

        public static Optional<Term> from(String value) {
            if (knownTerms.contains(Term.valueOf(value.toUpperCase()))) {
                return Optional.of(Term.valueOf(value.toUpperCase()));
            } else {
                return Optional.empty();
            }
        }
    }

    protected final OperatorCategory category;
    protected final String name;

    // hardcode to use in BuildOperatorTemplate
    // it is needed to process first few items in the queue
    // and save entire queue to process further
    protected static final int OPERATOR_TEMPLATE_PARAM_COUNT = 3;

    public OperatorTemplate(Builder<?> builder) {
        this.name = builder.name;
        this.category = builder.category;
    }

    abstract static class Builder<T extends Builder<T>> {
        private String name;
        private OperatorCategory category;

        public void setName(String name) {
            this.name = name;
        }

        public void setCategory(OperatorCategory category) {
            this.category = category;
        }

        abstract OperatorTemplate build();

        protected abstract T self();
    }

    public OperatorCategory getCategory() {
        return category;
    }

    public String getName() {
        return name;
    }
}
