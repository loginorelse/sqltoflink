package org.translator.sql.templates;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;

public enum OperatorCategory {
    DATASTREAM, SQL, BUILD, ENVIRONMENT_PARAM, CLUSTER_PARAM;

    private static final HashSet<OperatorCategory> knownTerms = new HashSet<>(Arrays.stream(OperatorCategory.values()).toList());

    public static Optional<OperatorCategory> from(String value) {
        if (knownTerms.contains(OperatorCategory.valueOf(value.toUpperCase()))) {
            return Optional.of(OperatorCategory.valueOf(value.toUpperCase()));
        } else {
            return Optional.empty();
        }
    }
}
