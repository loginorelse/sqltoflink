package org.translator.sql.strategies.build;

import org.translator.sql.ProcessStrategy;
import org.translator.sql.entities.Definition;

import java.util.Queue;

public abstract class BuildStrategy implements ProcessStrategy {
    protected String construct(Queue<Definition> definitions) {
        StringBuilder expression = new StringBuilder();

        while (!definitions.isEmpty()) {
            buildUpCode(expression, definitions.poll());
        }

        expression.append(getBuild());

        return expression.toString();
    }

    protected abstract void buildUpCode(StringBuilder expression, Definition definition);

    private String getBuild() {
        return "\t\t\t\t.build();\n\n";
    }
}
