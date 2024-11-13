package org.translator.sql;

import java.util.Set;

public record Code(String executable, Set<String> imports) {}
