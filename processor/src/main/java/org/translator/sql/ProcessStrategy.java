package org.translator.sql;

/**
 * Each concrete strategy must generate a logically independent part of code (i.e. a part
 * of Flink app).
 */
public interface ProcessStrategy {
    Code getCode();
}
