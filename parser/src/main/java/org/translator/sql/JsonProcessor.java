package org.translator.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.translator.sql.entities.Program;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

public class JsonProcessor {
    public static Optional<Program> processJson(File json) {
        ObjectMapper mapper = new ObjectMapper();
        Optional<Program> input;

        try {
            input = Optional.ofNullable(mapper.readValue(json, Program.class));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return input;
    }
}
