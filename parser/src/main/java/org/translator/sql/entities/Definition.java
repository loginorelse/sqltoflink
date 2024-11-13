package org.translator.sql.entities;

import lombok.*;
import lombok.experimental.FieldDefaults;

@Getter
@FieldDefaults(level= AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
public class Definition {
    String term;
    String content;
}
