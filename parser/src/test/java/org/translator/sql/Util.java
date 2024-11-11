package org.translator.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.translator.sql.entities.Definition;
import org.translator.sql.entities.Program;
import org.translator.sql.entities.Statement;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class Util {
    @SneakyThrows
    public static void createPrintTemplateJson() {
        List<Statement> statements = new ArrayList<>();

        List<Definition> clusterParams = new ArrayList<>() {{
            add(new Definition("category", "cluster_param"));
            add(new Definition("driver_cores", "2"));
            add(new Definition("driver_cores_limit", "3"));
            add(new Definition("driver_memory", "1024m"));
            add(new Definition("executor_num", "2"));
            add(new Definition("executor_cores", "4"));
            add(new Definition("executor_cores_limit", "6"));
            add(new Definition("executor_memory", "4"));
        }};

        statements.add(new Statement(clusterParams));

        Definition sql0 = new Definition(
                "operation",
                """
                        CREATE TABLE src (
                        product_id STRING,
                        price DOUBLE,
                        sales INT,
                        product_category STRING
                        ) WITH (
                        'connector' = 'filesystem',
                        'format' = 'csv',
                        'csv.ignore-parse-errors' = 'true',
                        'csv.allow-comments' = 'true',
                        'path' = 'path-to-csv'
                        )
                        """);

        List<Definition> sql0Definitions = new ArrayList<>() {{
            add(new Definition("category", "SQL"));
            add(new Definition("type", "create_table"));
            add(new Definition("name", "sql0"));
            add(sql0);
        }};

        statements.add(new Statement(sql0Definitions));

        Definition sql1 = new Definition(
                "operation",
                """
                        INSERT INTO src VALUES
                        ('1', 'Alice in Wonderland, 855.0, 153, 'books'),
                        ('2', 'MartinIden', 730..0, 203, 'books'),
                        ('3', 'The battle from Algiers', 935.0, 17, 'DVD'),
                        ('4', 'Sherlock Holmes', 812.3, 197, 'books'),
                        ('5', '8 Mile', 954.0, 53, 'DVD'),
                        ('6', 'The big Bang theory', 799.0, 584, 'DVD');
                        """);

        List<Definition> sql1Definitions = new ArrayList<>() {{
            add(new Definition("category", "SQL"));
            add(new Definition("type", "insert_into"));
            add(new Definition("name", ""));
            add(sql1);
        }};

        statements.add(new Statement(sql1Definitions));

        Definition sql2 = new Definition(
                "operation",
                """
                        SELECT product_name, price, product_category
                        FROM (
                          SELECT *,
                          ROW_NUMBER() OVER (PARTITION BY product_category ORDER BY sales DESC) AS row_num
                          FROM src)
                        WHERE row_num <= 2""");

        List<Definition> sql2Definitions = new ArrayList<>() {{
            add(new Definition("category", "SQL"));
            add(new Definition("type", "select"));
            add(new Definition("name", "topN"));
            add(sql2);
        }};

        statements.add(new Statement(sql2Definitions));

        List<Definition> ds0 = new ArrayList<>() {{
            add(new Definition("category", "data_stream"));
            add(new Definition("type", "filter"));
            add(new Definition("name", "filter"));
            add(new Definition("operation", "(((Double) item.getFieldAs(\"price\")) * 0.85) >= 750"));
            add(new Definition("from", "topN"));
        }};

        statements.add(new Statement(ds0));

        Program program = new Program(statements);

        ObjectMapper mapper = new ObjectMapper();

        mapper.writeValue(new File("path"), program);
    }
}
