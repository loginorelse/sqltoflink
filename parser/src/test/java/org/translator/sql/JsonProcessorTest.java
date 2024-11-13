package org.translator.sql;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JsonProcessorTest {
    private final String clusterParamsExpected = """
            {"statements":[\
                {"definitions":[\
                    {"term":"category","content":"cluster_param"},\
                    {"term":"driver_cores","content":"2"},\
                    {"term":"driver_cores_limit","content":"3"},\
                    {"term":"driver_memory","content":"1024m"},\
                    {"term":"executor_num","content":"2"},\
                    {"term":"executor_cores","content":"4"},\
                    {"term":"executor_cores_limit","content":"6"},\
                    {"term":"executor_memory","content":"4"}\
                ]}\
            ]}""".replaceAll("(\\s\\s+)", "");

    private final String dataStreamExpected = """
            {"statements":[\
                {"definitions":[\
                    {"term":"category","content":"data_stream"},\
                    {"term":"type","content":"filter"},\
                    {"term":"name","content":"filter"},\
                    {"term":"operation","content":"(((Double) item.getFieldAs(\\"price\\")) * 0.85) >= 750"},\
                    {"term":"from","content":"topN"}\
                ]}\
            ]}""".replaceAll("(\\s\\s+)", "");

    private final String ddlExpected = """
            {"statements":[\
                {"definitions":[\
                    {"term":"category","content":"SQL"},\
                    {"term":"type","content":"create_table"},\
                    {"term":"name","content":"sql0"},\
                    {"term":"operation","content":\
                        "CREATE TABLE src (\
                        product_id STRING,\
                        price DOUBLE,\
                        sales INT,\
                        product_category STRING\
                        ) WITH (\
                        'connector' = 'filesystem',\
                        'format' = 'csv',\
                        'csv.ignore-parse-errors' = 'true',\
                        'csv.allow-comments' = 'true',\
                        'path' = 'path-to-csv'\
                        )"\
                    }\
                ]}\
            ]}""".replaceAll("(\\s\\s+)", "");

    private final String readExpected = """
            {"statements":[\
                {"definitions":[\
                    {"term":"category","content":"SQL"},\
                    {"term":"type","content":"select"},\
                    {"term":"name","content":"topN"},\
                    {"term":"operation","content":\
                        "SELECT product_name, price, product_category\
                        FROM (\
                            SELECT *,ROW_NUMBER() OVER (PARTITION BY product_category ORDER BY sales DESC) AS row_num\
                            FROM src)\
                        WHERE row_num <= 2"\
                    }\
                ]}\
            ]}""".replaceAll("(\\s\\s+)", "");

    private final String writeExpected = """
            {"statements":[\
                {"definitions":[\
                    {"term":"category","content":"SQL"},\
                    {"term":"type","content":"insert_into"},\
                    {"term":"name","content":""},\
                    {"term":"operation","content":\
                        "INSERT INTO src VALUES\
                            ('1', 'Alice in Wonderland, 855.0, 153, 'books'),\
                            ('2', 'MartinIden', 730..0, 203, 'books'),\
                            ('3', 'The battle from Algiers', 935.0, 17, 'DVD'),\
                            ('4', 'Sherlock Holmes', 812.3, 197, 'books'),\
                            ('5', '8 Mile', 954.0, 53, 'DVD'),\
                            ('6', 'The big Bang theory', 799.0, 584, 'DVD');"\
                    }\
                ]}\
            ]}""".replaceAll("(\\s\\s+)", "");
    @Test
    public void testWriteJsonProcessing() {
        assertEquals(writeExpected, Util.createInsertIntoJson());
    }

    @Test
    public void testReadJsonProcessing() {
        assertEquals(readExpected, Util.createReadJson());
    }

    @Test
    public void testDDLJsonProcessing() {
        assertEquals(ddlExpected, Util.createDDLJson());
    }

    @Test
    public void testDataStreamJsonProcessing() {
        assertEquals(dataStreamExpected, Util.createDataStreamJson());
    }

    @Test
    public void testClusterParamsJsonProcessing() {
        assertEquals(clusterParamsExpected, Util.createClusterParamsJson());
    }
}
