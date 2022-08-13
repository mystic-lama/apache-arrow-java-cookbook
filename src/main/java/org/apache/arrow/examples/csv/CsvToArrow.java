package org.apache.arrow.examples.csv;

import com.google.common.base.Charsets;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.avro.Schema.Type;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public class CsvToArrow {

    public void csvToArrow(File csvFile, File arrowFile) throws IOException {
        CsvParserSettings csvParserSettings = new CsvParserSettings();
        CsvParser parser = new CsvParser(csvParserSettings);
        parser.beginParsing(csvFile);

        Field id = new Field("id", new FieldType(false, new ArrowType.Int(32, false), null), null);
        Field name = new Field("name", new FieldType(false, new ArrowType.Utf8(), null), null);

        Schema schema = new Schema(asList(id, name));
        List<FieldVector> vectors = new ArrayList<>();

        try (
                BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        ) {

            try (FileOutputStream fileOutputStream = new FileOutputStream(arrowFile);
                 VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                 ArrowFileWriter arrowWriter = new ArrowFileWriter(root, null, fileOutputStream.getChannel())) {
                arrowWriter.start();
                String[] row;

                schema.getFields().forEach(field -> vectors.add(field.createVector(allocator)));
                vectors.forEach(vec -> vec.allocateNew());
                int index = 0;
                while ((row = parser.parseNext()) != null) {
                    for (int i = 0; i < row.length; i++) {
                        FieldVector vector = vectors.get(i);

                        switch (vector.getField().getType().getTypeID()) {
                            case Int:
                                ((UInt4Vector) vector).set(index, Integer.valueOf(row[i]));
                                break;

                            case Utf8:
                                ((VarCharVector) vector).set(index, row[i].getBytes(Charsets.UTF_8));
                                break;

                            default:
                                System.out.println("Unkown type : " + vector.getField());

                        }
                        index++;
                    }
                }
                vectors.forEach(vec -> vec.setValueCount(3));
                arrowWriter.end();
                System.out.println("Output file size: " + arrowFile.length());
            }
            finally {
                vectors.forEach(vec -> vec.close());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        CsvToArrow csvToArrow = new CsvToArrow();
        csvToArrow.csvToArrow(new File("src/main/resources/csv/data1.csv"), new File("csvToArrow.arrow"));
    }

}
