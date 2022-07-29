package org.apache.arrow.examples;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import java.io.File;
import java.io.FileOutputStream;

public class AvroHelper {
    public void generateAvroFile()  throws Exception {
        Schema schema = new Schema.Parser().parse(new File("src/main/resources/avro/user.avsc"));

        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Alyssa");
        user1.put("favorite_number", 256);

        GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name", "Ben");
        user2.put("favorite_number", 7);
        user2.put("favorite_color", "red");

        File file = new File("users.avro");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema, file);
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.close();
    }

    public void generateAvroFileSchemaless() throws Exception {
        Schema schema = new Schema.Parser().parse(new File("src/main/resources/avro/user.avsc"));

        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Alyssa");
        user1.put("favorite_number", 256);

        GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name", "Ben");
        user2.put("favorite_number", 7);
        user2.put("favorite_color", "red");

        File file = new File("users_noschema.avro");
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(new FileOutputStream(file), null);
        DatumWriter writer = new GenericDatumWriter(schema);
        writer.write(user1, encoder);
        writer.write(user2, encoder);
    }

    public static void main(String[] args) throws Exception  {
        AvroHelper avroHelper = new AvroHelper();
//        avroHelper.generateAvroFile();
        avroHelper.generateAvroFileSchemaless();
    }
}
