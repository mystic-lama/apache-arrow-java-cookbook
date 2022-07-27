package org.apache.arrow.examples;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VectorSchemaRoot;
import static java.util.Arrays.asList;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class StreamingIO {

    public void writeToFile() {
        try (BufferAllocator rootAllocator = new RootAllocator()) {
            Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
            Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
            Schema schemaPerson = new Schema(asList(name, age));
            try(
                    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator)
            ){
                VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
                nameVector.allocateNew(3);
                nameVector.set(0, "David".getBytes());
                nameVector.set(1, "Gladis".getBytes());
                nameVector.set(2, "Juan".getBytes());
                IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
                ageVector.allocateNew(3);
                ageVector.set(0, 10);
                ageVector.set(1, 20);
                ageVector.set(2, 30);
                vectorSchemaRoot.setRowCount(3);
                File file = new File("streaming_to_file.arrow");
                try (
                        FileOutputStream fileOutputStream = new FileOutputStream(file);
                        ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, fileOutputStream.getChannel())
                ){
                    writer.start();
                    writer.writeBatch();
                    System.out.println("Number of rows written: " + vectorSchemaRoot.getRowCount());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void writeToBuffer() {
        try (BufferAllocator rootAllocator = new RootAllocator()) {
            Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
            Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
            Schema schemaPerson = new Schema(asList(name, age));
            try(
                    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator)
            ){
                VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
                nameVector.allocateNew(3);
                nameVector.set(0, "David".getBytes());
                nameVector.set(1, "Gladis".getBytes());
                nameVector.set(2, "Juan".getBytes());
                IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
                ageVector.allocateNew(3);
                ageVector.set(0, 10);
                ageVector.set(1, 20);
                ageVector.set(2, 30);
                vectorSchemaRoot.setRowCount(3);
                try (
                        ByteArrayOutputStream out = new ByteArrayOutputStream();
                        ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, Channels.newChannel(out))
                ){
                    writer.start();
                    writer.writeBatch();
                    System.out.println("Number of rows written: " + vectorSchemaRoot.getRowCount());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void readFromFile() {
        File file = new File("src/main/resources/streaming.arrow");
        try(
                BufferAllocator rootAllocator = new RootAllocator();
                FileInputStream fileInputStreamForStream = new FileInputStream(file);
                ArrowStreamReader reader = new ArrowStreamReader(fileInputStreamForStream, rootAllocator)
        ) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot vectorSchemaRootRecover = reader.getVectorSchemaRoot();
                System.out.print(vectorSchemaRootRecover.contentToTSVString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readFromBuffer() {
        Path path = Paths.get("src/main/resources/streaming.arrow");
        try(
                BufferAllocator rootAllocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(
                        Files.readAllBytes(path)), rootAllocator)
        ) {
            while(reader.loadNextBatch()){
                System.out.print(reader.getVectorSchemaRoot().contentToTSVString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        StreamingIO streaningIO = new StreamingIO();
        streaningIO.writeToFile();
        streaningIO.writeToBuffer();
        streaningIO.readFromBuffer();
        streaningIO.readFromFile();
    }

}
