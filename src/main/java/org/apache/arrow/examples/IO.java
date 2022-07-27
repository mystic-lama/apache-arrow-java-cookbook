package org.apache.arrow.examples;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.*;

import static java.util.Arrays.asList;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class IO {

    public void writeToFile() {
        try (BufferAllocator allocator = new RootAllocator()) {
            Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
            Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
            Schema schemaPerson = new Schema(asList(name, age));
            try(
                    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, allocator)
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
                File file = new File("randon_access_to_file.arrow");
                try (
                        FileOutputStream fileOutputStream = new FileOutputStream(file);
                        ArrowFileWriter writer = new ArrowFileWriter(vectorSchemaRoot, null, fileOutputStream.getChannel())
                ) {
                    writer.start();
                    writer.writeBatch();
                    writer.end();
                    System.out.println("Record batches written: " + writer.getRecordBlocks().size() + ". Number of rows written: " + vectorSchemaRoot.getRowCount());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void writeToBuffer() {
        try (BufferAllocator allocator = new RootAllocator()) {
            Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
            Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
            Schema schemaPerson = new Schema(asList(name, age));
            try(
                    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, allocator)
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
                        ArrowFileWriter writer = new ArrowFileWriter(vectorSchemaRoot, null, Channels.newChannel(out))
                ) {
                    writer.start();
                    writer.writeBatch();

                    System.out.println("Record batches written: " + writer.getRecordBlocks().size() +
                            ". Number of rows written: " + vectorSchemaRoot.getRowCount());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void readFromFile() {
        File file = new File("src/main/resources/random_access.arrow");
        try(
                BufferAllocator rootAllocator = new RootAllocator();
                FileInputStream fileInputStream = new FileInputStream(file);
                ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), rootAllocator)
        ){
            System.out.println("Record batches in file: " + reader.getRecordBlocks().size());
            for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
                reader.loadRecordBatch(arrowBlock);
                VectorSchemaRoot vectorSchemaRootRecover = reader.getVectorSchemaRoot();
                System.out.print(vectorSchemaRootRecover.contentToTSVString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readFromBuffer() {
        Path path = Paths.get("src/main/resources/random_access.arrow");
        try(
                BufferAllocator rootAllocator = new RootAllocator();
                ArrowFileReader reader = new ArrowFileReader(new SeekableReadChannel(new ByteArrayReadableSeekableByteChannel(
                        Files.readAllBytes(path))), rootAllocator)
        ) {
            System.out.println("Record batches in file: " + reader.getRecordBlocks().size());
            for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
                reader.loadRecordBatch(arrowBlock);
                VectorSchemaRoot vectorSchemaRootRecover = reader.getVectorSchemaRoot();
                System.out.print(vectorSchemaRootRecover.contentToTSVString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeReadDataWithDictionary() {
        final DictionaryEncoding dictionaryEncoding = new DictionaryEncoding(
                /*id=*/666L, /*ordered=*/false, /*indexType=*/
                new ArrowType.Int(8, true)
        );
        try (BufferAllocator root = new RootAllocator();
             VarCharVector countries = new VarCharVector("country-dict", root);
             VarCharVector appUserCountriesUnencoded = new VarCharVector(
                     "app-use-country-dict",
                     new FieldType(true, Types.MinorType.VARCHAR.getType(), dictionaryEncoding),
                     root)
        ) {
            countries.allocateNew(10);
            countries.set(0, "Andorra".getBytes(StandardCharsets.UTF_8));
            countries.set(1, "Cuba".getBytes(StandardCharsets.UTF_8));
            countries.set(2, "Grecia".getBytes(StandardCharsets.UTF_8));
            countries.set(3, "Guinea".getBytes(StandardCharsets.UTF_8));
            countries.set(4, "Islandia".getBytes(StandardCharsets.UTF_8));
            countries.set(5, "Malta".getBytes(StandardCharsets.UTF_8));
            countries.set(6, "Tailandia".getBytes(StandardCharsets.UTF_8));
            countries.set(7, "Uganda".getBytes(StandardCharsets.UTF_8));
            countries.set(8, "Yemen".getBytes(StandardCharsets.UTF_8));
            countries.set(9, "Zambia".getBytes(StandardCharsets.UTF_8));
            countries.setValueCount(10);

            Dictionary countriesDictionary = new Dictionary(countries, dictionaryEncoding);
            System.out.println("Dictionary: " + countriesDictionary);

            appUserCountriesUnencoded.allocateNew(5);
            appUserCountriesUnencoded.set(0, "Andorra".getBytes(StandardCharsets.UTF_8));
            appUserCountriesUnencoded.set(1, "Guinea".getBytes(StandardCharsets.UTF_8));
            appUserCountriesUnencoded.set(2, "Islandia".getBytes(StandardCharsets.UTF_8));
            appUserCountriesUnencoded.set(3, "Malta".getBytes(StandardCharsets.UTF_8));
            appUserCountriesUnencoded.set(4, "Uganda".getBytes(StandardCharsets.UTF_8));
            appUserCountriesUnencoded.setValueCount(5);
            System.out.println("Unencoded data: " + appUserCountriesUnencoded);

            File file = new File("random_access_file_with_dictionary.arrow");
            DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
            provider.put(countriesDictionary);
            try (FieldVector appUseCountryDictionaryEncoded = (FieldVector) DictionaryEncoder
                    .encode(appUserCountriesUnencoded, countriesDictionary);
                 VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.of(appUseCountryDictionaryEncoded);
                 FileOutputStream fileOutputStream = new FileOutputStream(file);
                 ArrowFileWriter writer = new ArrowFileWriter(vectorSchemaRoot, provider, fileOutputStream.getChannel())
            ) {
                System.out.println("Dictionary-encoded data: " +appUseCountryDictionaryEncoded);
                System.out.println("Dictionary-encoded ID: " +appUseCountryDictionaryEncoded.getField().getDictionary().getId());
                writer.start();
                writer.writeBatch();
                writer.end();
                System.out.println("Record batches written: " + writer.getRecordBlocks().size() + ". Number of rows written: " + vectorSchemaRoot.getRowCount());
                try(
                        BufferAllocator rootAllocator = new RootAllocator();
                        FileInputStream fileInputStream = new FileInputStream(file);
                        ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), rootAllocator)
                ){
                    for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
                        reader.loadRecordBatch(arrowBlock);
                        FieldVector appUseCountryDictionaryEncodedRead = reader.getVectorSchemaRoot().getVector("app-use-country-dict");
                        DictionaryEncoding dictionaryEncodingRead = appUseCountryDictionaryEncodedRead.getField().getDictionary();
                        System.out.println("Dictionary-encoded ID recovered: " + dictionaryEncodingRead.getId());
                        Dictionary appUseCountryDictionaryRead = reader.getDictionaryVectors().get(dictionaryEncodingRead.getId());
                        System.out.println("Dictionary-encoded data recovered: " + appUseCountryDictionaryEncodedRead);
                        System.out.println("Dictionary recovered: " + appUseCountryDictionaryRead);
                        try (ValueVector readVector = DictionaryEncoder.decode(appUseCountryDictionaryEncodedRead, appUseCountryDictionaryRead)) {
                            System.out.println("Decoded data: " + readVector);
                        }
                    }
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        IO io = new IO();
        io.writeToFile();
        io.writeToBuffer();
        io.readFromFile();
        io.readFromBuffer();
        io.writeReadDataWithDictionary();
    }

}
