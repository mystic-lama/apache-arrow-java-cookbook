package org.apache.arrow.examples;

import org.apache.arrow.AvroToArrow;
import org.apache.arrow.AvroToArrowConfig;
import org.apache.arrow.AvroToArrowConfigBuilder;
import org.apache.arrow.AvroToArrowVectorIterator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.File;
import java.io.FileInputStream;

public class AvroToArrowExample {

    public void avroToArrow() throws Exception {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        AvroToArrowConfig config = new AvroToArrowConfigBuilder(allocator).build();

        BinaryDecoder decoder = new DecoderFactory().binaryDecoder(new FileInputStream("users_noschema.avro"), null);

        Schema schema = new Schema.Parser().parse(new File("src/main/resources/avro/user.avsc"));
        AvroToArrowVectorIterator avroToArrowVectorIterator = AvroToArrow.avroToArrowIterator(schema, decoder, config);

        while(avroToArrowVectorIterator.hasNext()) {
            VectorSchemaRoot root = avroToArrowVectorIterator.next();
            System.out.println(root.contentToTSVString());
        }
    }

    public static void main(String[] args) throws  Exception {
        AvroToArrowExample avroToArrowExample = new AvroToArrowExample();
        avroToArrowExample.avroToArrow();
    }

}
