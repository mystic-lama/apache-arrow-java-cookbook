package org.apache.arrow.examples;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

public class SchemaExamples {

    public void createFields() {
        Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        System.out.println(name);

        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
        System.out.println(age);

        FieldType intType = new FieldType(true, new ArrowType.Int(32, true), null);
        FieldType listType = new FieldType(true, new ArrowType.List(), null);
        Field childField = new Field("intCol", intType, null);
        List<Field> childFields = new ArrayList<>();
        childFields.add(childField);
        Field points = new Field("points", listType, childFields);

        System.out.println(points);
    }

    public void createSchema() {
        Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Field document = new Field("document", new FieldType(true, new ArrowType.Utf8(), null), null);
        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
        FieldType intType = new FieldType(true, new ArrowType.Int(32, true), /*dictionary=*/null);
        FieldType listType = new FieldType(true, new ArrowType.List(), /*dictionary=*/null);
        Field childField = new Field("intCol", intType, null);
        List<Field> childFields = new ArrayList<>();
        childFields.add(childField);
        Field points = new Field("points", listType, childFields);
        Schema schemaPerson = new Schema(asList(name, document, age, points));

        System.out.println(schemaPerson);
    }

    public void addMetadata() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("A", "Id card");
        metadata.put("B", "Passport");
        metadata.put("C", "Visa");
        Field document = new Field("document", new FieldType(true, new ArrowType.Utf8(), null, metadata), null);

        System.out.println(document.getMetadata());
    }

    public void addMetadataToSchema() {
        Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Field document = new Field("document", new FieldType(true, new ArrowType.Utf8(), null), null);
        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
        FieldType intType = new FieldType(true, new ArrowType.Int(32, true), /*dictionary=*/null);
        FieldType listType = new FieldType(true, new ArrowType.List(), /*dictionary=*/null);
        Field childField = new Field("intCol", intType, null);
        List<Field> childFields = new ArrayList<>();
        childFields.add(childField);
        Field points = new Field("points", listType, childFields);
        Map<String, String> metadataSchema = new HashMap<>();
        metadataSchema.put("Key-1", "Value-1");
        Schema schemaPerson = new Schema(asList(name, document, age, points), metadataSchema);

        System.out.println(schemaPerson);
    }

    public void createVectorSchemaRoot() {
        Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
        FieldType intType = new FieldType(true, new ArrowType.Int(32, true), null);
        FieldType listType = new FieldType(true, new ArrowType.List(), null);
        Field childField = new Field("intCol", intType, null);
        List<Field> childFields = new ArrayList<>();
        childFields.add(childField);
        Field points = new Field("points", listType, childFields);
        Schema schema = new Schema(asList(name, age, points));
        try(
                BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)
        ){
            VarCharVector nameVector = (VarCharVector) root.getVector("name");
            nameVector.allocateNew(3);
            nameVector.set(0, "David".getBytes());
            nameVector.set(1, "Gladis".getBytes());
            nameVector.set(2, "Juan".getBytes());
            nameVector.setValueCount(3);
            IntVector ageVector = (IntVector) root.getVector("age");
            ageVector.allocateNew(3);
            ageVector.set(0, 10);
            ageVector.set(1, 20);
            ageVector.set(2, 30);
            ageVector.setValueCount(3);
            ListVector listVector = (ListVector) root.getVector("points");
            UnionListWriter listWriter = listVector.getWriter();
            int[] data = new int[] { 4, 8, 12, 10, 20, 30, 5, 10, 15 };
            int tmp_index = 0;
            for(int i = 0; i < 3; i++) {
                listWriter.setPosition(i);
                listWriter.startList();
                for(int j = 0; j < 3; j++) {
                    listWriter.writeInt(data[tmp_index]);
                    tmp_index = tmp_index + 1;
                }
                listWriter.setValueCount(2);
                listWriter.endList();
            }
            listVector.setValueCount(3);
            root.setRowCount(3);

            System.out.print(root.contentToTSVString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        SchemaExamples schemaExamples = new SchemaExamples();
        schemaExamples.createFields();
        schemaExamples.createSchema();
        schemaExamples.addMetadata();
        schemaExamples.addMetadataToSchema();
        schemaExamples.createVectorSchemaRoot();
    }

}
