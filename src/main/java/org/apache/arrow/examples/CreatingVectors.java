package org.apache.arrow.examples;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;

public class CreatingVectors {

    public void createVectors() {
        try(
                BufferAllocator allocator = new RootAllocator();
                IntVector intVector = new IntVector("intVector", allocator)
        ) {
            intVector.allocateNew(3);
            intVector.set(0, 1);
            intVector.set(1, 2);
            intVector.set(2, 3);
            intVector.setValueCount(3);

            System.out.print(intVector);
        }
    }

    public static void main(String[] args) {
        CreatingVectors creatingVectors = new CreatingVectors();
        creatingVectors.createVectors();
    }

}
