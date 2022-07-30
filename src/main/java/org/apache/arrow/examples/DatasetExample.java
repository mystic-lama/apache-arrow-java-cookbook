package org.apache.arrow.examples;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.StreamSupport;

public class DatasetExample {

    public void createDSAutoInferredSchema() {
        String uri = "file:" + System.getProperty("user.dir") + "/src/main/resources/parquetfiles/data1.parquet";
        ScanOptions options = new ScanOptions(/*batchSize*/ 100);
        try (
                BufferAllocator allocator = new RootAllocator();
                DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
                Dataset dataset = datasetFactory.finish();
                Scanner scanner = dataset.newScan(options)
        ) {
            System.out.println(StreamSupport.stream(scanner.scan().spliterator(), false).count());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createDSWithSpecifiedSchema() {
        String uri = "file:" + System.getProperty("user.dir") + "/src/main/resources/parquetfiles/data1.parquet";
        ScanOptions options = new ScanOptions(/*batchSize*/ 100);
        try (
                BufferAllocator allocator = new RootAllocator();
                DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
                Dataset dataset = datasetFactory.finish(datasetFactory.inspect());
                Scanner scanner = dataset.newScan(options)
        ) {
            System.out.println(StreamSupport.stream(scanner.scan().spliterator(), false).count());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getSchema() {
        String uri = "file:" + System.getProperty("user.dir") + "/src/main/resources/parquetfiles/data1.parquet";
        try (
                BufferAllocator allocator = new RootAllocator();
                DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri)
        ) {
            Schema schema = datasetFactory.inspect();

            System.out.println(schema);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getSchemaFromDS() {
        String uri = "file:" + System.getProperty("user.dir") + "/src/main/resources/parquetfiles/data1.parquet";
        ScanOptions options = new ScanOptions(/*batchSize*/ 1);
        try (
                BufferAllocator allocator = new RootAllocator();
                DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
                Dataset dataset = datasetFactory.finish();
                Scanner scanner = dataset.newScan(options)
        ) {
            Schema schema = scanner.schema();

            System.out.println(schema);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void queryParquetFile() {
        String uri = "file:" + System.getProperty("user.dir") + "/src/main/resources/parquetfiles/data1.parquet";
        ScanOptions options = new ScanOptions(/*batchSize*/ 100);
        try (
                BufferAllocator allocator = new RootAllocator();
                DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
                Dataset dataset = datasetFactory.finish();
                Scanner scanner = dataset.newScan(options)
        ) {
            scanner.scan().forEach(scanTask -> {
                try (ArrowReader reader = scanTask.execute()) {
                    while (reader.loadNextBatch()) {
                        try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
                            System.out.print(root.contentToTSVString());
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void queryDataContentForDirectory() {
        String uri = "file:" + System.getProperty("user.dir") + "/src/main/resources/parquetfiles";
        ScanOptions options = new ScanOptions(/*batchSize*/ 100);
        try (BufferAllocator allocator = new RootAllocator();
             DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
             Dataset dataset = datasetFactory.finish();
             Scanner scanner = dataset.newScan(options)
        ) {

            scanner.scan().forEach(scanTask-> {
                final int[] count = {1};
                try (ArrowReader reader = scanTask.execute()) {
                    while (reader.loadNextBatch()) {
                        try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
                            System.out.println("Batch: " + count[0]++ + ", RowCount: " + root.getRowCount());
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void queryDataContentWithProjection() {
        String uri = "file:" + System.getProperty("user.dir") + "/src/main/resources/parquetfiles/data1.parquet";
        String[] projection = new String[] {"name"};
        ScanOptions options = new ScanOptions(/*batchSize*/ 100, Optional.of(projection));
        try (
                BufferAllocator allocator = new RootAllocator();
                DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
                Dataset dataset = datasetFactory.finish();
                Scanner scanner = dataset.newScan(options)
        ) {
            scanner.scan().forEach(scanTask-> {
                try (ArrowReader reader = scanTask.execute()) {
                    while (reader.loadNextBatch()) {
                        try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
                            System.out.print(root.contentToTSVString());
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        DatasetExample datasetExample = new DatasetExample();
        datasetExample.createDSAutoInferredSchema();
        datasetExample.createDSWithSpecifiedSchema();
        datasetExample.getSchema();
        datasetExample.getSchemaFromDS();
        datasetExample.queryParquetFile();
        datasetExample.queryDataContentForDirectory();
        datasetExample.queryDataContentWithProjection();
    }
}
