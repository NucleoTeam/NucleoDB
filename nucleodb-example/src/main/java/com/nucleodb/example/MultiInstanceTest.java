package com.nucleodb.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleodb.grpc.NucleoDBGrpcServer;
import com.nucleodb.grpc.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Multi-instance consistency test.
 *
 * Starts TWO NucleoDB gRPC servers (on different ports) each with their own
 * database instance (using Local MQS). Tests that:
 * - CRUD operations work independently on each instance
 * - Data created on one instance can be managed on that instance
 * - Both instances can handle concurrent workloads
 *
 * For full cross-instance consistency (data created on instance 1 visible
 * on instance 2), use Kafka MQS mode with a running Kafka broker:
 *   MQS_MODE=kafka ./run-tests.sh
 */
public class MultiInstanceTest {
    private static final Logger logger = Logger.getLogger(MultiInstanceTest.class.getName());
    private static final ObjectMapper mapper = new ObjectMapper();

    private int passed = 0;
    private int failed = 0;

    private void check(String test, boolean condition) {
        if (condition) {
            passed++;
            logger.info("  PASS: " + test);
        } else {
            failed++;
            logger.severe("  FAIL: " + test);
        }
    }

    public static void main(String[] args) throws Exception {
        MultiInstanceTest test = new MultiInstanceTest();

        // Start two independent servers
        logger.info("=== Starting Instance 1 (port 9093, local MQS) ===");
        NucleoDBGrpcServer server1 = ExampleServer.startServer(9093, "local");

        logger.info("=== Starting Instance 2 (port 9094, local MQS) ===");
        NucleoDBGrpcServer server2 = ExampleServer.startServer(9094, "local");

        ManagedChannel ch1 = ManagedChannelBuilder.forTarget("localhost:9093")
            .usePlaintext().build();
        ManagedChannel ch2 = ManagedChannelBuilder.forTarget("localhost:9094")
            .usePlaintext().build();

        DataEntryServiceGrpc.DataEntryServiceBlockingStub data1 =
            DataEntryServiceGrpc.newBlockingStub(ch1);
        DataEntryServiceGrpc.DataEntryServiceBlockingStub data2 =
            DataEntryServiceGrpc.newBlockingStub(ch2);
        ConnectionServiceGrpc.ConnectionServiceBlockingStub conn1 =
            ConnectionServiceGrpc.newBlockingStub(ch1);
        ConnectionServiceGrpc.ConnectionServiceBlockingStub conn2 =
            ConnectionServiceGrpc.newBlockingStub(ch2);

        try {
            Thread.sleep(1000);

            // === Test 1: Independent CRUD on Instance 1 ===
            logger.info("");
            logger.info("=== Test: Instance 1 independent CRUD ===");

            String cust1Json = mapper.writeValueAsString(
                Map.of("name", "Instance1Customer", "email", "i1@example.com"));
            CreateDataEntryResponse cust1 = data1.create(
                CreateDataEntryRequest.newBuilder()
                    .setTable("customer").setDataJson(cust1Json).build());
            test.check("instance 1: create customer", cust1.getStatus().getSuccess());
            String cust1Key = cust1.getEntry().getKey();

            String prod1Json = mapper.writeValueAsString(
                Map.of("name", "I1Product", "category", "test", "price", 10.0));
            CreateDataEntryResponse prod1 = data1.create(
                CreateDataEntryRequest.newBuilder()
                    .setTable("product").setDataJson(prod1Json).build());
            test.check("instance 1: create product", prod1.getStatus().getSuccess());
            String prod1Key = prod1.getEntry().getKey();

            CreateConnectionResponse conn1Resp = conn1.create(
                CreateConnectionRequest.newBuilder()
                    .setConnectionType("PURCHASED")
                    .setFromKey(cust1Key).setToKey(prod1Key)
                    .putAllMetadata(Map.of("via", "instance1"))
                    .build());
            test.check("instance 1: create connection", conn1Resp.getStatus().getSuccess());
            String conn1Uuid = conn1Resp.getConnection().getUuid();

            Thread.sleep(300);

            // Verify on instance 1
            GetDataEntryResponse getCust1 = data1.get(
                GetDataEntryRequest.newBuilder()
                    .setTable("customer").setKey(cust1Key).build());
            test.check("instance 1: read customer", getCust1.getStatus().getSuccess());

            GetConnectionsByFromResponse fromConns1 = conn1.getByFrom(
                GetConnectionsByFromRequest.newBuilder()
                    .setConnectionType("PURCHASED").setFromKey(cust1Key).build());
            test.check("instance 1: read connections by from", fromConns1.getConnectionsCount() == 1);

            // === Test 2: Independent CRUD on Instance 2 ===
            logger.info("");
            logger.info("=== Test: Instance 2 independent CRUD ===");

            String cust2Json = mapper.writeValueAsString(
                Map.of("name", "Instance2Customer", "email", "i2@example.com"));
            CreateDataEntryResponse cust2 = data2.create(
                CreateDataEntryRequest.newBuilder()
                    .setTable("customer").setDataJson(cust2Json).build());
            test.check("instance 2: create customer", cust2.getStatus().getSuccess());
            String cust2Key = cust2.getEntry().getKey();

            String prod2Json = mapper.writeValueAsString(
                Map.of("name", "I2Product", "category", "test", "price", 20.0));
            CreateDataEntryResponse prod2 = data2.create(
                CreateDataEntryRequest.newBuilder()
                    .setTable("product").setDataJson(prod2Json).build());
            test.check("instance 2: create product", prod2.getStatus().getSuccess());
            String prod2Key = prod2.getEntry().getKey();

            CreateConnectionResponse conn2Resp = conn2.create(
                CreateConnectionRequest.newBuilder()
                    .setConnectionType("PURCHASED")
                    .setFromKey(cust2Key).setToKey(prod2Key)
                    .putAllMetadata(Map.of("via", "instance2"))
                    .build());
            test.check("instance 2: create connection", conn2Resp.getStatus().getSuccess());
            String conn2Uuid = conn2Resp.getConnection().getUuid();

            Thread.sleep(300);

            GetDataEntryResponse getCust2 = data2.get(
                GetDataEntryRequest.newBuilder()
                    .setTable("customer").setKey(cust2Key).build());
            test.check("instance 2: read customer", getCust2.getStatus().getSuccess());

            // === Test 3: Concurrent operations ===
            logger.info("");
            logger.info("=== Test: Concurrent CRUD across instances ===");

            // Create 5 customers on instance 1, 5 on instance 2 concurrently
            List<String> keys1 = new ArrayList<>();
            List<String> keys2 = new ArrayList<>();

            for (int i = 0; i < 5; i++) {
                String json1 = mapper.writeValueAsString(
                    Map.of("name", "Batch1_" + i, "email", "b1_" + i + "@example.com"));
                CreateDataEntryResponse r1 = data1.create(
                    CreateDataEntryRequest.newBuilder()
                        .setTable("customer").setDataJson(json1).build());
                keys1.add(r1.getEntry().getKey());

                String json2 = mapper.writeValueAsString(
                    Map.of("name", "Batch2_" + i, "email", "b2_" + i + "@example.com"));
                CreateDataEntryResponse r2 = data2.create(
                    CreateDataEntryRequest.newBuilder()
                        .setTable("customer").setDataJson(json2).build());
                keys2.add(r2.getEntry().getKey());
            }

            Thread.sleep(500);

            // Verify counts on each instance
            GetAllDataEntriesResponse all1 = data1.getAll(
                GetAllDataEntriesRequest.newBuilder()
                    .setTable("customer")
                    .setPagination(Pagination.newBuilder().setSkip(0).setLimit(100).build())
                    .build());
            test.check("instance 1: has 6 customers (1+5 batch)",
                all1.getTotalCount() == 6);

            GetAllDataEntriesResponse all2 = data2.getAll(
                GetAllDataEntriesRequest.newBuilder()
                    .setTable("customer")
                    .setPagination(Pagination.newBuilder().setSkip(0).setLimit(100).build())
                    .build());
            test.check("instance 2: has 6 customers (1+5 batch)",
                all2.getTotalCount() == 6);

            // === Test 4: Update on each instance ===
            logger.info("");
            logger.info("=== Test: Updates on each instance ===");

            String upd1 = mapper.writeValueAsString(
                Map.of("name", "Instance1Customer", "email", "updated@i1.com"));
            UpdateDataEntryResponse updResp1 = data1.update(
                UpdateDataEntryRequest.newBuilder()
                    .setTable("customer").setKey(cust1Key)
                    .setDataJson(upd1).build());
            test.check("instance 1: update customer", updResp1.getStatus().getSuccess());

            String upd2 = mapper.writeValueAsString(
                Map.of("name", "Instance2Customer", "email", "updated@i2.com"));
            UpdateDataEntryResponse updResp2 = data2.update(
                UpdateDataEntryRequest.newBuilder()
                    .setTable("customer").setKey(cust2Key)
                    .setDataJson(upd2).build());
            test.check("instance 2: update customer", updResp2.getStatus().getSuccess());

            // Verify updates
            GetDataEntryResponse verify1 = data1.get(
                GetDataEntryRequest.newBuilder()
                    .setTable("customer").setKey(cust1Key).build());
            JsonNode d1 = mapper.readTree(verify1.getEntry().getDataJson());
            test.check("instance 1: email updated",
                "updated@i1.com".equals(d1.get("email").asText()));

            GetDataEntryResponse verify2 = data2.get(
                GetDataEntryRequest.newBuilder()
                    .setTable("customer").setKey(cust2Key).build());
            JsonNode d2 = mapper.readTree(verify2.getEntry().getDataJson());
            test.check("instance 2: email updated",
                "updated@i2.com".equals(d2.get("email").asText()));

            // === Test 5: Delete on each instance ===
            logger.info("");
            logger.info("=== Test: Deletes on each instance ===");

            // Delete connections first
            DeleteConnectionResponse delC1 = conn1.delete(
                DeleteConnectionRequest.newBuilder()
                    .setConnectionType("PURCHASED").setUuid(conn1Uuid).build());
            test.check("instance 1: delete connection", delC1.getStatus().getSuccess());

            DeleteConnectionResponse delC2 = conn2.delete(
                DeleteConnectionRequest.newBuilder()
                    .setConnectionType("PURCHASED").setUuid(conn2Uuid).build());
            test.check("instance 2: delete connection", delC2.getStatus().getSuccess());

            // Delete batch entries
            for (String key : keys1) {
                data1.delete(DeleteDataEntryRequest.newBuilder()
                    .setTable("customer").setKey(key).build());
            }
            for (String key : keys2) {
                data2.delete(DeleteDataEntryRequest.newBuilder()
                    .setTable("customer").setKey(key).build());
            }

            // Delete initial entries
            data1.delete(DeleteDataEntryRequest.newBuilder()
                .setTable("product").setKey(prod1Key).build());
            data1.delete(DeleteDataEntryRequest.newBuilder()
                .setTable("customer").setKey(cust1Key).build());
            data2.delete(DeleteDataEntryRequest.newBuilder()
                .setTable("product").setKey(prod2Key).build());
            data2.delete(DeleteDataEntryRequest.newBuilder()
                .setTable("customer").setKey(cust2Key).build());

            Thread.sleep(300);

            // Verify all cleaned up
            GetAllDataEntriesResponse final1 = data1.getAll(
                GetAllDataEntriesRequest.newBuilder()
                    .setTable("customer")
                    .setPagination(Pagination.newBuilder().setSkip(0).setLimit(100).build())
                    .build());
            test.check("instance 1: 0 customers after cleanup", final1.getTotalCount() == 0);

            GetAllDataEntriesResponse final2 = data2.getAll(
                GetAllDataEntriesRequest.newBuilder()
                    .setTable("customer")
                    .setPagination(Pagination.newBuilder().setSkip(0).setLimit(100).build())
                    .build());
            test.check("instance 2: 0 customers after cleanup", final2.getTotalCount() == 0);

        } finally {
            ch1.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            ch2.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            server1.stop();
            server2.stop();
        }

        logger.info("");
        logger.info("==========================================");
        logger.info("  MULTI-INSTANCE RESULTS: " + test.passed + " passed, " + test.failed + " failed");
        logger.info("==========================================");
        System.exit(test.failed > 0 ? 1 : 0);
    }
}
