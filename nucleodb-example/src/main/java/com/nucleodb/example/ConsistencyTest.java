package com.nucleodb.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleodb.grpc.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Tests data consistency across multiple NucleoDB instances.
 *
 * Writes data through one instance, then reads from another instance
 * to verify that all changes propagate via Kafka.
 *
 * Usage: java ... com.nucleodb.example.ConsistencyTest host1:port1 host2:port2
 */
public class ConsistencyTest {
    private static final Logger logger = Logger.getLogger(ConsistencyTest.class.getName());
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

    private static DataEntryServiceGrpc.DataEntryServiceBlockingStub dataStub(ManagedChannel ch) {
        return DataEntryServiceGrpc.newBlockingStub(ch);
    }

    private static ConnectionServiceGrpc.ConnectionServiceBlockingStub connStub(ManagedChannel ch) {
        return ConnectionServiceGrpc.newBlockingStub(ch);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ConsistencyTest <host1:port1> <host2:port2>");
            System.exit(1);
        }

        ConsistencyTest test = new ConsistencyTest();
        ManagedChannel ch1 = ManagedChannelBuilder.forTarget(args[0]).usePlaintext().build();
        ManagedChannel ch2 = ManagedChannelBuilder.forTarget(args[1]).usePlaintext().build();

        try {
            test.run(ch1, ch2);
        } finally {
            ch1.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            ch2.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }

        test.printSummary();
        System.exit(test.failed > 0 ? 1 : 0);
    }

    private void run(ManagedChannel ch1, ManagedChannel ch2) throws Exception {
        logger.info("=== Multi-Instance Consistency Test ===");

        // 1. Create customer on instance 1
        logger.info("[Step 1] Create customer on instance 1");
        String custJson = mapper.writeValueAsString(
            Map.of("name", "ConsistencyUser", "email", "consist@example.com"));
        CreateDataEntryResponse createCust = dataStub(ch1).create(
            CreateDataEntryRequest.newBuilder()
                .setTable("customer")
                .setDataJson(custJson)
                .build());
        check("create customer on instance 1", createCust.getStatus().getSuccess());
        String custKey = createCust.getEntry().getKey();
        logger.info("  Customer key: " + custKey);

        // 2. Create product on instance 1
        logger.info("[Step 2] Create product on instance 1");
        String prodJson = mapper.writeValueAsString(
            Map.of("name", "ConsistencyProduct", "category", "test", "price", 42.0));
        CreateDataEntryResponse createProd = dataStub(ch1).create(
            CreateDataEntryRequest.newBuilder()
                .setTable("product")
                .setDataJson(prodJson)
                .build());
        check("create product on instance 1", createProd.getStatus().getSuccess());
        String prodKey = createProd.getEntry().getKey();

        // 3. Create connection on instance 1
        logger.info("[Step 3] Create connection on instance 1");
        CreateConnectionResponse createConn = connStub(ch1).create(
            CreateConnectionRequest.newBuilder()
                .setConnectionType("PURCHASED")
                .setFromKey(custKey)
                .setToKey(prodKey)
                .putAllMetadata(Map.of("channel", "test"))
                .build());
        check("create connection on instance 1", createConn.getStatus().getSuccess());
        String connUuid = createConn.getConnection().getUuid();

        // 4. Wait for propagation through Kafka
        logger.info("[Step 4] Waiting for Kafka propagation...");
        Thread.sleep(3000);

        // 5. Read customer from instance 2
        logger.info("[Step 5] Verify customer on instance 2");
        GetDataEntryResponse getCust2 = dataStub(ch2).get(
            GetDataEntryRequest.newBuilder()
                .setTable("customer")
                .setKey(custKey)
                .build());
        check("customer exists on instance 2", getCust2.getStatus().getSuccess());
        if (getCust2.getStatus().getSuccess()) {
            JsonNode custData = mapper.readTree(getCust2.getEntry().getDataJson());
            check("customer name matches on instance 2",
                "ConsistencyUser".equals(custData.get("name").asText()));
        }

        // 6. Read product from instance 2
        logger.info("[Step 6] Verify product on instance 2");
        GetDataEntryResponse getProd2 = dataStub(ch2).get(
            GetDataEntryRequest.newBuilder()
                .setTable("product")
                .setKey(prodKey)
                .build());
        check("product exists on instance 2", getProd2.getStatus().getSuccess());

        // 7. Read connection from instance 2
        logger.info("[Step 7] Verify connection on instance 2");
        GetConnectionResponse getConn2 = connStub(ch2).get(
            GetConnectionRequest.newBuilder()
                .setConnectionType("PURCHASED")
                .setUuid(connUuid)
                .build());
        check("connection exists on instance 2", getConn2.getStatus().getSuccess());
        if (getConn2.getStatus().getSuccess()) {
            check("connection metadata matches on instance 2",
                "test".equals(getConn2.getConnection().getMetadataMap().get("channel")));
        }

        // 8. Update customer on instance 2, verify on instance 1
        logger.info("[Step 8] Update customer on instance 2");
        String updatedJson = mapper.writeValueAsString(
            Map.of("name", "ConsistencyUser", "email", "updated@example.com"));
        UpdateDataEntryResponse updateResp = dataStub(ch2).update(
            UpdateDataEntryRequest.newBuilder()
                .setTable("customer")
                .setKey(custKey)
                .setDataJson(updatedJson)
                .build());
        check("update customer on instance 2", updateResp.getStatus().getSuccess());

        Thread.sleep(3000); // propagation

        logger.info("[Step 9] Verify update on instance 1");
        GetDataEntryResponse getCust1 = dataStub(ch1).get(
            GetDataEntryRequest.newBuilder()
                .setTable("customer")
                .setKey(custKey)
                .build());
        check("updated customer exists on instance 1", getCust1.getStatus().getSuccess());
        if (getCust1.getStatus().getSuccess()) {
            JsonNode data = mapper.readTree(getCust1.getEntry().getDataJson());
            check("updated email propagated to instance 1",
                "updated@example.com".equals(data.get("email").asText()));
        }

        // 10. Delete on instance 2, verify gone on instance 1
        logger.info("[Step 10] Delete connection on instance 2");
        DeleteConnectionResponse delConn = connStub(ch2).delete(
            DeleteConnectionRequest.newBuilder()
                .setConnectionType("PURCHASED")
                .setUuid(connUuid)
                .build());
        check("delete connection on instance 2", delConn.getStatus().getSuccess());

        Thread.sleep(3000);

        logger.info("[Step 11] Verify connection deleted on instance 1");
        GetConnectionResponse verifyGone = connStub(ch1).get(
            GetConnectionRequest.newBuilder()
                .setConnectionType("PURCHASED")
                .setUuid(connUuid)
                .build());
        check("connection deleted on instance 1", !verifyGone.getStatus().getSuccess());

        // Cleanup
        logger.info("[Cleanup] Deleting remaining entries");
        dataStub(ch1).delete(DeleteDataEntryRequest.newBuilder()
            .setTable("product").setKey(prodKey).build());
        dataStub(ch1).delete(DeleteDataEntryRequest.newBuilder()
            .setTable("customer").setKey(custKey).build());
    }

    private void printSummary() {
        logger.info("==========================================");
        logger.info("  CONSISTENCY TEST RESULTS: " + passed + " passed, " + failed + " failed");
        logger.info("==========================================");
    }
}
