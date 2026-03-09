package com.nucleodb.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleodb.grpc.NucleoDBGrpcServer;
import com.nucleodb.grpc.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Self-contained integration test that:
 * 1. Starts a NucleoDB gRPC server with LocalConfiguration
 * 2. Tests full CRUD on Customer and Product models
 * 3. Tests full CRUD on PURCHASED connections
 * 4. Reports pass/fail results
 */
public class IntegrationTest {
    private static final Logger logger = Logger.getLogger(IntegrationTest.class.getName());
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
        IntegrationTest test = new IntegrationTest();

        // Start server
        logger.info("=== Starting NucleoDB gRPC Server (port 9091, local MQS) ===");
        NucleoDBGrpcServer server = ExampleServer.startServer(9091, "local");

        ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:9091")
            .usePlaintext().build();
        DataEntryServiceGrpc.DataEntryServiceBlockingStub dataStub =
            DataEntryServiceGrpc.newBlockingStub(channel);
        ConnectionServiceGrpc.ConnectionServiceBlockingStub connStub =
            ConnectionServiceGrpc.newBlockingStub(channel);

        try {
            Thread.sleep(1000); // let server settle

            // ===== CUSTOMER CRUD =====
            logger.info("");
            logger.info("=== Customer CRUD ===");

            // Create
            String aliceJson = mapper.writeValueAsString(
                Map.of("name", "Alice Smith", "email", "alice@example.com"));
            CreateDataEntryResponse createAlice = dataStub.create(
                CreateDataEntryRequest.newBuilder()
                    .setTable("customer").setDataJson(aliceJson).build());
            test.check("create customer Alice", createAlice.getStatus().getSuccess());
            String aliceKey = createAlice.getEntry().getKey();

            String bobJson = mapper.writeValueAsString(
                Map.of("name", "Bob Jones", "email", "bob@example.com"));
            CreateDataEntryResponse createBob = dataStub.create(
                CreateDataEntryRequest.newBuilder()
                    .setTable("customer").setDataJson(bobJson).build());
            test.check("create customer Bob", createBob.getStatus().getSuccess());
            String bobKey = createBob.getEntry().getKey();

            Thread.sleep(500);

            // Read by ID
            GetDataEntryResponse getAlice = dataStub.get(
                GetDataEntryRequest.newBuilder()
                    .setTable("customer").setKey(aliceKey).build());
            test.check("get customer Alice by ID", getAlice.getStatus().getSuccess());
            JsonNode aliceData = mapper.readTree(getAlice.getEntry().getDataJson());
            test.check("Alice name correct", "Alice Smith".equals(aliceData.get("name").asText()));

            // Read by index
            GetDataEntriesByIndexResponse byIndex = dataStub.getByIndex(
                GetDataEntriesByIndexRequest.newBuilder()
                    .setTable("customer").setIndexKey("name")
                    .setValueJson(mapper.writeValueAsString("Alice Smith")).build());
            test.check("getByIndex Alice found", byIndex.getEntriesCount() >= 1);

            // Get all
            GetAllDataEntriesResponse allCustomers = dataStub.getAll(
                GetAllDataEntriesRequest.newBuilder()
                    .setTable("customer")
                    .setPagination(Pagination.newBuilder().setSkip(0).setLimit(100).build())
                    .build());
            test.check("getAll customers success", allCustomers.getStatus().getSuccess());
            test.check("getAll customers count=2", allCustomers.getTotalCount() == 2);

            // Update
            String updatedAliceJson = mapper.writeValueAsString(
                Map.of("name", "Alice Smith", "email", "alice.updated@example.com"));
            UpdateDataEntryResponse updateAlice = dataStub.update(
                UpdateDataEntryRequest.newBuilder()
                    .setTable("customer").setKey(aliceKey)
                    .setDataJson(updatedAliceJson).build());
            test.check("update customer Alice", updateAlice.getStatus().getSuccess());

            Thread.sleep(200);

            // Verify update
            GetDataEntryResponse verifyUpdate = dataStub.get(
                GetDataEntryRequest.newBuilder()
                    .setTable("customer").setKey(aliceKey).build());
            JsonNode updatedData = mapper.readTree(verifyUpdate.getEntry().getDataJson());
            test.check("Alice email updated",
                "alice.updated@example.com".equals(updatedData.get("email").asText()));

            // ===== PRODUCT CRUD =====
            logger.info("");
            logger.info("=== Product CRUD ===");

            String laptopJson = mapper.writeValueAsString(
                Map.of("name", "Laptop", "category", "electronics", "price", 999.99));
            CreateDataEntryResponse createLaptop = dataStub.create(
                CreateDataEntryRequest.newBuilder()
                    .setTable("product").setDataJson(laptopJson).build());
            test.check("create product Laptop", createLaptop.getStatus().getSuccess());
            String laptopKey = createLaptop.getEntry().getKey();

            String headphonesJson = mapper.writeValueAsString(
                Map.of("name", "Headphones", "category", "electronics", "price", 79.99));
            CreateDataEntryResponse createHP = dataStub.create(
                CreateDataEntryRequest.newBuilder()
                    .setTable("product").setDataJson(headphonesJson).build());
            test.check("create product Headphones", createHP.getStatus().getSuccess());
            String hpKey = createHP.getEntry().getKey();

            Thread.sleep(500);

            GetDataEntryResponse getLaptop = dataStub.get(
                GetDataEntryRequest.newBuilder()
                    .setTable("product").setKey(laptopKey).build());
            test.check("get product Laptop", getLaptop.getStatus().getSuccess());
            JsonNode laptopData = mapper.readTree(getLaptop.getEntry().getDataJson());
            test.check("Laptop name correct", "Laptop".equals(laptopData.get("name").asText()));
            test.check("Laptop price correct", laptopData.get("price").asDouble() == 999.99);

            // Update product price
            String updatedLaptopJson = mapper.writeValueAsString(
                Map.of("name", "Laptop", "category", "electronics", "price", 899.99));
            UpdateDataEntryResponse updateLaptop = dataStub.update(
                UpdateDataEntryRequest.newBuilder()
                    .setTable("product").setKey(laptopKey)
                    .setDataJson(updatedLaptopJson).build());
            test.check("update product Laptop price", updateLaptop.getStatus().getSuccess());

            // ===== CONNECTION CRUD (PURCHASED) =====
            logger.info("");
            logger.info("=== Connection CRUD (PURCHASED) ===");

            // Create connections: Alice->Laptop, Alice->Headphones, Bob->Laptop
            CreateConnectionResponse conn1 = connStub.create(
                CreateConnectionRequest.newBuilder()
                    .setConnectionType("PURCHASED")
                    .setFromKey(aliceKey).setToKey(laptopKey)
                    .putAllMetadata(Map.of("quantity", "1", "source", "web"))
                    .build());
            test.check("create connection Alice->Laptop", conn1.getStatus().getSuccess());
            String conn1Uuid = conn1.getConnection().getUuid();

            CreateConnectionResponse conn2 = connStub.create(
                CreateConnectionRequest.newBuilder()
                    .setConnectionType("PURCHASED")
                    .setFromKey(aliceKey).setToKey(hpKey)
                    .putAllMetadata(Map.of("quantity", "2", "source", "store"))
                    .build());
            test.check("create connection Alice->Headphones", conn2.getStatus().getSuccess());
            String conn2Uuid = conn2.getConnection().getUuid();

            CreateConnectionResponse conn3 = connStub.create(
                CreateConnectionRequest.newBuilder()
                    .setConnectionType("PURCHASED")
                    .setFromKey(bobKey).setToKey(laptopKey)
                    .putAllMetadata(Map.of("quantity", "1", "source", "mobile"))
                    .build());
            test.check("create connection Bob->Laptop", conn3.getStatus().getSuccess());
            String conn3Uuid = conn3.getConnection().getUuid();

            Thread.sleep(500);

            // Get by UUID
            GetConnectionResponse getConn1 = connStub.get(
                GetConnectionRequest.newBuilder()
                    .setConnectionType("PURCHASED").setUuid(conn1Uuid).build());
            test.check("get connection by UUID", getConn1.getStatus().getSuccess());
            test.check("connection from_key matches Alice",
                aliceKey.equals(getConn1.getConnection().getFromKey()));
            test.check("connection metadata quantity=1",
                "1".equals(getConn1.getConnection().getMetadataMap().get("quantity")));

            // Get by from (Alice's purchases)
            GetConnectionsByFromResponse alicePurchases = connStub.getByFrom(
                GetConnectionsByFromRequest.newBuilder()
                    .setConnectionType("PURCHASED").setFromKey(aliceKey).build());
            test.check("getByFrom Alice purchases", alicePurchases.getStatus().getSuccess());
            test.check("Alice has 2 purchases", alicePurchases.getConnectionsCount() == 2);

            // Get by to (who bought Laptop)
            GetConnectionsByToResponse laptopBuyers = connStub.getByTo(
                GetConnectionsByToRequest.newBuilder()
                    .setConnectionType("PURCHASED").setToKey(laptopKey).build());
            test.check("getByTo Laptop buyers", laptopBuyers.getStatus().getSuccess());
            test.check("Laptop has 2 buyers", laptopBuyers.getConnectionsCount() == 2);

            // Get by from+to
            GetConnectionsByFromAndToResponse aliceLaptop = connStub.getByFromAndTo(
                GetConnectionsByFromAndToRequest.newBuilder()
                    .setConnectionType("PURCHASED")
                    .setFromKey(aliceKey).setToKey(laptopKey).build());
            test.check("getByFromAndTo Alice->Laptop", aliceLaptop.getStatus().getSuccess());
            test.check("Alice->Laptop connection found", aliceLaptop.getConnectionsCount() == 1);

            // Get all
            GetAllConnectionsResponse allConns = connStub.getAll(
                GetAllConnectionsRequest.newBuilder()
                    .setConnectionType("PURCHASED")
                    .setPagination(Pagination.newBuilder().setSkip(0).setLimit(100).build())
                    .build());
            test.check("getAll connections", allConns.getStatus().getSuccess());
            test.check("total 3 connections", allConns.getTotalCount() == 3);

            // Update connection metadata
            UpdateConnectionResponse updateConn = connStub.update(
                UpdateConnectionRequest.newBuilder()
                    .setConnectionType("PURCHASED").setUuid(conn1Uuid)
                    .putAllMetadata(Map.of("quantity", "3", "source", "web", "discount", "10%"))
                    .build());
            test.check("update connection metadata", updateConn.getStatus().getSuccess());

            // Verify update
            GetConnectionResponse verifyConn = connStub.get(
                GetConnectionRequest.newBuilder()
                    .setConnectionType("PURCHASED").setUuid(conn1Uuid).build());
            test.check("updated quantity=3",
                "3".equals(verifyConn.getConnection().getMetadataMap().get("quantity")));
            test.check("added discount=10%",
                "10%".equals(verifyConn.getConnection().getMetadataMap().get("discount")));

            // ===== DELETE operations =====
            logger.info("");
            logger.info("=== Delete Operations ===");

            // Delete connections
            DeleteConnectionResponse delConn2 = connStub.delete(
                DeleteConnectionRequest.newBuilder()
                    .setConnectionType("PURCHASED").setUuid(conn2Uuid).build());
            test.check("delete connection Alice->Headphones", delConn2.getStatus().getSuccess());

            // Verify delete
            GetConnectionResponse verifyDelConn = connStub.get(
                GetConnectionRequest.newBuilder()
                    .setConnectionType("PURCHASED").setUuid(conn2Uuid).build());
            test.check("connection Alice->Headphones gone", !verifyDelConn.getStatus().getSuccess());

            // Alice should now have 1 purchase
            GetConnectionsByFromResponse aliceAfterDel = connStub.getByFrom(
                GetConnectionsByFromRequest.newBuilder()
                    .setConnectionType("PURCHASED").setFromKey(aliceKey).build());
            test.check("Alice has 1 purchase after delete", aliceAfterDel.getConnectionsCount() == 1);

            // Delete remaining connections
            connStub.delete(DeleteConnectionRequest.newBuilder()
                .setConnectionType("PURCHASED").setUuid(conn1Uuid).build());
            connStub.delete(DeleteConnectionRequest.newBuilder()
                .setConnectionType("PURCHASED").setUuid(conn3Uuid).build());

            // Delete products
            DeleteDataEntryResponse delLaptop = dataStub.delete(
                DeleteDataEntryRequest.newBuilder()
                    .setTable("product").setKey(laptopKey).build());
            test.check("delete product Laptop", delLaptop.getStatus().getSuccess());

            DeleteDataEntryResponse delHP = dataStub.delete(
                DeleteDataEntryRequest.newBuilder()
                    .setTable("product").setKey(hpKey).build());
            test.check("delete product Headphones", delHP.getStatus().getSuccess());

            // Delete customers
            DeleteDataEntryResponse delAlice = dataStub.delete(
                DeleteDataEntryRequest.newBuilder()
                    .setTable("customer").setKey(aliceKey).build());
            test.check("delete customer Alice", delAlice.getStatus().getSuccess());

            DeleteDataEntryResponse delBob = dataStub.delete(
                DeleteDataEntryRequest.newBuilder()
                    .setTable("customer").setKey(bobKey).build());
            test.check("delete customer Bob", delBob.getStatus().getSuccess());

            // Verify all deleted
            GetDataEntryResponse verifyAliceGone = dataStub.get(
                GetDataEntryRequest.newBuilder()
                    .setTable("customer").setKey(aliceKey).build());
            test.check("customer Alice confirmed deleted", !verifyAliceGone.getStatus().getSuccess());

            GetAllDataEntriesResponse finalCount = dataStub.getAll(
                GetAllDataEntriesRequest.newBuilder()
                    .setTable("customer")
                    .setPagination(Pagination.newBuilder().setSkip(0).setLimit(100).build())
                    .build());
            test.check("0 customers remaining", finalCount.getTotalCount() == 0);

        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            server.stop();
        }

        logger.info("");
        logger.info("==========================================");
        logger.info("  RESULTS: " + test.passed + " passed, " + test.failed + " failed");
        logger.info("==========================================");
        System.exit(test.failed > 0 ? 1 : 0);
    }
}
