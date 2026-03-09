package com.nucleodb.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleodb.grpc.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Tests CRUD operations for models and connections against a running
 * NucleoDB gRPC server backed by Kafka.
 *
 * Usage: java ... com.nucleodb.example.CrudTest [host:port]
 */
public class CrudTest {
    private static final Logger logger = Logger.getLogger(CrudTest.class.getName());
    private static final ObjectMapper mapper = new ObjectMapper();

    private final DataEntryServiceGrpc.DataEntryServiceBlockingStub dataStub;
    private final ConnectionServiceGrpc.ConnectionServiceBlockingStub connStub;
    private final ManagedChannel channel;

    private int passed = 0;
    private int failed = 0;

    public CrudTest(String target) {
        channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        dataStub = DataEntryServiceGrpc.newBlockingStub(channel);
        connStub = ConnectionServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    // -------- assertion helpers --------
    private void check(String test, boolean condition) {
        if (condition) {
            passed++;
            logger.info("  PASS: " + test);
        } else {
            failed++;
            logger.severe("  FAIL: " + test);
        }
    }

    // -------- Customer CRUD --------
    public String testCreateCustomer(String name, String email) throws Exception {
        logger.info("[Customer] Create");
        String json = mapper.writeValueAsString(Map.of("name", name, "email", email));
        CreateDataEntryResponse resp = dataStub.create(
            CreateDataEntryRequest.newBuilder()
                .setTable("customer")
                .setDataJson(json)
                .build());
        check("create customer success", resp.getStatus().getSuccess());
        check("create customer has key", !resp.getEntry().getKey().isEmpty());
        String key = resp.getEntry().getKey();
        logger.info("  Created customer key=" + key);
        return key;
    }

    public void testGetCustomer(String key, String expectedName) throws Exception {
        logger.info("[Customer] Get key=" + key);
        GetDataEntryResponse resp = dataStub.get(
            GetDataEntryRequest.newBuilder()
                .setTable("customer")
                .setKey(key)
                .build());
        check("get customer success", resp.getStatus().getSuccess());
        JsonNode data = mapper.readTree(resp.getEntry().getDataJson());
        check("get customer name matches", expectedName.equals(data.get("name").asText()));
    }

    public void testGetCustomerByIndex(String name) throws Exception {
        logger.info("[Customer] GetByIndex name=" + name);
        GetDataEntriesByIndexResponse resp = dataStub.getByIndex(
            GetDataEntriesByIndexRequest.newBuilder()
                .setTable("customer")
                .setIndexKey("name")
                .setValueJson(mapper.writeValueAsString(name))
                .build());
        check("getByIndex customer success", resp.getStatus().getSuccess());
        check("getByIndex customer found at least 1", resp.getEntriesCount() >= 1);
    }

    public void testUpdateCustomer(String key, String newEmail) throws Exception {
        logger.info("[Customer] Update key=" + key);
        // First fetch current data
        GetDataEntryResponse getResp = dataStub.get(
            GetDataEntryRequest.newBuilder()
                .setTable("customer")
                .setKey(key)
                .build());
        JsonNode current = mapper.readTree(getResp.getEntry().getDataJson());
        String name = current.get("name").asText();

        String json = mapper.writeValueAsString(Map.of("name", name, "email", newEmail));
        UpdateDataEntryResponse resp = dataStub.update(
            UpdateDataEntryRequest.newBuilder()
                .setTable("customer")
                .setKey(key)
                .setDataJson(json)
                .build());
        check("update customer success", resp.getStatus().getSuccess());

        // Verify update
        GetDataEntryResponse verifyResp = dataStub.get(
            GetDataEntryRequest.newBuilder()
                .setTable("customer")
                .setKey(key)
                .build());
        JsonNode updated = mapper.readTree(verifyResp.getEntry().getDataJson());
        check("update customer email changed", newEmail.equals(updated.get("email").asText()));
    }

    public void testDeleteCustomer(String key) throws Exception {
        logger.info("[Customer] Delete key=" + key);
        DeleteDataEntryResponse resp = dataStub.delete(
            DeleteDataEntryRequest.newBuilder()
                .setTable("customer")
                .setKey(key)
                .build());
        check("delete customer success", resp.getStatus().getSuccess());

        // Verify deletion
        GetDataEntryResponse verifyResp = dataStub.get(
            GetDataEntryRequest.newBuilder()
                .setTable("customer")
                .setKey(key)
                .build());
        check("delete customer confirmed gone", !verifyResp.getStatus().getSuccess());
    }

    // -------- Product CRUD --------
    public String testCreateProduct(String name, String category, double price) throws Exception {
        logger.info("[Product] Create");
        String json = mapper.writeValueAsString(Map.of(
            "name", name, "category", category, "price", price));
        CreateDataEntryResponse resp = dataStub.create(
            CreateDataEntryRequest.newBuilder()
                .setTable("product")
                .setDataJson(json)
                .build());
        check("create product success", resp.getStatus().getSuccess());
        check("create product has key", !resp.getEntry().getKey().isEmpty());
        String key = resp.getEntry().getKey();
        logger.info("  Created product key=" + key);
        return key;
    }

    public void testGetProduct(String key, String expectedName) throws Exception {
        logger.info("[Product] Get key=" + key);
        GetDataEntryResponse resp = dataStub.get(
            GetDataEntryRequest.newBuilder()
                .setTable("product")
                .setKey(key)
                .build());
        check("get product success", resp.getStatus().getSuccess());
        JsonNode data = mapper.readTree(resp.getEntry().getDataJson());
        check("get product name matches", expectedName.equals(data.get("name").asText()));
    }

    public void testUpdateProduct(String key, double newPrice) throws Exception {
        logger.info("[Product] Update key=" + key);
        GetDataEntryResponse getResp = dataStub.get(
            GetDataEntryRequest.newBuilder()
                .setTable("product")
                .setKey(key)
                .build());
        JsonNode current = mapper.readTree(getResp.getEntry().getDataJson());

        String json = mapper.writeValueAsString(Map.of(
            "name", current.get("name").asText(),
            "category", current.get("category").asText(),
            "price", newPrice));
        UpdateDataEntryResponse resp = dataStub.update(
            UpdateDataEntryRequest.newBuilder()
                .setTable("product")
                .setKey(key)
                .setDataJson(json)
                .build());
        check("update product success", resp.getStatus().getSuccess());
    }

    public void testDeleteProduct(String key) throws Exception {
        logger.info("[Product] Delete key=" + key);
        DeleteDataEntryResponse resp = dataStub.delete(
            DeleteDataEntryRequest.newBuilder()
                .setTable("product")
                .setKey(key)
                .build());
        check("delete product success", resp.getStatus().getSuccess());
    }

    // -------- Connection CRUD (PURCHASED) --------
    public String testCreateConnection(String customerKey, String productKey) throws Exception {
        logger.info("[Connection] Create PURCHASED");
        CreateConnectionResponse resp = connStub.create(
            CreateConnectionRequest.newBuilder()
                .setConnectionType("PURCHASED")
                .setFromKey(customerKey)
                .setToKey(productKey)
                .putAllMetadata(Map.of("quantity", "2", "source", "web"))
                .build());
        check("create connection success", resp.getStatus().getSuccess());
        check("create connection has uuid", !resp.getConnection().getUuid().isEmpty());
        String uuid = resp.getConnection().getUuid();
        logger.info("  Created connection uuid=" + uuid);
        return uuid;
    }

    public void testGetConnection(String uuid) throws Exception {
        logger.info("[Connection] Get uuid=" + uuid);
        GetConnectionResponse resp = connStub.get(
            GetConnectionRequest.newBuilder()
                .setConnectionType("PURCHASED")
                .setUuid(uuid)
                .build());
        check("get connection success", resp.getStatus().getSuccess());
        check("get connection from_key present", !resp.getConnection().getFromKey().isEmpty());
    }

    public void testGetConnectionByFrom(String customerKey) throws Exception {
        logger.info("[Connection] GetByFrom customer=" + customerKey);
        GetConnectionsByFromResponse resp = connStub.getByFrom(
            GetConnectionsByFromRequest.newBuilder()
                .setConnectionType("PURCHASED")
                .setFromKey(customerKey)
                .build());
        check("getByFrom success", resp.getStatus().getSuccess());
        check("getByFrom found connections", resp.getConnectionsCount() >= 1);
    }

    public void testUpdateConnection(String uuid) throws Exception {
        logger.info("[Connection] Update uuid=" + uuid);
        UpdateConnectionResponse resp = connStub.update(
            UpdateConnectionRequest.newBuilder()
                .setConnectionType("PURCHASED")
                .setUuid(uuid)
                .putAllMetadata(Map.of("quantity", "5", "source", "mobile", "discount", "10%"))
                .build());
        check("update connection success", resp.getStatus().getSuccess());

        // Verify
        GetConnectionResponse verifyResp = connStub.get(
            GetConnectionRequest.newBuilder()
                .setConnectionType("PURCHASED")
                .setUuid(uuid)
                .build());
        check("update connection metadata changed",
            "5".equals(verifyResp.getConnection().getMetadataMap().get("quantity")));
    }

    public void testDeleteConnection(String uuid) throws Exception {
        logger.info("[Connection] Delete uuid=" + uuid);
        DeleteConnectionResponse resp = connStub.delete(
            DeleteConnectionRequest.newBuilder()
                .setConnectionType("PURCHASED")
                .setUuid(uuid)
                .build());
        check("delete connection success", resp.getStatus().getSuccess());

        // Verify
        GetConnectionResponse verifyResp = connStub.get(
            GetConnectionRequest.newBuilder()
                .setConnectionType("PURCHASED")
                .setUuid(uuid)
                .build());
        check("delete connection confirmed gone", !verifyResp.getStatus().getSuccess());
    }

    // -------- GetAll --------
    public void testGetAllCustomers(int expectedMin) {
        logger.info("[Customer] GetAll");
        GetAllDataEntriesResponse resp = dataStub.getAll(
            GetAllDataEntriesRequest.newBuilder()
                .setTable("customer")
                .setPagination(Pagination.newBuilder().setSkip(0).setLimit(100).build())
                .build());
        check("getAll customers success", resp.getStatus().getSuccess());
        check("getAll customers count >= " + expectedMin, resp.getTotalCount() >= expectedMin);
    }

    public void testGetAllConnections(int expectedMin) {
        logger.info("[Connection] GetAll PURCHASED");
        GetAllConnectionsResponse resp = connStub.getAll(
            GetAllConnectionsRequest.newBuilder()
                .setConnectionType("PURCHASED")
                .setPagination(Pagination.newBuilder().setSkip(0).setLimit(100).build())
                .build());
        check("getAll connections success", resp.getStatus().getSuccess());
        check("getAll connections count >= " + expectedMin, resp.getTotalCount() >= expectedMin);
    }

    public void printSummary() {
        logger.info("==========================================");
        logger.info("  RESULTS: " + passed + " passed, " + failed + " failed");
        logger.info("==========================================");
    }

    public int getFailed() {
        return failed;
    }

    // -------- Main --------
    public static void main(String[] args) throws Exception {
        String target = args.length > 0 ? args[0] : "localhost:9090";
        logger.info("Running CRUD tests against " + target);

        CrudTest test = new CrudTest(target);
        try {
            // Customer CRUD
            String custKey1 = test.testCreateCustomer("Alice Smith", "alice@example.com");
            String custKey2 = test.testCreateCustomer("Bob Jones", "bob@example.com");
            Thread.sleep(500); // allow propagation
            test.testGetCustomer(custKey1, "Alice Smith");
            test.testGetCustomerByIndex("Alice Smith");
            test.testUpdateCustomer(custKey1, "alice.new@example.com");
            test.testGetAllCustomers(2);

            // Product CRUD
            String prodKey1 = test.testCreateProduct("Laptop", "electronics", 999.99);
            String prodKey2 = test.testCreateProduct("Headphones", "electronics", 79.99);
            Thread.sleep(500);
            test.testGetProduct(prodKey1, "Laptop");
            test.testUpdateProduct(prodKey2, 59.99);

            // Connection CRUD
            String connUuid1 = test.testCreateConnection(custKey1, prodKey1);
            String connUuid2 = test.testCreateConnection(custKey1, prodKey2);
            String connUuid3 = test.testCreateConnection(custKey2, prodKey1);
            Thread.sleep(500);
            test.testGetConnection(connUuid1);
            test.testGetConnectionByFrom(custKey1);
            test.testUpdateConnection(connUuid1);
            test.testGetAllConnections(3);

            // Cleanup: delete connections first, then entries
            test.testDeleteConnection(connUuid2);
            test.testDeleteConnection(connUuid3);
            test.testDeleteConnection(connUuid1);
            test.testDeleteProduct(prodKey2);
            test.testDeleteProduct(prodKey1);
            test.testDeleteCustomer(custKey2);
            test.testDeleteCustomer(custKey1);

            test.printSummary();
        } finally {
            test.shutdown();
        }
        System.exit(test.getFailed() > 0 ? 1 : 0);
    }
}
