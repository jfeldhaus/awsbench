package com.awsbench.workloads.dw;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.text.DecimalFormat;
import java.util.Set;
import java.util.HashSet;

/**
 * Data Warehouse Workload Simulation
 *
 * Supports Oracle, PostgreSQL, and MySQL. The correct JDBC driver is
 * selected automatically from the -url prefix.
 * Supports two modes:
 * - populate: Generate synthetic data for dimension and fact tables
 * - workload: Run configurable mix of warehouse-style transactions
 */
public class DWWork {

    static final int DB_ORACLE     = 1;
    static final int DB_POSTGRESQL = 2;
    static final int DB_MYSQL      = 3;

    static int detectDbType(String url) {
        if (url.startsWith("jdbc:oracle:"))     return DB_ORACLE;
        if (url.startsWith("jdbc:postgresql:")) return DB_POSTGRESQL;
        if (url.startsWith("jdbc:mysql:"))      return DB_MYSQL;
        throw new IllegalArgumentException("Unsupported database URL: " + url);
    }

    static void loadDriver(int dbType) throws ClassNotFoundException {
        switch (dbType) {
            case DB_ORACLE:     Class.forName("oracle.jdbc.OracleDriver"); break;
            case DB_POSTGRESQL: Class.forName("org.postgresql.Driver"); break;
            case DB_MYSQL:      Class.forName("com.mysql.cj.jdbc.Driver"); break;
        }
    }

    // ========================================================================
    // Configuration
    // ========================================================================

    private static class Config {
        String mode = null;
        int threads = 4;
        int duration = 60;
        String url = null;
        int dbType;
        String uid = null;
        String pwd = null;
        int batchSize = 1000;
        int scale = 1;

        // Transaction mix percentages
        int pctPoint = 10;
        int pctRange = 20;
        int pctAgg = 30;
        int pctJoin = 25;
        int pctAnalytic = 10;
        int pctDml = 5;

        boolean validate() {
            if (mode == null) {
                System.err.println("Error: -mode is required");
                return false;
            }
            if (!mode.equals("populate") && !mode.equals("workload")) {
                System.err.println("Error: -mode must be 'populate' or 'workload'");
                return false;
            }
            if (url == null) {
                System.err.println("Error: -url is required");
                return false;
            }
            try {
                dbType = detectDbType(url);
            } catch (IllegalArgumentException e) {
                System.err.println("Error: " + e.getMessage());
                return false;
            }
            int total = pctPoint + pctRange + pctAgg + pctJoin + pctAnalytic + pctDml;
            if (total != 100) {
                System.err.println("Error: Transaction percentages must sum to 100 (current: " + total + ")");
                return false;
            }
            return true;
        }
    }

    // ========================================================================
    // Metrics Collection
    // ========================================================================

    private static class Metrics {
        private final ConcurrentHashMap<String, LongAdder> txCounts = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, LongAdder> txLatencies = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, ConcurrentSkipListMap<Long, LongAdder>> latencyHistograms = new ConcurrentHashMap<>();
        private final LongAdder totalTransactions = new LongAdder();
        private final LongAdder totalLatency = new LongAdder();
        private final AtomicLong minLatency = new AtomicLong(Long.MAX_VALUE);
        private final AtomicLong maxLatency = new AtomicLong(0);
        private final List<Long> allLatencies = Collections.synchronizedList(new ArrayList<>());

        // Error tracking
        private final LongAdder totalErrors = new LongAdder();
        private final ConcurrentHashMap<String, LongAdder> txErrors = new ConcurrentHashMap<>();

        void recordTransaction(String txType, long latencyNanos) {
            long latencyMicros = latencyNanos / 1000;

            txCounts.computeIfAbsent(txType, k -> new LongAdder()).increment();
            txLatencies.computeIfAbsent(txType, k -> new LongAdder()).add(latencyMicros);

            totalTransactions.increment();
            totalLatency.add(latencyMicros);

            // Update min/max
            long currentMin;
            do {
                currentMin = minLatency.get();
                if (latencyMicros >= currentMin) break;
            } while (!minLatency.compareAndSet(currentMin, latencyMicros));

            long currentMax;
            do {
                currentMax = maxLatency.get();
                if (latencyMicros <= currentMax) break;
            } while (!maxLatency.compareAndSet(currentMax, latencyMicros));

            // Store for percentile calculation (sample if too many)
            if (allLatencies.size() < 1000000) {
                allLatencies.add(latencyMicros);
            }
        }

        void recordError(String txType) {
            totalErrors.increment();
            txErrors.computeIfAbsent(txType, k -> new LongAdder()).increment();
        }

        void printReport(long elapsedSeconds) {
            DecimalFormat df = new DecimalFormat("#,##0.00");
            DecimalFormat df0 = new DecimalFormat("#,##0");

            long total = totalTransactions.sum();
            double tps = elapsedSeconds > 0 ? (double) total / elapsedSeconds : 0;

            System.out.println("\n" + "=".repeat(70));
            System.out.println("WORKLOAD RESULTS");
            System.out.println("=".repeat(70));
            System.out.println();

            long errors = totalErrors.sum();
            double errorRate = total > 0 ? (double) errors / (total + errors) * 100 : 0;

            System.out.println("Duration:             " + elapsedSeconds + " seconds");
            System.out.println("Total Transactions:   " + df0.format(total));
            System.out.println("Total Errors:         " + df0.format(errors));
            System.out.println("Error Rate:           " + df.format(errorRate) + "%");
            System.out.println("Throughput (TPS):     " + df.format(tps));
            System.out.println();

            // Latency statistics
            System.out.println("-".repeat(70));
            System.out.println("LATENCY STATISTICS (microseconds)");
            System.out.println("-".repeat(70));

            if (total > 0) {
                double avgLatency = (double) totalLatency.sum() / total;
                System.out.println("Min:    " + df.format(minLatency.get()));
                System.out.println("Avg:    " + df.format(avgLatency));
                System.out.println("Max:    " + df.format(maxLatency.get()));

                // Calculate percentiles
                if (!allLatencies.isEmpty()) {
                    List<Long> sorted = new ArrayList<>(allLatencies);
                    Collections.sort(sorted);
                    int size = sorted.size();

                    System.out.println("P50:    " + df.format(sorted.get((int)(size * 0.50))));
                    System.out.println("P95:    " + df.format(sorted.get((int)(size * 0.95))));
                    System.out.println("P99:    " + df.format(sorted.get(Math.min((int)(size * 0.99), size - 1))));
                }
            }

            // Per-transaction breakdown
            System.out.println();
            System.out.println("-".repeat(70));
            System.out.println("TRANSACTION BREAKDOWN");
            System.out.println("-".repeat(70));
            System.out.printf("%-20s %10s %10s %10s %10s%n", "Transaction Type", "Count", "Errors", "Avg Lat(us)", "TPS");
            System.out.println("-".repeat(70));

            // Collect all transaction types (from both success and error maps)
            Set<String> allTxTypes = new HashSet<>(txCounts.keySet());
            allTxTypes.addAll(txErrors.keySet());

            for (String txType : allTxTypes) {
                long count = txCounts.containsKey(txType) ? txCounts.get(txType).sum() : 0;
                long errCount = txErrors.containsKey(txType) ? txErrors.get(txType).sum() : 0;
                long latSum = txLatencies.containsKey(txType) ? txLatencies.get(txType).sum() : 0;
                double avgLat = count > 0 ? (double) latSum / count : 0;
                double txTps = elapsedSeconds > 0 ? (double) count / elapsedSeconds : 0;

                System.out.printf("%-20s %10s %10s %10s %10s%n",
                    txType, df0.format(count), df0.format(errCount), df.format(avgLat), df.format(txTps));
            }

            System.out.println("=".repeat(70));
        }
    }

    // ========================================================================
    // Transaction Types
    // ========================================================================

    private enum TxType {
        POINT_CUSTOMER("POINT_CUSTOMER"),
        POINT_PRODUCT("POINT_PRODUCT"),
        POINT_SALES("POINT_SALES"),
        RANGE_SALES_DATE("RANGE_SALES_DATE"),
        RANGE_PRODUCT_PRICE("RANGE_PRODUCT_PRICE"),
        RANGE_CUSTOMER_REGION("RANGE_CUSTOMER_REGION"),
        AGG_DAILY_STORE("AGG_DAILY_STORE"),
        AGG_MONTHLY_CATEGORY("AGG_MONTHLY_CATEGORY"),
        AGG_QUARTERLY_SEGMENT("AGG_QUARTERLY_SEGMENT"),
        AGG_YOY_COMPARE("AGG_YOY_COMPARE"),
        JOIN_FULL_SALES("JOIN_FULL_SALES"),
        JOIN_TOP_PRODUCTS("JOIN_TOP_PRODUCTS"),
        JOIN_CUST_HISTORY("JOIN_CUST_HISTORY"),
        ANALYTIC_RUNNING("ANALYTIC_RUNNING"),
        ANALYTIC_RANKING("ANALYTIC_RANKING"),
        ANALYTIC_PERCENT("ANALYTIC_PERCENT"),
        DML_STG_INSERT("DML_STG_INSERT"),
        DML_AGG_UPDATE("DML_AGG_UPDATE"),
        DML_BATCH_INSERT("DML_BATCH_INSERT");

        final String name;
        TxType(String name) { this.name = name; }
    }

    // ========================================================================
    // Prepared Statement Cache
    // ========================================================================

    private static class StatementCache {
        private final Map<String, PreparedStatement> cache = new HashMap<>();
        private final Connection connection;
        private final int dbType;

        // Data ranges for random selection
        int maxDateKey;
        int minDateKey;
        int maxProductKey;
        int maxCustomerKey;
        int maxStoreKey;
        int maxSalesKey;

        StatementCache(Connection conn, int dbType) throws SQLException {
            this.connection = conn;
            this.dbType = dbType;
            loadDataRanges();
            prepareAllStatements();
        }

        private String rowLimit(int n) {
            return dbType == DB_ORACLE
                ? " FETCH FIRST " + n + " ROWS ONLY"
                : " LIMIT " + n;
        }

        private String nextval(String seqName) {
            switch (dbType) {
                case DB_ORACLE:     return seqName + ".NEXTVAL";
                case DB_POSTGRESQL: return "nextval('" + seqName.toLowerCase() + "')";
                default:            return "NULL"; // MySQL: AUTO_INCREMENT
            }
        }

        private void loadDataRanges() throws SQLException {
            try (Statement stmt = connection.createStatement()) {
                ResultSet rs;

                rs = stmt.executeQuery("SELECT MIN(DATE_KEY), MAX(DATE_KEY) FROM DIM_DATE");
                if (rs.next()) {
                    minDateKey = rs.getInt(1);
                    maxDateKey = rs.getInt(2);
                }
                rs.close();

                rs = stmt.executeQuery("SELECT MAX(PRODUCT_KEY) FROM DIM_PRODUCT");
                if (rs.next()) maxProductKey = rs.getInt(1);
                rs.close();

                rs = stmt.executeQuery("SELECT MAX(CUSTOMER_KEY) FROM DIM_CUSTOMER");
                if (rs.next()) maxCustomerKey = rs.getInt(1);
                rs.close();

                rs = stmt.executeQuery("SELECT MAX(STORE_KEY) FROM DIM_STORE");
                if (rs.next()) maxStoreKey = rs.getInt(1);
                rs.close();

                rs = stmt.executeQuery("SELECT MAX(SALES_KEY) FROM FACT_SALES");
                if (rs.next()) maxSalesKey = rs.getInt(1);
                rs.close();
            }

            // Ensure valid ranges
            if (maxDateKey == 0) maxDateKey = 20241231;
            if (minDateKey == 0) minDateKey = 20240101;
            if (maxProductKey == 0) maxProductKey = 1000;
            if (maxCustomerKey == 0) maxCustomerKey = 10000;
            if (maxStoreKey == 0) maxStoreKey = 100;
            if (maxSalesKey == 0) maxSalesKey = 100000;
        }

        private void prepareAllStatements() throws SQLException {
            // Point queries
            cache.put("POINT_CUSTOMER", connection.prepareStatement(
                "SELECT * FROM DIM_CUSTOMER WHERE CUSTOMER_KEY = ?"));

            cache.put("POINT_PRODUCT", connection.prepareStatement(
                "SELECT * FROM DIM_PRODUCT WHERE PRODUCT_KEY = ?"));

            cache.put("POINT_SALES", connection.prepareStatement(
                "SELECT * FROM FACT_SALES WHERE SALES_KEY = ?"));

            // Range queries
            cache.put("RANGE_SALES_DATE", connection.prepareStatement(
                "SELECT DATE_KEY, SUM(SALES_AMOUNT) AS TOTAL " +
                "FROM FACT_SALES WHERE DATE_KEY BETWEEN ? AND ? " +
                "GROUP BY DATE_KEY ORDER BY DATE_KEY"));

            cache.put("RANGE_PRODUCT_PRICE", connection.prepareStatement(
                "SELECT * FROM DIM_PRODUCT WHERE UNIT_PRICE BETWEEN ? AND ? " +
                "ORDER BY UNIT_PRICE"));

            cache.put("RANGE_CUSTOMER_REGION", connection.prepareStatement(
                "SELECT * FROM DIM_CUSTOMER WHERE REGION = ? " +
                "ORDER BY CUSTOMER_KEY"));

            // Aggregation queries
            cache.put("AGG_DAILY_STORE", connection.prepareStatement(
                "SELECT f.DATE_KEY, f.STORE_KEY, SUM(f.QUANTITY) AS QTY, " +
                "SUM(f.SALES_AMOUNT) AS SALES, COUNT(*) AS TXN_COUNT " +
                "FROM FACT_SALES f WHERE f.DATE_KEY = ? " +
                "GROUP BY f.DATE_KEY, f.STORE_KEY ORDER BY SALES DESC"));

            cache.put("AGG_MONTHLY_CATEGORY", connection.prepareStatement(
                "SELECT d.YEAR_NUM, d.MONTH_NUM, p.CATEGORY_NAME, " +
                "SUM(f.SALES_AMOUNT) AS REVENUE, SUM(f.PROFIT_AMOUNT) AS PROFIT " +
                "FROM FACT_SALES f " +
                "JOIN DIM_DATE d ON f.DATE_KEY = d.DATE_KEY " +
                "JOIN DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY " +
                "WHERE d.YEAR_NUM = ? AND d.MONTH_NUM = ? " +
                "GROUP BY d.YEAR_NUM, d.MONTH_NUM, p.CATEGORY_NAME"));

            cache.put("AGG_QUARTERLY_SEGMENT", connection.prepareStatement(
                "SELECT d.YEAR_NUM, d.QUARTER_NUM, c.CUSTOMER_SEGMENT, " +
                "COUNT(DISTINCT f.CUSTOMER_KEY) AS CUSTOMERS, " +
                "SUM(f.SALES_AMOUNT) AS SALES " +
                "FROM FACT_SALES f " +
                "JOIN DIM_DATE d ON f.DATE_KEY = d.DATE_KEY " +
                "JOIN DIM_CUSTOMER c ON f.CUSTOMER_KEY = c.CUSTOMER_KEY " +
                "WHERE d.YEAR_NUM = ? AND d.QUARTER_NUM = ? " +
                "GROUP BY d.YEAR_NUM, d.QUARTER_NUM, c.CUSTOMER_SEGMENT"));

            cache.put("AGG_YOY_COMPARE", connection.prepareStatement(
                "SELECT d.MONTH_NUM, " +
                "SUM(CASE WHEN d.YEAR_NUM = ? THEN f.SALES_AMOUNT ELSE 0 END) AS CURR_YEAR, " +
                "SUM(CASE WHEN d.YEAR_NUM = ? THEN f.SALES_AMOUNT ELSE 0 END) AS PREV_YEAR " +
                "FROM FACT_SALES f " +
                "JOIN DIM_DATE d ON f.DATE_KEY = d.DATE_KEY " +
                "WHERE d.YEAR_NUM IN (?, ?) " +
                "GROUP BY d.MONTH_NUM ORDER BY d.MONTH_NUM"));

            // Join queries
            cache.put("JOIN_FULL_SALES", connection.prepareStatement(
                "SELECT f.SALES_KEY, d.FULL_DATE, p.PRODUCT_NAME, c.LAST_NAME, " +
                "s.STORE_NAME, f.QUANTITY, f.SALES_AMOUNT " +
                "FROM FACT_SALES f " +
                "JOIN DIM_DATE d ON f.DATE_KEY = d.DATE_KEY " +
                "JOIN DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY " +
                "JOIN DIM_CUSTOMER c ON f.CUSTOMER_KEY = c.CUSTOMER_KEY " +
                "JOIN DIM_STORE s ON f.STORE_KEY = s.STORE_KEY " +
                "WHERE f.DATE_KEY = ? " +
                "ORDER BY f.SALES_AMOUNT DESC" + rowLimit(100)));

            cache.put("JOIN_TOP_PRODUCTS", connection.prepareStatement(
                "SELECT p.CATEGORY_NAME, p.PRODUCT_NAME, SUM(f.SALES_AMOUNT) AS REVENUE " +
                "FROM FACT_SALES f " +
                "JOIN DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY " +
                "JOIN DIM_DATE d ON f.DATE_KEY = d.DATE_KEY " +
                "WHERE d.YEAR_NUM = ? " +
                "GROUP BY p.CATEGORY_NAME, p.PRODUCT_NAME " +
                "ORDER BY REVENUE DESC" + rowLimit(50)));

            cache.put("JOIN_CUST_HISTORY", connection.prepareStatement(
                "SELECT d.FULL_DATE, p.PRODUCT_NAME, f.QUANTITY, f.SALES_AMOUNT " +
                "FROM FACT_SALES f " +
                "JOIN DIM_DATE d ON f.DATE_KEY = d.DATE_KEY " +
                "JOIN DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY " +
                "WHERE f.CUSTOMER_KEY = ? " +
                "ORDER BY d.FULL_DATE DESC" + rowLimit(100)));

            // Analytic queries
            cache.put("ANALYTIC_RUNNING", connection.prepareStatement(
                "SELECT DATE_KEY, SALES_AMOUNT, " +
                "SUM(SALES_AMOUNT) OVER (ORDER BY DATE_KEY ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS MOVING_AVG " +
                "FROM (SELECT DATE_KEY, SUM(SALES_AMOUNT) AS SALES_AMOUNT " +
                "      FROM FACT_SALES WHERE DATE_KEY BETWEEN ? AND ? " +
                "      GROUP BY DATE_KEY) sub " +
                "ORDER BY DATE_KEY"));

            cache.put("ANALYTIC_RANKING", connection.prepareStatement(
                "SELECT PRODUCT_KEY, TOTAL_SALES, " +
                "RANK() OVER (ORDER BY TOTAL_SALES DESC) AS SALES_RANK " +
                "FROM (SELECT PRODUCT_KEY, SUM(SALES_AMOUNT) AS TOTAL_SALES " +
                "      FROM FACT_SALES WHERE DATE_KEY BETWEEN ? AND ? " +
                "      GROUP BY PRODUCT_KEY) sub" + rowLimit(20)));

            cache.put("ANALYTIC_PERCENT", connection.prepareStatement(
                "SELECT STORE_KEY, SALES_AMOUNT, " +
                "SALES_AMOUNT / SUM(SALES_AMOUNT) OVER () * 100 AS PCT_OF_TOTAL " +
                "FROM (SELECT STORE_KEY, SUM(SALES_AMOUNT) AS SALES_AMOUNT " +
                "      FROM FACT_SALES WHERE DATE_KEY = ? " +
                "      GROUP BY STORE_KEY) sub " +
                "ORDER BY SALES_AMOUNT DESC"));

            // DML operations
            cache.put("DML_STG_INSERT", connection.prepareStatement(
                "INSERT INTO STG_SALES (STG_SALES_KEY, SOURCE_SYSTEM, TRANSACTION_DATE, " +
                "PRODUCT_ID, CUSTOMER_ID, STORE_ID, QUANTITY, UNIT_PRICE) " +
                "VALUES (" + nextval("SEQ_STG_SALES") + ", 'WORKLOAD', CURRENT_TIMESTAMP, ?, ?, ?, ?, ?)"));

            cache.put("DML_AGG_UPDATE", connection.prepareStatement(
                "UPDATE AGG_SALES_DAILY SET LAST_UPDATE = CURRENT_TIMESTAMP " +
                "WHERE DATE_KEY = ? AND STORE_KEY = ?"));

            cache.put("DML_BATCH_SELECT", connection.prepareStatement(
                "SELECT PRODUCT_KEY, CUSTOMER_KEY, STORE_KEY FROM FACT_SALES " +
                "WHERE SALES_KEY = ?"));
        }

        PreparedStatement get(String name) {
            return cache.get(name);
        }

        void close() {
            for (PreparedStatement ps : cache.values()) {
                try { ps.close(); } catch (SQLException e) { /* ignore */ }
            }
        }
    }

    // ========================================================================
    // Workload Thread
    // ========================================================================

    private static class WorkloadThread implements Runnable {
        private final int threadId;
        private final Config config;
        private final Metrics metrics;
        private final AtomicBoolean running;
        private final Random random = new Random();

        private Connection connection;
        private StatementCache stmtCache;

        // Transaction selection thresholds
        private final int threshPoint;
        private final int threshRange;
        private final int threshAgg;
        private final int threshJoin;
        private final int threshAnalytic;

        private static final String[] REGIONS = {"EAST", "WEST", "NORTH", "SOUTH", "CENTRAL"};

        WorkloadThread(int threadId, Config config, Metrics metrics, AtomicBoolean running) {
            this.threadId = threadId;
            this.config = config;
            this.metrics = metrics;
            this.running = running;

            // Calculate cumulative thresholds
            this.threshPoint = config.pctPoint;
            this.threshRange = threshPoint + config.pctRange;
            this.threshAgg = threshRange + config.pctAgg;
            this.threshJoin = threshAgg + config.pctJoin;
            this.threshAnalytic = threshJoin + config.pctAnalytic;
        }

        @Override
        public void run() {
            try {
                // Create dedicated connection
                loadDriver(config.dbType);
                connection = DriverManager.getConnection(
                    config.url, config.uid, config.pwd);
                connection.setAutoCommit(true);

                // Prepare all statements before workload
                stmtCache = new StatementCache(connection, config.dbType);

                System.out.println("Thread " + threadId + " ready with " +
                    stmtCache.cache.size() + " prepared statements");

                // Run transactions until stopped
                while (running.get()) {
                    executeRandomTransaction();
                }

            } catch (Exception e) {
                System.err.println("Thread " + threadId + " error: " + e.getMessage());
                e.printStackTrace();
            } finally {
                cleanup();
            }
        }

        private void executeRandomTransaction() {
            int roll = random.nextInt(100);
            String txCategory = null;

            try {
                if (roll < threshPoint) {
                    txCategory = "POINT";
                    executePointQuery();
                } else if (roll < threshRange) {
                    txCategory = "RANGE";
                    executeRangeQuery();
                } else if (roll < threshAgg) {
                    txCategory = "AGG";
                    executeAggregation();
                } else if (roll < threshJoin) {
                    txCategory = "JOIN";
                    executeJoinQuery();
                } else if (roll < threshAnalytic) {
                    txCategory = "ANALYTIC";
                    executeAnalyticQuery();
                } else {
                    txCategory = "DML";
                    executeDmlOperation();
                }
            } catch (SQLException e) {
                // Record error with category
                if (txCategory != null) {
                    metrics.recordError(txCategory + "_ERROR");
                }
            }
        }

        private void executePointQuery() throws SQLException {
            int choice = random.nextInt(3);
            long start = System.nanoTime();
            String txName;

            switch (choice) {
                case 0: {
                    txName = "POINT_CUSTOMER";
                    PreparedStatement ps = stmtCache.get(txName);
                    ps.setInt(1, random.nextInt(stmtCache.maxCustomerKey) + 1);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) { /* consume */ }
                    }
                    break;
                }
                case 1: {
                    txName = "POINT_PRODUCT";
                    PreparedStatement ps = stmtCache.get(txName);
                    ps.setInt(1, random.nextInt(stmtCache.maxProductKey) + 1);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) { /* consume */ }
                    }
                    break;
                }
                default: {
                    txName = "POINT_SALES";
                    PreparedStatement ps = stmtCache.get(txName);
                    ps.setInt(1, random.nextInt(stmtCache.maxSalesKey) + 1);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) { /* consume */ }
                    }
                    break;
                }
            }

            metrics.recordTransaction(txName, System.nanoTime() - start);
        }

        private void executeRangeQuery() throws SQLException {
            int choice = random.nextInt(3);
            long start = System.nanoTime();
            String txName;

            switch (choice) {
                case 0: {
                    txName = "RANGE_SALES_DATE";
                    PreparedStatement ps = stmtCache.get(txName);
                    int startDate = stmtCache.minDateKey + random.nextInt(
                        Math.max(1, stmtCache.maxDateKey - stmtCache.minDateKey - 30));
                    ps.setInt(1, startDate);
                    ps.setInt(2, startDate + 30);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) { /* consume */ }
                    }
                    break;
                }
                case 1: {
                    txName = "RANGE_PRODUCT_PRICE";
                    PreparedStatement ps = stmtCache.get(txName);
                    double minPrice = random.nextDouble() * 100;
                    ps.setDouble(1, minPrice);
                    ps.setDouble(2, minPrice + 50);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) { /* consume */ }
                    }
                    break;
                }
                default: {
                    txName = "RANGE_CUSTOMER_REGION";
                    PreparedStatement ps = stmtCache.get(txName);
                    ps.setString(1, REGIONS[random.nextInt(REGIONS.length)]);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) { /* consume */ }
                    }
                    break;
                }
            }

            metrics.recordTransaction(txName, System.nanoTime() - start);
        }

        private void executeAggregation() throws SQLException {
            int choice = random.nextInt(4);
            long start = System.nanoTime();
            String txName;

            switch (choice) {
                case 0: {
                    txName = "AGG_DAILY_STORE";
                    PreparedStatement ps = stmtCache.get(txName);
                    int dateKey = stmtCache.minDateKey + random.nextInt(
                        Math.max(1, stmtCache.maxDateKey - stmtCache.minDateKey));
                    ps.setInt(1, dateKey);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) { /* consume */ }
                    }
                    break;
                }
                case 1: {
                    txName = "AGG_MONTHLY_CATEGORY";
                    PreparedStatement ps = stmtCache.get(txName);
                    ps.setInt(1, 2024);
                    ps.setInt(2, random.nextInt(12) + 1);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) { /* consume */ }
                    }
                    break;
                }
                case 2: {
                    txName = "AGG_QUARTERLY_SEGMENT";
                    PreparedStatement ps = stmtCache.get(txName);
                    ps.setInt(1, 2024);
                    ps.setInt(2, random.nextInt(4) + 1);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) { /* consume */ }
                    }
                    break;
                }
                default: {
                    txName = "AGG_YOY_COMPARE";
                    PreparedStatement ps = stmtCache.get(txName);
                    ps.setInt(1, 2024);
                    ps.setInt(2, 2023);
                    ps.setInt(3, 2024);
                    ps.setInt(4, 2023);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) { /* consume */ }
                    }
                    break;
                }
            }

            metrics.recordTransaction(txName, System.nanoTime() - start);
        }

        private void executeJoinQuery() throws SQLException {
            int choice = random.nextInt(3);
            long start = System.nanoTime();
            String txName;

            switch (choice) {
                case 0: {
                    txName = "JOIN_FULL_SALES";
                    PreparedStatement ps = stmtCache.get(txName);
                    int dateKey = stmtCache.minDateKey + random.nextInt(
                        Math.max(1, stmtCache.maxDateKey - stmtCache.minDateKey));
                    ps.setInt(1, dateKey);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) { /* consume */ }
                    }
                    break;
                }
                case 1: {
                    txName = "JOIN_TOP_PRODUCTS";
                    PreparedStatement ps = stmtCache.get(txName);
                    ps.setInt(1, 2024);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) { /* consume */ }
                    }
                    break;
                }
                default: {
                    txName = "JOIN_CUST_HISTORY";
                    PreparedStatement ps = stmtCache.get(txName);
                    ps.setInt(1, random.nextInt(stmtCache.maxCustomerKey) + 1);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) { /* consume */ }
                    }
                    break;
                }
            }

            metrics.recordTransaction(txName, System.nanoTime() - start);
        }

        private void executeAnalyticQuery() throws SQLException {
            int choice = random.nextInt(3);
            long start = System.nanoTime();
            String txName;

            switch (choice) {
                case 0: {
                    txName = "ANALYTIC_RUNNING";
                    PreparedStatement ps = stmtCache.get(txName);
                    int startDate = stmtCache.minDateKey + random.nextInt(
                        Math.max(1, stmtCache.maxDateKey - stmtCache.minDateKey - 30));
                    ps.setInt(1, startDate);
                    ps.setInt(2, startDate + 30);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) { /* consume */ }
                    }
                    break;
                }
                case 1: {
                    txName = "ANALYTIC_RANKING";
                    PreparedStatement ps = stmtCache.get(txName);
                    int startDate = stmtCache.minDateKey + random.nextInt(
                        Math.max(1, stmtCache.maxDateKey - stmtCache.minDateKey - 90));
                    ps.setInt(1, startDate);
                    ps.setInt(2, startDate + 90);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) { /* consume */ }
                    }
                    break;
                }
                default: {
                    txName = "ANALYTIC_PERCENT";
                    PreparedStatement ps = stmtCache.get(txName);
                    int dateKey = stmtCache.minDateKey + random.nextInt(
                        Math.max(1, stmtCache.maxDateKey - stmtCache.minDateKey));
                    ps.setInt(1, dateKey);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) { /* consume */ }
                    }
                    break;
                }
            }

            metrics.recordTransaction(txName, System.nanoTime() - start);
        }

        private void executeDmlOperation() throws SQLException {
            int choice = random.nextInt(3);
            long start = System.nanoTime();
            String txName;

            switch (choice) {
                case 0: {
                    txName = "DML_STG_INSERT";
                    PreparedStatement ps = stmtCache.get(txName);
                    ps.setString(1, "P" + (random.nextInt(stmtCache.maxProductKey) + 1));
                    ps.setString(2, "C" + (random.nextInt(stmtCache.maxCustomerKey) + 1));
                    ps.setString(3, "S" + (random.nextInt(stmtCache.maxStoreKey) + 1));
                    ps.setInt(4, random.nextInt(10) + 1);
                    ps.setDouble(5, random.nextDouble() * 100 + 10);
                    ps.executeUpdate();
                    break;
                }
                case 1: {
                    txName = "DML_AGG_UPDATE";
                    PreparedStatement ps = stmtCache.get(txName);
                    int dateKey = stmtCache.minDateKey + random.nextInt(
                        Math.max(1, stmtCache.maxDateKey - stmtCache.minDateKey));
                    ps.setInt(1, dateKey);
                    ps.setInt(2, random.nextInt(stmtCache.maxStoreKey) + 1);
                    ps.executeUpdate();
                    break;
                }
                default: {
                    // Batch insert simulation - read then insert to staging
                    txName = "DML_BATCH_INSERT";
                    PreparedStatement selectPs = stmtCache.get("DML_BATCH_SELECT");
                    selectPs.setInt(1, random.nextInt(stmtCache.maxSalesKey) + 1);
                    try (ResultSet rs = selectPs.executeQuery()) {
                        if (rs.next()) {
                            PreparedStatement insertPs = stmtCache.get("DML_STG_INSERT");
                            insertPs.setString(1, "P" + rs.getInt(1));
                            insertPs.setString(2, "C" + rs.getInt(2));
                            insertPs.setString(3, "S" + rs.getInt(3));
                            insertPs.setInt(4, random.nextInt(10) + 1);
                            insertPs.setDouble(5, random.nextDouble() * 100 + 10);
                            insertPs.executeUpdate();
                        }
                    }
                    break;
                }
            }

            metrics.recordTransaction(txName, System.nanoTime() - start);
        }

        private void cleanup() {
            if (stmtCache != null) {
                stmtCache.close();
            }
            if (connection != null) {
                try { connection.close(); } catch (SQLException e) { /* ignore */ }
            }
        }
    }

    // ========================================================================
    // Data Population
    // ========================================================================

    private static class DataPopulator {
        private final Config config;
        private final Random random = new Random();
        private Connection connection;

        private static final String[] CATEGORIES = {
            "Electronics", "Clothing", "Home & Garden", "Sports", "Books",
            "Toys", "Food & Beverage", "Health & Beauty", "Automotive", "Office"
        };

        private static final String[] SUBCATEGORIES = {
            "Premium", "Standard", "Budget", "Clearance", "New Arrival"
        };

        private static final String[] BRANDS = {
            "BrandA", "BrandB", "BrandC", "BrandD", "BrandE",
            "BrandF", "BrandG", "BrandH", "BrandI", "BrandJ"
        };

        private static final String[] REGIONS = {"EAST", "WEST", "NORTH", "SOUTH", "CENTRAL"};
        private static final String[] SEGMENTS = {"PREMIUM", "STANDARD", "BUDGET"};
        private static final String[] TIERS = {"GOLD", "SILVER", "BRONZE"};
        private static final String[] STORE_TYPES = {"RETAIL", "WAREHOUSE", "OUTLET"};
        private static final String[] PROMO_TYPES = {"DISCOUNT", "BOGO", "BUNDLE", "CLEARANCE"};

        private static final String[] FIRST_NAMES = {
            "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
            "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica"
        };

        private static final String[] LAST_NAMES = {
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
            "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas"
        };

        private static final String[] CITIES = {
            "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
            "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"
        };

        private static final String[] STATES = {
            "NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA"
        };

        DataPopulator(Config config) {
            this.config = config;
        }

        private String nextval(String seqName) {
            switch (config.dbType) {
                case DB_ORACLE:     return seqName + ".NEXTVAL";
                case DB_POSTGRESQL: return "nextval('" + seqName.toLowerCase() + "')";
                default:            return "NULL"; // MySQL: AUTO_INCREMENT
            }
        }

        void populate() throws Exception {
            loadDriver(config.dbType);
            connection = DriverManager.getConnection(
                config.url, config.uid, config.pwd);
            connection.setAutoCommit(false);

            try {
                new DWSchema(connection, config.dbType).createOrRecreate();

                System.out.println("Starting data population with scale factor: " + config.scale);

                long start = System.currentTimeMillis();

                populateDateDimension();
                populateProductDimension();
                populateCustomerDimension();
                populateStoreDimension();
                populatePromotionDimension();
                populateSalesFact();
                populateInventoryFact();
                populateAggregates();

                long elapsed = (System.currentTimeMillis() - start) / 1000;
                System.out.println("\nData population completed in " + elapsed + " seconds");

            } finally {
                connection.close();
            }
        }

        private void populateDateDimension() throws SQLException {
            System.out.print("Populating DIM_DATE...");

            String sql = "INSERT INTO DIM_DATE (DATE_KEY, FULL_DATE, DAY_OF_WEEK, DAY_NAME, " +
                "DAY_OF_MONTH, DAY_OF_YEAR, WEEK_OF_YEAR, MONTH_NUM, MONTH_NAME, QUARTER_NUM, " +
                "QUARTER_NAME, YEAR_NUM, IS_WEEKEND, FISCAL_YEAR, FISCAL_QUARTER) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            String[] dayNames = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};
            String[] monthNames = {"January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"};

            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                java.util.Calendar cal = java.util.Calendar.getInstance();
                cal.set(2020, 0, 1); // Start from 2020

                int count = 0;
                int years = 5 * config.scale; // 5 years per scale factor

                for (int y = 0; y < years; y++) {
                    for (int d = 0; d < 365; d++) {
                        int year = cal.get(java.util.Calendar.YEAR);
                        int month = cal.get(java.util.Calendar.MONTH) + 1;
                        int dayOfMonth = cal.get(java.util.Calendar.DAY_OF_MONTH);
                        int dayOfWeek = cal.get(java.util.Calendar.DAY_OF_WEEK);
                        int dayOfYear = cal.get(java.util.Calendar.DAY_OF_YEAR);
                        int weekOfYear = cal.get(java.util.Calendar.WEEK_OF_YEAR);
                        int quarter = (month - 1) / 3 + 1;

                        int dateKey = year * 10000 + month * 100 + dayOfMonth;

                        ps.setInt(1, dateKey);
                        ps.setDate(2, new java.sql.Date(cal.getTimeInMillis()));
                        ps.setInt(3, dayOfWeek);
                        ps.setString(4, dayNames[dayOfWeek - 1]);
                        ps.setInt(5, dayOfMonth);
                        ps.setInt(6, dayOfYear);
                        ps.setInt(7, weekOfYear);
                        ps.setInt(8, month);
                        ps.setString(9, monthNames[month - 1]);
                        ps.setInt(10, quarter);
                        ps.setString(11, "Q" + quarter);
                        ps.setInt(12, year);
                        ps.setInt(13, (dayOfWeek == 1 || dayOfWeek == 7) ? 1 : 0);
                        ps.setInt(14, year);
                        ps.setInt(15, quarter);

                        ps.addBatch();
                        count++;

                        if (count % config.batchSize == 0) {
                            ps.executeBatch();
                            connection.commit();
                        }

                        cal.add(java.util.Calendar.DAY_OF_YEAR, 1);
                    }
                }

                ps.executeBatch();
                connection.commit();
                System.out.println(" " + count + " rows");
            }
        }

        private void populateProductDimension() throws SQLException {
            System.out.print("Populating DIM_PRODUCT...");

            String sql = "INSERT INTO DIM_PRODUCT (PRODUCT_KEY, PRODUCT_ID, PRODUCT_NAME, " +
                "PRODUCT_DESC, CATEGORY_ID, CATEGORY_NAME, SUBCATEGORY_ID, SUBCATEGORY_NAME, " +
                "BRAND, SUPPLIER_ID, SUPPLIER_NAME, UNIT_COST, UNIT_PRICE, STATUS, " +
                "EFFECTIVE_DATE, IS_CURRENT) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', CURRENT_TIMESTAMP, 1)";

            int numProducts = 1000 * config.scale;

            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                for (int i = 1; i <= numProducts; i++) {
                    int catIdx = random.nextInt(CATEGORIES.length);
                    int subIdx = random.nextInt(SUBCATEGORIES.length);
                    int brandIdx = random.nextInt(BRANDS.length);

                    double cost = 10 + random.nextDouble() * 90;
                    double price = cost * (1.2 + random.nextDouble() * 0.5);

                    ps.setInt(1, i);
                    ps.setString(2, "P" + String.format("%06d", i));
                    ps.setString(3, CATEGORIES[catIdx] + " Item " + i);
                    ps.setString(4, "Description for product " + i);
                    ps.setInt(5, catIdx + 1);
                    ps.setString(6, CATEGORIES[catIdx]);
                    ps.setInt(7, subIdx + 1);
                    ps.setString(8, SUBCATEGORIES[subIdx]);
                    ps.setString(9, BRANDS[brandIdx]);
                    ps.setInt(10, random.nextInt(100) + 1);
                    ps.setString(11, "Supplier " + (random.nextInt(50) + 1));
                    ps.setDouble(12, cost);
                    ps.setDouble(13, price);

                    ps.addBatch();

                    if (i % config.batchSize == 0) {
                        ps.executeBatch();
                        connection.commit();
                    }
                }

                ps.executeBatch();
                connection.commit();
                System.out.println(" " + numProducts + " rows");
            }
        }

        private void populateCustomerDimension() throws SQLException {
            System.out.print("Populating DIM_CUSTOMER...");

            String sql = "INSERT INTO DIM_CUSTOMER (CUSTOMER_KEY, CUSTOMER_ID, FIRST_NAME, " +
                "LAST_NAME, EMAIL, CITY, STATE, COUNTRY, REGION, CUSTOMER_SEGMENT, " +
                "LOYALTY_TIER, STATUS) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, 'USA', ?, ?, ?, 'ACTIVE')";

            int numCustomers = 10000 * config.scale;

            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                for (int i = 1; i <= numCustomers; i++) {
                    String firstName = FIRST_NAMES[random.nextInt(FIRST_NAMES.length)];
                    String lastName = LAST_NAMES[random.nextInt(LAST_NAMES.length)];
                    int cityIdx = random.nextInt(CITIES.length);

                    ps.setInt(1, i);
                    ps.setString(2, "C" + String.format("%08d", i));
                    ps.setString(3, firstName);
                    ps.setString(4, lastName);
                    ps.setString(5, firstName.toLowerCase() + "." + lastName.toLowerCase() + i + "@email.com");
                    ps.setString(6, CITIES[cityIdx]);
                    ps.setString(7, STATES[cityIdx]);
                    ps.setString(8, REGIONS[random.nextInt(REGIONS.length)]);
                    ps.setString(9, SEGMENTS[random.nextInt(SEGMENTS.length)]);
                    ps.setString(10, TIERS[random.nextInt(TIERS.length)]);

                    ps.addBatch();

                    if (i % config.batchSize == 0) {
                        ps.executeBatch();
                        connection.commit();
                    }
                }

                ps.executeBatch();
                connection.commit();
                System.out.println(" " + numCustomers + " rows");
            }
        }

        private void populateStoreDimension() throws SQLException {
            System.out.print("Populating DIM_STORE...");

            String sql = "INSERT INTO DIM_STORE (STORE_KEY, STORE_ID, STORE_NAME, STORE_TYPE, " +
                "CITY, STATE, COUNTRY, REGION, STATUS) " +
                "VALUES (?, ?, ?, ?, ?, ?, 'USA', ?, 'OPEN')";

            int numStores = 100 * config.scale;

            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                for (int i = 1; i <= numStores; i++) {
                    int cityIdx = random.nextInt(CITIES.length);

                    ps.setInt(1, i);
                    ps.setString(2, "S" + String.format("%04d", i));
                    ps.setString(3, CITIES[cityIdx] + " Store #" + i);
                    ps.setString(4, STORE_TYPES[random.nextInt(STORE_TYPES.length)]);
                    ps.setString(5, CITIES[cityIdx]);
                    ps.setString(6, STATES[cityIdx]);
                    ps.setString(7, REGIONS[random.nextInt(REGIONS.length)]);

                    ps.addBatch();

                    if (i % config.batchSize == 0) {
                        ps.executeBatch();
                        connection.commit();
                    }
                }

                ps.executeBatch();
                connection.commit();
                System.out.println(" " + numStores + " rows");
            }
        }

        private void populatePromotionDimension() throws SQLException {
            System.out.print("Populating DIM_PROMOTION...");

            String sql = "INSERT INTO DIM_PROMOTION (PROMOTION_KEY, PROMOTION_ID, PROMOTION_NAME, " +
                "PROMOTION_TYPE, DISCOUNT_PERCENT, START_DATE, END_DATE, STATUS) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, 'ACTIVE')";

            int numPromos = 50 * config.scale;

            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                java.util.Calendar cal = java.util.Calendar.getInstance();

                for (int i = 1; i <= numPromos; i++) {
                    cal.set(2020 + random.nextInt(5), random.nextInt(12), random.nextInt(28) + 1);
                    java.sql.Date startDate = new java.sql.Date(cal.getTimeInMillis());
                    cal.add(java.util.Calendar.DAY_OF_YEAR, 7 + random.nextInt(30));
                    java.sql.Date endDate = new java.sql.Date(cal.getTimeInMillis());

                    String promoType = PROMO_TYPES[random.nextInt(PROMO_TYPES.length)];

                    ps.setInt(1, i);
                    ps.setString(2, "PR" + String.format("%04d", i));
                    ps.setString(3, promoType + " Promo #" + i);
                    ps.setString(4, promoType);
                    ps.setDouble(5, 5 + random.nextInt(46));
                    ps.setDate(6, startDate);
                    ps.setDate(7, endDate);

                    ps.addBatch();

                    if (i % config.batchSize == 0) {
                        ps.executeBatch();
                        connection.commit();
                    }
                }

                ps.executeBatch();
                connection.commit();
                System.out.println(" " + numPromos + " rows");
            }
        }

        private void populateSalesFact() throws SQLException {
            System.out.print("Populating FACT_SALES...");

            // Get dimension key ranges
            int maxDateKey, minDateKey, maxProductKey, maxCustomerKey, maxStoreKey, maxPromoKey;

            try (Statement stmt = connection.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT MIN(DATE_KEY), MAX(DATE_KEY) FROM DIM_DATE");
                rs.next();
                minDateKey = rs.getInt(1);
                maxDateKey = rs.getInt(2);
                rs.close();

                rs = stmt.executeQuery("SELECT MAX(PRODUCT_KEY) FROM DIM_PRODUCT");
                rs.next();
                maxProductKey = rs.getInt(1);
                rs.close();

                rs = stmt.executeQuery("SELECT MAX(CUSTOMER_KEY) FROM DIM_CUSTOMER");
                rs.next();
                maxCustomerKey = rs.getInt(1);
                rs.close();

                rs = stmt.executeQuery("SELECT MAX(STORE_KEY) FROM DIM_STORE");
                rs.next();
                maxStoreKey = rs.getInt(1);
                rs.close();

                rs = stmt.executeQuery("SELECT MAX(PROMOTION_KEY) FROM DIM_PROMOTION");
                rs.next();
                maxPromoKey = rs.getInt(1);
                rs.close();
            }

            String sql = "INSERT INTO FACT_SALES (SALES_KEY, DATE_KEY, PRODUCT_KEY, CUSTOMER_KEY, " +
                "STORE_KEY, PROMOTION_KEY, TRANSACTION_ID, QUANTITY, UNIT_PRICE, UNIT_COST, " +
                "DISCOUNT_AMOUNT, SALES_AMOUNT, COST_AMOUNT, PROFIT_AMOUNT) " +
                "VALUES (" + nextval("SEQ_SALES") + ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            int numSales = 100000 * config.scale;
            int dateRange = maxDateKey - minDateKey;

            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                for (int i = 1; i <= numSales; i++) {
                    // Generate random date within range (simplified - doesn't handle month lengths)
                    int dateOffset = random.nextInt(Math.max(1, dateRange));
                    int dateKey = minDateKey + dateOffset;

                    int productKey = random.nextInt(maxProductKey) + 1;
                    int customerKey = random.nextInt(maxCustomerKey) + 1;
                    int storeKey = random.nextInt(maxStoreKey) + 1;
                    Integer promoKey = random.nextInt(10) < 3 ? random.nextInt(maxPromoKey) + 1 : null;

                    int quantity = random.nextInt(10) + 1;
                    double unitCost = 10 + random.nextDouble() * 90;
                    double unitPrice = unitCost * (1.2 + random.nextDouble() * 0.5);
                    double discount = random.nextInt(10) < 2 ? unitPrice * quantity * random.nextDouble() * 0.2 : 0;
                    double salesAmount = unitPrice * quantity - discount;
                    double costAmount = unitCost * quantity;
                    double profitAmount = salesAmount - costAmount;

                    ps.setInt(1, dateKey);
                    ps.setInt(2, productKey);
                    ps.setInt(3, customerKey);
                    ps.setInt(4, storeKey);
                    if (promoKey != null) {
                        ps.setInt(5, promoKey);
                    } else {
                        ps.setNull(5, Types.INTEGER);
                    }
                    ps.setString(6, "TXN" + String.format("%010d", i));
                    ps.setInt(7, quantity);
                    ps.setDouble(8, unitPrice);
                    ps.setDouble(9, unitCost);
                    ps.setDouble(10, discount);
                    ps.setDouble(11, salesAmount);
                    ps.setDouble(12, costAmount);
                    ps.setDouble(13, profitAmount);

                    ps.addBatch();

                    if (i % config.batchSize == 0) {
                        ps.executeBatch();
                        connection.commit();
                        System.out.print("\rPopulating FACT_SALES... " + i + "/" + numSales);
                    }
                }

                ps.executeBatch();
                connection.commit();
                System.out.println("\rPopulating FACT_SALES... " + numSales + " rows");
            }
        }

        private void populateInventoryFact() throws SQLException {
            System.out.print("Populating FACT_INVENTORY...");

            // Get dimension key ranges
            int maxDateKey, minDateKey, maxProductKey, maxStoreKey;

            try (Statement stmt = connection.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT MIN(DATE_KEY), MAX(DATE_KEY) FROM DIM_DATE");
                rs.next();
                minDateKey = rs.getInt(1);
                maxDateKey = rs.getInt(2);
                rs.close();

                rs = stmt.executeQuery("SELECT MAX(PRODUCT_KEY) FROM DIM_PRODUCT");
                rs.next();
                maxProductKey = rs.getInt(1);
                rs.close();

                rs = stmt.executeQuery("SELECT MAX(STORE_KEY) FROM DIM_STORE");
                rs.next();
                maxStoreKey = rs.getInt(1);
                rs.close();
            }

            String sql = "INSERT INTO FACT_INVENTORY (INVENTORY_KEY, DATE_KEY, PRODUCT_KEY, " +
                "STORE_KEY, QUANTITY_ON_HAND, QUANTITY_ON_ORDER, REORDER_POINT, INVENTORY_VALUE) " +
                "VALUES (" + nextval("SEQ_INVENTORY") + ", ?, ?, ?, ?, ?, ?, ?)";

            // Generate weekly snapshots
            int weeksOfData = 52 * config.scale;
            int dateKey = minDateKey;
            int count = 0;

            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                for (int w = 0; w < weeksOfData && dateKey <= maxDateKey; w++) {
                    for (int p = 1; p <= maxProductKey; p++) {
                        for (int s = 1; s <= maxStoreKey; s++) {
                            int onHand = random.nextInt(1000);
                            int onOrder = random.nextInt(10) < 3 ? random.nextInt(500) : 0;
                            int reorderPoint = 50 + random.nextInt(100);
                            double value = onHand * (10 + random.nextDouble() * 90);

                            ps.setInt(1, dateKey);
                            ps.setInt(2, p);
                            ps.setInt(3, s);
                            ps.setInt(4, onHand);
                            ps.setInt(5, onOrder);
                            ps.setInt(6, reorderPoint);
                            ps.setDouble(7, value);

                            ps.addBatch();
                            count++;

                            if (count % config.batchSize == 0) {
                                ps.executeBatch();
                                connection.commit();
                            }
                        }
                    }
                    dateKey += 7; // Weekly snapshots
                }

                ps.executeBatch();
                connection.commit();
                System.out.println(" " + count + " rows");
            }
        }

        private void populateAggregates() throws SQLException {
            System.out.print("Populating AGG_SALES_DAILY...");

            String sql = "INSERT INTO AGG_SALES_DAILY (DATE_KEY, STORE_KEY, PRODUCT_KEY, " +
                "TOTAL_QUANTITY, TOTAL_SALES, TOTAL_COST, TOTAL_PROFIT, TOTAL_DISCOUNT, " +
                "TRANSACTION_COUNT, AVG_UNIT_PRICE) " +
                "SELECT DATE_KEY, STORE_KEY, PRODUCT_KEY, SUM(QUANTITY), SUM(SALES_AMOUNT), " +
                "SUM(COST_AMOUNT), SUM(PROFIT_AMOUNT), SUM(DISCOUNT_AMOUNT), COUNT(*), " +
                "AVG(UNIT_PRICE) " +
                "FROM FACT_SALES GROUP BY DATE_KEY, STORE_KEY, PRODUCT_KEY";

            try (Statement stmt = connection.createStatement()) {
                int rows = stmt.executeUpdate(sql);
                connection.commit();
                System.out.println(" " + rows + " rows");
            }

            System.out.print("Populating AGG_SALES_MONTHLY...");

            sql = "INSERT INTO AGG_SALES_MONTHLY (YEAR_NUM, MONTH_NUM, STORE_KEY, CATEGORY_ID, " +
                "TOTAL_QUANTITY, TOTAL_SALES, TOTAL_COST, TOTAL_PROFIT, TRANSACTION_COUNT, " +
                "UNIQUE_CUSTOMERS) " +
                "SELECT d.YEAR_NUM, d.MONTH_NUM, f.STORE_KEY, p.CATEGORY_ID, " +
                "SUM(f.QUANTITY), SUM(f.SALES_AMOUNT), SUM(f.COST_AMOUNT), " +
                "SUM(f.PROFIT_AMOUNT), COUNT(*), COUNT(DISTINCT f.CUSTOMER_KEY) " +
                "FROM FACT_SALES f " +
                "JOIN DIM_DATE d ON f.DATE_KEY = d.DATE_KEY " +
                "JOIN DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY " +
                "GROUP BY d.YEAR_NUM, d.MONTH_NUM, f.STORE_KEY, p.CATEGORY_ID";

            try (Statement stmt = connection.createStatement()) {
                int rows = stmt.executeUpdate(sql);
                connection.commit();
                System.out.println(" " + rows + " rows");
            }
        }
    }

    // ========================================================================
    // Main Entry Point
    // ========================================================================

    public static void main(String[] args) {
        Config config = parseArgs(args);

        if (config == null) {
            printHelp();
            System.exit(1);
        }

        if (!config.validate()) {
            printHelp();
            System.exit(1);
        }

        try {
            if (config.mode.equals("populate")) {
                runPopulate(config);
            } else {
                runWorkload(config);
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static Config parseArgs(String[] args) {
        Config config = new Config();

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            if (arg.equals("-h") || arg.equals("-help")) {
                return null;
            }

            if (arg.startsWith("-") && i + 1 < args.length) {
                String value = args[++i];

                switch (arg) {
                    case "-mode": config.mode = value; break;
                    case "-threads": config.threads = Integer.parseInt(value); break;
                    case "-duration": config.duration = Integer.parseInt(value); break;
                    case "-url": config.url = value; break;
                    case "-uid": config.uid = value; break;
                    case "-pwd": config.pwd = value; break;
                    case "-batchSize": config.batchSize = Integer.parseInt(value); break;
                    case "-scale": config.scale = Integer.parseInt(value); break;
                    case "-pctPoint": config.pctPoint = Integer.parseInt(value); break;
                    case "-pctRange": config.pctRange = Integer.parseInt(value); break;
                    case "-pctAgg": config.pctAgg = Integer.parseInt(value); break;
                    case "-pctJoin": config.pctJoin = Integer.parseInt(value); break;
                    case "-pctAnalytic": config.pctAnalytic = Integer.parseInt(value); break;
                    case "-pctDml": config.pctDml = Integer.parseInt(value); break;
                    default:
                        System.err.println("Unknown option: " + arg);
                        return null;
                }
            } else if (arg.startsWith("-")) {
                System.err.println("Missing value for option: " + arg);
                return null;
            }
        }

        return config;
    }

    private static void printHelp() {
        System.out.println();
        System.out.println("Data Warehouse Workload Simulation");
        System.out.println("===================================");
        System.out.println();
        System.out.println("Usage: java DWWork [options]");
        System.out.println();
        System.out.println("Required Options:");
        System.out.println("  -mode <populate|workload>  Operation mode");
        System.out.println("  -url <jdbc-url>            JDBC URL (oracle/postgresql/mysql detected from prefix)");
        System.out.println();
        System.out.println("Connection Options:");
        System.out.println("  -uid <username>            Database username");
        System.out.println("  -pwd <password>            Database password");
        System.out.println();
        System.out.println("Populate Mode Options:");
        System.out.println("  -batchSize <n>             Batch size for inserts (default: 1000)");
        System.out.println("  -scale <n>                 Data scale factor (default: 1)");
        System.out.println("                             scale=1: ~100K sales, 1K products, 10K customers");
        System.out.println();
        System.out.println("Workload Mode Options:");
        System.out.println("  -threads <n>               Number of worker threads (default: 4)");
        System.out.println("  -duration <seconds>        Workload duration in seconds (default: 60)");
        System.out.println();
        System.out.println("Transaction Mix (must sum to 100):");
        System.out.println("  -pctPoint <n>              Point query percentage (default: 10)");
        System.out.println("  -pctRange <n>              Range scan percentage (default: 20)");
        System.out.println("  -pctAgg <n>                Aggregation query percentage (default: 30)");
        System.out.println("  -pctJoin <n>               Join query percentage (default: 25)");
        System.out.println("  -pctAnalytic <n>           Complex analytic percentage (default: 10)");
        System.out.println("  -pctDml <n>                DML operation percentage (default: 5)");
        System.out.println();
        System.out.println("Help:");
        System.out.println("  -h, -help                  Show this help message");
        System.out.println();
        System.out.println("Examples:");
        System.out.println();
        System.out.println("  # Populate with default scale");
        System.out.println("  java DWWork -mode populate -url jdbc:postgresql://host:5432/db -uid admin -pwd secret");
        System.out.println();
        System.out.println("  # Run workload with 8 threads for 5 minutes");
        System.out.println("  java DWWork -mode workload -url jdbc:oracle:thin:@host:1521:sid -uid admin -pwd secret \\");
        System.out.println("       -threads 8 -duration 300");
        System.out.println();
        System.out.println("  # Custom transaction mix (heavy aggregation)");
        System.out.println("  java DWWork -mode workload -url jdbc:mysql://host:3306/db -uid admin -pwd secret \\");
        System.out.println("       -pctPoint 5 -pctRange 10 -pctAgg 50 -pctJoin 20 -pctAnalytic 10 -pctDml 5");
        System.out.println();
    }

    private static void runPopulate(Config config) throws Exception {
        System.out.println();
        System.out.println("=== Data Warehouse Population ===");
        System.out.println("URL:        " + config.url);
        System.out.println("Scale:      " + config.scale);
        System.out.println("Batch Size: " + config.batchSize);
        System.out.println();

        DataPopulator populator = new DataPopulator(config);
        populator.populate();
    }

    private static void runWorkload(Config config) throws Exception {
        System.out.println();
        System.out.println("=== Data Warehouse Workload ===");
        System.out.println("URL:        " + config.url);
        System.out.println("Threads:    " + config.threads);
        System.out.println("Duration:   " + config.duration + " seconds");
        System.out.println();
        System.out.println("Transaction Mix:");
        System.out.println("  Point Queries:   " + config.pctPoint + "%");
        System.out.println("  Range Scans:     " + config.pctRange + "%");
        System.out.println("  Aggregations:    " + config.pctAgg + "%");
        System.out.println("  Join Queries:    " + config.pctJoin + "%");
        System.out.println("  Analytics:       " + config.pctAnalytic + "%");
        System.out.println("  DML Operations:  " + config.pctDml + "%");
        System.out.println();

        Metrics metrics = new Metrics();
        AtomicBoolean running = new AtomicBoolean(true);

        // Create and start worker threads
        ExecutorService executor = Executors.newFixedThreadPool(config.threads);
        List<Future<?>> futures = new ArrayList<>();

        System.out.println("Starting " + config.threads + " worker threads...");

        for (int i = 0; i < config.threads; i++) {
            WorkloadThread worker = new WorkloadThread(i, config, metrics, running);
            futures.add(executor.submit(worker));
        }

        // Wait for threads to initialize
        Thread.sleep(2000);

        System.out.println("\nRunning workload for " + config.duration + " seconds...");
        System.out.println();

        // Progress reporting
        long startTime = System.currentTimeMillis();
        long endTime = startTime + (config.duration * 1000L);

        while (System.currentTimeMillis() < endTime) {
            Thread.sleep(5000);
            long elapsed = (System.currentTimeMillis() - startTime) / 1000;
            long txns = metrics.totalTransactions.sum();
            long errs = metrics.totalErrors.sum();
            double tps = elapsed > 0 ? (double) txns / elapsed : 0;

            System.out.printf("Progress: %d/%d sec | Transactions: %,d | Errors: %,d | TPS: %.2f%n",
                elapsed, config.duration, txns, errs, tps);
        }

        // Stop workers
        running.set(false);
        System.out.println("\nStopping workers...");

        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        // Print final report
        long totalElapsed = (System.currentTimeMillis() - startTime) / 1000;
        metrics.printReport(totalElapsed);
    }
}
