package com.awsbench.workloads.airwork;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * AirWork - Aircraft Tracking System JSON Workload
 *
 * A high-performance workload generator that operates on JSON documents
 * simulating a real-time aircraft tracking system. Supports Oracle,
 * PostgreSQL and MySQL (including their AWS RDS/Aurora managed variants)
 * using each engine's native JSON / JSONB column types.
 *
 * Usage:
 *   java AirWork -url <jdbc_url> -uid <user> -pwd <password> -populate [-count <n>]
 *   java AirWork -url <jdbc_url> -uid <user> -pwd <password> -run [-threads <n>] [-duration <secs>]
 */
public class AirWork {

    // Supported database engines
    static final int DB_ORACLE     = 0;
    static final int DB_POSTGRESQL = 1;
    static final int DB_MYSQL      = 2;

    // Command line arguments
    private String jdbcUrl;
    private String username;
    private String password;
    private int dbms;
    private int threadCount = 1;
    private int duration = 60;
    private int populateCount = 1000;
    private boolean populateMode = false;
    private boolean runMode = false;

    // Statistics
    private final AtomicLong totalTransactions = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    private final AtomicLong[] txnCounts = new AtomicLong[TransactionType.values().length];
    private volatile boolean running = true;

    // Data generators
    private static final Random random = new Random();
    private static final String[] AIRLINES = {
        "UAL", "DAL", "AAL", "SWA", "JBU", "ASA", "FFT", "SKW", "RPA", "ENY"
    };
    private static final String[] AIRLINE_NAMES = {
        "United Airlines", "Delta Air Lines", "American Airlines", "Southwest Airlines",
        "JetBlue Airways", "Alaska Airlines", "Frontier Airlines", "SkyWest Airlines",
        "Republic Airways", "Envoy Air"
    };
    private static final String[] AIRCRAFT_TYPES = {
        "B738", "A320", "B77W", "A321", "B739", "E175", "A20N", "B737", "CRJ9", "E190"
    };
    private static final String[] MANUFACTURERS = {
        "Boeing", "Airbus", "Boeing", "Airbus", "Boeing", "Embraer", "Airbus", "Boeing", "Bombardier", "Embraer"
    };
    private static final String[] AIRPORTS = {
        "KJFK", "KLAX", "KORD", "KDFW", "KDEN", "KATL", "KSFO", "KLAS", "KMIA", "KBOS",
        "KSEA", "KMSP", "KPHX", "KDTW", "KEWR", "KIAH", "KMCO", "KFLL", "KBWI", "KDCA"
    };
    private static final String[] ALERT_TYPES = {
        "AIRSPACE_VIOLATION", "ALTITUDE_DEVIATION", "TCAS_RA", "SQUAWK_CHANGE",
        "EMERGENCY_DECLARED", "COMMUNICATION_LOST", "IDENT_ACTIVATED"
    };
    private static final String[] SEVERITIES = {"LOW", "MEDIUM", "HIGH", "CRITICAL"};
    private static final String[] HAZARD_TYPES = {
        "TURBULENCE", "ICING", "THUNDERSTORM", "VOLCANIC_ASH", "WIND_SHEAR", "LOW_VISIBILITY"
    };
    private static final String[] RADAR_SITES = {
        "ASR-11-JFK", "ASR-9-LAX", "ASR-11-ORD", "ASR-9-DFW", "ASR-11-DEN",
        "ASR-9-ATL", "ASR-11-SFO", "ASR-9-LAS", "ASR-11-MIA", "ASR-9-BOS"
    };

    // Transaction types with weights (probability distribution)
    enum TransactionType {
        // Queries (60% of workload)
        QUERY_POSITION_BY_ICAO(15),          // Use idx_position_icao24
        QUERY_FLIGHTS_BY_DEPARTURE(10),       // Use idx_flight_departure
        QUERY_FLIGHTS_BY_ARRIVAL(10),         // Use idx_flight_arrival
        QUERY_ALERTS_BY_SEVERITY(8),          // Use idx_alert_severity
        QUERY_ALERTS_BY_TYPE(7),              // Use idx_alert_type
        QUERY_HAZARDS_BY_TYPE(5),             // Use idx_hazard_type
        QUERY_RADAR_BY_SITE(5),               // Use idx_radar_site

        // DML Operations (40% of workload)
        INSERT_POSITION(12),                  // High-frequency position updates
        UPDATE_POSITION(10),                  // Update existing position
        INSERT_ALERT(5),                      // New alerts
        UPDATE_ALERT_RESOLUTION(3),           // Resolve alerts
        INSERT_RADAR_CONTACT(5),              // Radar returns
        INSERT_WEATHER_HAZARD(2),             // Weather reports
        UPDATE_FLIGHT_STATUS(3);              // Flight plan updates

        final int weight;
        TransactionType(int weight) { this.weight = weight; }
    }

    // Cumulative weights for transaction selection
    private static final int[] cumulativeWeights;
    private static final int totalWeight;
    static {
        TransactionType[] types = TransactionType.values();
        cumulativeWeights = new int[types.length];
        int sum = 0;
        for (int i = 0; i < types.length; i++) {
            sum += types[i].weight;
            cumulativeWeights[i] = sum;
        }
        totalWeight = sum;
    }

    public static void main(String[] args) {
        AirWork workload = new AirWork();
        try {
            workload.parseArgs(args);
            workload.run();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void parseArgs(String[] args) throws ClassNotFoundException {
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-url":
                    jdbcUrl = args[++i];
                    break;
                case "-uid":
                    username = args[++i];
                    break;
                case "-pwd":
                    password = args[++i];
                    break;
                case "-threads":
                    threadCount = Integer.parseInt(args[++i]);
                    break;
                case "-duration":
                    duration = Integer.parseInt(args[++i]);
                    break;
                case "-populate":
                    populateMode = true;
                    break;
                case "-run":
                    runMode = true;
                    break;
                case "-count":
                    populateCount = Integer.parseInt(args[++i]);
                    break;
                default:
                    printUsage();
                    throw new IllegalArgumentException("Unknown argument: " + args[i]);
            }
        }

        if (jdbcUrl == null) {
            printUsage();
            throw new IllegalArgumentException("Missing required argument: -url");
        }
        if (username == null) {
            printUsage();
            throw new IllegalArgumentException("Missing required argument: -uid");
        }
        if (password == null) {
            printUsage();
            throw new IllegalArgumentException("Missing required argument: -pwd");
        }
        if (!populateMode && !runMode) {
            printUsage();
            throw new IllegalArgumentException("Must specify either -populate or -run");
        }

        detectDialect();
    }

    private void detectDialect() throws ClassNotFoundException {
        if (jdbcUrl.startsWith("jdbc:oracle:"))
            dbms = DB_ORACLE;
        else if (jdbcUrl.startsWith("jdbc:postgresql:"))
            dbms = DB_POSTGRESQL;
        else if (jdbcUrl.startsWith("jdbc:mysql:"))
            dbms = DB_MYSQL;
        else
            throw new IllegalArgumentException("Unsupported or unrecognized JDBC URL: " + jdbcUrl);

        switch (dbms) {
            case DB_ORACLE:     Class.forName("oracle.jdbc.OracleDriver"); break;
            case DB_POSTGRESQL: Class.forName("org.postgresql.Driver"); break;
            case DB_MYSQL:      Class.forName("com.mysql.cj.jdbc.Driver"); break;
        }
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    // =========================================================================
    // Dialect helpers
    // =========================================================================

    /** ANSI row-limiting clause: Oracle uses FETCH FIRST, Postgres/MySQL use LIMIT. */
    private String limitSuffix(int n) {
        return (dbms == DB_ORACLE) ? "FETCH FIRST " + n + " ROWS ONLY" : "LIMIT " + n;
    }

    /**
     * Returns a SQL predicate fragment that extracts a scalar value at the given
     * dotted JSON path (e.g. "airports.departure.icao") from a JSON/JSONB column.
     *
     * Oracle and MySQL support JSON_VALUE natively. PostgreSQL only gained
     * JSON_VALUE in version 17 (which Aurora PostgreSQL may not yet support),
     * so on Postgres we use jsonb path operators instead.
     */
    private String jsonExtract(String column, String dottedPath) {
        switch (dbms) {
            case DB_POSTGRESQL: {
                String[] parts = dottedPath.split("\\.");
                if (parts.length == 1) {
                    return column + "->>'" + parts[0] + "'";
                }
                StringBuilder path = new StringBuilder();
                for (int i = 0; i < parts.length; i++) {
                    if (i > 0) path.append(',');
                    path.append(parts[i]);
                }
                return column + "#>>'{" + path + "}'";
            }
            case DB_ORACLE:
            case DB_MYSQL:
            default:
                return "JSON_VALUE(" + column + ", '$." + dottedPath + "')";
        }
    }

    /**
     * Bind placeholder for a JSON/JSONB column. PostgreSQL's JDBC driver sends
     * String parameters as "unknown"/varchar and won't implicitly cast them to
     * jsonb, so the placeholder must carry an explicit cast on that dialect.
     */
    private String jsonParam() {
        return (dbms == DB_POSTGRESQL) ? "?::jsonb" : "?";
    }

    /** True if the SQLException represents a unique-constraint / duplicate-key violation. */
    private static boolean isUniqueViolation(SQLException e) {
        String sqlState = e.getSQLState();
        if (sqlState == null) return false;
        // 23505 = PostgreSQL unique_violation; 23000 = MySQL/Oracle integrity constraint violation
        return sqlState.equals("23505") || sqlState.equals("23000");
    }

    private void printUsage() {
        System.out.println("Usage:");
        System.out.println("  java AirWork -url <jdbc_url> -uid <user> -pwd <password> -populate [-count <n>]");
        System.out.println("  java AirWork -url <jdbc_url> -uid <user> -pwd <password> -run [-threads <n>] [-duration <secs>]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -url         JDBC connection URL (required)");
        System.out.println("  -uid         Database username (required)");
        System.out.println("  -pwd         Database password (required)");
        System.out.println("  -populate    Populate the database with sample data");
        System.out.println("  -run         Run the transaction workload");
        System.out.println("  -count       Number of records per table for populate (default: 1000)");
        System.out.println("  -threads     Number of worker threads (default: 1)");
        System.out.println("  -duration    Workload duration in seconds (default: 60)");
    }

    private void run() throws Exception {
        // Initialize transaction counters
        for (int i = 0; i < txnCounts.length; i++) {
            txnCounts[i] = new AtomicLong(0);
        }

        if (populateMode) {
            populateDatabase();
        } else if (runMode) {
            runWorkload();
        }
    }

    // =========================================================================
    // Database Population
    // =========================================================================

    private void populateDatabase() throws Exception {
        System.out.println("Populating database with " + populateCount + " records per table...");
        long startTime = System.currentTimeMillis();

        try (Connection schemaConn = getConnection()) {
            new AirWorkSchema(schemaConn, dbms).create();
        }

        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);

            populateAircraftRegistry(conn);
            populateAircraftPositions(conn);
            populateFlightPlans(conn);
            populateAirspaceAlerts(conn);
            populateRadarContacts(conn);
            populateWeatherHazards(conn);

            conn.commit();

            // Update statistics for all tables
            updateStatistics(conn);
        }

        long elapsed = System.currentTimeMillis() - startTime;
        System.out.println("Population complete in " + elapsed + " ms");
    }

    private void updateStatistics(Connection conn) throws SQLException {
        String[] tables = {
            "aircraft_registry",
            "aircraft_position",
            "flight_plan",
            "airspace_alert",
            "radar_contact",
            "weather_hazard"
        };

        System.out.println("Updating table statistics...");
        try (Statement stmt = conn.createStatement()) {
            for (String table : tables) {
                switch (dbms) {
                    case DB_ORACLE:
                        stmt.execute("BEGIN DBMS_STATS.GATHER_TABLE_STATS(USER, '" + table + "'); END;");
                        break;
                    case DB_POSTGRESQL:
                        stmt.execute("ANALYZE " + table);
                        break;
                    case DB_MYSQL:
                        stmt.execute("ANALYZE TABLE " + table);
                        break;
                }
                System.out.println("  Updated statistics for " + table);
            }
        }
        conn.commit();
    }

    private void populateAircraftRegistry(Connection conn) throws SQLException {
        String sql = "INSERT INTO aircraft_registry (aircraft_id, aircraft_data) VALUES (?, " + jsonParam() + ")";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < populateCount; i++) {
                String id = String.format("AC-%010d", i + 1);
                String json = generateAircraftRegistryJson(i);
                ps.setString(1, id);
                ps.setString(2, json);
                ps.executeUpdate();

                if ((i + 1) % 1000 == 0) {
                    conn.commit();
                    System.out.println("  aircraft_registry: " + (i + 1) + " rows");
                }
            }
            conn.commit();
        }
        System.out.println("  aircraft_registry: " + populateCount + " rows (complete)");
    }

    private void populateAircraftPositions(Connection conn) throws SQLException {
        String sql = "INSERT INTO aircraft_position (track_id, position_data) VALUES (?, " + jsonParam() + ")";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < populateCount; i++) {
                String id = String.format("TRK-%012d", i + 1);
                String icao24 = generateIcao24(i);
                String json = generatePositionJson(icao24, i);
                ps.setString(1, id);
                ps.setString(2, json);
                ps.executeUpdate();

                if ((i + 1) % 1000 == 0) {
                    conn.commit();
                    System.out.println("  aircraft_position: " + (i + 1) + " rows");
                }
            }
            conn.commit();
        }
        System.out.println("  aircraft_position: " + populateCount + " rows (complete)");
    }

    private void populateFlightPlans(Connection conn) throws SQLException {
        String sql = "INSERT INTO flight_plan (flight_id, plan_data) VALUES (?, " + jsonParam() + ")";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < populateCount; i++) {
                String id = String.format("FLT-%012d", i + 1);
                String json = generateFlightPlanJson(i);
                ps.setString(1, id);
                ps.setString(2, json);
                ps.executeUpdate();

                if ((i + 1) % 1000 == 0) {
                    conn.commit();
                    System.out.println("  flight_plan: " + (i + 1) + " rows");
                }
            }
            conn.commit();
        }
        System.out.println("  flight_plan: " + populateCount + " rows (complete)");
    }

    private void populateAirspaceAlerts(Connection conn) throws SQLException {
        String sql = "INSERT INTO airspace_alert (alert_id, alert_data) VALUES (?, " + jsonParam() + ")";
        int alertCount = populateCount / 10; // Fewer alerts than positions
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < alertCount; i++) {
                String id = String.format("ALT-%012d", i + 1);
                String json = generateAlertJson(i);
                ps.setString(1, id);
                ps.setString(2, json);
                ps.executeUpdate();

                if ((i + 1) % 1000 == 0) {
                    conn.commit();
                    System.out.println("  airspace_alert: " + (i + 1) + " rows");
                }
            }
            conn.commit();
        }
        System.out.println("  airspace_alert: " + alertCount + " rows (complete)");
    }

    private void populateRadarContacts(Connection conn) throws SQLException {
        String sql = "INSERT INTO radar_contact (contact_id, contact_data) VALUES (?, " + jsonParam() + ")";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < populateCount; i++) {
                String id = String.format("RDR-%012d", i + 1);
                String json = generateRadarContactJson(i);
                ps.setString(1, id);
                ps.setString(2, json);
                ps.executeUpdate();

                if ((i + 1) % 1000 == 0) {
                    conn.commit();
                    System.out.println("  radar_contact: " + (i + 1) + " rows");
                }
            }
            conn.commit();
        }
        System.out.println("  radar_contact: " + populateCount + " rows (complete)");
    }

    private void populateWeatherHazards(Connection conn) throws SQLException {
        String sql = "INSERT INTO weather_hazard (hazard_id, hazard_data) VALUES (?, " + jsonParam() + ")";
        int hazardCount = populateCount / 20; // Fewer hazards
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < hazardCount; i++) {
                String id = String.format("WX-%012d", i + 1);
                String json = generateWeatherHazardJson(i);
                ps.setString(1, id);
                ps.setString(2, json);
                ps.executeUpdate();

                if ((i + 1) % 100 == 0) {
                    conn.commit();
                    System.out.println("  weather_hazard: " + (i + 1) + " rows");
                }
            }
            conn.commit();
        }
        System.out.println("  weather_hazard: " + hazardCount + " rows (complete)");
    }

    // =========================================================================
    // Workload Execution
    // =========================================================================

    private void runWorkload() throws Exception {
        System.out.println("Starting workload with " + threadCount + " threads for " + duration + " seconds...");
        System.out.println();

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<?>> futures = new ArrayList<>();

        // Start worker threads
        for (int i = 0; i < threadCount; i++) {
            futures.add(executor.submit(new WorkerThread(i)));
        }

        // Progress reporting thread
        Thread reporter = new Thread(() -> {
            long lastTotal = 0;
            while (running) {
                try {
                    Thread.sleep(5000);
                    long current = totalTransactions.get();
                    long delta = current - lastTotal;
                    double tps = delta / 5.0;
                    System.out.printf("Progress: %,d txns, %.1f TPS, %d errors%n",
                            current, tps, totalErrors.get());
                    lastTotal = current;
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        reporter.setDaemon(true);
        reporter.start();

        // Wait for duration
        Thread.sleep(duration * 1000L);
        running = false;

        // Shutdown
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        // Print results
        printResults();
    }

    private void printResults() {
        System.out.println();
        System.out.println("========================================");
        System.out.println("Workload Complete");
        System.out.println("========================================");
        System.out.println();
        System.out.printf("Total Transactions: %,d%n", totalTransactions.get());
        System.out.printf("Total Errors:       %,d%n", totalErrors.get());
        System.out.printf("Duration:           %d seconds%n", duration);
        System.out.printf("Throughput:         %.1f TPS%n", totalTransactions.get() / (double) duration);
        System.out.println();
        System.out.println("Transaction Breakdown:");
        System.out.println("----------------------------------------");
        for (TransactionType type : TransactionType.values()) {
            long count = txnCounts[type.ordinal()].get();
            double pct = totalTransactions.get() > 0 ?
                    (100.0 * count / totalTransactions.get()) : 0;
            System.out.printf("  %-30s %,10d (%5.1f%%)%n", type.name(), count, pct);
        }
        System.out.println("----------------------------------------");
    }

    // =========================================================================
    // Worker Thread
    // =========================================================================

    class WorkerThread implements Runnable {
        private final int threadId;
        private Connection conn;
        private final Map<TransactionType, PreparedStatement> statements = new EnumMap<>(TransactionType.class);

        // Cached IDs for updates (populated during queries)
        private final List<String> knownTrackIds = new ArrayList<>();
        private final List<String> knownAlertIds = new ArrayList<>();
        private final List<String> knownFlightIds = new ArrayList<>();

        WorkerThread(int threadId) {
            this.threadId = threadId;
        }

        @Override
        public void run() {
            try {
                conn = getConnection();
                conn.setAutoCommit(false);

                prepareStatements();

                // Pre-fetch some IDs for update operations
                prefetchIds();

                while (running) {
                    TransactionType type = selectTransaction();
                    try {
                        executeTransaction(type);
                        conn.commit();
                        totalTransactions.incrementAndGet();
                        txnCounts[type.ordinal()].incrementAndGet();
                    } catch (SQLException e) {
                        totalErrors.incrementAndGet();
                        try {
                            conn.rollback();
                        } catch (SQLException rollbackEx) {
                            // Ignore rollback errors
                        }
                        // Silently continue on unique-constraint violations (expected for inserts)
                        if (!isUniqueViolation(e)) {
                            System.err.println("Thread " + threadId + " error in " + type + ": " + e.getMessage());
                        }
                    }
                }

                closeStatements();
                conn.close();
            } catch (Exception e) {
                System.err.println("Thread " + threadId + " fatal error: " + e.getMessage());
                e.printStackTrace();
            }
        }

        private void prepareStatements() throws SQLException {
            // Query statements - using JSON indexes
            statements.put(TransactionType.QUERY_POSITION_BY_ICAO,
                    conn.prepareStatement(
                            "SELECT track_id, position_data FROM aircraft_position " +
                            "WHERE " + jsonExtract("position_data", "icao24") + " = ?"));

            statements.put(TransactionType.QUERY_FLIGHTS_BY_DEPARTURE,
                    conn.prepareStatement(
                            "SELECT flight_id, plan_data FROM flight_plan " +
                            "WHERE " + jsonExtract("plan_data", "airports.departure.icao") + " = ?"));

            statements.put(TransactionType.QUERY_FLIGHTS_BY_ARRIVAL,
                    conn.prepareStatement(
                            "SELECT flight_id, plan_data FROM flight_plan " +
                            "WHERE " + jsonExtract("plan_data", "airports.arrival.icao") + " = ?"));

            statements.put(TransactionType.QUERY_ALERTS_BY_SEVERITY,
                    conn.prepareStatement(
                            "SELECT alert_id, alert_data FROM airspace_alert " +
                            "WHERE " + jsonExtract("alert_data", "severity") + " = ?"));

            statements.put(TransactionType.QUERY_ALERTS_BY_TYPE,
                    conn.prepareStatement(
                            "SELECT alert_id, alert_data FROM airspace_alert " +
                            "WHERE " + jsonExtract("alert_data", "alertType") + " = ?"));

            statements.put(TransactionType.QUERY_HAZARDS_BY_TYPE,
                    conn.prepareStatement(
                            "SELECT hazard_id, hazard_data FROM weather_hazard " +
                            "WHERE " + jsonExtract("hazard_data", "hazardType") + " = ?"));

            statements.put(TransactionType.QUERY_RADAR_BY_SITE,
                    conn.prepareStatement(
                            "SELECT contact_id, contact_data FROM radar_contact " +
                            "WHERE " + jsonExtract("contact_data", "radarSite.id") + " = ?"));

            // DML statements
            statements.put(TransactionType.INSERT_POSITION,
                    conn.prepareStatement(
                            "INSERT INTO aircraft_position (track_id, position_data) VALUES (?, " + jsonParam() + ")"));

            statements.put(TransactionType.UPDATE_POSITION,
                    conn.prepareStatement(
                            "UPDATE aircraft_position SET position_data = " + jsonParam() + " " +
                            "WHERE track_id = ?"));

            statements.put(TransactionType.INSERT_ALERT,
                    conn.prepareStatement(
                            "INSERT INTO airspace_alert (alert_id, alert_data) VALUES (?, " + jsonParam() + ")"));

            statements.put(TransactionType.UPDATE_ALERT_RESOLUTION,
                    conn.prepareStatement(
                            "UPDATE airspace_alert SET alert_data = " + jsonParam() + " " +
                            "WHERE alert_id = ?"));

            statements.put(TransactionType.INSERT_RADAR_CONTACT,
                    conn.prepareStatement(
                            "INSERT INTO radar_contact (contact_id, contact_data) VALUES (?, " + jsonParam() + ")"));

            statements.put(TransactionType.INSERT_WEATHER_HAZARD,
                    conn.prepareStatement(
                            "INSERT INTO weather_hazard (hazard_id, hazard_data) VALUES (?, " + jsonParam() + ")"));

            statements.put(TransactionType.UPDATE_FLIGHT_STATUS,
                    conn.prepareStatement(
                            "UPDATE flight_plan SET plan_data = " + jsonParam() + " " +
                            "WHERE flight_id = ?"));

            // Prepare SELECT statements for read-modify-write updates
            prepareSelectStatements();
        }

        // Additional prepared statements for reading before update
        private PreparedStatement selectPositionStmt;
        private PreparedStatement selectAlertStmt;
        private PreparedStatement selectFlightStmt;

        private void prepareSelectStatements() throws SQLException {
            selectPositionStmt = conn.prepareStatement(
                    "SELECT position_data FROM aircraft_position WHERE track_id = ?");
            selectAlertStmt = conn.prepareStatement(
                    "SELECT alert_data FROM airspace_alert WHERE alert_id = ?");
            selectFlightStmt = conn.prepareStatement(
                    "SELECT plan_data FROM flight_plan WHERE flight_id = ?");
        }

        private void prefetchIds() throws SQLException {
            // Fetch some existing track IDs
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT track_id FROM aircraft_position " + limitSuffix(100))) {
                while (rs.next()) {
                    knownTrackIds.add(rs.getString(1));
                }
            }

            // Fetch some existing alert IDs
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT alert_id FROM airspace_alert " + limitSuffix(100))) {
                while (rs.next()) {
                    knownAlertIds.add(rs.getString(1));
                }
            }

            // Fetch some existing flight IDs
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT flight_id FROM flight_plan " + limitSuffix(100))) {
                while (rs.next()) {
                    knownFlightIds.add(rs.getString(1));
                }
            }
        }

        private void closeStatements() {
            for (PreparedStatement ps : statements.values()) {
                try {
                    if (ps != null) ps.close();
                } catch (SQLException ignored) {}
            }
            try { if (selectPositionStmt != null) selectPositionStmt.close(); } catch (SQLException ignored) {}
            try { if (selectAlertStmt != null) selectAlertStmt.close(); } catch (SQLException ignored) {}
            try { if (selectFlightStmt != null) selectFlightStmt.close(); } catch (SQLException ignored) {}
        }

        private TransactionType selectTransaction() {
            int r = random.nextInt(totalWeight);
            for (int i = 0; i < cumulativeWeights.length; i++) {
                if (r < cumulativeWeights[i]) {
                    return TransactionType.values()[i];
                }
            }
            return TransactionType.QUERY_POSITION_BY_ICAO;
        }

        private void executeTransaction(TransactionType type) throws SQLException {
            PreparedStatement ps = statements.get(type);

            switch (type) {
                case QUERY_POSITION_BY_ICAO:
                    executeQueryPositionByIcao(ps);
                    break;
                case QUERY_FLIGHTS_BY_DEPARTURE:
                    executeQueryFlightsByAirport(ps, true);
                    break;
                case QUERY_FLIGHTS_BY_ARRIVAL:
                    executeQueryFlightsByAirport(ps, false);
                    break;
                case QUERY_ALERTS_BY_SEVERITY:
                    executeQueryAlertsBySeverity(ps);
                    break;
                case QUERY_ALERTS_BY_TYPE:
                    executeQueryAlertsByType(ps);
                    break;
                case QUERY_HAZARDS_BY_TYPE:
                    executeQueryHazardsByType(ps);
                    break;
                case QUERY_RADAR_BY_SITE:
                    executeQueryRadarBySite(ps);
                    break;
                case INSERT_POSITION:
                    executeInsertPosition(ps);
                    break;
                case UPDATE_POSITION:
                    executeUpdatePosition(ps);
                    break;
                case INSERT_ALERT:
                    executeInsertAlert(ps);
                    break;
                case UPDATE_ALERT_RESOLUTION:
                    executeUpdateAlertResolution(ps);
                    break;
                case INSERT_RADAR_CONTACT:
                    executeInsertRadarContact(ps);
                    break;
                case INSERT_WEATHER_HAZARD:
                    executeInsertWeatherHazard(ps);
                    break;
                case UPDATE_FLIGHT_STATUS:
                    executeUpdateFlightStatus(ps);
                    break;
            }
        }

        // Query implementations
        private void executeQueryPositionByIcao(PreparedStatement ps) throws SQLException {
            String icao24 = generateIcao24(random.nextInt(populateCount));
            ps.setString(1, icao24);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String trackId = rs.getString(1);
                    if (knownTrackIds.size() < 1000 && !knownTrackIds.contains(trackId)) {
                        knownTrackIds.add(trackId);
                    }
                }
            }
        }

        private void executeQueryFlightsByAirport(PreparedStatement ps, boolean departure) throws SQLException {
            String airport = AIRPORTS[random.nextInt(AIRPORTS.length)];
            ps.setString(1, airport);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String flightId = rs.getString(1);
                    if (knownFlightIds.size() < 1000 && !knownFlightIds.contains(flightId)) {
                        knownFlightIds.add(flightId);
                    }
                }
            }
        }

        private void executeQueryAlertsBySeverity(PreparedStatement ps) throws SQLException {
            String severity = SEVERITIES[random.nextInt(SEVERITIES.length)];
            ps.setString(1, severity);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String alertId = rs.getString(1);
                    if (knownAlertIds.size() < 1000 && !knownAlertIds.contains(alertId)) {
                        knownAlertIds.add(alertId);
                    }
                }
            }
        }

        private void executeQueryAlertsByType(PreparedStatement ps) throws SQLException {
            String alertType = ALERT_TYPES[random.nextInt(ALERT_TYPES.length)];
            ps.setString(1, alertType);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    // Consume results
                }
            }
        }

        private void executeQueryHazardsByType(PreparedStatement ps) throws SQLException {
            String hazardType = HAZARD_TYPES[random.nextInt(HAZARD_TYPES.length)];
            ps.setString(1, hazardType);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    // Consume results
                }
            }
        }

        private void executeQueryRadarBySite(PreparedStatement ps) throws SQLException {
            String site = RADAR_SITES[random.nextInt(RADAR_SITES.length)];
            ps.setString(1, site);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    // Consume results
                }
            }
        }

        // DML implementations
        private void executeInsertPosition(PreparedStatement ps) throws SQLException {
            long id = System.nanoTime() ^ (threadId << 48);
            String trackId = String.format("TRK-%012X", id);
            String icao24 = generateIcao24(random.nextInt(populateCount));
            String json = generatePositionJson(icao24, (int) id);

            ps.setString(1, trackId);
            ps.setString(2, json);
            ps.executeUpdate();

            if (knownTrackIds.size() < 1000) {
                knownTrackIds.add(trackId);
            }
        }

        private void executeUpdatePosition(PreparedStatement ps) throws SQLException {
            if (knownTrackIds.isEmpty()) return;

            String trackId = knownTrackIds.get(random.nextInt(knownTrackIds.size()));

            // Read current JSON document
            selectPositionStmt.setString(1, trackId);
            String currentJson = null;
            try (ResultSet rs = selectPositionStmt.executeQuery()) {
                if (rs.next()) {
                    currentJson = rs.getString(1);
                }
            }
            if (currentJson == null) return;

            // Generate updated values
            int altitude = 25000 + random.nextInt(20000);
            double lat = 25.0 + random.nextDouble() * 25.0;
            double lon = -125.0 + random.nextDouble() * 60.0;
            int groundSpeed = 350 + random.nextInt(200);
            long timestamp = System.currentTimeMillis() / 1000;

            // Reconstruct JSON with updated position values
            String newJson = updatePositionJson(currentJson, altitude, lat, lon, groundSpeed, timestamp);

            ps.setString(1, newJson);
            ps.setString(2, trackId);
            ps.executeUpdate();
        }

        private void executeInsertAlert(PreparedStatement ps) throws SQLException {
            long id = System.nanoTime() ^ (threadId << 48);
            String alertId = String.format("ALT-%012X", id);
            String json = generateAlertJson((int) id);

            ps.setString(1, alertId);
            ps.setString(2, json);
            ps.executeUpdate();

            if (knownAlertIds.size() < 1000) {
                knownAlertIds.add(alertId);
            }
        }

        private void executeUpdateAlertResolution(PreparedStatement ps) throws SQLException {
            if (knownAlertIds.isEmpty()) return;

            String alertId = knownAlertIds.get(random.nextInt(knownAlertIds.size()));

            // Read current JSON document
            selectAlertStmt.setString(1, alertId);
            String currentJson = null;
            try (ResultSet rs = selectAlertStmt.executeQuery()) {
                if (rs.next()) {
                    currentJson = rs.getString(1);
                }
            }
            if (currentJson == null) return;

            // Generate updated values
            String status = random.nextBoolean() ? "RESOLVED" : "ACKNOWLEDGED";
            String resolvedAt = generateTimestamp();
            String notes = "Resolved by ATC coordination";

            // Reconstruct JSON with updated resolution
            String newJson = updateAlertResolutionJson(currentJson, status, resolvedAt, notes);

            ps.setString(1, newJson);
            ps.setString(2, alertId);
            ps.executeUpdate();
        }

        private void executeInsertRadarContact(PreparedStatement ps) throws SQLException {
            long id = System.nanoTime() ^ (threadId << 48);
            String contactId = String.format("RDR-%012X", id);
            String json = generateRadarContactJson((int) id);

            ps.setString(1, contactId);
            ps.setString(2, json);
            ps.executeUpdate();
        }

        private void executeInsertWeatherHazard(PreparedStatement ps) throws SQLException {
            long id = System.nanoTime() ^ (threadId << 48);
            String hazardId = String.format("WX-%012X", id);
            String json = generateWeatherHazardJson((int) id);

            ps.setString(1, hazardId);
            ps.setString(2, json);
            ps.executeUpdate();
        }

        private void executeUpdateFlightStatus(PreparedStatement ps) throws SQLException {
            if (knownFlightIds.isEmpty()) return;

            String flightId = knownFlightIds.get(random.nextInt(knownFlightIds.size()));

            // Read current JSON document
            selectFlightStmt.setString(1, flightId);
            String currentJson = null;
            try (ResultSet rs = selectFlightStmt.executeQuery()) {
                if (rs.next()) {
                    currentJson = rs.getString(1);
                }
            }
            if (currentJson == null) return;

            // Generate updated status
            String[] statuses = {"active", "departed", "enroute", "arrived", "cancelled"};
            String status = statuses[random.nextInt(statuses.length)];

            // Reconstruct JSON with updated status
            String newJson = updateFlightStatusJson(currentJson, status);

            ps.setString(1, newJson);
            ps.setString(2, flightId);
            ps.executeUpdate();
        }
    }

    // =========================================================================
    // JSON Update Helpers (read-modify-write for TimesTen)
    // =========================================================================

    private static String updatePositionJson(String json, int altitude, double lat, double lon,
                                              int groundSpeed, long timestamp) {
        // Update position.altitude
        json = json.replaceFirst(
            "(\"position\":\\s*\\{[^}]*\"altitude\":\\s*)\\d+",
            "$1" + altitude);
        // Update position.latitude
        json = json.replaceFirst(
            "(\"position\":\\s*\\{[^}]*\"latitude\":\\s*)[\\d.-]+",
            "$1" + String.format("%.4f", lat));
        // Update position.longitude
        json = json.replaceFirst(
            "(\"position\":\\s*\\{[^}]*\"longitude\":\\s*)[\\d.-]+",
            "$1" + String.format("%.4f", lon));
        // Update velocity.groundSpeed
        json = json.replaceFirst(
            "(\"velocity\":\\s*\\{[^}]*\"groundSpeed\":\\s*)\\d+",
            "$1" + groundSpeed);
        // Update timestamp
        json = json.replaceFirst(
            "(\"timestamp\":\\s*)\\d+",
            "$1" + timestamp);
        return json;
    }

    private static String updateAlertResolutionJson(String json, String status,
                                                     String resolvedAt, String notes) {
        // Update resolution.status
        json = json.replaceFirst(
            "(\"resolution\":\\s*\\{[^}]*\"status\":\\s*)\"[^\"]*\"",
            "$1\"" + status + "\"");
        // Update resolution.resolvedAt (handle null case)
        json = json.replaceFirst(
            "(\"resolution\":\\s*\\{[^}]*\"resolvedAt\":\\s*)(null|\"[^\"]*\")",
            "$1\"" + resolvedAt + "\"");
        // Update resolution.notes (handle null case)
        json = json.replaceFirst(
            "(\"resolution\":\\s*\\{[^}]*\"notes\":\\s*)(null|\"[^\"]*\")",
            "$1\"" + notes + "\"");
        return json;
    }

    private static String updateFlightStatusJson(String json, String status) {
        // Update status field
        json = json.replaceFirst(
            "(\"status\":\\s*)\"[^\"]*\"",
            "$1\"" + status + "\"");
        return json;
    }

    // =========================================================================
    // JSON Generation Helpers
    // =========================================================================

    private static String generateIcao24(int seed) {
        return String.format("%06X", (seed * 17 + 0xA00000) & 0xFFFFFF);
    }

    private static String generateTimestamp() {
        long now = System.currentTimeMillis();
        return String.format("%tFT%<tTZ", now);
    }

    private String generateAircraftRegistryJson(int seed) {
        int airlineIdx = seed % AIRLINES.length;
        int typeIdx = seed % AIRCRAFT_TYPES.length;
        String registration = String.format("N%05d", 10000 + seed);
        String icao24 = generateIcao24(seed);

        return String.format(
            "{" +
            "\"registration\":\"%s\"," +
            "\"icao24\":\"%s\"," +
            "\"type\":{" +
                "\"manufacturer\":\"%s\"," +
                "\"model\":\"%s\"," +
                "\"icaoType\":\"%s\"," +
                "\"wtc\":\"M\"," +
                "\"engineType\":\"Jet\"," +
                "\"engineCount\":2" +
            "}," +
            "\"operator\":{" +
                "\"name\":\"%s\"," +
                "\"icao\":\"%s\"," +
                "\"callsignPrefix\":\"%s\"" +
            "}," +
            "\"performance\":{" +
                "\"maxSpeed\":%d," +
                "\"cruiseSpeed\":%d," +
                "\"maxAltitude\":%d," +
                "\"range\":%d" +
            "}," +
            "\"equipment\":{" +
                "\"adsb\":true," +
                "\"tcas\":\"II\"," +
                "\"transponderModes\":[\"A\",\"C\",\"S\"]," +
                "\"rnavCapable\":true" +
            "}," +
            "\"lastUpdated\":\"%s\"" +
            "}",
            registration, icao24,
            MANUFACTURERS[typeIdx], AIRCRAFT_TYPES[typeIdx], AIRCRAFT_TYPES[typeIdx],
            AIRLINE_NAMES[airlineIdx], AIRLINES[airlineIdx], AIRLINES[airlineIdx],
            500 + random.nextInt(50), 420 + random.nextInt(50),
            35000 + random.nextInt(10000), 2000 + random.nextInt(2000),
            generateTimestamp()
        );
    }

    private String generatePositionJson(String icao24, int seed) {
        int absSeed = Math.abs(seed);
        int airlineIdx = absSeed % AIRLINES.length;
        String callsign = AIRLINES[airlineIdx] + (1000 + absSeed % 9000);
        double lat = 25.0 + random.nextDouble() * 25.0;
        double lon = -125.0 + random.nextDouble() * 60.0;
        int altitude = 25000 + random.nextInt(20000);
        int groundSpeed = 350 + random.nextInt(200);
        double heading = random.nextDouble() * 360.0;
        String squawk = String.format("%04d", 1000 + random.nextInt(6777));

        return String.format(
            "{" +
            "\"icao24\":\"%s\"," +
            "\"callsign\":\"%s\"," +
            "\"timestamp\":%d," +
            "\"timestampISO\":\"%s\"," +
            "\"position\":{" +
                "\"latitude\":%.4f," +
                "\"longitude\":%.4f," +
                "\"altitude\":%d," +
                "\"altitudeType\":\"barometric\"," +
                "\"geometricAltitude\":%d," +
                "\"onGround\":false" +
            "}," +
            "\"velocity\":{" +
                "\"groundSpeed\":%d," +
                "\"trueAirspeed\":%d," +
                "\"indicatedAirspeed\":%d," +
                "\"verticalRate\":%d," +
                "\"heading\":%.1f," +
                "\"track\":%.1f" +
            "}," +
            "\"flightStatus\":{" +
                "\"squawk\":\"%s\"," +
                "\"alert\":false," +
                "\"emergency\":\"none\"," +
                "\"spi\":false" +
            "}," +
            "\"accuracy\":{" +
                "\"nac_p\":%d," +
                "\"nac_v\":%d," +
                "\"sil\":%d," +
                "\"nic\":%d" +
            "}," +
            "\"source\":{" +
                "\"type\":\"ADS-B\"," +
                "\"receiverIds\":[\"RX%03d\",\"RX%03d\"]," +
                "\"signalStrength\":%.1f" +
            "}" +
            "}",
            icao24, callsign,
            System.currentTimeMillis() / 1000, generateTimestamp(),
            lat, lon, altitude, altitude + random.nextInt(200),
            groundSpeed, groundSpeed + 15, groundSpeed - 150 + random.nextInt(50),
            -500 + random.nextInt(1000), heading, heading + (random.nextDouble() * 4 - 2),
            squawk,
            7 + random.nextInt(3), 1 + random.nextInt(3), 2 + random.nextInt(2), 7 + random.nextInt(3),
            random.nextInt(100), random.nextInt(100),
            -90.0 + random.nextDouble() * 20
        );
    }

    private String generateFlightPlanJson(int seed) {
        int absSeed = Math.abs(seed);
        int airlineIdx = absSeed % AIRLINES.length;
        String callsign = AIRLINES[airlineIdx] + (1000 + absSeed % 9000);
        String icao24 = generateIcao24(absSeed);
        int depIdx = absSeed % AIRPORTS.length;
        int arrIdx = (absSeed + 5) % AIRPORTS.length;
        int altIdx = (absSeed + 10) % AIRPORTS.length;
        int filedAlt = 30000 + (absSeed % 12) * 1000;

        return String.format(
            "{" +
            "\"flightNumber\":\"%s\"," +
            "\"icao24\":\"%s\"," +
            "\"filed\":{" +
                "\"departureTime\":\"%s\"," +
                "\"arrivalTime\":\"%s\"," +
                "\"filedAt\":\"%s\"" +
            "}," +
            "\"airports\":{" +
                "\"departure\":{" +
                    "\"icao\":\"%s\"," +
                    "\"runway\":\"%s\"," +
                    "\"gate\":\"%s\"" +
                "}," +
                "\"arrival\":{" +
                    "\"icao\":\"%s\"," +
                    "\"runway\":\"%s\"," +
                    "\"gate\":\"%s\"" +
                "}," +
                "\"alternate\":{" +
                    "\"icao\":\"%s\"" +
                "}" +
            "}," +
            "\"route\":{" +
                "\"filedRoute\":\"%s DIRECT %s\"," +
                "\"distance\":%d," +
                "\"filedAltitude\":%d" +
            "}," +
            "\"fuel\":{" +
                "\"minimum\":%d," +
                "\"planned\":%d," +
                "\"reserve\":%d" +
            "}," +
            "\"passengers\":%d," +
            "\"status\":\"active\"" +
            "}",
            callsign, icao24,
            generateTimestamp(), generateTimestamp(), generateTimestamp(),
            AIRPORTS[depIdx], (random.nextInt(36) + 1) + (random.nextBoolean() ? "L" : "R"),
            (char)('A' + random.nextInt(6)) + String.valueOf(10 + random.nextInt(90)),
            AIRPORTS[arrIdx], (random.nextInt(36) + 1) + (random.nextBoolean() ? "L" : "R"),
            (char)('A' + random.nextInt(6)) + String.valueOf(10 + random.nextInt(90)),
            AIRPORTS[altIdx],
            AIRPORTS[depIdx], AIRPORTS[arrIdx],
            500 + random.nextInt(3000), filedAlt,
            10000 + random.nextInt(15000), 15000 + random.nextInt(15000),
            2000 + random.nextInt(3000),
            50 + random.nextInt(200)
        );
    }

    private String generateAlertJson(int seed) {
        int absSeed = Math.abs(seed);
        int airlineIdx = absSeed % AIRLINES.length;
        String callsign = AIRLINES[airlineIdx] + (1000 + absSeed % 9000);
        String icao24 = generateIcao24(absSeed);
        String alertType = ALERT_TYPES[absSeed % ALERT_TYPES.length];
        String severity = SEVERITIES[absSeed % SEVERITIES.length];
        double lat = 25.0 + random.nextDouble() * 25.0;
        double lon = -125.0 + random.nextDouble() * 60.0;

        return String.format(
            "{" +
            "\"alertType\":\"%s\"," +
            "\"severity\":\"%s\"," +
            "\"timestamp\":\"%s\"," +
            "\"aircraft\":{" +
                "\"icao24\":\"%s\"," +
                "\"callsign\":\"%s\"," +
                "\"registration\":\"N%05d\"" +
            "}," +
            "\"airspace\":{" +
                "\"type\":\"RESTRICTED\"," +
                "\"designator\":\"R-%04d\"," +
                "\"floor\":0," +
                "\"ceiling\":99999," +
                "\"center\":{\"lat\":%.2f,\"lon\":%.2f}" +
            "}," +
            "\"violation\":{" +
                "\"entryPoint\":{\"lat\":%.4f,\"lon\":%.4f,\"altitude\":%d}," +
                "\"entryTime\":\"%s\"," +
                "\"penetrationDepth\":%.1f," +
                "\"currentlyInside\":true" +
            "}," +
            "\"notifications\":[]," +
            "\"resolution\":{" +
                "\"status\":\"PENDING\"," +
                "\"assignedTo\":null," +
                "\"resolvedAt\":null," +
                "\"notes\":null" +
            "}" +
            "}",
            alertType, severity, generateTimestamp(),
            icao24, callsign, 10000 + absSeed,
            1000 + random.nextInt(5000), lat, lon,
            lat + random.nextDouble() * 0.5, lon + random.nextDouble() * 0.5,
            20000 + random.nextInt(25000),
            generateTimestamp(),
            random.nextDouble() * 10
        );
    }

    private String generateRadarContactJson(int seed) {
        int absSeed = Math.abs(seed);
        String site = RADAR_SITES[absSeed % RADAR_SITES.length];
        String icao24 = generateIcao24(absSeed);
        int airlineIdx = absSeed % AIRLINES.length;
        String callsign = AIRLINES[airlineIdx] + (1000 + absSeed % 9000);
        double azimuth = random.nextDouble() * 360.0;
        double range = 5.0 + random.nextDouble() * 55.0;
        double lat = 25.0 + random.nextDouble() * 25.0;
        double lon = -125.0 + random.nextDouble() * 60.0;

        return String.format(
            "{" +
            "\"radarSite\":{" +
                "\"id\":\"%s\"," +
                "\"type\":\"%s\"," +
                "\"location\":{\"lat\":%.4f,\"lon\":%.4f}," +
                "\"range\":60" +
            "}," +
            "\"timestamp\":\"%s\"," +
            "\"azimuth\":%.1f," +
            "\"range\":%.1f," +
            "\"primary\":{" +
                "\"detected\":true," +
                "\"amplitude\":%.1f," +
                "\"size\":\"MEDIUM\"" +
            "}," +
            "\"secondary\":{" +
                "\"detected\":true," +
                "\"mode3a\":\"%04d\"," +
                "\"modeC\":%d," +
                "\"modeS\":\"%s\"," +
                "\"callsign\":\"%s\"" +
            "}," +
            "\"computed\":{" +
                "\"latitude\":%.4f," +
                "\"longitude\":%.4f," +
                "\"groundSpeed\":%d," +
                "\"track\":%d" +
            "}," +
            "\"quality\":{" +
                "\"psr_confidence\":%.2f," +
                "\"ssr_confidence\":%.2f," +
                "\"multipath\":false," +
                "\"clutter\":false" +
            "}," +
            "\"correlation\":{" +
                "\"adsbCorrelated\":true," +
                "\"correlatedTrackId\":\"TRK-%012d\"," +
                "\"correlationScore\":%.2f" +
            "}" +
            "}",
            site, site.substring(0, 6), lat, lon,
            generateTimestamp(),
            azimuth, range,
            -60.0 + random.nextDouble() * 30,
            1000 + random.nextInt(6777), 200 + random.nextInt(400),
            icao24, callsign,
            lat + random.nextDouble() * 0.5, lon + random.nextDouble() * 0.5,
            350 + random.nextInt(200), random.nextInt(360),
            0.85 + random.nextDouble() * 0.15, 0.90 + random.nextDouble() * 0.10,
            absSeed,
            0.85 + random.nextDouble() * 0.15
        );
    }

    private String generateWeatherHazardJson(int seed) {
        int absSeed = Math.abs(seed);
        String hazardType = HAZARD_TYPES[absSeed % HAZARD_TYPES.length];
        String severity = SEVERITIES[absSeed % SEVERITIES.length];
        int airlineIdx = absSeed % AIRLINES.length;
        String reportedBy = AIRLINES[airlineIdx] + (1000 + absSeed % 9000);
        double lat = 25.0 + random.nextDouble() * 25.0;
        double lon = -125.0 + random.nextDouble() * 60.0;
        int altMin = 20000 + random.nextInt(10000);
        int altMax = altMin + 5000 + random.nextInt(10000);

        return String.format(
            "{" +
            "\"hazardType\":\"%s\"," +
            "\"severity\":\"%s\"," +
            "\"source\":{" +
                "\"type\":\"PIREP\"," +
                "\"reportedBy\":\"%s\"," +
                "\"reportTime\":\"%s\"," +
                "\"reliability\":\"HIGH\"" +
            "}," +
            "\"location\":{" +
                "\"latitude\":%.2f," +
                "\"longitude\":%.2f," +
                "\"altitudeMin\":%d," +
                "\"altitudeMax\":%d," +
                "\"radius\":%d" +
            "}," +
            "\"validity\":{" +
                "\"start\":\"%s\"," +
                "\"end\":\"%s\"," +
                "\"confidence\":%.2f" +
            "}," +
            "\"details\":{" +
                "\"turbulenceType\":\"%s\"," +
                "\"frequency\":\"INTERMITTENT\"," +
                "\"chop\":%s," +
                "\"icing\":%s" +
            "}," +
            "\"advisories\":{" +
                "\"sigmet\":\"SIGMET %s %d\"," +
                "\"airmet\":null" +
            "}," +
            "\"affectedFlights\":[]" +
            "}",
            hazardType, severity,
            reportedBy, generateTimestamp(),
            lat, lon, altMin, altMax, 10 + random.nextInt(50),
            generateTimestamp(), generateTimestamp(),
            0.70 + random.nextDouble() * 0.30,
            hazardType.equals("TURBULENCE") ? "CAT" : "CONVECTIVE",
            random.nextBoolean(), hazardType.equals("ICING"),
            (char)('A' + random.nextInt(26)), 1 + random.nextInt(5)
        );
    }

    // =========================================================================
    // AirWorkSchema — creates and drops the database schema per dialect
    // =========================================================================

    static class AirWorkSchema {

        private final Connection conn;
        private final int dbms;

        AirWorkSchema(Connection conn, int dbms) {
            this.conn = conn;
            this.dbms = dbms;
        }

        void create() throws SQLException {
            System.out.println("\n--- CREATING SCHEMA ---");
            dropTables();
            createTables();
            createIndexes();
            System.out.println("Schema created.\n");
        }

        private void dropTables() throws SQLException {
            String[] tables = {
                "weather_hazard", "radar_contact", "airspace_alert",
                "flight_plan", "aircraft_position", "aircraft_registry"
            };
            try (Statement stmt = conn.createStatement()) {
                String cascade = (dbms == DB_POSTGRESQL) ? " CASCADE" : "";
                for (String table : tables)
                    tryExecute(stmt, "DROP TABLE " + table + cascade);
            }
        }

        private void createTables() throws SQLException {
            System.out.println("  Creating tables...");
            String idType = (dbms == DB_ORACLE) ? "VARCHAR2(24)" : "VARCHAR(24)";
            String jsonType = (dbms == DB_POSTGRESQL) ? "JSONB" : "JSON";

            String[][] table = {
                {"aircraft_registry", "aircraft_id", "aircraft_data"},
                {"aircraft_position", "track_id", "position_data"},
                {"flight_plan", "flight_id", "plan_data"},
                {"airspace_alert", "alert_id", "alert_data"},
                {"radar_contact", "contact_id", "contact_data"},
                {"weather_hazard", "hazard_id", "hazard_data"}
            };
            try (Statement stmt = conn.createStatement()) {
                for (String[] t : table) {
                    stmt.execute(
                        "CREATE TABLE " + t[0] + " (" +
                        t[1] + " " + idType + " PRIMARY KEY, " +
                        t[2] + " " + jsonType +
                        ")");
                }
            }
        }

        private void createIndexes() throws SQLException {
            System.out.println("  Creating indexes...");
            try (Statement stmt = conn.createStatement()) {
                createIndex(stmt, "idx_position_icao24", "aircraft_position", "position_data", "icao24");
                createIndex(stmt, "idx_flight_departure", "flight_plan", "plan_data", "airports.departure.icao");
                createIndex(stmt, "idx_flight_arrival", "flight_plan", "plan_data", "airports.arrival.icao");
                createIndex(stmt, "idx_alert_severity", "airspace_alert", "alert_data", "severity");
                createIndex(stmt, "idx_alert_type", "airspace_alert", "alert_data", "alertType");
                createIndex(stmt, "idx_radar_site", "radar_contact", "contact_data", "radarSite.id");
                createIndex(stmt, "idx_hazard_type", "weather_hazard", "hazard_data", "hazardType");
                createIndex(stmt, "idx_hazard_severity", "weather_hazard", "hazard_data", "severity");
            }
        }

        /**
         * Creates a function-based / expression index on a scalar JSON path.
         * Oracle and MySQL index a JSON_VALUE expression (MySQL needs an explicit
         * CAST so the optimizer knows the indexed key's type/length); PostgreSQL
         * indexes a jsonb operator expression directly on JSONB columns.
         */
        private void createIndex(Statement stmt, String indexName, String table,
                                  String column, String dottedPath) throws SQLException {
            String expr;
            switch (dbms) {
                case DB_POSTGRESQL: {
                    String[] parts = dottedPath.split("\\.");
                    if (parts.length == 1) {
                        expr = column + "->>'" + parts[0] + "'";
                    } else {
                        expr = column + "#>>'{" + String.join(",", parts) + "}'";
                    }
                    break;
                }
                case DB_MYSQL:
                    expr = "CAST(" + column + "->>'$." + dottedPath + "' AS CHAR(64))";
                    break;
                case DB_ORACLE:
                default:
                    expr = "JSON_VALUE(" + column + ", '$." + dottedPath + "')";
                    break;
            }
            // Oracle recognizes function calls as function-based index expressions
            // directly; PostgreSQL and MySQL require the extra parens to distinguish
            // an expression index from a plain column reference.
            String wrapped = (dbms == DB_ORACLE) ? "(" + expr + ")" : "((" + expr + "))";
            stmt.execute("CREATE INDEX " + indexName + " ON " + table + " " + wrapped);
        }

        private static void tryExecute(Statement stmt, String sql) {
            try {
                stmt.execute(sql);
            } catch (SQLException ignored) {
                // Object likely doesn't exist yet — safe to ignore during drop phase
            }
        }
    }
}
