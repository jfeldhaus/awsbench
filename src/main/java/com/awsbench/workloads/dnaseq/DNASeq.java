package com.awsbench.workloads.dnaseq;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * High-Performance DNA Sequencing Stress Test
 *
 * Modes:
 *   1. POPULATE - Load initial data into tables
 *   2. WORKLOAD - Run multi-threaded OLTP workload
 *
 * Usage:
 *   java DNASeq -mode populate -samples 1000 -runs 500 -variants 100000
 *   java DNASeq -mode workload -threads 16 -duration 300 -workload-mix 70:20:10
 */
public class DNASeq {

    static public final int DB_ORACLE     = 0;
    static public final int DB_POSTGRESQL = 2;
    static public final int DB_MYSQL      = 3;

    // Configuration
    private static class Config {
        String jdbcUrl = null;
        String username = "admin";
        String password = "password";
        int dbms = -1;
        
        String mode = "workload";
        int threadCount = 8;
        int durationSeconds = 60;
        
        // Population settings
        int numSamples = 100;
        int numRuns = 50;
        int numReadsPerRun = 10000;
        int numVariantsPerRun = 1000;
        int numGeneAnnotations = 5000;
        
        // Workload mix (read%, update%, insert%, delete%)
        int readPercent = 70;
        int updatePercent = 15;
        int insertPercent = 10;
        int deletePercent = 5;
        
        int batchSize = 1000;
        int commitInterval = 10000;
        boolean verboseOutput = false;
    }
    
    private static Config config = new Config();
    
    // Performance metrics
    private static class Metrics {
        AtomicLong totalOperations = new AtomicLong(0);
        AtomicLong successfulOps = new AtomicLong(0);
        AtomicLong failedOps = new AtomicLong(0);
        
        AtomicLong readOps = new AtomicLong(0);
        AtomicLong insertOps = new AtomicLong(0);
        AtomicLong updateOps = new AtomicLong(0);
        AtomicLong deleteOps = new AtomicLong(0);
        
        AtomicLong totalLatencyMs = new AtomicLong(0);
        AtomicLong minLatencyMs = new AtomicLong(Long.MAX_VALUE);
        AtomicLong maxLatencyMs = new AtomicLong(0);
        
        ConcurrentHashMap<String, AtomicLong> operationCounts = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, AtomicLong> operationLatencies = new ConcurrentHashMap<>();
        
        long startTime;
        long endTime;
        
        public void recordOperation(String opType, long latencyMs, boolean success) {
            totalOperations.incrementAndGet();
            if (success) {
                successfulOps.incrementAndGet();
            } else {
                failedOps.incrementAndGet();
            }
            
            totalLatencyMs.addAndGet(latencyMs);
            
            // Update min/max
            long currentMin;
            do {
                currentMin = minLatencyMs.get();
            } while (latencyMs < currentMin && !minLatencyMs.compareAndSet(currentMin, latencyMs));
            
            long currentMax;
            do {
                currentMax = maxLatencyMs.get();
            } while (latencyMs > currentMax && !maxLatencyMs.compareAndSet(currentMax, latencyMs));
            
            // Track by operation type
            operationCounts.computeIfAbsent(opType, k -> new AtomicLong(0)).incrementAndGet();
            operationLatencies.computeIfAbsent(opType, k -> new AtomicLong(0)).addAndGet(latencyMs);
        }
        
        public void printReport() {
            endTime = System.currentTimeMillis();
            double durationSec = (endTime - startTime) / 1000.0;
            
            System.out.println("\n" + "=".repeat(80));
            System.out.println("PERFORMANCE REPORT");
            System.out.println("=".repeat(80));
            
            System.out.printf("Duration: %.2f seconds\n", durationSec);
            System.out.printf("Total Operations: %,d\n", totalOperations.get());
            System.out.printf("Successful: %,d\n", successfulOps.get());
            System.out.printf("Failed: %,d\n", failedOps.get());
            System.out.printf("Throughput: %,.2f ops/sec\n", totalOperations.get() / durationSec);
            
            System.out.println("\nOperation Breakdown:");
            System.out.printf("  Read Operations: %,d (%.1f%%)\n",
                readOps.get(), 100.0 * readOps.get() / totalOperations.get());
            System.out.printf("  Update Operations: %,d (%.1f%%)\n",
                updateOps.get(), 100.0 * updateOps.get() / totalOperations.get());
            System.out.printf("  Insert Operations: %,d (%.1f%%)\n",
                insertOps.get(), 100.0 * insertOps.get() / totalOperations.get());
            System.out.printf("  Delete Operations: %,d (%.1f%%)\n",
                deleteOps.get(), 100.0 * deleteOps.get() / totalOperations.get());
            
            System.out.println("\nLatency Statistics:");
            if (successfulOps.get() > 0) {
                System.out.printf("  Average: %.2f ms\n", 
                    (double) totalLatencyMs.get() / successfulOps.get());
                System.out.printf("  Minimum: %d ms\n", minLatencyMs.get());
                System.out.printf("  Maximum: %d ms\n", maxLatencyMs.get());
            }
            
            System.out.println("\nDetailed Operation Statistics:");
            System.out.println(String.format("%-40s %15s %15s", 
                "Operation Type", "Count", "Avg Latency (ms)"));
            System.out.println("-".repeat(80));
            
            operationCounts.forEach((opType, count) -> {
                long totalLatency = operationLatencies.getOrDefault(opType, new AtomicLong(0)).get();
                double avgLatency = count.get() > 0 ? (double) totalLatency / count.get() : 0;
                System.out.printf("%-40s %,15d %15.2f\n", opType, count.get(), avgLatency);
            });
            
            System.out.println("=".repeat(80));
        }
    }
    
    private static Metrics metrics = new Metrics();
    
    // Random data generators
    private static Random random = new Random();
    private static String[] CHROMOSOMES = {
        "chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10",
        "chr11", "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19", 
        "chr20", "chr21", "chr22", "chrX", "chrY"
    };
    
    private static String[] PLATFORMS = {"Illumina NovaSeq", "Illumina MiSeq", "PacBio Sequel", "Oxford Nanopore"};
    private static String[] SAMPLE_TYPES = {"Blood", "Tissue", "Saliva", "Cell Line"};
    private static String[] TISSUE_SOURCES = {"Peripheral Blood", "Tumor Biopsy", "Normal Tissue", "Buccal Swab"};
    private static String[] VARIANT_TYPES = {"SNP", "INSERTION", "DELETION", "MNP"};
    private static String[] GENE_TYPES = {"protein_coding", "lncRNA", "miRNA", "pseudogene"};
    private static String[] BASES = {"A", "C", "G", "T"};
    
    public static void main(String[] args) {
        parseArguments(args);
        
        System.out.println("DNA Sequencing Stress Test");
        System.out.println("Mode: " + config.mode.toUpperCase());
        System.out.println("JDBC URL: " + config.jdbcUrl);

        try {
            if (config.jdbcUrl == null || config.jdbcUrl.isEmpty()) {
                System.err.println("'-url' is required");
                System.exit(1);
            }

            if (config.jdbcUrl.startsWith("jdbc:oracle:"))
                config.dbms = DB_ORACLE;
            else if (config.jdbcUrl.startsWith("jdbc:postgresql:"))
                config.dbms = DB_POSTGRESQL;
            else if (config.jdbcUrl.startsWith("jdbc:mysql:"))
                config.dbms = DB_MYSQL;
            else {
                System.err.println("Unsupported database URL: " + config.jdbcUrl);
                System.exit(1);
            }

            switch (config.dbms) {
                case DB_ORACLE:     Class.forName("oracle.jdbc.OracleDriver"); break;
                case DB_POSTGRESQL: Class.forName("org.postgresql.Driver"); break;
                case DB_MYSQL:      Class.forName("com.mysql.cj.jdbc.Driver"); break;
            }
            
            if (config.mode.equalsIgnoreCase("populate")) {
                runPopulateMode();
            } else if (config.mode.equalsIgnoreCase("workload")) {
                runWorkloadMode();
            } else {
                System.err.println("Invalid mode: " + config.mode);
                printUsage();
                System.exit(1);
            }
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void parseArguments(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("-")) {
                String key = arg.substring(1);
                String value = "true";

                // Check for help flags
                if (key.equals("h") || key.equals("help")) {
                    printUsage();
                    System.exit(0);
                }

                // Check if next argument exists and is not another option
                if (i + 1 < args.length && !args[i + 1].startsWith("-")) {
                    value = args[i + 1];
                    i++; // Skip next argument as it's the value
                }

                switch (key) {
                    case "mode": config.mode = value; break;
                    case "url": config.jdbcUrl = value; break;
                    case "uid": config.username = value; break;
                    case "pwd": config.password = value; break;
                    case "threads": config.threadCount = Integer.parseInt(value); break;
                    case "duration": config.durationSeconds = Integer.parseInt(value); break;
                    case "samples": config.numSamples = Integer.parseInt(value); break;
                    case "runs": config.numRuns = Integer.parseInt(value); break;
                    case "reads-per-run": config.numReadsPerRun = Integer.parseInt(value); break;
                    case "variants-per-run": config.numVariantsPerRun = Integer.parseInt(value); break;
                    case "genes": config.numGeneAnnotations = Integer.parseInt(value); break;
                    case "batch-size": config.batchSize = Integer.parseInt(value); break;
                    case "workload-mix": parseWorkloadMix(value); break;
                    case "verbose": config.verboseOutput = Boolean.parseBoolean(value); break;
                    default:
                        System.err.println("Unknown parameter: -" + key);
                        printUsage();
                        System.exit(1);
                }
            }
        }
    }
    
    private static void parseWorkloadMix(String mix) {
        String[] parts = mix.split(":");
        if (parts.length == 4) {
            config.readPercent = Integer.parseInt(parts[0]);
            config.updatePercent = Integer.parseInt(parts[1]);
            config.insertPercent = Integer.parseInt(parts[2]);
            config.deletePercent = Integer.parseInt(parts[3]);
        } else if (parts.length == 3) {
            // Legacy 3-parameter format (no deletes)
            config.readPercent = Integer.parseInt(parts[0]);
            config.updatePercent = Integer.parseInt(parts[1]);
            config.insertPercent = Integer.parseInt(parts[2]);
            config.deletePercent = 0;
        }
    }
    
    private static void printUsage() {
        System.out.println("\nUsage:");
        System.out.println("  Population Mode:");
        System.out.println("    java DNASeq -mode populate -samples 1000 -runs 500");
        System.out.println("\n  Workload Mode:");
        System.out.println("    java DNASeq -mode workload -threads 16 -duration 300");
        System.out.println("\nParameters:");
        System.out.println("  -mode <populate|workload>  : Execution mode");
        System.out.println("  -url <jdbc-url>            : JDBC connection URL");
        System.out.println("  -uid <user>                : Database username");
        System.out.println("  -pwd <pass>                : Database password");
        System.out.println("  -threads <n>               : Number of worker threads (workload mode)");
        System.out.println("  -duration <seconds>        : Test duration (workload mode)");
        System.out.println("  -samples <n>               : Number of samples to create");
        System.out.println("  -runs <n>                  : Number of sequencing runs");
        System.out.println("  -reads-per-run <n>         : Reads per sequencing run");
        System.out.println("  -variants-per-run <n>      : Variants per run");
        System.out.println("  -genes <n>                 : Number of gene annotations");
        System.out.println("  -workload-mix <r:u:i:d>    : Workload mix (read%:update%:insert%:delete%)");
        System.out.println("  -batch-size <n>            : Batch size for bulk operations");
        System.out.println("  -verbose                   : Enable verbose error output with stack traces");
        System.out.println("  -h, -help                  : Display this help message");
    }
    
    // ========================================================================
    // POPULATE MODE
    // ========================================================================
    
    private static void runPopulateMode() throws SQLException {
        System.out.println("\n--- POPULATE MODE ---");
        System.out.printf("Creating %,d samples, %,d runs, %,d reads per run, %,d variants per run\n",
            config.numSamples, config.numRuns, config.numReadsPerRun, config.numVariantsPerRun);
        
        try (Connection schemaConn = getConnection()) {
            new DNASchema(schemaConn, config.dbms).create();
        }

        long startTime = System.currentTimeMillis();

        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);
            
            // Step 1: Create reference genome
            System.out.println("\n[1/6] Creating reference genome...");
            createReferenceGenome(conn);
            
            // Step 2: Create gene annotations
            System.out.println("[2/6] Creating gene annotations...");
            createGeneAnnotations(conn, config.numGeneAnnotations);
            
            // Step 3: Create samples
            System.out.println("[3/6] Creating samples...");
            createSamples(conn, config.numSamples);
            
            // Step 4: Create sequencing runs
            System.out.println("[4/6] Creating sequencing runs...");
            createSequencingRuns(conn, config.numRuns);
            
            // Step 5: Create sequencing reads
            System.out.println("[5/6] Creating sequencing reads...");
            createSequencingReads(conn, config.numReadsPerRun);
            
            // Step 6: Create variants
            System.out.println("[6/6] Creating variants and annotations...");
            createVariants(conn, config.numVariantsPerRun);

            // Step 7: Synchronize sequences with existing data
            System.out.println("[7/8] Synchronizing sequences...");
            synchronizeSequences(conn);

            // Step 8: Update statistics for optimizer
            System.out.println("[8/8] Updating table statistics...");
            updateStatistics(conn);

            conn.commit();
        }
        
        long endTime = System.currentTimeMillis();
        double durationSec = (endTime - startTime) / 1000.0;
        
        System.out.println("\n" + "=".repeat(80));
        System.out.printf("Population completed in %.2f seconds\n", durationSec);
        System.out.println("=".repeat(80));
        
        // Print row counts
        printTableCounts();
    }
    
    private static void createReferenceGenome(Connection conn) throws SQLException {
        String sql = "INSERT INTO reference_genomes VALUES (1, 'GRCh38', 'p14', 'Homo sapiens', " +
                     "3099750718, 24, " + nowFn() + ", 'NCBI/GRC', " + nowFn() + ")";
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        }
        System.out.println("  Created reference genome GRCh38");
    }
    
    private static void createGeneAnnotations(Connection conn, int count) throws SQLException {
        String sql = "INSERT INTO gene_annotations VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int i = 1; i <= count; i++) {
                String chr = CHROMOSOMES[random.nextInt(CHROMOSOMES.length)];
                long startPos = 1000000 + random.nextInt(100000000);
                long endPos = startPos + 1000 + random.nextInt(50000);
                
                pstmt.setInt(1, i);
                pstmt.setString(2, chr);
                pstmt.setString(3, "GENE" + i);
                pstmt.setString(4, "Gene Name " + i);
                pstmt.setLong(5, startPos);
                pstmt.setLong(6, endPos);
                pstmt.setString(7, random.nextBoolean() ? "+" : "-");
                pstmt.setString(8, GENE_TYPES[random.nextInt(GENE_TYPES.length)]);
                pstmt.setString(9, "ENST" + String.format("%011d", i));
                
                pstmt.addBatch();
                
                if (i % config.batchSize == 0) {
                    pstmt.executeBatch();
                    conn.commit();
                    System.out.printf("  Created %,d / %,d gene annotations\r", i, count);
                }
            }
            pstmt.executeBatch();
            conn.commit();
        }
        System.out.printf("  Created %,d gene annotations%n", count);
    }
    
    private static void createSamples(Connection conn, int count) throws SQLException {
        String sql = "INSERT INTO samples VALUES (?, ?, ?, ?, ?, " + nowFn() + ", 'Homo sapiens', " +
                     "NULL, " + nowFn() + ", NULL)";
        
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int i = 1; i <= count; i++) {
                pstmt.setInt(1, i);
                pstmt.setString(2, "SAMPLE" + String.format("%06d", i));
                pstmt.setString(3, "PATIENT" + String.format("%06d", (i % 1000) + 1));
                pstmt.setString(4, SAMPLE_TYPES[random.nextInt(SAMPLE_TYPES.length)]);
                pstmt.setString(5, TISSUE_SOURCES[random.nextInt(TISSUE_SOURCES.length)]);
                
                pstmt.addBatch();
                
                if (i % config.batchSize == 0) {
                    pstmt.executeBatch();
                    conn.commit();
                    System.out.printf("  Created %,d / %,d samples\r", i, count);
                }
            }
            pstmt.executeBatch();
            conn.commit();
        }
        System.out.printf("  Created %,d samples%n", count);
    }
    
    private static void createSequencingRuns(Connection conn, int count) throws SQLException {
        String sql = "INSERT INTO sequencing_runs VALUES (?, ?, ?, 1, ?, 'v3', ?, ?, ?, 'Y', " +
                     "?, " + nowFn() + ", 'COMPLETED', ?, ?, " + nowFn() + ", " + nowFn() + ")";
        
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int i = 1; i <= count; i++) {
                int sampleId = (i % config.numSamples) + 1;
                
                pstmt.setInt(1, i);
                pstmt.setString(2, "RUN" + String.format("%06d", i));
                pstmt.setInt(3, sampleId);
                pstmt.setString(4, PLATFORMS[random.nextInt(PLATFORMS.length)]);
                pstmt.setString(5, "INST" + (random.nextInt(10) + 1));
                pstmt.setString(6, "FC" + String.format("%08d", i));
                pstmt.setInt(7, random.nextBoolean() ? 150 : 250);
                pstmt.setInt(8, 30 + random.nextInt(70));
                pstmt.setLong(9, config.numReadsPerRun);
                pstmt.setDouble(10, 30 + random.nextDouble() * 10);
                
                pstmt.addBatch();
                
                if (i % config.batchSize == 0) {
                    pstmt.executeBatch();
                    conn.commit();
                    System.out.printf("  Created %,d / %,d runs\r", i, count);
                }
            }
            pstmt.executeBatch();
            conn.commit();
        }
        System.out.printf("  Created %,d sequencing runs%n", count);
    }
    
    private static void createSequencingReads(Connection conn, int readsPerRun) throws SQLException {
        String sql = "INSERT INTO sequencing_reads VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            long readId = 1;
            
            for (int runId = 1; runId <= config.numRuns; runId++) {
                for (int i = 0; i < readsPerRun; i++) {
                    String chr = CHROMOSOMES[random.nextInt(CHROMOSOMES.length)];
                    long position = 1000 + random.nextInt(100000000);
                    
                    pstmt.setLong(1, readId);
                    pstmt.setInt(2, runId);
                    pstmt.setString(3, "READ" + readId);
                    pstmt.setString(4, chr);
                    pstmt.setLong(5, position);
                    pstmt.setInt(6, random.nextInt(60));
                    pstmt.setString(7, "100M");
                    pstmt.setString(8, generateRandomSequence(100));
                    pstmt.setString(9, generateRandomQuality(100));
                    pstmt.setInt(10, random.nextInt(256));
                    pstmt.setLong(11, position + 100 + random.nextInt(500));
                    pstmt.setInt(12, 200 + random.nextInt(600));
                    pstmt.setInt(13, random.nextInt(100));
                    
                    pstmt.addBatch();
                    readId++;
                    
                    if (readId % config.batchSize == 0) {
                        pstmt.executeBatch();
                        if (readId % config.commitInterval == 0) {
                            conn.commit();
                            System.out.printf("  Created %,d reads (Run %d/%d)\r", 
                                readId - 1, runId, config.numRuns);
                        }
                    }
                }
            }
            pstmt.executeBatch();
            conn.commit();
        }
        System.out.printf("  Created %,d total reads%n", (long) config.numRuns * readsPerRun);
    }
    
    private static void createVariants(Connection conn, int variantsPerRun) throws SQLException {
        String variantSql = "INSERT INTO variants VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PASS', ?, ?, " +
                           "'0/1', ?, ?, ?, ?, ?, " + nowFn() + ")";
        String annotSql = "INSERT INTO variant_annotations VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, " +
                         "?, ?, ?, NULL, NULL, ?, NULL, ?, NULL, ?, ?, " + nowFn() + ")";
        
        try (PreparedStatement variantStmt = conn.prepareStatement(variantSql);
             PreparedStatement annotStmt = conn.prepareStatement(annotSql)) {
            
            long variantId = 1;
            long annotId = 1;
            
            for (int runId = 1; runId <= config.numRuns; runId++) {
                for (int i = 0; i < variantsPerRun; i++) {
                    String chr = CHROMOSOMES[random.nextInt(CHROMOSOMES.length)];
                    long position = 1000 + random.nextInt(100000000);
                    String varType = VARIANT_TYPES[random.nextInt(VARIANT_TYPES.length)];
                    String ref = BASES[random.nextInt(BASES.length)];
                    String alt = BASES[random.nextInt(BASES.length)];
                    
                    // Insert variant
                    variantStmt.setLong(1, variantId);
                    variantStmt.setInt(2, runId);
                    variantStmt.setString(3, chr);
                    variantStmt.setLong(4, position);
                    variantStmt.setString(5, ref);
                    variantStmt.setString(6, alt);
                    variantStmt.setString(7, varType);
                    variantStmt.setDouble(8, 30 + random.nextDouble() * 70);
                    variantStmt.setInt(9, 20 + random.nextInt(200));
                    variantStmt.setDouble(10, random.nextDouble());
                    variantStmt.setInt(11, random.nextInt(99));
                    variantStmt.setDouble(12, -5 + random.nextDouble() * 10);
                    variantStmt.setDouble(13, -5 + random.nextDouble() * 10);
                    variantStmt.setDouble(14, -5 + random.nextDouble() * 10);
                    variantStmt.setDouble(15, -5 + random.nextDouble() * 10);
                    variantStmt.addBatch();
                    
                    // Insert annotation
                    annotStmt.setLong(1, annotId);
                    annotStmt.setLong(2, variantId);
                    annotStmt.setString(3, "GENE" + (random.nextInt(config.numGeneAnnotations) + 1));
                    annotStmt.setString(4, "ENST" + String.format("%011d", random.nextInt(config.numGeneAnnotations) + 1));
                    annotStmt.setString(5, "missense_variant");
                    annotStmt.setString(6, random.nextBoolean() ? "MODERATE" : "LOW");
                    annotStmt.setInt(7, random.nextInt(1000));
                    annotStmt.setString(8, String.valueOf(random.nextInt(5)));
                    annotStmt.setString(9, "rs" + variantId);
                    annotStmt.setDouble(10, random.nextDouble() * 0.01);
                    annotStmt.setDouble(11, random.nextDouble());
                    annotStmt.setDouble(12, random.nextDouble());
                    annotStmt.setDouble(13, random.nextDouble() * 40);
                    annotStmt.addBatch();
                    
                    variantId++;
                    annotId++;
                    
                    if (variantId % config.batchSize == 0) {
                        variantStmt.executeBatch();
                        annotStmt.executeBatch();
                        if (variantId % config.commitInterval == 0) {
                            conn.commit();
                            System.out.printf("  Created %,d variants (Run %d/%d)\r", 
                                variantId - 1, runId, config.numRuns);
                        }
                    }
                }
            }
            variantStmt.executeBatch();
            annotStmt.executeBatch();
            conn.commit();
        }
        System.out.printf("  Created %,d total variants with annotations%n",
            (long) config.numRuns * variantsPerRun);
    }

    private static void synchronizeSequences(Connection conn) throws SQLException {
        if (config.dbms == DB_MYSQL) {
            System.out.println("  Skipping sequence sync (MySQL uses AUTO_INCREMENT)");
            return;
        }
        try (Statement stmt = conn.createStatement()) {
            // Sync seq_variant_id with max variant_id
            ResultSet rs = stmt.executeQuery("SELECT COALESCE(MAX(variant_id), 0) FROM variants");
            if (rs.next()) {
                long maxVariantId = rs.getLong(1);
                long nextValue = maxVariantId + 1;
                // Drop and recreate sequence starting at max + 1
                stmt.execute("DROP SEQUENCE seq_variant_id");
                stmt.execute("CREATE SEQUENCE seq_variant_id START WITH " + nextValue +
                           " INCREMENT BY 1 CACHE 10000");
                System.out.printf("  Synchronized seq_variant_id to %d\n", nextValue);
            }
            rs.close();

            // Sync seq_stat_id with max stat_id
            rs = stmt.executeQuery("SELECT COALESCE(MAX(stat_id), 0) FROM alignment_stats");
            if (rs.next()) {
                int maxStatId = rs.getInt(1);
                int nextValue = maxStatId + 1;
                // Drop and recreate sequence starting at max + 1
                stmt.execute("DROP SEQUENCE seq_stat_id");
                stmt.execute("CREATE SEQUENCE seq_stat_id START WITH " + nextValue +
                           " INCREMENT BY 1 CACHE 100");
                System.out.printf("  Synchronized seq_stat_id to %d\n", nextValue);
            }
            rs.close();
        }
    }

    private static void updateStatistics(Connection conn) throws SQLException {
        String[] tables = {
            "reference_genomes",
            "gene_annotations",
            "samples",
            "sequencing_runs",
            "sequencing_reads",
            "alignment_stats",
            "variants",
            "variant_annotations"
        };

        try (Statement stmt = conn.createStatement()) {
            for (String table : tables) {
                if (config.dbms == DB_MYSQL) {
                    stmt.execute("ANALYZE TABLE " + table);
                } else if (config.dbms == DB_POSTGRESQL) {
                    stmt.execute("ANALYZE " + table);
                } else {
                    // Oracle: gather table stats via DBMS_STATS
                    try (CallableStatement cstmt = conn.prepareCall(
                            "BEGIN DBMS_STATS.GATHER_TABLE_STATS(NULL, ?, NULL, DBMS_STATS.AUTO_SAMPLE_SIZE); END;")) {
                        cstmt.setString(1, table.toUpperCase());
                        cstmt.execute();
                    }
                }
                System.out.printf("  Updated statistics for %s\n", table);
            }
        }
        System.out.println("  Statistics update completed");
    }

    private static String generateRandomSequence(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(BASES[random.nextInt(BASES.length)]);
        }
        return sb.toString();
    }
    
    private static String generateRandomQuality(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) (33 + random.nextInt(40))); // ASCII 33-72
        }
        return sb.toString();
    }
    
    private static void printTableCounts() throws SQLException {
        System.out.println("\nTable Row Counts:");
        System.out.println("-".repeat(50));
        
        String[] tables = {
            "reference_genomes", "gene_annotations", "samples", "sequencing_runs",
            "sequencing_reads", "variants", "variant_annotations"
        };
        
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            
            for (String table : tables) {
                String sql = "SELECT COUNT(*) FROM " + table;
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    if (rs.next()) {
                        System.out.printf("%-25s: %,12d\n", table, rs.getLong(1));
                    }
                }
            }
        }
        System.out.println("-".repeat(50));
    }
    
    // ========================================================================
    // WORKLOAD MODE
    // ========================================================================
    
    private static void runWorkloadMode() throws SQLException, InterruptedException {
        System.out.println("\n--- WORKLOAD MODE ---");
        System.out.printf("Threads: %d, Duration: %d seconds\n", 
            config.threadCount, config.durationSeconds);
        System.out.printf("Workload Mix - Read: %d%%, Update: %d%%, Insert: %d%%, Delete: %d%%\n",
            config.readPercent, config.updatePercent, config.insertPercent, config.deletePercent);
        
        // Get data ranges for workload
        DataRanges ranges = getDataRanges();
        System.out.printf("\nData Ranges:\n");
        System.out.printf("  Samples: 1-%d\n", ranges.maxSampleId);
        System.out.printf("  Runs: 1-%d\n", ranges.maxRunId);
        System.out.printf("  Variants: 1-%d\n", ranges.maxVariantId);
        
        // Prepare statements once for each thread (each thread gets its own connection and statements)
        System.out.println("\nPreparing statements for worker threads...");
        List<PreparedStatements> allStatements = new ArrayList<>();
        List<Connection> allConnections = new ArrayList<>();

        try {
            for (int i = 0; i < config.threadCount; i++) {
                Connection conn = getConnection();
                conn.setAutoCommit(true);
                PreparedStatements stmts = prepareStatements(conn);
                allConnections.add(conn);
                allStatements.add(stmts);
            }
            System.out.printf("  Prepared statements for %d threads\n", config.threadCount);
        } catch (SQLException e) {
            // Cleanup on error
            for (PreparedStatements stmts : allStatements) stmts.closeAll();
            for (Connection conn : allConnections) {
                try { conn.close(); } catch (SQLException ex) { /* ignore */ }
            }
            throw e;
        }

        // Create thread pool
        ExecutorService executor = Executors.newFixedThreadPool(config.threadCount);
        CountDownLatch latch = new CountDownLatch(config.threadCount);
        AtomicBoolean stopFlag = new AtomicBoolean(false);

        metrics.startTime = System.currentTimeMillis();

        // Start worker threads
        System.out.println("\nStarting workload threads...\n");
        for (int i = 0; i < config.threadCount; i++) {
            final int threadId = i;
            final Connection conn = allConnections.get(i);
            final PreparedStatements stmts = allStatements.get(i);
            executor.submit(() -> {
                runWorkerThread(threadId, ranges, conn, stmts, stopFlag, latch);
            });
        }
        
        // Progress monitor
        Thread monitor = new Thread(() -> {
            try {
                while (!stopFlag.get()) {
                    Thread.sleep(5000);
                    long elapsed = (System.currentTimeMillis() - metrics.startTime) / 1000;
                    double tps = metrics.totalOperations.get() / (double) elapsed;
                    System.out.printf("[%ds] Operations: %,d | TPS: %,.2f | Success: %,d | Failed: %,d\n",
                        elapsed, metrics.totalOperations.get(), tps, 
                        metrics.successfulOps.get(), metrics.failedOps.get());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        monitor.setDaemon(true);
        monitor.start();
        
        // Wait for duration
        Thread.sleep(config.durationSeconds * 1000L);
        
        // Stop workers
        stopFlag.set(true);
        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        // Cleanup connections and statements
        System.out.println("\nCleaning up connections...");
        for (PreparedStatements stmts : allStatements) {
            stmts.closeAll();
        }
        for (Connection conn : allConnections) {
            try { conn.close(); } catch (SQLException e) { /* ignore */ }
        }

        // Print results
        metrics.printReport();
    }
    
    private static class DataRanges {
        int maxSampleId;
        int maxRunId;
        long maxVariantId;
        long maxReadId;
    }

    private static class PreparedStatements {
        // Query statements
        PreparedStatement queryVariantsByRegion;
        PreparedStatement querySampleDetails;
        PreparedStatement queryRunStats;
        PreparedStatement queryHighQualityVariants;
        PreparedStatement queryVariantAnnotations;

        // Update statements
        PreparedStatement updateRunStatus;
        PreparedStatement updateVariantQuality;
        PreparedStatement updateSampleMetadata;

        // Insert statements
        PreparedStatement insertVariant;
        PreparedStatement insertAlignmentStats;
        PreparedStatement checkAlignmentStatsExists;
        PreparedStatement getNextVariantId;
        PreparedStatement getNextStatId;

        // Delete statements
        PreparedStatement deleteVariantAnnotations;
        PreparedStatement deleteVariant;
        PreparedStatement deleteAlignmentStats;

        void closeAll() {
            try {
                if (queryVariantsByRegion != null) queryVariantsByRegion.close();
                if (querySampleDetails != null) querySampleDetails.close();
                if (queryRunStats != null) queryRunStats.close();
                if (queryHighQualityVariants != null) queryHighQualityVariants.close();
                if (queryVariantAnnotations != null) queryVariantAnnotations.close();
                if (updateRunStatus != null) updateRunStatus.close();
                if (updateVariantQuality != null) updateVariantQuality.close();
                if (updateSampleMetadata != null) updateSampleMetadata.close();
                if (insertVariant != null) insertVariant.close();
                if (insertAlignmentStats != null) insertAlignmentStats.close();
                if (checkAlignmentStatsExists != null) checkAlignmentStatsExists.close();
                if (getNextVariantId != null) getNextVariantId.close();
                if (getNextStatId != null) getNextStatId.close();
                if (deleteVariantAnnotations != null) deleteVariantAnnotations.close();
                if (deleteVariant != null) deleteVariant.close();
                if (deleteAlignmentStats != null) deleteAlignmentStats.close();
            } catch (SQLException e) {
                // Ignore close errors
            }
        }
    }
    
    private static DataRanges getDataRanges() throws SQLException {
        DataRanges ranges = new DataRanges();
        
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            
            try (ResultSet rs = stmt.executeQuery("SELECT MAX(sample_id) FROM samples")) {
                if (rs.next()) ranges.maxSampleId = rs.getInt(1);
            }
            
            try (ResultSet rs = stmt.executeQuery("SELECT MAX(run_id) FROM sequencing_runs")) {
                if (rs.next()) ranges.maxRunId = rs.getInt(1);
            }
            
            try (ResultSet rs = stmt.executeQuery("SELECT MAX(variant_id) FROM variants")) {
                if (rs.next()) ranges.maxVariantId = rs.getLong(1);
            }
            
            try (ResultSet rs = stmt.executeQuery("SELECT MAX(read_id) FROM sequencing_reads")) {
                if (rs.next()) ranges.maxReadId = rs.getLong(1);
            }
        }
        
        return ranges;
    }

    private static PreparedStatements prepareStatements(Connection conn) throws SQLException {
        PreparedStatements stmts = new PreparedStatements();

        // Query statements
        stmts.queryVariantsByRegion = conn.prepareStatement(
            "SELECT v.*, va.gene_symbol, va.consequence FROM variants v " +
            "LEFT JOIN variant_annotations va ON v.variant_id = va.variant_id " +
            "WHERE v.chromosome = ? AND v.position BETWEEN ? AND ? " +
            "AND v.quality_score >= 30 " + limitSuffix(100));

        stmts.querySampleDetails = conn.prepareStatement(
            "SELECT s.sample_id, s.sample_name, s.patient_id, s.sample_type, " +
            "s.tissue_source, s.collection_date, s.organism, s.created_date, s.updated_date, " +
            "COUNT(sr.run_id) as run_count FROM samples s " +
            "LEFT JOIN sequencing_runs sr ON s.sample_id = sr.sample_id " +
            "WHERE s.sample_id = ? " +
            "GROUP BY s.sample_id, s.sample_name, s.patient_id, s.sample_type, " +
            "s.tissue_source, s.collection_date, s.organism, s.created_date, s.updated_date");

        stmts.queryRunStats = conn.prepareStatement(
            "SELECT sr.*, COUNT(v.variant_id) as variant_count FROM sequencing_runs sr " +
            "LEFT JOIN variants v ON sr.run_id = v.run_id " +
            "WHERE sr.run_id = ? " +
            "GROUP BY sr.run_id, sr.run_name, sr.sample_id, sr.genome_id, sr.platform, " +
            "sr.chemistry, sr.instrument_id, sr.flow_cell_id, sr.read_length, sr.paired_end, " +
            "sr.target_coverage, sr.run_date, sr.status, sr.total_reads, sr.quality_score, " +
            "sr.created_date, sr.completed_date");

        stmts.queryHighQualityVariants = conn.prepareStatement(
            "SELECT * FROM variants WHERE run_id = ? AND quality_score >= 50 " +
            "ORDER BY quality_score DESC " + limitSuffix(50));

        stmts.queryVariantAnnotations = conn.prepareStatement(
            "SELECT * FROM variant_annotations WHERE variant_id = ?");

        // Update statements
        stmts.updateRunStatus = conn.prepareStatement(
            "UPDATE sequencing_runs SET status = ? WHERE run_id = ?");

        stmts.updateVariantQuality = conn.prepareStatement(
            "UPDATE variants SET quality_score = ? WHERE variant_id = ?");

        stmts.updateSampleMetadata = conn.prepareStatement(
            "UPDATE samples SET updated_date = " + nowFn() + " WHERE sample_id = ?");

        // Insert statements
        stmts.insertVariant = conn.prepareStatement(
            "INSERT INTO variants (variant_id, run_id, chromosome, position, " +
            "reference_allele, alternate_allele, variant_type, quality_score, " +
            "filter_status, depth, allele_frequency, genotype, genotype_quality, " +
            "strand_bias, base_quality_rank_sum, mapping_quality_rank_sum, " +
            "read_pos_rank_sum, variant_call_date) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PASS', ?, ?, '0/1', ?, ?, ?, ?, ?, " + nowFn() + ")");

        stmts.insertAlignmentStats = conn.prepareStatement(
            "INSERT INTO alignment_stats VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + nowFn() + ")");

        stmts.checkAlignmentStatsExists = conn.prepareStatement(
            "SELECT COUNT(*) FROM alignment_stats WHERE run_id = ? AND chromosome = ?");

        stmts.getNextVariantId = conn.prepareStatement(nextValSql("seq_variant_id"));

        stmts.getNextStatId = conn.prepareStatement(nextValSql("seq_stat_id"));

        // Delete statements
        stmts.deleteVariantAnnotations = conn.prepareStatement(
            "DELETE FROM variant_annotations WHERE variant_id = ?");

        stmts.deleteVariant = conn.prepareStatement(
            "DELETE FROM variants WHERE variant_id = ?");

        stmts.deleteAlignmentStats = conn.prepareStatement(
            "DELETE FROM alignment_stats WHERE run_id = ? AND chromosome = ?");

        return stmts;
    }

    private static void runWorkerThread(int threadId, DataRanges ranges, Connection conn,
                                       PreparedStatements stmts, AtomicBoolean stopFlag, CountDownLatch latch) {
        try {
            ThreadLocalRandom tlr = ThreadLocalRandom.current();
            
            while (!stopFlag.get()) {
                int opType = tlr.nextInt(100);
                
                try {
                    if (opType < config.readPercent) {
                        // Read operation
                        performReadOperation(stmts, ranges, tlr);
                    } else if (opType < config.readPercent + config.updatePercent) {
                        // Update operation
                        performUpdateOperation(stmts, ranges, tlr);
                    } else if (opType < config.readPercent + config.updatePercent + config.insertPercent) {
                        // Insert operation
                        performInsertOperation(stmts, ranges, tlr);
                    } else {
                        // Delete operation
                        performDeleteOperation(stmts, ranges, tlr);
                    }
                } catch (SQLException e) {
                    metrics.failedOps.incrementAndGet();
                    if (config.verboseOutput) {
                        System.err.printf("[Thread %d] SQL Error: %s\n", threadId, e.getMessage());
                        e.printStackTrace();
                    }
                }
            }
        } finally {
            latch.countDown();
        }
    }
    
    private static void performReadOperation(PreparedStatements stmts, DataRanges ranges,
                                            ThreadLocalRandom tlr) throws SQLException {
        int readType = tlr.nextInt(5);
        long startTime = System.currentTimeMillis();
        boolean success = false;
        String opName = "";

        try {
            switch (readType) {
                case 0: // Query variants by region
                    opName = "QueryVariantsByRegion";
                    queryVariantsByRegion(stmts, ranges, tlr);
                    break;
                case 1: // Query sample details
                    opName = "QuerySampleDetails";
                    querySampleDetails(stmts, ranges, tlr);
                    break;
                case 2: // Query run statistics
                    opName = "QueryRunStats";
                    queryRunStats(stmts, ranges, tlr);
                    break;
                case 3: // Query high quality variants
                    opName = "QueryHighQualityVariants";
                    queryHighQualityVariants(stmts, ranges, tlr);
                    break;
                case 4: // Query variant annotations
                    opName = "QueryVariantAnnotations";
                    queryVariantAnnotations(stmts, ranges, tlr);
                    break;
            }
            success = true;
            metrics.readOps.incrementAndGet();
        } finally {
            long latency = System.currentTimeMillis() - startTime;
            metrics.recordOperation(opName, latency, success);
        }
    }
    
    private static void performUpdateOperation(PreparedStatements stmts, DataRanges ranges,
                                              ThreadLocalRandom tlr) throws SQLException {
        int updateType = tlr.nextInt(3);
        long startTime = System.currentTimeMillis();
        boolean success = false;
        String opName = "";

        try {
            switch (updateType) {
                case 0: // Update run status
                    opName = "UpdateRunStatus";
                    updateRunStatus(stmts, ranges, tlr);
                    break;
                case 1: // Update variant quality
                    opName = "UpdateVariantQuality";
                    updateVariantQuality(stmts, ranges, tlr);
                    break;
                case 2: // Update sample metadata
                    opName = "UpdateSampleMetadata";
                    updateSampleMetadata(stmts, ranges, tlr);
                    break;
            }
            success = true;
            metrics.updateOps.incrementAndGet();
        } finally {
            long latency = System.currentTimeMillis() - startTime;
            metrics.recordOperation(opName, latency, success);
        }
    }
    
    private static void performInsertOperation(PreparedStatements stmts, DataRanges ranges,
                                              ThreadLocalRandom tlr) throws SQLException {
        int insertType = tlr.nextInt(2);
        long startTime = System.currentTimeMillis();
        boolean success = false;
        String opName = "";

        try {
            switch (insertType) {
                case 0: // Insert new variant
                    opName = "InsertVariant";
                    insertNewVariant(stmts, ranges, tlr);
                    break;
                case 1: // Insert alignment stats
                    opName = "InsertAlignmentStats";
                    insertAlignmentStats(stmts, ranges, tlr);
                    break;
            }
            success = true;
            metrics.insertOps.incrementAndGet();
        } finally {
            long latency = System.currentTimeMillis() - startTime;
            metrics.recordOperation(opName, latency, success);
        }
    }
    
    // Query implementations
    private static void queryVariantsByRegion(PreparedStatements stmts, DataRanges ranges,
                                             ThreadLocalRandom tlr) throws SQLException {
        String chr = CHROMOSOMES[tlr.nextInt(CHROMOSOMES.length)];
        long startPos = tlr.nextLong(1000000, 100000000);
        long endPos = startPos + tlr.nextLong(10000, 1000000);

        stmts.queryVariantsByRegion.setString(1, chr);
        stmts.queryVariantsByRegion.setLong(2, startPos);
        stmts.queryVariantsByRegion.setLong(3, endPos);
        try (ResultSet rs = stmts.queryVariantsByRegion.executeQuery()) {
            int count = 0;
            while (rs.next()) count++;
        }
    }
    
    private static void querySampleDetails(PreparedStatements stmts, DataRanges ranges,
                                          ThreadLocalRandom tlr) throws SQLException {
        int sampleId = tlr.nextInt(1, ranges.maxSampleId + 1);

        stmts.querySampleDetails.setInt(1, sampleId);
        try (ResultSet rs = stmts.querySampleDetails.executeQuery()) {
            while (rs.next()) {
                // Process result
            }
        }
    }
    
    private static void queryRunStats(PreparedStatements stmts, DataRanges ranges,
                                     ThreadLocalRandom tlr) throws SQLException {
        int runId = tlr.nextInt(1, ranges.maxRunId + 1);

        stmts.queryRunStats.setInt(1, runId);
        try (ResultSet rs = stmts.queryRunStats.executeQuery()) {
            while (rs.next()) {
                // Process result
            }
        }
    }
    
    private static void queryHighQualityVariants(PreparedStatements stmts, DataRanges ranges,
                                                 ThreadLocalRandom tlr) throws SQLException {
        int runId = tlr.nextInt(1, ranges.maxRunId + 1);

        stmts.queryHighQualityVariants.setInt(1, runId);
        try (ResultSet rs = stmts.queryHighQualityVariants.executeQuery()) {
            while (rs.next()) {
                // Process result
            }
        }
    }
    
    private static void queryVariantAnnotations(PreparedStatements stmts, DataRanges ranges,
                                                ThreadLocalRandom tlr) throws SQLException {
        long variantId = tlr.nextLong(1, ranges.maxVariantId + 1);

        stmts.queryVariantAnnotations.setLong(1, variantId);
        try (ResultSet rs = stmts.queryVariantAnnotations.executeQuery()) {
            while (rs.next()) {
                // Process result
            }
        }
    }
    
    // Update implementations
    private static void updateRunStatus(PreparedStatements stmts, DataRanges ranges,
                                       ThreadLocalRandom tlr) throws SQLException {
        int runId = tlr.nextInt(1, ranges.maxRunId + 1);
        String[] statuses = {"PENDING", "RUNNING", "COMPLETED", "FAILED"};
        String status = statuses[tlr.nextInt(statuses.length)];

        stmts.updateRunStatus.setString(1, status);
        stmts.updateRunStatus.setInt(2, runId);
        stmts.updateRunStatus.executeUpdate();
    }
    
    private static void updateVariantQuality(PreparedStatements stmts, DataRanges ranges,
                                            ThreadLocalRandom tlr) throws SQLException {
        long variantId = tlr.nextLong(1, ranges.maxVariantId + 1);
        double newQuality = 30 + tlr.nextDouble() * 70;

        stmts.updateVariantQuality.setDouble(1, newQuality);
        stmts.updateVariantQuality.setLong(2, variantId);
        stmts.updateVariantQuality.executeUpdate();
    }
    
    private static void updateSampleMetadata(PreparedStatements stmts, DataRanges ranges,
                                            ThreadLocalRandom tlr) throws SQLException {
        int sampleId = tlr.nextInt(1, ranges.maxSampleId + 1);

        stmts.updateSampleMetadata.setInt(1, sampleId);
        stmts.updateSampleMetadata.executeUpdate();
    }
    
    // Insert implementations
    private static void insertNewVariant(PreparedStatements stmts, DataRanges ranges,
                                        ThreadLocalRandom tlr) throws SQLException {
        int runId = tlr.nextInt(1, ranges.maxRunId + 1);
        String chr = CHROMOSOMES[tlr.nextInt(CHROMOSOMES.length)];
        long position = tlr.nextLong(1000, 100000000);

        // Get next variant_id from sequence
        long nextVariantId = 0;
        try (ResultSet rs = stmts.getNextVariantId.executeQuery()) {
            if (rs.next()) {
                nextVariantId = rs.getLong(1);
            }
        }

        stmts.insertVariant.setLong(1, nextVariantId);
        stmts.insertVariant.setInt(2, runId);
        stmts.insertVariant.setString(3, chr);
        stmts.insertVariant.setLong(4, position);
        stmts.insertVariant.setString(5, BASES[tlr.nextInt(BASES.length)]);
        stmts.insertVariant.setString(6, BASES[tlr.nextInt(BASES.length)]);
        stmts.insertVariant.setString(7, VARIANT_TYPES[tlr.nextInt(VARIANT_TYPES.length)]);
        stmts.insertVariant.setDouble(8, 30 + tlr.nextDouble() * 70);
        stmts.insertVariant.setInt(9, 20 + tlr.nextInt(200));
        stmts.insertVariant.setDouble(10, tlr.nextDouble());
        stmts.insertVariant.setInt(11, tlr.nextInt(99));
        stmts.insertVariant.setDouble(12, -5 + tlr.nextDouble() * 10);
        stmts.insertVariant.setDouble(13, -5 + tlr.nextDouble() * 10);
        stmts.insertVariant.setDouble(14, -5 + tlr.nextDouble() * 10);
        stmts.insertVariant.setDouble(15, -5 + tlr.nextDouble() * 10);
        stmts.insertVariant.executeUpdate();
    }
    
    private static void insertAlignmentStats(PreparedStatements stmts, DataRanges ranges,
                                            ThreadLocalRandom tlr) throws SQLException {
        int runId = tlr.nextInt(1, ranges.maxRunId + 1);
        String chr = CHROMOSOMES[tlr.nextInt(CHROMOSOMES.length)];

        // Check if stats already exist
        stmts.checkAlignmentStatsExists.setInt(1, runId);
        stmts.checkAlignmentStatsExists.setString(2, chr);
        try (ResultSet rs = stmts.checkAlignmentStatsExists.executeQuery()) {
            if (rs.next() && rs.getInt(1) > 0) {
                return; // Already exists
            }
        }

        // Get next stat_id from sequence
        int nextStatId = 0;
        try (ResultSet rs = stmts.getNextStatId.executeQuery()) {
            if (rs.next()) {
                nextStatId = rs.getInt(1);
            }
        }

        long totalReads = tlr.nextLong(10000, 1000000);
        long mappedReads = (long) (totalReads * (0.9 + tlr.nextDouble() * 0.09));

        stmts.insertAlignmentStats.setInt(1, nextStatId);
        stmts.insertAlignmentStats.setInt(2, runId);
        stmts.insertAlignmentStats.setString(3, chr);
        stmts.insertAlignmentStats.setLong(4, totalReads);
        stmts.insertAlignmentStats.setLong(5, mappedReads);
        stmts.insertAlignmentStats.setLong(6, (long) (mappedReads * (0.9 + tlr.nextDouble() * 0.09)));
        stmts.insertAlignmentStats.setLong(7, (long) (totalReads * 0.05 * tlr.nextDouble()));
        stmts.insertAlignmentStats.setDouble(8, 30 + tlr.nextDouble() * 50);
        stmts.insertAlignmentStats.setDouble(9, 25 + tlr.nextDouble() * 45);
        stmts.insertAlignmentStats.setDouble(10, 5 + tlr.nextDouble() * 15);
        stmts.insertAlignmentStats.setDouble(11, 35 + tlr.nextDouble() * 5);
        stmts.insertAlignmentStats.setDouble(12, 85 + tlr.nextDouble() * 10);
        stmts.insertAlignmentStats.executeUpdate();
    }

    private static void performDeleteOperation(PreparedStatements stmts, DataRanges ranges,
                                              ThreadLocalRandom tlr) throws SQLException {
        int deleteType = tlr.nextInt(2);
        long startTime = System.currentTimeMillis();
        boolean success = false;
        String opName = "";

        try {
            switch (deleteType) {
                case 0: // Delete variant
                    opName = "DeleteVariant";
                    deleteVariant(stmts, ranges, tlr);
                    break;
                case 1: // Delete alignment stats
                    opName = "DeleteAlignmentStats";
                    deleteAlignmentStats(stmts, ranges, tlr);
                    break;
            }
            success = true;
            metrics.deleteOps.incrementAndGet();
        } finally {
            long latency = System.currentTimeMillis() - startTime;
            metrics.recordOperation(opName, latency, success);
        }
    }

    // Delete implementations
    private static void deleteVariant(PreparedStatements stmts, DataRanges ranges,
                                     ThreadLocalRandom tlr) throws SQLException {
        // Select a random variant ID to delete
        long variantId = tlr.nextLong(1, ranges.maxVariantId + 1);

        // Delete child annotations first (to avoid foreign key violation)
        stmts.deleteVariantAnnotations.setLong(1, variantId);
        stmts.deleteVariantAnnotations.executeUpdate();

        // Then delete the parent variant
        stmts.deleteVariant.setLong(1, variantId);
        stmts.deleteVariant.executeUpdate();
    }

    private static void deleteAlignmentStats(PreparedStatements stmts, DataRanges ranges,
                                            ThreadLocalRandom tlr) throws SQLException {
        // Select a random run and chromosome to delete
        int runId = tlr.nextInt(1, ranges.maxRunId + 1);
        String chr = CHROMOSOMES[tlr.nextInt(CHROMOSOMES.length)];

        stmts.deleteAlignmentStats.setInt(1, runId);
        stmts.deleteAlignmentStats.setString(2, chr);
        stmts.deleteAlignmentStats.executeUpdate();
    }

    private static String nowFn() {
        return (config.dbms == DB_ORACLE) ? "SYSDATE" : "NOW()";
    }

    private static String limitSuffix(int n) {
        return (config.dbms == DB_ORACLE) ? "FETCH FIRST " + n + " ROWS ONLY" : "LIMIT " + n;
    }

    private static String nextValSql(String seqName) {
        if (config.dbms == DB_ORACLE)
            return "SELECT " + seqName + ".NEXTVAL FROM dual";
        else if (config.dbms == DB_POSTGRESQL)
            return "SELECT NEXTVAL('" + seqName + "')";
        else
            return "SELECT UUID_SHORT()";  // MySQL: no sequences
    }

    // Connection management
    private static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(config.jdbcUrl, config.username, config.password);
    }

    // ========================================================================
    // DNASchema — creates and drops the database schema
    // ========================================================================

    static class DNASchema {

        private final Connection conn;
        private final int dbms;

        DNASchema(Connection conn, int dbms) {
            this.conn = conn;
            this.dbms = dbms;
        }

        void create() throws SQLException {
            System.out.println("\n--- CREATING SCHEMA ---");
            dropViews();
            if (dbms != DB_MYSQL) dropSequences();
            dropTables();
            createTables();
            createIndexes();
            if (dbms != DB_MYSQL) createSequences();
            createViews();
            System.out.println("Schema created.\n");
        }

        // Drop phase — errors silently ignored (object may not exist yet)

        private void dropViews() throws SQLException {
            try (Statement stmt = conn.createStatement()) {
                tryExecute(stmt, "DROP VIEW v_high_impact_variants");
                tryExecute(stmt, "DROP VIEW v_run_summary");
            }
        }

        private void dropSequences() throws SQLException {
            String[] seqs = {
                "seq_var_annotation_id", "seq_variant_id", "seq_stat_id",
                "seq_read_id", "seq_run_id", "seq_sample_id",
                "seq_annotation_id", "seq_genome_id"
            };
            try (Statement stmt = conn.createStatement()) {
                for (String seq : seqs)
                    tryExecute(stmt, "DROP SEQUENCE " + seq);
            }
        }

        private void dropTables() throws SQLException {
            String[] tables = {
                "variant_annotations", "variants", "alignment_stats",
                "sequencing_reads", "sequencing_runs", "samples",
                "gene_annotations", "reference_genomes"
            };
            try (Statement stmt = conn.createStatement()) {
                if (dbms == DB_MYSQL)
                    stmt.execute("SET FOREIGN_KEY_CHECKS=0");
                String cascade = (dbms == DB_ORACLE)     ? " CASCADE CONSTRAINTS"
                               : (dbms == DB_POSTGRESQL) ? " CASCADE"
                               : "";
                for (String table : tables)
                    tryExecute(stmt, "DROP TABLE " + table + cascade);
                if (dbms == DB_MYSQL)
                    stmt.execute("SET FOREIGN_KEY_CHECKS=1");
            }
        }

        // Create phase

        private void createTables() throws SQLException {
            System.out.println("  Creating tables...");
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(sqlReferenceGenomes());
                stmt.execute(sqlGeneAnnotations());
                stmt.execute(sqlSamples());
                stmt.execute(sqlSequencingRuns());
                stmt.execute(sqlSequencingReads());
                stmt.execute(sqlAlignmentStats());
                stmt.execute(sqlVariants());
                stmt.execute(sqlVariantAnnotations());
            }
        }

        private void createIndexes() throws SQLException {
            System.out.println("  Creating indexes...");
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE INDEX idx_gene_location ON gene_annotations(genome_id, chromosome, start_position, end_position)");
                stmt.execute("CREATE INDEX idx_gene_symbol ON gene_annotations(gene_symbol)");
                stmt.execute("CREATE INDEX idx_sample_patient ON samples(patient_id)");
                stmt.execute("CREATE INDEX idx_sample_type ON samples(sample_type)");
                stmt.execute("CREATE INDEX idx_run_sample ON sequencing_runs(sample_id)");
                stmt.execute("CREATE INDEX idx_run_status ON sequencing_runs(status)");
                stmt.execute("CREATE INDEX idx_run_date ON sequencing_runs(run_date)");
                stmt.execute("CREATE INDEX idx_reads_location ON sequencing_reads(run_id, chromosome, position)");
                stmt.execute("CREATE INDEX idx_reads_run ON sequencing_reads(run_id)");
                stmt.execute("CREATE INDEX idx_stats_run ON alignment_stats(run_id)");
                stmt.execute("CREATE INDEX idx_variant_location ON variants(chromosome, position)");
                stmt.execute("CREATE INDEX idx_variant_run_location ON variants(run_id, chromosome, position)");
                stmt.execute("CREATE INDEX idx_variant_type ON variants(variant_type)");
                // Partial index (WHERE clause) is PostgreSQL-only
                if (dbms == DB_POSTGRESQL)
                    stmt.execute("CREATE INDEX idx_variant_quality ON variants(quality_score) WHERE quality_score >= 30");
                else
                    stmt.execute("CREATE INDEX idx_variant_quality ON variants(quality_score)");
                stmt.execute("CREATE INDEX idx_annot_variant ON variant_annotations(variant_id)");
                stmt.execute("CREATE INDEX idx_annot_gene ON variant_annotations(gene_symbol)");
                stmt.execute("CREATE INDEX idx_annot_impact ON variant_annotations(impact)");
                stmt.execute("CREATE INDEX idx_annot_clinvar ON variant_annotations(clinvar_significance)");
            }
        }

        private void createSequences() throws SQLException {
            System.out.println("  Creating sequences...");
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(seqDdl("seq_genome_id",         1, 1,   100));
                stmt.execute(seqDdl("seq_annotation_id",     1, 1,  1000));
                stmt.execute(seqDdl("seq_sample_id",         1, 1,   100));
                stmt.execute(seqDdl("seq_run_id",            1, 1,   100));
                stmt.execute(seqDdl("seq_read_id",           1, 1, 10000));
                stmt.execute(seqDdl("seq_stat_id",           1, 1,   100));
                stmt.execute(seqDdl("seq_variant_id",        1, 1, 10000));
                stmt.execute(seqDdl("seq_var_annotation_id", 1, 1, 10000));
            }
        }

        private void createViews() throws SQLException {
            System.out.println("  Creating views...");
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(sqlRunSummaryView());
                stmt.execute(sqlHighImpactVariantsView());
            }
        }

        // Table DDL — dialect-aware

        private String sqlReferenceGenomes() {
            return "CREATE TABLE reference_genomes (" +
                "genome_id INTEGER NOT NULL PRIMARY KEY, " +
                "genome_name " + varchar(100) + " NOT NULL, " +
                "version " + varchar(50) + " NOT NULL, " +
                "organism " + varchar(100) + " NOT NULL, " +
                "total_length " + bigint() + " NOT NULL, " +
                "chromosome_count INTEGER NOT NULL, " +
                "build_date DATE, " +
                "source " + varchar(200) + ", " +
                "created_date " + timestamp() + " NOT NULL)";
        }

        private String sqlGeneAnnotations() {
            return "CREATE TABLE gene_annotations (" +
                "annotation_id INTEGER NOT NULL PRIMARY KEY, " +
                "genome_id INTEGER NOT NULL, " +
                "chromosome " + varchar(10) + " NOT NULL, " +
                "gene_symbol " + varchar(50) + " NOT NULL, " +
                "gene_name " + varchar(200) + ", " +
                "start_position " + bigint() + " NOT NULL, " +
                "end_position " + bigint() + " NOT NULL, " +
                "strand CHAR(1), " +
                "gene_type " + varchar(50) + ", " +
                "transcript_id " + varchar(50) + ", " +
                "FOREIGN KEY (genome_id) REFERENCES reference_genomes(genome_id))";
        }

        private String sqlSamples() {
            return "CREATE TABLE samples (" +
                "sample_id INTEGER NOT NULL PRIMARY KEY, " +
                "sample_name " + varchar(100) + " NOT NULL UNIQUE, " +
                "patient_id " + varchar(50) + ", " +
                "sample_type " + varchar(50) + " NOT NULL, " +
                "tissue_source " + varchar(100) + ", " +
                "collection_date DATE, " +
                "organism " + varchar(100) + " DEFAULT 'Homo sapiens', " +
                "metadata " + clob() + ", " +
                "created_date " + timestamp() + " NOT NULL, " +
                "updated_date " + timestamp() + ")";
        }

        private String sqlSequencingRuns() {
            return "CREATE TABLE sequencing_runs (" +
                "run_id INTEGER NOT NULL PRIMARY KEY, " +
                "run_name " + varchar(100) + " NOT NULL UNIQUE, " +
                "sample_id INTEGER NOT NULL, " +
                "genome_id INTEGER NOT NULL, " +
                "platform " + varchar(50) + " NOT NULL, " +
                "chemistry " + varchar(50) + ", " +
                "instrument_id " + varchar(50) + ", " +
                "flow_cell_id " + varchar(50) + ", " +
                "read_length INTEGER, " +
                "paired_end CHAR(1) DEFAULT 'Y', " +
                "target_coverage INTEGER, " +
                "run_date DATE NOT NULL, " +
                "status " + varchar(20) + " DEFAULT 'PENDING', " +
                "total_reads " + bigint() + ", " +
                "quality_score DECIMAL(5,2), " +
                "created_date " + timestamp() + " NOT NULL, " +
                "completed_date " + timestamp() + ", " +
                "FOREIGN KEY (sample_id) REFERENCES samples(sample_id), " +
                "FOREIGN KEY (genome_id) REFERENCES reference_genomes(genome_id))";
        }

        private String sqlSequencingReads() {
            return "CREATE TABLE sequencing_reads (" +
                "read_id " + bigint() + " NOT NULL PRIMARY KEY, " +
                "run_id INTEGER NOT NULL, " +
                "read_name " + varchar(100) + " NOT NULL, " +
                "chromosome " + varchar(10) + " NOT NULL, " +
                "position " + bigint() + " NOT NULL, " +
                "mapping_quality INTEGER, " +
                "cigar_string " + varchar(500) + ", " +
                "sequence " + varchar(1000) + ", " +
                "quality_scores " + varchar(1000) + ", " +
                "flags INTEGER, " +
                "mate_position " + bigint() + ", " +
                "insert_size INTEGER, " +
                "alignment_score INTEGER, " +
                "FOREIGN KEY (run_id) REFERENCES sequencing_runs(run_id))";
        }

        private String sqlAlignmentStats() {
            return "CREATE TABLE alignment_stats (" +
                "stat_id INTEGER NOT NULL PRIMARY KEY, " +
                "run_id INTEGER NOT NULL, " +
                "chromosome " + varchar(10) + " NOT NULL, " +
                "total_reads " + bigint() + " NOT NULL, " +
                "mapped_reads " + bigint() + " NOT NULL, " +
                "properly_paired " + bigint() + ", " +
                "duplicates " + bigint() + ", " +
                "mean_coverage DECIMAL(10,2), " +
                "median_coverage DECIMAL(10,2), " +
                "std_coverage DECIMAL(10,2), " +
                "mean_quality DECIMAL(5,2), " +
                "pct_bases_q30 DECIMAL(5,2), " +
                "created_date " + timestamp() + " NOT NULL, " +
                "FOREIGN KEY (run_id) REFERENCES sequencing_runs(run_id))";
        }

        private String sqlVariants() {
            return "CREATE TABLE variants (" +
                "variant_id " + bigint() + " NOT NULL PRIMARY KEY, " +
                "run_id INTEGER NOT NULL, " +
                "chromosome " + varchar(10) + " NOT NULL, " +
                "position " + bigint() + " NOT NULL, " +
                "reference_allele " + varchar(1000) + " NOT NULL, " +
                "alternate_allele " + varchar(1000) + " NOT NULL, " +
                "variant_type " + varchar(20) + " NOT NULL, " +
                "quality_score DECIMAL(10,2), " +
                "filter_status " + varchar(50) + ", " +
                "depth INTEGER, " +
                "allele_frequency DECIMAL(5,4), " +
                "genotype " + varchar(10) + ", " +
                "genotype_quality INTEGER, " +
                "strand_bias DECIMAL(5,4), " +
                "base_quality_rank_sum DECIMAL(8,4), " +
                "mapping_quality_rank_sum DECIMAL(8,4), " +
                "read_pos_rank_sum DECIMAL(8,4), " +
                "variant_call_date " + timestamp() + " NOT NULL, " +
                "FOREIGN KEY (run_id) REFERENCES sequencing_runs(run_id))";
        }

        private String sqlVariantAnnotations() {
            return "CREATE TABLE variant_annotations (" +
                "annotation_id " + bigint() + " NOT NULL PRIMARY KEY, " +
                "variant_id " + bigint() + " NOT NULL, " +
                "gene_symbol " + varchar(50) + ", " +
                "transcript_id " + varchar(50) + ", " +
                "consequence " + varchar(100) + ", " +
                "impact " + varchar(20) + ", " +
                "amino_acid_change " + varchar(100) + ", " +
                "codon_change " + varchar(100) + ", " +
                "protein_position INTEGER, " +
                "exon_number " + varchar(20) + ", " +
                "dbsnp_id " + varchar(50) + ", " +
                "cosmic_id " + varchar(50) + ", " +
                "clinvar_significance " + varchar(100) + ", " +
                "gnomad_af DECIMAL(8,7), " +
                "sift_prediction " + varchar(20) + ", " +
                "sift_score DECIMAL(5,4), " +
                "polyphen_prediction " + varchar(20) + ", " +
                "polyphen_score DECIMAL(5,4), " +
                "cadd_score DECIMAL(6,3), " +
                "annotation_date " + timestamp() + " NOT NULL, " +
                "FOREIGN KEY (variant_id) REFERENCES variants(variant_id))";
        }

        // View DDL — standard SQL, compatible across all three databases

        private String sqlRunSummaryView() {
            return "CREATE VIEW v_run_summary AS " +
                "SELECT sr.run_id, sr.run_name, s.sample_name, sr.platform, sr.run_date, " +
                "sr.status, sr.total_reads, sr.quality_score, " +
                "COUNT(DISTINCT v.variant_id) AS variant_count, " +
                "AVG(a.mean_coverage) AS avg_coverage " +
                "FROM sequencing_runs sr " +
                "JOIN samples s ON sr.sample_id = s.sample_id " +
                "LEFT JOIN variants v ON sr.run_id = v.run_id " +
                "LEFT JOIN alignment_stats a ON sr.run_id = a.run_id " +
                "GROUP BY sr.run_id, sr.run_name, s.sample_name, sr.platform, " +
                "sr.run_date, sr.status, sr.total_reads, sr.quality_score";
        }

        private String sqlHighImpactVariantsView() {
            return "CREATE VIEW v_high_impact_variants AS " +
                "SELECT v.variant_id, sr.run_name, s.sample_name, v.chromosome, v.position, " +
                "v.reference_allele, v.alternate_allele, v.variant_type, " +
                "va.gene_symbol, va.consequence, va.impact, va.amino_acid_change, " +
                "va.clinvar_significance, v.quality_score, v.allele_frequency " +
                "FROM variants v " +
                "JOIN sequencing_runs sr ON v.run_id = sr.run_id " +
                "JOIN samples s ON sr.sample_id = s.sample_id " +
                "LEFT JOIN variant_annotations va ON v.variant_id = va.variant_id " +
                "WHERE va.impact IN ('HIGH', 'MODERATE') " +
                "OR va.clinvar_significance LIKE '%Pathogenic%'";
        }

        // Type helpers

        private String bigint() {
            return (dbms == DB_ORACLE) ? "NUMBER(19)" : "BIGINT";
        }

        private String varchar(int n) {
            return (dbms == DB_ORACLE) ? "VARCHAR2(" + n + ")" : "VARCHAR(" + n + ")";
        }

        private String clob() {
            if (dbms == DB_ORACLE) return "CLOB";
            if (dbms == DB_MYSQL)  return "LONGTEXT";
            return "TEXT";
        }

        private String timestamp() {
            return (dbms == DB_MYSQL) ? "DATETIME" : "TIMESTAMP";
        }

        private String seqDdl(String name, long start, long incr, long cache) {
            return "CREATE SEQUENCE " + name +
                   " START WITH " + start +
                   " INCREMENT BY " + incr +
                   " CACHE " + cache;
        }

        private void tryExecute(Statement stmt, String sql) {
            try {
                stmt.execute(sql);
            } catch (SQLException e) {
                // Ignore — typically "object does not exist" during drops
            }
        }
    }
}
