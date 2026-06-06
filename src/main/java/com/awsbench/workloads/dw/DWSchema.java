package com.awsbench.workloads.dw;

import java.sql.*;

class DWSchema {

    private final Connection connection;
    private final int dbType;

    DWSchema(Connection connection, int dbType) {
        this.connection = connection;
        this.dbType = dbType;
    }

    void createOrRecreate() throws SQLException {
        boolean prevAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(true);
        try {
            System.out.println("Dropping existing schema objects...");
            dropAll();
            System.out.println("Creating schema...");
            createAll();
            System.out.println("Schema ready.");
        } finally {
            connection.setAutoCommit(prevAutoCommit);
        }
    }

    // ================================================================
    // DDL type helpers
    // ================================================================

    private String num(int p) {
        return dbType == DWWork.DB_ORACLE ? "NUMBER(" + p + ")" : "NUMERIC(" + p + ")";
    }

    private String num(int p, int s) {
        return dbType == DWWork.DB_ORACLE ? "NUMBER(" + p + "," + s + ")" : "NUMERIC(" + p + "," + s + ")";
    }

    private String vc(int n) {
        return dbType == DWWork.DB_ORACLE ? "VARCHAR2(" + n + ")" : "VARCHAR(" + n + ")";
    }

    private String tsDefault() {
        return dbType == DWWork.DB_ORACLE
            ? "TIMESTAMP DEFAULT SYSDATE"
            : "TIMESTAMP DEFAULT CURRENT_TIMESTAMP";
    }

    // For fact/staging PKs: Oracle/PG use a sequence in the INSERT; MySQL uses AUTO_INCREMENT.
    private String seqPk(int p) {
        return dbType == DWWork.DB_MYSQL ? "BIGINT NOT NULL AUTO_INCREMENT" : num(p) + " NOT NULL";
    }

    // ================================================================
    // Execution helpers
    // ================================================================

    private void exec(String sql) throws SQLException {
        try (Statement s = connection.createStatement()) {
            s.execute(sql);
        }
    }

    private void tryDrop(String sql) {
        try { exec(sql); } catch (SQLException ignored) {}
    }

    private String dropTableSql(String name) {
        return dbType == DWWork.DB_ORACLE
            ? "DROP TABLE " + name + " CASCADE CONSTRAINTS"
            : "DROP TABLE IF EXISTS " + name;
    }

    private String dropViewSql(String name) {
        return dbType == DWWork.DB_ORACLE ? "DROP VIEW " + name : "DROP VIEW IF EXISTS " + name;
    }

    private String dropSeqSql(String name) {
        if (dbType == DWWork.DB_ORACLE)     return "DROP SEQUENCE " + name;
        if (dbType == DWWork.DB_POSTGRESQL) return "DROP SEQUENCE IF EXISTS " + name.toLowerCase();
        return null; // MySQL has no sequences
    }

    // ================================================================
    // Drop all objects (reverse dependency order)
    // ================================================================

    private void dropAll() {
        tryDrop(dropViewSql("V_SALES_SUMMARY"));
        tryDrop(dropViewSql("V_PRODUCT_PERFORMANCE"));
        tryDrop(dropViewSql("V_CUSTOMER_SEGMENTS"));

        tryDrop(dropTableSql("AGG_SALES_DAILY"));
        tryDrop(dropTableSql("AGG_SALES_MONTHLY"));
        tryDrop(dropTableSql("STG_SALES"));
        tryDrop(dropTableSql("STG_INVENTORY"));
        tryDrop(dropTableSql("FACT_SALES"));
        tryDrop(dropTableSql("FACT_INVENTORY"));
        tryDrop(dropTableSql("DIM_DATE"));
        tryDrop(dropTableSql("DIM_PRODUCT"));
        tryDrop(dropTableSql("DIM_CUSTOMER"));
        tryDrop(dropTableSql("DIM_STORE"));
        tryDrop(dropTableSql("DIM_PROMOTION"));

        String sql;
        if ((sql = dropSeqSql("SEQ_SALES"))     != null) tryDrop(sql);
        if ((sql = dropSeqSql("SEQ_INVENTORY")) != null) tryDrop(sql);
        if ((sql = dropSeqSql("SEQ_STG_SALES")) != null) tryDrop(sql);
    }

    // ================================================================
    // Create all objects
    // ================================================================

    private void createAll() throws SQLException {
        createSequences();
        createDimDate();
        createDimProduct();
        createDimCustomer();
        createDimStore();
        createDimPromotion();
        createFactSales();
        createFactInventory();
        createStgSales();
        createStgInventory();
        createAggSalesDaily();
        createAggSalesMonthly();
        createViews();
    }

    private void createSequences() throws SQLException {
        if (dbType == DWWork.DB_MYSQL) return;
        boolean oracle = dbType == DWWork.DB_ORACLE;
        exec("CREATE SEQUENCE " + (oracle ? "SEQ_SALES"     : "seq_sales")     + " START WITH 1 INCREMENT BY 1");
        exec("CREATE SEQUENCE " + (oracle ? "SEQ_INVENTORY" : "seq_inventory") + " START WITH 1 INCREMENT BY 1");
        exec("CREATE SEQUENCE " + (oracle ? "SEQ_STG_SALES" : "seq_stg_sales") + " START WITH 1 INCREMENT BY 1");
    }

    private void createDimDate() throws SQLException {
        exec("CREATE TABLE DIM_DATE (" +
            "DATE_KEY       " + num(8)  + " NOT NULL," +
            "FULL_DATE      DATE NOT NULL," +
            "DAY_OF_WEEK    " + num(1)  + " NOT NULL," +
            "DAY_NAME       " + vc(10)  + " NOT NULL," +
            "DAY_OF_MONTH   " + num(2)  + " NOT NULL," +
            "DAY_OF_YEAR    " + num(3)  + " NOT NULL," +
            "WEEK_OF_YEAR   " + num(2)  + " NOT NULL," +
            "MONTH_NUM      " + num(2)  + " NOT NULL," +
            "MONTH_NAME     " + vc(10)  + " NOT NULL," +
            "QUARTER_NUM    " + num(1)  + " NOT NULL," +
            "QUARTER_NAME   " + vc(2)   + " NOT NULL," +
            "YEAR_NUM       " + num(4)  + " NOT NULL," +
            "IS_WEEKEND     " + num(1)  + " NOT NULL," +
            "IS_HOLIDAY     " + num(1)  + " DEFAULT 0," +
            "FISCAL_YEAR    " + num(4)  + "," +
            "FISCAL_QUARTER " + num(1)  + "," +
            "PRIMARY KEY (DATE_KEY))");

        exec("CREATE INDEX IDX_DIM_DATE_YEAR    ON DIM_DATE(YEAR_NUM)");
        exec("CREATE INDEX IDX_DIM_DATE_MONTH   ON DIM_DATE(YEAR_NUM, MONTH_NUM)");
        exec("CREATE INDEX IDX_DIM_DATE_QUARTER ON DIM_DATE(YEAR_NUM, QUARTER_NUM)");
    }

    private void createDimProduct() throws SQLException {
        exec("CREATE TABLE DIM_PRODUCT (" +
            "PRODUCT_KEY      " + num(10)   + " NOT NULL," +
            "PRODUCT_ID       " + vc(20)    + " NOT NULL," +
            "PRODUCT_NAME     " + vc(100)   + " NOT NULL," +
            "PRODUCT_DESC     " + vc(500)   + "," +
            "CATEGORY_ID      " + num(5)    + " NOT NULL," +
            "CATEGORY_NAME    " + vc(50)    + " NOT NULL," +
            "SUBCATEGORY_ID   " + num(5)    + " NOT NULL," +
            "SUBCATEGORY_NAME " + vc(50)    + " NOT NULL," +
            "BRAND            " + vc(50)    + "," +
            "SUPPLIER_ID      " + num(10)   + "," +
            "SUPPLIER_NAME    " + vc(100)   + "," +
            "UNIT_COST        " + num(12,2) + "," +
            "UNIT_PRICE       " + num(12,2) + "," +
            "WEIGHT           " + num(10,3) + "," +
            "SIZE_DESC        " + vc(20)    + "," +
            "COLOR            " + vc(30)    + "," +
            "STATUS           " + vc(20)    + " DEFAULT 'ACTIVE'," +
            "EFFECTIVE_DATE   DATE NOT NULL," +
            "EXPIRY_DATE      DATE," +
            "IS_CURRENT       " + num(1)    + " DEFAULT 1," +
            "PRIMARY KEY (PRODUCT_KEY))");

        exec("CREATE INDEX IDX_DIM_PRODUCT_CAT    ON DIM_PRODUCT(CATEGORY_ID)");
        exec("CREATE INDEX IDX_DIM_PRODUCT_SUBCAT ON DIM_PRODUCT(SUBCATEGORY_ID)");
        exec("CREATE INDEX IDX_DIM_PRODUCT_BRAND  ON DIM_PRODUCT(BRAND)");
        exec("CREATE INDEX IDX_DIM_PRODUCT_STATUS ON DIM_PRODUCT(STATUS)");
        exec("CREATE UNIQUE INDEX IDX_DIM_PRODUCT_ID ON DIM_PRODUCT(PRODUCT_ID)");
    }

    private void createDimCustomer() throws SQLException {
        exec("CREATE TABLE DIM_CUSTOMER (" +
            "CUSTOMER_KEY       " + num(10) + " NOT NULL," +
            "CUSTOMER_ID        " + vc(20)  + " NOT NULL," +
            "FIRST_NAME         " + vc(50)  + " NOT NULL," +
            "LAST_NAME          " + vc(50)  + " NOT NULL," +
            "EMAIL              " + vc(100) + "," +
            "PHONE              " + vc(20)  + "," +
            "ADDRESS_LINE1      " + vc(100) + "," +
            "ADDRESS_LINE2      " + vc(100) + "," +
            "CITY               " + vc(50)  + "," +
            "STATE              " + vc(50)  + "," +
            "POSTAL_CODE        " + vc(20)  + "," +
            "COUNTRY            " + vc(50)  + " DEFAULT 'USA'," +
            "REGION             " + vc(20)  + "," +
            "CUSTOMER_SEGMENT   " + vc(20)  + "," +
            "LOYALTY_TIER       " + vc(20)  + "," +
            "ACQUISITION_DATE   DATE," +
            "ACQUISITION_SOURCE " + vc(50)  + "," +
            "BIRTH_DATE         DATE," +
            "GENDER             " + vc(10)  + "," +
            "INCOME_BRACKET     " + vc(20)  + "," +
            "STATUS             " + vc(20)  + " DEFAULT 'ACTIVE'," +
            "PRIMARY KEY (CUSTOMER_KEY))");

        exec("CREATE INDEX IDX_DIM_CUSTOMER_REGION  ON DIM_CUSTOMER(REGION)");
        exec("CREATE INDEX IDX_DIM_CUSTOMER_SEGMENT ON DIM_CUSTOMER(CUSTOMER_SEGMENT)");
        exec("CREATE INDEX IDX_DIM_CUSTOMER_TIER    ON DIM_CUSTOMER(LOYALTY_TIER)");
        exec("CREATE INDEX IDX_DIM_CUSTOMER_CITY    ON DIM_CUSTOMER(CITY, STATE)");
        exec("CREATE UNIQUE INDEX IDX_DIM_CUSTOMER_ID ON DIM_CUSTOMER(CUSTOMER_ID)");
    }

    private void createDimStore() throws SQLException {
        exec("CREATE TABLE DIM_STORE (" +
            "STORE_KEY      " + num(10) + " NOT NULL," +
            "STORE_ID       " + vc(20)  + " NOT NULL," +
            "STORE_NAME     " + vc(100) + " NOT NULL," +
            "STORE_TYPE     " + vc(30)  + "," +
            "ADDRESS_LINE1  " + vc(100) + "," +
            "CITY           " + vc(50)  + "," +
            "STATE          " + vc(50)  + "," +
            "POSTAL_CODE    " + vc(20)  + "," +
            "COUNTRY        " + vc(50)  + " DEFAULT 'USA'," +
            "REGION         " + vc(20)  + "," +
            "DISTRICT       " + vc(50)  + "," +
            "MANAGER_NAME   " + vc(100) + "," +
            "PHONE          " + vc(20)  + "," +
            "OPEN_DATE      DATE," +
            "CLOSE_DATE     DATE," +
            "SQUARE_FOOTAGE " + num(10) + "," +
            "EMPLOYEE_COUNT " + num(5)  + "," +
            "STATUS         " + vc(20)  + " DEFAULT 'OPEN'," +
            "PRIMARY KEY (STORE_KEY))");

        exec("CREATE INDEX IDX_DIM_STORE_REGION ON DIM_STORE(REGION)");
        exec("CREATE INDEX IDX_DIM_STORE_TYPE   ON DIM_STORE(STORE_TYPE)");
        exec("CREATE INDEX IDX_DIM_STORE_STATUS ON DIM_STORE(STATUS)");
        exec("CREATE UNIQUE INDEX IDX_DIM_STORE_ID ON DIM_STORE(STORE_ID)");
    }

    private void createDimPromotion() throws SQLException {
        exec("CREATE TABLE DIM_PROMOTION (" +
            "PROMOTION_KEY    " + num(10)   + " NOT NULL," +
            "PROMOTION_ID     " + vc(20)    + " NOT NULL," +
            "PROMOTION_NAME   " + vc(100)   + " NOT NULL," +
            "PROMOTION_TYPE   " + vc(30)    + "," +
            "DISCOUNT_PERCENT " + num(5,2)  + "," +
            "DISCOUNT_AMOUNT  " + num(12,2) + "," +
            "MIN_PURCHASE     " + num(12,2) + "," +
            "START_DATE       DATE NOT NULL," +
            "END_DATE         DATE NOT NULL," +
            "MEDIA_TYPE       " + vc(30)    + "," +
            "CAMPAIGN_NAME    " + vc(100)   + "," +
            "COST             " + num(12,2) + "," +
            "STATUS           " + vc(20)    + " DEFAULT 'ACTIVE'," +
            "PRIMARY KEY (PROMOTION_KEY))");

        exec("CREATE INDEX IDX_DIM_PROMO_TYPE  ON DIM_PROMOTION(PROMOTION_TYPE)");
        exec("CREATE INDEX IDX_DIM_PROMO_DATES ON DIM_PROMOTION(START_DATE, END_DATE)");
        exec("CREATE UNIQUE INDEX IDX_DIM_PROMO_ID ON DIM_PROMOTION(PROMOTION_ID)");
    }

    private void createFactSales() throws SQLException {
        exec("CREATE TABLE FACT_SALES (" +
            "SALES_KEY       " + seqPk(15) + "," +
            "DATE_KEY        " + num(8)    + " NOT NULL," +
            "PRODUCT_KEY     " + num(10)   + " NOT NULL," +
            "CUSTOMER_KEY    " + num(10)   + " NOT NULL," +
            "STORE_KEY       " + num(10)   + " NOT NULL," +
            "PROMOTION_KEY   " + num(10)   + "," +
            "TRANSACTION_ID  " + vc(30)    + " NOT NULL," +
            "TRANSACTION_TIME TIMESTAMP," +
            "QUANTITY        " + num(10)   + " NOT NULL," +
            "UNIT_PRICE      " + num(12,2) + " NOT NULL," +
            "UNIT_COST       " + num(12,2) + "," +
            "DISCOUNT_AMOUNT " + num(12,2) + " DEFAULT 0," +
            "SALES_AMOUNT    " + num(15,2) + " NOT NULL," +
            "COST_AMOUNT     " + num(15,2) + "," +
            "PROFIT_AMOUNT   " + num(15,2) + "," +
            "TAX_AMOUNT      " + num(12,2) + " DEFAULT 0," +
            "SHIPPING_COST   " + num(12,2) + " DEFAULT 0," +
            "PRIMARY KEY (SALES_KEY))");

        exec("CREATE INDEX IDX_FACT_SALES_DATE         ON FACT_SALES(DATE_KEY)");
        exec("CREATE INDEX IDX_FACT_SALES_PRODUCT      ON FACT_SALES(PRODUCT_KEY)");
        exec("CREATE INDEX IDX_FACT_SALES_CUSTOMER     ON FACT_SALES(CUSTOMER_KEY)");
        exec("CREATE INDEX IDX_FACT_SALES_STORE        ON FACT_SALES(STORE_KEY)");
        exec("CREATE INDEX IDX_FACT_SALES_PROMO        ON FACT_SALES(PROMOTION_KEY)");
        exec("CREATE INDEX IDX_FACT_SALES_TXN          ON FACT_SALES(TRANSACTION_ID)");
        exec("CREATE INDEX IDX_FACT_SALES_DATE_STORE   ON FACT_SALES(DATE_KEY, STORE_KEY)");
        exec("CREATE INDEX IDX_FACT_SALES_DATE_PRODUCT ON FACT_SALES(DATE_KEY, PRODUCT_KEY)");
        exec("CREATE INDEX IDX_FACT_SALES_CUST_DATE    ON FACT_SALES(CUSTOMER_KEY, DATE_KEY)");
    }

    private void createFactInventory() throws SQLException {
        exec("CREATE TABLE FACT_INVENTORY (" +
            "INVENTORY_KEY     " + seqPk(15) + "," +
            "DATE_KEY          " + num(8)    + " NOT NULL," +
            "PRODUCT_KEY       " + num(10)   + " NOT NULL," +
            "STORE_KEY         " + num(10)   + " NOT NULL," +
            "QUANTITY_ON_HAND  " + num(10)   + " NOT NULL," +
            "QUANTITY_ON_ORDER " + num(10)   + " DEFAULT 0," +
            "QUANTITY_RESERVED " + num(10)   + " DEFAULT 0," +
            "REORDER_POINT     " + num(10)   + "," +
            "SAFETY_STOCK      " + num(10)   + "," +
            "INVENTORY_VALUE   " + num(15,2) + "," +
            "DAYS_OF_SUPPLY    " + num(5)    + "," +
            "LAST_RECEIPT_DATE DATE," +
            "LAST_SALE_DATE    DATE," +
            "PRIMARY KEY (INVENTORY_KEY))");

        exec("CREATE INDEX IDX_FACT_INV_DATE      ON FACT_INVENTORY(DATE_KEY)");
        exec("CREATE INDEX IDX_FACT_INV_PRODUCT   ON FACT_INVENTORY(PRODUCT_KEY)");
        exec("CREATE INDEX IDX_FACT_INV_STORE     ON FACT_INVENTORY(STORE_KEY)");
        exec("CREATE INDEX IDX_FACT_INV_DATE_PROD ON FACT_INVENTORY(DATE_KEY, PRODUCT_KEY)");
    }

    private void createStgSales() throws SQLException {
        exec("CREATE TABLE STG_SALES (" +
            "STG_SALES_KEY    " + seqPk(15) + "," +
            "LOAD_TIMESTAMP   " + tsDefault() + "," +
            "SOURCE_SYSTEM    " + vc(50)    + "," +
            "TRANSACTION_DATE DATE," +
            "TRANSACTION_TIME " + vc(20)    + "," +
            "PRODUCT_ID       " + vc(20)    + "," +
            "CUSTOMER_ID      " + vc(20)    + "," +
            "STORE_ID         " + vc(20)    + "," +
            "PROMOTION_ID     " + vc(20)    + "," +
            "QUANTITY         " + num(10)   + "," +
            "UNIT_PRICE       " + num(12,2) + "," +
            "DISCOUNT_PERCENT " + num(5,2)  + "," +
            "PROCESSED_FLAG   " + num(1)    + " DEFAULT 0," +
            "ERROR_MESSAGE    " + vc(500)   + "," +
            "PRIMARY KEY (STG_SALES_KEY))");

        exec("CREATE INDEX IDX_STG_SALES_PROC ON STG_SALES(PROCESSED_FLAG)");
        exec("CREATE INDEX IDX_STG_SALES_DATE ON STG_SALES(TRANSACTION_DATE)");
    }

    private void createStgInventory() throws SQLException {
        exec("CREATE TABLE STG_INVENTORY (" +
            "STG_INVENTORY_KEY " + seqPk(15) + "," +
            "LOAD_TIMESTAMP    " + tsDefault() + "," +
            "SOURCE_SYSTEM     " + vc(50)    + "," +
            "SNAPSHOT_DATE     DATE," +
            "PRODUCT_ID        " + vc(20)    + "," +
            "STORE_ID          " + vc(20)    + "," +
            "QUANTITY_ON_HAND  " + num(10)   + "," +
            "QUANTITY_ON_ORDER " + num(10)   + "," +
            "PROCESSED_FLAG    " + num(1)    + " DEFAULT 0," +
            "ERROR_MESSAGE     " + vc(500)   + "," +
            "PRIMARY KEY (STG_INVENTORY_KEY))");

        exec("CREATE INDEX IDX_STG_INV_PROC ON STG_INVENTORY(PROCESSED_FLAG)");
    }

    private void createAggSalesDaily() throws SQLException {
        exec("CREATE TABLE AGG_SALES_DAILY (" +
            "DATE_KEY          " + num(8)    + " NOT NULL," +
            "STORE_KEY         " + num(10)   + " NOT NULL," +
            "PRODUCT_KEY       " + num(10)   + " NOT NULL," +
            "TOTAL_QUANTITY    " + num(15)   + " NOT NULL," +
            "TOTAL_SALES       " + num(18,2) + " NOT NULL," +
            "TOTAL_COST        " + num(18,2) + "," +
            "TOTAL_PROFIT      " + num(18,2) + "," +
            "TOTAL_DISCOUNT    " + num(15,2) + "," +
            "TRANSACTION_COUNT " + num(10)   + " NOT NULL," +
            "AVG_UNIT_PRICE    " + num(12,2) + "," +
            "LAST_UPDATE       " + tsDefault() + "," +
            "PRIMARY KEY (DATE_KEY, STORE_KEY, PRODUCT_KEY))");

        exec("CREATE INDEX IDX_AGG_DAILY_DATE    ON AGG_SALES_DAILY(DATE_KEY)");
        exec("CREATE INDEX IDX_AGG_DAILY_STORE   ON AGG_SALES_DAILY(STORE_KEY)");
        exec("CREATE INDEX IDX_AGG_DAILY_PRODUCT ON AGG_SALES_DAILY(PRODUCT_KEY)");
    }

    private void createAggSalesMonthly() throws SQLException {
        exec("CREATE TABLE AGG_SALES_MONTHLY (" +
            "YEAR_NUM          " + num(4)    + " NOT NULL," +
            "MONTH_NUM         " + num(2)    + " NOT NULL," +
            "STORE_KEY         " + num(10)   + " NOT NULL," +
            "CATEGORY_ID       " + num(5)    + " NOT NULL," +
            "TOTAL_QUANTITY    " + num(18)   + " NOT NULL," +
            "TOTAL_SALES       " + num(20,2) + " NOT NULL," +
            "TOTAL_COST        " + num(20,2) + "," +
            "TOTAL_PROFIT      " + num(20,2) + "," +
            "TRANSACTION_COUNT " + num(15)   + " NOT NULL," +
            "UNIQUE_CUSTOMERS  " + num(10)   + "," +
            "LAST_UPDATE       " + tsDefault() + "," +
            "PRIMARY KEY (YEAR_NUM, MONTH_NUM, STORE_KEY, CATEGORY_ID))");

        exec("CREATE INDEX IDX_AGG_MONTHLY_YM    ON AGG_SALES_MONTHLY(YEAR_NUM, MONTH_NUM)");
        exec("CREATE INDEX IDX_AGG_MONTHLY_STORE ON AGG_SALES_MONTHLY(STORE_KEY)");
    }

    private void createViews() throws SQLException {
        exec("CREATE VIEW V_SALES_SUMMARY AS " +
            "SELECT d.YEAR_NUM, d.QUARTER_NAME, d.MONTH_NAME, " +
            "s.STORE_NAME, s.REGION AS STORE_REGION, p.CATEGORY_NAME, p.SUBCATEGORY_NAME, " +
            "SUM(f.QUANTITY) AS TOTAL_QUANTITY, SUM(f.SALES_AMOUNT) AS TOTAL_SALES, " +
            "SUM(f.PROFIT_AMOUNT) AS TOTAL_PROFIT, COUNT(*) AS TRANSACTION_COUNT " +
            "FROM FACT_SALES f " +
            "JOIN DIM_DATE d ON f.DATE_KEY = d.DATE_KEY " +
            "JOIN DIM_STORE s ON f.STORE_KEY = s.STORE_KEY " +
            "JOIN DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY " +
            "GROUP BY d.YEAR_NUM, d.QUARTER_NAME, d.MONTH_NAME, " +
            "s.STORE_NAME, s.REGION, p.CATEGORY_NAME, p.SUBCATEGORY_NAME");

        exec("CREATE VIEW V_PRODUCT_PERFORMANCE AS " +
            "SELECT p.PRODUCT_KEY, p.PRODUCT_NAME, p.CATEGORY_NAME, p.BRAND, " +
            "SUM(f.QUANTITY) AS TOTAL_UNITS_SOLD, SUM(f.SALES_AMOUNT) AS TOTAL_REVENUE, " +
            "SUM(f.PROFIT_AMOUNT) AS TOTAL_PROFIT, AVG(f.UNIT_PRICE) AS AVG_SELLING_PRICE, " +
            "COUNT(DISTINCT f.CUSTOMER_KEY) AS UNIQUE_CUSTOMERS " +
            "FROM DIM_PRODUCT p " +
            "JOIN FACT_SALES f ON p.PRODUCT_KEY = f.PRODUCT_KEY " +
            "GROUP BY p.PRODUCT_KEY, p.PRODUCT_NAME, p.CATEGORY_NAME, p.BRAND");

        exec("CREATE VIEW V_CUSTOMER_SEGMENTS AS " +
            "SELECT c.CUSTOMER_SEGMENT, c.LOYALTY_TIER, c.REGION, " +
            "COUNT(DISTINCT c.CUSTOMER_KEY) AS CUSTOMER_COUNT, " +
            "SUM(f.SALES_AMOUNT) AS TOTAL_SALES, AVG(f.SALES_AMOUNT) AS AVG_TRANSACTION_VALUE, " +
            "SUM(f.QUANTITY) AS TOTAL_ITEMS_PURCHASED " +
            "FROM DIM_CUSTOMER c " +
            "LEFT JOIN FACT_SALES f ON c.CUSTOMER_KEY = f.CUSTOMER_KEY " +
            "GROUP BY c.CUSTOMER_SEGMENT, c.LOYALTY_TIER, c.REGION");
    }
}
