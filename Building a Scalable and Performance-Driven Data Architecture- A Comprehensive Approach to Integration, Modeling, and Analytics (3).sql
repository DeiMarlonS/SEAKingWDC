-- ===================================================
-- Star Schema Design for Enterprise Data Architecture
-- ===================================================
-- Purpose: Implement a scalable and high-performance star schema for analytics
-- DBMS: PostgreSQL
-- ===================================================

-- 1. Create Fact Table: Client_Fact
CREATE TABLE Client_Fact (
    ClientID INT PRIMARY KEY, 
    Age TINYINT NOT NULL CHECK (Age > 0 AND Age < 120), -- Valid age range
    Gender VARCHAR(10) NOT NULL CHECK (Gender IN ('M', 'F', 'O')), 
    Ethnicity VARCHAR(50) NOT NULL CHECK (Ethnicity IN ('Hispanic', 'White', 'Black', 'Asian', 'Other')), 
    EnrollmentCount INT DEFAULT 0 CHECK (EnrollmentCount >= 0), 
    ActiveEnrollments INT DEFAULT 0 CHECK (ActiveEnrollments >= 0), 
    TotalExits INT DEFAULT 0 CHECK (TotalExits >= 0), 
    AverageEnrollmentDuration INTERVAL, -- Derived field for average time in programs
    LastUpdated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    DataQualityScore DECIMAL(5, 2) DEFAULT 100.00 CHECK (DataQualityScore >= 0 AND DataQualityScore <= 100.00) -- Score for validation metrics
);

-- 2. Create Dimension Table: Enrollment_Dim
CREATE TABLE Enrollment_Dim (
    EnrollmentID INT PRIMARY KEY, 
    ClientID INT NOT NULL, 
    ProgramID INT NOT NULL, 
    StartDate DATE NOT NULL, 
    CompletionDate DATE, 
    Duration INTERVAL GENERATED ALWAYS AS (CompletionDate - StartDate) STORED, -- Automatic calculation
    Status VARCHAR(20) NOT NULL CHECK (Status IN ('Active', 'Complete', 'Withdrawn', 'Suspended')), 
    LastUpdated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ClientID) REFERENCES Client_Fact(ClientID) ON DELETE CASCADE, 
    FOREIGN KEY (ProgramID) REFERENCES Program_Dim(ProgramID) ON DELETE CASCADE
) PARTITION BY RANGE (StartDate); -- Table partitioning based on StartDate

-- Create Enrollment Partitions
CREATE TABLE Enrollment_Dim_2023 PARTITION OF Enrollment_Dim FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE Enrollment_Dim_2022 PARTITION OF Enrollment_Dim FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE Enrollment_Dim_2021 PARTITION OF Enrollment_Dim FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');

-- Default Partition for Overflow Records
CREATE TABLE Enrollment_Dim_Default PARTITION OF Enrollment_Dim DEFAULT;

-- 3. Create Dimension Table: Program_Dim
CREATE TABLE Program_Dim (
    ProgramID INT PRIMARY KEY, 
    ProgramName VARCHAR(100) NOT NULL UNIQUE, 
    ProgramType VARCHAR(50) NOT NULL CHECK (ProgramType IN ('Training', 'Support', 'Education', 'Internship')), 
    Provider VARCHAR(100), 
    ProgramDescription TEXT DEFAULT NULL, 
    ProgramStartDate DATE NOT NULL,
    IsActive BOOLEAN GENERATED ALWAYS AS (CURRENT_DATE < ProgramStartDate + INTERVAL '365 DAYS') STORED, -- Auto-determine active programs
    LastUpdated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Create Dimension Table: Time_Dim
CREATE TABLE Time_Dim (
    DateID INT PRIMARY KEY, 
    Year INT NOT NULL CHECK (Year > 1900), 
    Month INT NOT NULL CHECK (Month BETWEEN 1 AND 12), 
    Day INT NOT NULL CHECK (Day BETWEEN 1 AND 31), 
    Weekday VARCHAR(10) NOT NULL CHECK (Weekday IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')), 
    FiscalQuarter INT NOT NULL CHECK (FiscalQuarter BETWEEN 1 AND 4),
    IsWeekend BOOLEAN GENERATED ALWAYS AS (Weekday IN ('Saturday', 'Sunday')) STORED,
    HolidayFlag BOOLEAN DEFAULT FALSE, -- Boolean flag for holidays
    LastUpdated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ===================================================
-- Indexing Strategy
-- ===================================================
CREATE INDEX idx_client_gender_ethnicity ON Client_Fact (Gender, Ethnicity);
CREATE INDEX idx_enrollment_status_duration ON Enrollment_Dim (Status, Duration);
CREATE INDEX idx_program_active_type ON Program_Dim (IsActive, ProgramType);
CREATE INDEX idx_time_fiscal_quarter ON Time_Dim (FiscalQuarter);

-- Composite Indexes for Optimized Joins
CREATE INDEX idx_enrollment_client_program ON Enrollment_Dim (ClientID, ProgramID);
CREATE INDEX idx_client_fact_aggregate ON Client_Fact (EnrollmentCount, ActiveEnrollments);

-- ===================================================
-- Advanced Security Implementation
-- ===================================================
-- Role for Read-Only Analytics Access
CREATE ROLE analytics_user LOGIN PASSWORD 'secure_password';
GRANT CONNECT ON DATABASE analytics_db TO analytics_user;
GRANT USAGE ON SCHEMA public TO analytics_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO analytics_user;

-- Audit Logs for Data Changes
CREATE TABLE AuditLog (
    LogID SERIAL PRIMARY KEY,
    TableName VARCHAR(50),
    OperationType VARCHAR(10), -- INSERT, UPDATE, DELETE
    OperationTime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    OldValue JSONB,
    NewValue JSONB,
    UserName VARCHAR(50) DEFAULT SESSION_USER
);

-- Audit Trigger Function
CREATE OR REPLACE FUNCTION audit_log_function() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO AuditLog (TableName, OperationType, OldValue, NewValue)
    VALUES (
        TG_TABLE_NAME,
        TG_OP,
        ROW_TO_JSON(OLD),
        ROW_TO_JSON(NEW)
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Attach Trigger to All Tables
CREATE TRIGGER audit_client_fact
AFTER INSERT OR UPDATE OR DELETE ON Client_Fact
FOR EACH ROW EXECUTE FUNCTION audit_log_function();

CREATE TRIGGER audit_enrollment_dim
AFTER INSERT OR UPDATE OR DELETE ON Enrollment_Dim
FOR EACH ROW EXECUTE FUNCTION audit_log_function();

-- ===================================================
-- Populate Tables with Advanced Data
-- ===================================================
-- Populate Time_Dim Table
DO $$
DECLARE
    d DATE;
BEGIN
    FOR d IN '2000-01-01'::DATE..'2030-12-31'::DATE LOOP
        INSERT INTO Time_Dim (DateID, Year, Month, Day, Weekday, FiscalQuarter, HolidayFlag)
        VALUES (
            EXTRACT(EPOCH FROM d)::INT,
            EXTRACT(YEAR FROM d)::INT,
            EXTRACT(MONTH FROM d)::INT,
            EXTRACT(DAY FROM d)::INT,
            TO_CHAR(d, 'Day')::VARCHAR,
            CASE 
                WHEN EXTRACT(MONTH FROM d) IN (1, 2, 3) THEN 1
                WHEN EXTRACT(MONTH FROM d) IN (4, 5, 6) THEN 2
                WHEN EXTRACT(MONTH FROM d) IN (7, 8, 9) THEN 3
                ELSE 4
            END,
            FALSE -- Default non-holiday
        );
    END LOOP;
END $$;

-- Refresh Materialized Views Periodically
CREATE MATERIALIZED VIEW EnrollmentMetrics AS
SELECT 
    cf.ClientID,
    COUNT(ed.EnrollmentID) AS TotalEnrollments,
    SUM(CASE WHEN ed.Status = 'Active' THEN 1 ELSE 0 END) AS ActiveEnrollments,
    COUNT(ed.CompletionDate) AS CompletedEnrollments,
    MIN(ed.StartDate) AS EarliestEnrollment,
    MAX(ed.CompletionDate) AS LatestCompletion,
    AVG(ed.Duration) AS AverageEnrollmentDuration
FROM Client_Fact cf
JOIN Enrollment_Dim ed ON cf.ClientID = ed.ClientID
GROUP BY cf.ClientID;

REFRESH MATERIALIZED VIEW CONCURRENTLY EnrollmentMetrics;

-- Schedule View Refresh (Example for pg_cron)
-- Install pg_cron Extension: CREATE EXTENSION pg_cron;
-- Schedule Periodic Refresh
SELECT cron.schedule('Refresh Enrollment Metrics', '0 0 * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY EnrollmentMetrics;');

-- ===================================================
-- Notes for Further Enhancements
-- ===================================================
-- 1. 
-- ===================================================
-- End of Schema Script
-- ===================================================
