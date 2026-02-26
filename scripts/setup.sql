-- Copyright 2026 Snowflake Inc.
-- SPDX-License-Identifier: Apache-2.0
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

/*******************************************************************************
 * IPA DEMO - COMPLETE SETUP SCRIPT
 * 
 * Purpose: Creates the complete Intelligent Production Assistant demo
 * 
 * What This Script Does:
 * 1. Creates role and grants privileges
 * 2. Creates database, schemas, tables with sample data (187k+ rows)
 * 3. Creates 3 detection services (Sentinel, Guardian, Fiscal)
 * 4. Creates Cortex Search service for document retrieval
 * 6. Creates Git integration for private repository
 * 7. Creates Cortex Agent (IPA_AGENT) for Q&A with text-to-SQL + RAG
 * 8. Deploys Streamlit app from Git repository
 *
 * Version: 1.0 
 ******************************************************************************/

USE ROLE ACCOUNTADMIN;

-- Set query tag for tracking
ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"oil_gas_operational_intelligence","version":{"major":2,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}';

SET USERNAME = (SELECT CURRENT_USER());

--------------------------------------------------------------------------------
-- STEP 1: CREATE ROLE AND GRANT PRIVILEGES
--------------------------------------------------------------------------------

-- Create role for this demo
CREATE OR REPLACE ROLE IPA_DEMO_ROLE;
GRANT ROLE IPA_DEMO_ROLE TO USER IDENTIFIER($USERNAME);

-- Grant necessary privileges
GRANT CREATE DATABASE ON ACCOUNT TO ROLE IPA_DEMO_ROLE;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE IPA_DEMO_ROLE;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE IPA_DEMO_ROLE;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE IPA_DEMO_ROLE;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE IPA_DEMO_ROLE;

-- Snowflake Intelligence and Cortex privileges
GRANT CREATE SNOWFLAKE INTELLIGENCE ON ACCOUNT TO ROLE IPA_DEMO_ROLE;
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE IPA_DEMO_ROLE;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE IPA_DEMO_ROLE;

-- Enable cross-region Cortex (required for accounts not in Cortex-enabled regions)
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- Create Snowflake Intelligence object
CREATE SNOWFLAKE INTELLIGENCE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT;

-- Grant Snowflake Intelligence usage
GRANT USAGE ON SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT TO ROLE IPA_DEMO_ROLE;
GRANT MODIFY ON SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT TO ROLE IPA_DEMO_ROLE;
GRANT USAGE ON SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT TO ROLE PUBLIC;

-- Switch to demo role
USE ROLE IPA_DEMO_ROLE;

--------------------------------------------------------------------------------
-- STEP 2: CREATE WAREHOUSE AND DATABASE
--------------------------------------------------------------------------------

-- Create warehouse (Large for faster setup)
CREATE OR REPLACE WAREHOUSE IPA_WH 
    WAREHOUSE_SIZE = 'LARGE'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    COMMENT = 'Warehouse for IPA demo';

USE WAREHOUSE IPA_WH;

-- Create database and schemas
CREATE OR REPLACE DATABASE IPA
    COMMENT = 'Intelligent Production Assistant - Oil & Gas Operations Intelligence Demo';

CREATE SCHEMA IF NOT EXISTS IPA.SCADA_CORE
    COMMENT = 'Core operational data: assets, sensors, alerts, permits, financials';

CREATE SCHEMA IF NOT EXISTS IPA.KNOWLEDGE_BASE
    COMMENT = 'Document repository for RAG (Retrieval Augmented Generation)';

CREATE SCHEMA IF NOT EXISTS IPA.APP
    COMMENT = 'Application layer: ML models, Cortex services, Streamlit';

-- Grant AI/Cortex component creation privileges on APP schema
GRANT CREATE AGENT ON SCHEMA IPA.APP TO ROLE IPA_DEMO_ROLE;
GRANT CREATE CORTEX SEARCH SERVICE ON SCHEMA IPA.APP TO ROLE IPA_DEMO_ROLE;
GRANT CREATE SEMANTIC VIEW ON SCHEMA IPA.APP TO ROLE IPA_DEMO_ROLE;
GRANT CREATE STREAMLIT ON SCHEMA IPA.APP TO ROLE IPA_DEMO_ROLE;

USE SCHEMA IPA.APP;

--------------------------------------------------------------------------------
-- STEP 3: CREATE TABLES
--------------------------------------------------------------------------------

-- Asset Master
CREATE OR REPLACE TABLE IPA.SCADA_CORE.ASSET_MASTER (
    ASSET_ID VARCHAR(50) PRIMARY KEY,
    ASSET_TYPE VARCHAR(50),
    ASSET_NAME VARCHAR(200),
    BASIN VARCHAR(50),
    GEO_ZONE VARCHAR(50),
    STATUS VARCHAR(20) DEFAULT 'ACTIVE',
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Master list of production assets (wells, rigs, compressors, sensors)';

-- Tag Registry (Sensor Metadata)
CREATE OR REPLACE TABLE IPA.SCADA_CORE.TAG_REGISTRY (
    TAG_ID VARCHAR(100) PRIMARY KEY,
    ASSET_ID VARCHAR(50),
    ATTRIBUTE_NAME VARCHAR(100),
    UOM VARCHAR(20),
    MIN_VALUE FLOAT,
    MAX_VALUE FLOAT
) COMMENT = 'Sensor/tag metadata registry mapping tags to assets and attributes';

-- Tag History (Time-Series SCADA Data)
CREATE OR REPLACE TABLE IPA.SCADA_CORE.TAG_HISTORY (
    TAG_ID VARCHAR(100),
    TIMESTAMP TIMESTAMP_NTZ,
    VALUE FLOAT,
    QUALITY INTEGER DEFAULT 192
) CLUSTER BY (TIMESTAMP)
  COMMENT = 'Time-series sensor readings (SCADA data) - 187,200 rows per deployment';

-- Financial Actuals
CREATE OR REPLACE TABLE IPA.SCADA_CORE.FINANCIAL_ACTUALS (
    ID INTEGER AUTOINCREMENT PRIMARY KEY,
    COST_CENTER_ID VARCHAR(50),
    ASSET_ID VARCHAR(50),
    DATE DATE,
    EXPENSE_CATEGORY VARCHAR(100),
    VENDOR VARCHAR(200),
    DESCRIPTION VARCHAR(500),
    AMOUNT FLOAT
) COMMENT = 'Financial transactions for cost variance detection';

-- Mission Control Alerts
CREATE OR REPLACE TABLE IPA.SCADA_CORE.MISSION_CONTROL_ALERTS (
    ALERT_ID INTEGER AUTOINCREMENT PRIMARY KEY,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    AGENT_TYPE VARCHAR(50),
    SEVERITY VARCHAR(20),
    ASSET_ID VARCHAR(50),
    TITLE VARCHAR(200),
    DESCRIPTION VARCHAR(2000),
    STATUS VARCHAR(20) DEFAULT 'NEW',
    ACKNOWLEDGED_AT TIMESTAMP_NTZ,
    ACKNOWLEDGED_BY VARCHAR(100)
) COMMENT = 'Shared alert fabric populated by detection services';

-- Active Permits
CREATE OR REPLACE TABLE IPA.SCADA_CORE.ACTIVE_PERMITS (
    PERMIT_ID VARCHAR(50) PRIMARY KEY,
    PERMIT_TYPE VARCHAR(50),
    ZONE VARCHAR(50),
    WORK_TYPE VARCHAR(100),
    DESCRIPTION VARCHAR(500),
    MAX_H2S_LIMIT FLOAT,
    APPROVER VARCHAR(100),
    VALID_FROM TIMESTAMP_NTZ,
    VALID_TO TIMESTAMP_NTZ,
    STATUS VARCHAR(20) DEFAULT 'ACTIVE'
) COMMENT = 'Active work permits with safety constraints (used by Guardian)';

-- Knowledge Base Documents
CREATE OR REPLACE TABLE IPA.KNOWLEDGE_BASE.DOCUMENTS_CHUNKED (
    DOC_ID VARCHAR(100),
    CHUNK_ID VARCHAR(100) PRIMARY KEY,
    TEXT_CHUNK VARCHAR(16000),
    DOC_TYPE VARCHAR(50),
    ASSET_ID VARCHAR(50),
    DOC_DATE DATE,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Chunked documents for Cortex Search (31 docs: 4 signal + 27 noise)';

-- Inspection Results
CREATE OR REPLACE TABLE IPA.SCADA_CORE.INSPECTION_RESULTS (
    INSPECTION_ID VARCHAR(50) PRIMARY KEY,
    ASSET_ID VARCHAR(50),
    INSPECTION_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    IMAGE_PATH VARCHAR(500),
    AI_ANALYSIS VARIANT,
    CONDITION_SCORE INTEGER,
    STATUS VARCHAR(20)
) COMMENT = 'Safety inspection results from Visual Inspector (AI_COMPLETE)';

--------------------------------------------------------------------------------
-- STEP 4: INSERT SAMPLE DATA
--------------------------------------------------------------------------------

-- Assets (12 assets)
INSERT INTO IPA.SCADA_CORE.ASSET_MASTER (ASSET_ID, ASSET_TYPE, ASSET_NAME, BASIN, GEO_ZONE)
VALUES 
    ('WELL-RP-05', 'Rod Pump', 'Rod Pump Well 05', 'Permian', 'Zone-A'),
    ('WELL-A10', 'Gas Lift', 'Gas Lift Well A10', 'Delaware', 'Zone-B'),
    ('RIG-9', 'Drilling Rig', 'Rig 9 Delaware', 'Delaware', 'Zone-C'),
    ('WELL-B01', 'Rod Pump', 'Well B01', 'Bakken', 'Zone-D'),
    ('WELL-B02', 'Gas Lift', 'Well B02', 'Bakken', 'Zone-D'),
    ('WELL-B03', 'PCP', 'Well B03', 'Eagle Ford', 'Zone-E'),
    ('WELL-B04', 'Rod Pump', 'Well B04', 'Permian', 'Zone-A'),
    ('WELL-B05', 'Gas Lift', 'Well B05', 'Delaware', 'Zone-B'),
    ('WELL-B06', 'Rod Pump', 'Well B06', 'Bakken', 'Zone-D'),
    ('WELL-B07', 'PCP', 'Well B07', 'Eagle Ford', 'Zone-E'),
    ('COMP-01', 'Compressor', 'Main Compressor Station', 'Permian', 'Zone-A'),
    ('SENSOR-ZONE-B', 'Gas Detector', 'Zone B H2S Monitor', 'Delaware', 'Zone-B');

-- Tag Registry (13 sensors)
INSERT INTO IPA.SCADA_CORE.TAG_REGISTRY (TAG_ID, ASSET_ID, ATTRIBUTE_NAME, UOM, MIN_VALUE, MAX_VALUE)
VALUES
    ('WELL-RP-05.DYN_LOAD', 'WELL-RP-05', 'Dynamometer_Load', 'Lbs', 0, 25000),
    ('WELL-RP-05.TUBING_PRESS', 'WELL-RP-05', 'Tubing_Pressure', 'PSI', 0, 3000),
    ('WELL-RP-05.CASING_PRESS', 'WELL-RP-05', 'Casing_Pressure', 'PSI', 0, 3000),
    ('WELL-A10.GAS_INJ_RATE', 'WELL-A10', 'Gas_Injection_Rate', 'MSCF/D', 0, 5000),
    ('WELL-A10.CASING_PRESS', 'WELL-A10', 'Casing_Pressure', 'PSI', 0, 3000),
    ('RIG-9.ROP', 'RIG-9', 'Rate_Of_Penetration', 'ft/hr', 0, 300),
    ('RIG-9.WOB', 'RIG-9', 'Weight_On_Bit', 'Klbs', 0, 50),
    ('RIG-9.HOOKLOAD', 'RIG-9', 'Hookload', 'Klbs', 0, 500),
    ('ZONE-B-H2S', 'SENSOR-ZONE-B', 'H2S_Concentration', 'PPM', 0, 100),
    ('ZONE-B-LEL', 'SENSOR-ZONE-B', 'LEL_Percent', '%', 0, 100),
    ('WELL-B03.PUMP_SPEED', 'WELL-B03', 'Pump_Speed', 'RPM', 0, 500),
    ('WELL-B03.INTAKE_PRESS', 'WELL-B03', 'Intake_Pressure', 'PSI', 0, 1000),
    ('WELL-B03.TORQUE', 'WELL-B03', 'Drive_Torque', 'ft-lb', 0, 2000);

-- Active Permits (2 permits)
INSERT INTO IPA.SCADA_CORE.ACTIVE_PERMITS (PERMIT_ID, PERMIT_TYPE, ZONE, WORK_TYPE, DESCRIPTION, MAX_H2S_LIMIT, APPROVER, VALID_FROM, VALID_TO, STATUS)
VALUES
    ('HWP-2026-001', 'Hot Work Permit', 'Zone-B', 'Welding', 'Welding on flowline repair', 10.0, 'Offshore Installation Manager', DATEADD(day, -1, CURRENT_TIMESTAMP()), DATEADD(day, 1, CURRENT_TIMESTAMP()), 'ACTIVE'),
    ('CWP-2026-002', 'Confined Space Entry', 'Zone-A', 'Tank Inspection', 'Annual tank inspection', 5.0, 'Safety Manager', DATEADD(day, -2, CURRENT_TIMESTAMP()), DATEADD(day, 2, CURRENT_TIMESTAMP()), 'ACTIVE');

-- Financial Data (7 transactions)
INSERT INTO IPA.SCADA_CORE.FINANCIAL_ACTUALS (COST_CENTER_ID, ASSET_ID, DATE, EXPENSE_CATEGORY, VENDOR, DESCRIPTION, AMOUNT)
VALUES
    ('CC-DEL-001', 'WELL-A10', DATEADD(day, -1, CURRENT_DATE()), 'Chemicals', 'Apex Chemicals', 'Emergency acid stimulation treatment to remove scale buildup', 50000),
    ('CC-DEL-001', 'WELL-A10', DATEADD(day, -5, CURRENT_DATE()), 'Chemicals', 'ChemServ Inc', 'Routine chemical injection', 2500),
    ('CC-DEL-001', 'WELL-A10', DATEADD(day, -10, CURRENT_DATE()), 'Chemicals', 'ChemServ Inc', 'Routine chemical injection', 2500),
    ('CC-PER-001', 'WELL-RP-05', DATEADD(day, -3, CURRENT_DATE()), 'Maintenance', 'PumpTech Services', 'Routine pump inspection', 3500),
    ('CC-PER-001', 'WELL-B01', DATEADD(day, -2, CURRENT_DATE()), 'Rental Equipment', 'OilField Rentals', 'Workover rig rental', 15000),
    ('CC-DEL-002', 'RIG-9', DATEADD(day, -1, CURRENT_DATE()), 'Maintenance', 'DrillTech', 'Mud motor replacement', 25000),
    ('CC-DEL-002', 'RIG-9', DATEADD(day, -7, CURRENT_DATE()), 'Supplies', 'Mud Systems Inc', 'Drilling fluids', 8000);

-- Sample Alerts (3 initial alerts)
INSERT INTO IPA.SCADA_CORE.MISSION_CONTROL_ALERTS (AGENT_TYPE, SEVERITY, ASSET_ID, TITLE, DESCRIPTION, STATUS)
VALUES
    ('SENTINEL', 'CRITICAL', 'WELL-RP-05', 'Rod Pump Failure Detected', 'Well RP-05 Down. Pattern: Rod Part. History of fluid pound detected in dynamometer cards over previous 24 hours.', 'NEW'),
    ('GUARDIAN', 'CRITICAL', 'SENSOR-ZONE-B', 'H2S Safety Violation', 'CRITICAL: Hot Work active in Zone B with H2S at 15ppm. Permit limit is 10ppm. STOP WORK REQUIRED.', 'NEW'),
    ('SENTINEL', 'WARNING', 'RIG-9', 'Non-Productive Time Detected', 'Rig-9 experienced 6 hours of zero ROP yesterday. Correlates with mud motor failure.', 'ACKNOWLEDGED');

--------------------------------------------------------------------------------
-- STEP 5: GENERATE TIME-SERIES DATA (187,200 rows)
-- This will take 1-2 minutes
--------------------------------------------------------------------------------

-- Rod Pump Dynamometer Data (with failure pattern) - 14,400 rows
INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
WITH time_series AS (
    SELECT 
        DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts,
        SEQ4() as seq
    FROM TABLE(GENERATOR(ROWCOUNT => 14400))
)
SELECT 
    'WELL-RP-05.DYN_LOAD' AS TAG_ID,
    ts AS TIMESTAMP,
    CASE 
        WHEN ts < DATEADD(hour, -36, CURRENT_TIMESTAMP()) 
            THEN 15000 + SIN(seq * 0.1) * 5000 + UNIFORM(-500, 500, RANDOM())
        WHEN ts BETWEEN DATEADD(hour, -36, CURRENT_TIMESTAMP()) AND DATEADD(hour, -8, CURRENT_TIMESTAMP())
            THEN GREATEST(0, 10000 + SIN(seq * 0.1) * 2500 + UNIFORM(-200, 200, RANDOM()))
        ELSE 0
    END AS VALUE,
    192 AS QUALITY
FROM time_series;

-- Rod Pump Tubing Pressure - 14,400 rows
INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
WITH time_series AS (
    SELECT 
        DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts,
        SEQ4() as seq
    FROM TABLE(GENERATOR(ROWCOUNT => 14400))
)
SELECT 
    'WELL-RP-05.TUBING_PRESS' AS TAG_ID,
    ts AS TIMESTAMP,
    850 + SIN(seq * 0.05) * 100 + UNIFORM(-25, 25, RANDOM()) AS VALUE,
    192 AS QUALITY
FROM time_series;

-- Rod Pump Casing Pressure - 14,400 rows
INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
WITH time_series AS (
    SELECT 
        DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts,
        SEQ4() as seq
    FROM TABLE(GENERATOR(ROWCOUNT => 14400))
)
SELECT 
    'WELL-RP-05.CASING_PRESS' AS TAG_ID,
    ts AS TIMESTAMP,
    450 + SIN(seq * 0.03) * 50 + UNIFORM(-15, 15, RANDOM()) AS VALUE,
    192 AS QUALITY
FROM time_series;

-- H2S Data (with spike) - 14,400 rows
INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
WITH time_series AS (
    SELECT 
        DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts,
        SEQ4() as seq
    FROM TABLE(GENERATOR(ROWCOUNT => 14400))
)
SELECT 
    'ZONE-B-H2S' AS TAG_ID,
    ts AS TIMESTAMP,
    CASE 
        WHEN ts > DATEADD(hour, -2, CURRENT_TIMESTAMP()) 
            THEN 15 + UNIFORM(-2, 2, RANDOM())
        ELSE UNIFORM(0, 3, RANDOM())
    END AS VALUE,
    192 AS QUALITY
FROM time_series;

-- LEL Percent Data - 14,400 rows
INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
WITH time_series AS (
    SELECT 
        DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts,
        SEQ4() as seq
    FROM TABLE(GENERATOR(ROWCOUNT => 14400))
)
SELECT 
    'ZONE-B-LEL' AS TAG_ID,
    ts AS TIMESTAMP,
    CASE 
        WHEN ts > DATEADD(hour, -2, CURRENT_TIMESTAMP()) 
            THEN 8 + UNIFORM(-1, 1, RANDOM())
        ELSE UNIFORM(0, 2, RANDOM())
    END AS VALUE,
    192 AS QUALITY
FROM time_series;

-- RIG-9 ROP Data (with NPT event) - 14,400 rows
INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
WITH time_series AS (
    SELECT 
        DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts,
        SEQ4() as seq
    FROM TABLE(GENERATOR(ROWCOUNT => 14400))
)
SELECT 
    'RIG-9.ROP' AS TAG_ID,
    ts AS TIMESTAMP,
    CASE 
        WHEN ts BETWEEN DATEADD(hour, -38, CURRENT_TIMESTAMP()) AND DATEADD(hour, -32, CURRENT_TIMESTAMP())
            THEN 0
        ELSE 150 + UNIFORM(-20, 20, RANDOM())
    END AS VALUE,
    192 AS QUALITY
FROM time_series;

-- RIG-9 Weight on Bit - 14,400 rows
INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
WITH time_series AS (
    SELECT 
        DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts,
        SEQ4() as seq
    FROM TABLE(GENERATOR(ROWCOUNT => 14400))
)
SELECT 
    'RIG-9.WOB' AS TAG_ID,
    ts AS TIMESTAMP,
    CASE 
        WHEN ts BETWEEN DATEADD(hour, -38, CURRENT_TIMESTAMP()) AND DATEADD(hour, -32, CURRENT_TIMESTAMP())
            THEN 0
        ELSE 25 + UNIFORM(-5, 5, RANDOM())
    END AS VALUE,
    192 AS QUALITY
FROM time_series;

-- RIG-9 Hookload - 14,400 rows
INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
WITH time_series AS (
    SELECT 
        DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts,
        SEQ4() as seq
    FROM TABLE(GENERATOR(ROWCOUNT => 14400))
)
SELECT 
    'RIG-9.HOOKLOAD' AS TAG_ID,
    ts AS TIMESTAMP,
    CASE 
        WHEN ts BETWEEN DATEADD(hour, -38, CURRENT_TIMESTAMP()) AND DATEADD(hour, -32, CURRENT_TIMESTAMP())
            THEN 180 + UNIFORM(-10, 10, RANDOM())
        ELSE 250 + UNIFORM(-20, 20, RANDOM())
    END AS VALUE,
    192 AS QUALITY
FROM time_series;

-- Gas Lift Well A10 - Gas Injection Rate - 14,400 rows
INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
WITH time_series AS (
    SELECT 
        DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts,
        SEQ4() as seq
    FROM TABLE(GENERATOR(ROWCOUNT => 14400))
)
SELECT 
    'WELL-A10.GAS_INJ_RATE' AS TAG_ID,
    ts AS TIMESTAMP,
    1500 + SIN(seq * 0.02) * 200 + UNIFORM(-50, 50, RANDOM()) AS VALUE,
    192 AS QUALITY
FROM time_series;

-- Gas Lift Well A10 - Casing Pressure - 14,400 rows
INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
WITH time_series AS (
    SELECT 
        DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts,
        SEQ4() as seq
    FROM TABLE(GENERATOR(ROWCOUNT => 14400))
)
SELECT 
    'WELL-A10.CASING_PRESS' AS TAG_ID,
    ts AS TIMESTAMP,
    1200 + SIN(seq * 0.04) * 100 + UNIFORM(-30, 30, RANDOM()) AS VALUE,
    192 AS QUALITY
FROM time_series;

-- PCP Well B03 - Pump Speed (with stall pattern) - 14,400 rows
INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
WITH time_series AS (
    SELECT 
        DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts,
        SEQ4() as seq
    FROM TABLE(GENERATOR(ROWCOUNT => 14400))
)
SELECT 
    'WELL-B03.PUMP_SPEED' AS TAG_ID,
    ts AS TIMESTAMP,
    CASE 
        WHEN seq < 120 THEN 0
        ELSE 280 + UNIFORM(-20, 20, RANDOM())
    END AS VALUE,
    192 AS QUALITY
FROM time_series;

-- PCP Well B03 - Intake Pressure - 14,400 rows
INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
WITH time_series AS (
    SELECT 
        DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts,
        SEQ4() as seq
    FROM TABLE(GENERATOR(ROWCOUNT => 14400))
)
SELECT 
    'WELL-B03.INTAKE_PRESS' AS TAG_ID,
    ts AS TIMESTAMP,
    CASE 
        WHEN seq < 120 THEN 50 + UNIFORM(0, 30, RANDOM())
        ELSE 350 + UNIFORM(-50, 50, RANDOM())
    END AS VALUE,
    192 AS QUALITY
FROM time_series;

-- PCP Well B03 - Drive Torque (high when stalled) - 14,400 rows
INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
WITH time_series AS (
    SELECT 
        DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts,
        SEQ4() as seq
    FROM TABLE(GENERATOR(ROWCOUNT => 14400))
)
SELECT 
    'WELL-B03.TORQUE' AS TAG_ID,
    ts AS TIMESTAMP,
    CASE 
        WHEN seq < 120 THEN 1800 + UNIFORM(0, 200, RANDOM())
        ELSE 800 + UNIFORM(-100, 100, RANDOM())
    END AS VALUE,
    192 AS QUALITY
FROM time_series;

-- Total: 13 tags × 14,400 rows = 187,200 rows

--------------------------------------------------------------------------------
-- STEP 6: INSERT KNOWLEDGE BASE DOCUMENTS (31 docs)
--------------------------------------------------------------------------------

-- Signal documents (4 docs with answers)
INSERT INTO IPA.KNOWLEDGE_BASE.DOCUMENTS_CHUNKED (DOC_ID, CHUNK_ID, TEXT_CHUNK, DOC_TYPE, ASSET_ID, DOC_DATE)
VALUES
    ('DOC-SIGNAL-001', 'CHUNK-001', 'Field Service Ticket - Apex Chemicals
Date: Yesterday
Asset: Well-A10
Description: Emergency acid stimulation treatment to remove scale buildup.
Cost: $50,000
Note: Treatment approved by Superintendent to restore flow. Scale buildup was restricting production by 40%. Immediate intervention required to prevent complete blockage.', 'Invoice', 'WELL-A10', DATEADD(day, -1, CURRENT_DATE())),
    
    ('DOC-SIGNAL-002', 'CHUNK-002', 'Hot Work Permit HWP-2026-001
Zone: Zone B
Date: Today
Work Type: Welding on flowline repair
Gas Test Requirement: Continuous monitoring
Stop Work Trigger: H2S > 10 ppm
Approver: Offshore Installation Manager
Work Description: Repair corroded section of flowline. Weld joint replacement required.', 'Work Permit', 'SENSOR-ZONE-B', CURRENT_DATE()),
    
    ('DOC-SIGNAL-003', 'CHUNK-003', 'Well Failure Analysis Report
Asset: Well-RP-05
Date: Today
Incident: Unit shut down on low load alarm at 08:15
Diagnosis: Sucker rod parted at approximately 2,000 ft depth
Root Cause: Review of dynamometer cards from previous 24 hours shows severe fluid pound pattern due to gas interference, causing fatigue stress on the rod string. Recommend gas anchor installation upon workover.', 'Tech Note', 'WELL-RP-05', CURRENT_DATE()),
    
    ('DOC-SIGNAL-004', 'CHUNK-004', 'Daily Drilling Report - Rig 9
Date: Yesterday
Time Log:
00:00-10:00: Drilling ahead at 150 ft/hr average
10:00-16:00: Drilling halted - NPT Event
16:00-24:00: Resumed drilling
Activity Description: At 8,500 ft MD, lost pump pressure. POOH (Pull Out Of Hole). Found mud motor stator rubber chunked out. Lay down motor, pick up new BHA. Total NPT: 6 hours.
Code: Unplanned Maintenance / NPT', 'Drilling Report', 'RIG-9', DATEADD(day, -1, CURRENT_DATE()));

-- Noise documents (27 routine logs)
INSERT INTO IPA.KNOWLEDGE_BASE.DOCUMENTS_CHUNKED (DOC_ID, CHUNK_ID, TEXT_CHUNK, DOC_TYPE, ASSET_ID, DOC_DATE)
SELECT 
    'DOC-NOISE-' || LPAD(seq::VARCHAR, 3, '0') AS DOC_ID,
    'CHUNK-' || LPAD((seq + 4)::VARCHAR, 3, '0') AS CHUNK_ID,
    CASE MOD(seq, 3)
        WHEN 0 THEN 'Daily Operations Report - ' || well || '
Date: ' || seq || ' days ago
Production steady, no variance from targets.
Routine chemical injection rate verified at standard levels.
All equipment operating within normal parameters.
No safety incidents reported.'
        WHEN 1 THEN 'Routine Maintenance Log - ' || well || '
Date: ' || seq || ' days ago
Preventive maintenance completed on surface equipment.
Choke adjusted to optimize flow.
Pressure readings within acceptable range.
No anomalies detected.'
        ELSE 'Production Summary - ' || well || '
Date: ' || seq || ' days ago
Oil production: On target
Gas production: On target
Water cut: Stable
Operating hours: 24/24
Downtime: None'
    END AS TEXT_CHUNK,
    CASE MOD(seq, 3)
        WHEN 0 THEN 'Operations Report'
        WHEN 1 THEN 'Maintenance Log'
        ELSE 'Production Report'
    END AS DOC_TYPE,
    well AS ASSET_ID,
    DATEADD(day, -MOD(seq, 7) - 1, CURRENT_DATE()) AS DOC_DATE
FROM (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY 1) as seq,
        wells.well
    FROM TABLE(GENERATOR(ROWCOUNT => 27)) g,
    (SELECT column1 as well FROM VALUES 
        ('WELL-B01'), ('WELL-B02'), ('WELL-B03'), ('WELL-B04'), 
        ('WELL-B05'), ('WELL-B06'), ('WELL-B07'), ('COMP-01')
    ) wells
    LIMIT 27
);

--------------------------------------------------------------------------------
-- STEP 7: CREATE DETECTION SERVICES
--------------------------------------------------------------------------------

-- Sentinel (Production Monitoring)
CREATE OR REPLACE PROCEDURE IPA.SCADA_CORE.RUN_SENTINEL_AGENT()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run_sentinel'
COMMENT = 'Sentinel: Production monitoring - detects sensor anomalies (>10% deviation)'
AS
$$
def run_sentinel(session):
    anomaly_count = 0
    query = """
        SELECT 
            r.TAG_ID, r.ASSET_ID, r.ATTRIBUTE_NAME,
            AVG(CASE WHEN t.TIMESTAMP > DATEADD(day, -1, CURRENT_TIMESTAMP()) THEN t.VALUE END) as RECENT_AVG,
            AVG(CASE WHEN t.TIMESTAMP BETWEEN DATEADD(day, -7, CURRENT_TIMESTAMP()) AND DATEADD(day, -1, CURRENT_TIMESTAMP()) THEN t.VALUE END) as BASELINE_AVG
        FROM IPA.SCADA_CORE.TAG_REGISTRY r
        JOIN IPA.SCADA_CORE.TAG_HISTORY t ON r.TAG_ID = t.TAG_ID
        WHERE r.ATTRIBUTE_NAME IN ('Dynamometer_Load', 'Tubing_Pressure', 'Casing_Pressure')
        GROUP BY r.TAG_ID, r.ASSET_ID, r.ATTRIBUTE_NAME
        HAVING RECENT_AVG IS NOT NULL AND BASELINE_AVG IS NOT NULL
           AND ABS(RECENT_AVG - BASELINE_AVG) > (BASELINE_AVG * 0.10)
    """
    try:
        anomalies = session.sql(query).collect()
        for row in anomalies:
            tag_id = row['TAG_ID']
            asset_id = row['ASSET_ID']
            attr = row['ATTRIBUTE_NAME']
            recent = float(row['RECENT_AVG']) if row['RECENT_AVG'] else 0
            baseline = float(row['BASELINE_AVG']) if row['BASELINE_AVG'] else 1
            deviation = abs(recent - baseline) / baseline * 100 if baseline else 0
            prompt = f"Analyze: {attr} on {asset_id} changed from {baseline:.1f} to {recent:.1f} ({deviation:.1f}% deviation). Brief 1-sentence assessment."
            prompt = prompt.replace("'", "''")
            result = session.sql(f"SELECT AI_COMPLETE('claude-3-5-sonnet', '{prompt}')").collect()
            analysis = result[0][0] if result else "Analysis unavailable"
            analysis = analysis.replace("'", "''")
            severity = 'CRITICAL' if deviation > 25 else 'WARNING'
            exists = session.sql(f"""
                SELECT COUNT(*) as CNT FROM IPA.SCADA_CORE.MISSION_CONTROL_ALERTS
                WHERE AGENT_TYPE = 'SENTINEL' AND ASSET_ID = '{asset_id}' AND STATUS = 'NEW'
                  AND CREATED_AT > DATEADD(hour, -4, CURRENT_TIMESTAMP())
            """).collect()[0]['CNT']
            if exists == 0:
                session.sql(f"""
                    INSERT INTO IPA.SCADA_CORE.MISSION_CONTROL_ALERTS 
                    (AGENT_TYPE, SEVERITY, ASSET_ID, TITLE, DESCRIPTION, STATUS)
                    VALUES ('SENTINEL', '{severity}', '{asset_id}', '🤖 {attr} Deviation: {deviation:.1f}%', '[Cortex AI Analysis] {analysis}', 'NEW')
                """).collect()
                anomaly_count += 1
    except Exception as e:
        return f"Sentinel: Error - {str(e)[:50]}"
    return f"Sentinel: {anomaly_count} anomalies"
$$;

-- Guardian (Safety Monitoring)
CREATE OR REPLACE PROCEDURE IPA.SCADA_CORE.RUN_GUARDIAN_AGENT()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run_guardian'
COMMENT = 'Guardian: HSE safety monitoring - detects H2S and LEL threshold violations'
AS
$$
def run_guardian(session):
    alert_count = 0
    query = """
        SELECT r.ASSET_ID, r.TAG_ID, r.ATTRIBUTE_NAME, MAX(t.VALUE) as MAX_VALUE, AVG(t.VALUE) as AVG_VALUE
        FROM IPA.SCADA_CORE.TAG_HISTORY t
        JOIN IPA.SCADA_CORE.TAG_REGISTRY r ON t.TAG_ID = r.TAG_ID
        WHERE r.ATTRIBUTE_NAME IN ('H2S_Concentration', 'LEL_Percent')
        GROUP BY r.ASSET_ID, r.TAG_ID, r.ATTRIBUTE_NAME
        HAVING MAX(t.VALUE) > 10
    """
    try:
        alerts = session.sql(query).collect()
        for row in alerts:
            asset_id = row['ASSET_ID']
            attr = row['ATTRIBUTE_NAME']
            max_val = float(row['MAX_VALUE'])
            avg_val = float(row['AVG_VALUE'])
            prompt = f"HSE Alert: {attr} at {asset_id} shows max {max_val:.1f}, avg {avg_val:.1f}. Brief safety recommendation in 1 sentence."
            prompt = prompt.replace("'", "''")
            result = session.sql(f"SELECT AI_COMPLETE('claude-3-5-sonnet', '{prompt}')").collect()
            analysis = result[0][0] if result else "Analysis unavailable"
            analysis = analysis.replace("'", "''")
            severity = 'CRITICAL' if max_val > 15 else 'WARNING'
            exists = session.sql(f"""
                SELECT COUNT(*) as CNT FROM IPA.SCADA_CORE.MISSION_CONTROL_ALERTS
                WHERE AGENT_TYPE = 'GUARDIAN' AND ASSET_ID = '{asset_id}' AND STATUS = 'NEW'
                  AND CREATED_AT > DATEADD(hour, -1, CURRENT_TIMESTAMP())
            """).collect()[0]['CNT']
            if exists == 0:
                session.sql(f"""
                    INSERT INTO IPA.SCADA_CORE.MISSION_CONTROL_ALERTS 
                    (AGENT_TYPE, SEVERITY, ASSET_ID, TITLE, DESCRIPTION, STATUS)
                    VALUES ('GUARDIAN', '{severity}', '{asset_id}', '🤖 {attr}: {max_val:.1f}', '[Cortex AI Analysis] {analysis}', 'NEW')
                """).collect()
                alert_count += 1
    except Exception as e:
        return f"Guardian: Error - {str(e)[:50]}"
    return f"Guardian: {alert_count} checks"
$$;

-- Fiscal (Cost Analytics)
CREATE OR REPLACE PROCEDURE IPA.SCADA_CORE.RUN_FISCAL_AGENT()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run_fiscal'
COMMENT = 'Fiscal: Cost analytics - detects high-spend transactions (>$10k)'
AS
$$
def run_fiscal(session):
    insight_count = 0
    query = """
        SELECT f.ASSET_ID, f.EXPENSE_CATEGORY, f.VENDOR, f.DESCRIPTION, f.AMOUNT, a.ASSET_TYPE
        FROM IPA.SCADA_CORE.FINANCIAL_ACTUALS f
        JOIN IPA.SCADA_CORE.ASSET_MASTER a ON f.ASSET_ID = a.ASSET_ID
        WHERE f.AMOUNT > 10000
        ORDER BY f.AMOUNT DESC
        LIMIT 5
    """
    try:
        costs = session.sql(query).collect()
        for row in costs:
            asset_id = row['ASSET_ID']
            category = row['EXPENSE_CATEGORY']
            vendor = row['VENDOR']
            desc = row['DESCRIPTION']
            amount = float(row['AMOUNT'])
            asset_type = row['ASSET_TYPE']
            prompt = f"Cost analysis: ${amount:,.0f} spent on {category} for {asset_id} ({asset_type}) - {desc}. Brief 1-sentence recommendation."
            prompt = prompt.replace("'", "''")
            result = session.sql(f"SELECT AI_COMPLETE('claude-3-5-sonnet', '{prompt}')").collect()
            analysis = result[0][0] if result else "Analysis unavailable"
            analysis = analysis.replace("'", "''")
            severity = 'WARNING' if amount > 25000 else 'INFO'
            exists = session.sql(f"""
                SELECT COUNT(*) as CNT FROM IPA.SCADA_CORE.MISSION_CONTROL_ALERTS
                WHERE AGENT_TYPE = 'FISCAL' AND ASSET_ID = '{asset_id}' AND STATUS = 'NEW'
                  AND CREATED_AT > DATEADD(day, -1, CURRENT_TIMESTAMP())
            """).collect()[0]['CNT']
            if exists == 0:
                session.sql(f"""
                    INSERT INTO IPA.SCADA_CORE.MISSION_CONTROL_ALERTS 
                    (AGENT_TYPE, SEVERITY, ASSET_ID, TITLE, DESCRIPTION, STATUS)
                    VALUES ('FISCAL', '{severity}', '{asset_id}', '🤖 {category}: ${amount:,.0f}', '[Cortex AI Analysis] {analysis}', 'NEW')
                """).collect()
                insight_count += 1
    except Exception as e:
        return f"Fiscal: Error - {str(e)[:50]}"
    return f"Fiscal: {insight_count} insights"
$$;

-- Master runner
CREATE OR REPLACE PROCEDURE IPA.SCADA_CORE.RUN_ALL_AGENTS()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run_all'
COMMENT = 'Master procedure - runs all 3 detection services'
AS
$$
def run_all(session):
    try:
        sentinel = session.sql("CALL IPA.SCADA_CORE.RUN_SENTINEL_AGENT()").collect()[0][0]
    except Exception as e:
        sentinel = f"Sentinel: Error"
    try:
        guardian = session.sql("CALL IPA.SCADA_CORE.RUN_GUARDIAN_AGENT()").collect()[0][0]
    except Exception as e:
        guardian = f"Guardian: Error"
    try:
        fiscal = session.sql("CALL IPA.SCADA_CORE.RUN_FISCAL_AGENT()").collect()[0][0]
    except Exception as e:
        fiscal = f"Fiscal: Error"
    return f"{sentinel} | {guardian} | {fiscal}"
$$;

--------------------------------------------------------------------------------
-- STEP 8: CREATE CORTEX SEARCH SERVICE
--------------------------------------------------------------------------------

CREATE OR REPLACE CORTEX SEARCH SERVICE IPA.KNOWLEDGE_BASE.DOCUMENT_SEARCH
  ON TEXT_CHUNK
  ATTRIBUTES ASSET_ID, DOC_TYPE
  WAREHOUSE = IPA_WH
  TARGET_LAG = '1 hour'
  AS (
    SELECT CHUNK_ID, TEXT_CHUNK, ASSET_ID, DOC_TYPE, DOC_DATE
    FROM IPA.KNOWLEDGE_BASE.DOCUMENTS_CHUNKED
  );

--------------------------------------------------------------------------------
-- STEP 9: CREATE STAGES
--------------------------------------------------------------------------------

-- Stage for semantic model
CREATE OR REPLACE STAGE IPA.APP.SEMANTIC_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for semantic model YAML (used by IPA_AGENT)';

-- Stage for inspection images
CREATE OR REPLACE STAGE IPA.APP.INSPECTION_IMAGES
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Stage for safety inspection images (used by Visual Inspector)';

--------------------------------------------------------------------------------
-- STEP 10: CREATE GIT INTEGRATION (PUBLIC REPO)
--------------------------------------------------------------------------------

-- Switch to ACCOUNTADMIN for API integration
USE ROLE ACCOUNTADMIN;

-- Create API integration (no secret needed for public repos)
CREATE OR REPLACE API INTEGRATION IPA_GIT_API_INTEGRATION
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/Snowflake-Labs/')
  ENABLED = TRUE
  COMMENT = 'Git integration with Snowflake Labs GitHub (public repo)';

-- Grant usage on integration to demo role
GRANT USAGE ON INTEGRATION IPA_GIT_API_INTEGRATION TO ROLE IPA_DEMO_ROLE;

-- Switch back to demo role for repo creation
USE ROLE IPA_DEMO_ROLE;

-- Create Git repository integration (no credentials needed for public repo)
CREATE OR REPLACE GIT REPOSITORY IPA.APP.IPA_REPO
  API_INTEGRATION = IPA_GIT_API_INTEGRATION
  ORIGIN = 'https://github.com/Snowflake-Labs/sfguide-oil-gas-operational-intelligence.git'
  COMMENT = 'IPA demo repository';

-- Fetch latest files from Git
ALTER GIT REPOSITORY IPA.APP.IPA_REPO FETCH;

--------------------------------------------------------------------------------
-- STEP 11: UPLOAD SEMANTIC MODEL FROM GIT
--------------------------------------------------------------------------------

-- Copy semantic model from Git to semantic stage
COPY FILES
  INTO @IPA.APP.SEMANTIC_STAGE
  FROM @IPA.APP.IPA_REPO/branches/main/scripts/semantic_model/
  FILES = ('ipa_semantic_model.yaml');

--------------------------------------------------------------------------------
-- STEP 11a: UPLOAD SAMPLE INSPECTION IMAGES FROM GIT
--------------------------------------------------------------------------------

-- Copy sample images from Git to inspection images stage
-- These are pre-loaded safety inspection images for Visual Inspector demos
COPY FILES
  INTO @IPA.APP.INSPECTION_IMAGES
  FROM @IPA.APP.IPA_REPO/branches/main/streamlit/assets/
  PATTERN='.*\.(jpg|jpeg|png|webp|gif)';

-- Refresh stage to register files with DIRECTORY() function
ALTER STAGE IPA.APP.INSPECTION_IMAGES REFRESH;

-- Verify images were uploaded
SELECT RELATIVE_PATH, SIZE, LAST_MODIFIED 
FROM DIRECTORY(@IPA.APP.INSPECTION_IMAGES)
WHERE LOWER(RELATIVE_PATH) LIKE '%.jpg' 
   OR LOWER(RELATIVE_PATH) LIKE '%.jpeg'
   OR LOWER(RELATIVE_PATH) LIKE '%.png'
   OR LOWER(RELATIVE_PATH) LIKE '%.gif'
   OR LOWER(RELATIVE_PATH) LIKE '%.webp'
ORDER BY LAST_MODIFIED DESC;

--------------------------------------------------------------------------------
-- STEP 12: CREATE CORTEX AGENT (IPA_AGENT)
--------------------------------------------------------------------------------

CREATE OR REPLACE AGENT IPA.APP.IPA_AGENT
  COMMENT = 'Intelligent Production Assistant - Reasoning engine with text-to-SQL and document search'
  FROM SPECIFICATION
  $$
  models:
    orchestration: claude-3-5-sonnet

  instructions:
    system: |
      You are the Intelligent Production Assistant (IPA) for oil and gas operations.

      You have access to two tools:
      1. production_data: Use this to query structured operational data (sensors, alerts, assets, costs)
      2. docs_search: Use this to search technical documents, reports, and work permits

      Guidelines:
      - For questions about metrics, trends, or structured data, use production_data
      - For questions about failures, incidents, or technical details, use docs_search
      - Combine both tools when needed for comprehensive answers
      - Always cite sources from documents when using docs_search
      - Be concise and action-oriented

      Your goal is to help operations teams quickly understand production issues, safety risks, and cost variances.
    sample_questions:
      # Combined (Both Tools) - 4 prompts
      - question: "What are the current critical alerts and what do the documents say about them?"
        answer: "I'll query for critical alerts, then search documents for related failure analysis."
      - question: "What is the H2S level in Zone B and what does the permit say about limits?"
        answer: "I'll check the current H2S sensor reading and search permit documents for allowable limits."
      - question: "Show me WELL-A10 expenses and find the invoice details"
        answer: "I'll query the cost data and search documents for invoice information."
      - question: "Which wells have critical alerts and what are the root causes?"
        answer: "I'll query active alerts by well and search documents for failure patterns."
      
      # SQL Only (Cortex Analyst) - 4 prompts
      - question: "How many assets do we have by type?"
        answer: "I'll query the asset master to count assets grouped by type."
      - question: "Show me all critical alerts from the last 24 hours"
        answer: "I'll query the alerts table for critical severity alerts from the past day."
      - question: "What is the total spend on WELL-A10?"
        answer: "I'll query the financial actuals to sum expenses for this asset."
      - question: "What is the current H2S reading in Zone B?"
        answer: "I'll query the latest sensor readings for H2S in Zone B."
      
      # Document Search Only (Cortex Search) - 4 prompts
      - question: "Why did WELL-RP-05 fail?"
        answer: "I'll search the failure analysis documents for root cause information."
      - question: "What are the hot work permit requirements for Zone B?"
        answer: "I'll search permit documents for hot work safety requirements."
      - question: "What happened during the RIG-9 non-productive time event?"
        answer: "I'll search drilling reports for NPT event details."
      - question: "Find details about the $50k chemical treatment on WELL-A10"
        answer: "I'll search invoice and cost variance documents for treatment details."

  tools:
    - tool_spec:
        type: "cortex_analyst_text_to_sql"
        name: "production_data"
    - tool_spec:
        type: "cortex_search"
        name: "docs_search"

  tool_resources:
    production_data:
      semantic_model_file: "@IPA.APP.SEMANTIC_STAGE/ipa_semantic_model.yaml"
      execution_environment:
        type: "warehouse"
        warehouse: "IPA_WH"
    docs_search:
      name: "IPA.KNOWLEDGE_BASE.DOCUMENT_SEARCH"
      max_results: "5"
      title_column: "DOC_TYPE"
      id_column: "CHUNK_ID"
  $$;

-- Register agent with Snowflake Intelligence
ALTER SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT
  ADD AGENT IPA.APP.IPA_AGENT;

--------------------------------------------------------------------------------
-- STEP 13: CREATE DEMO DATA REFRESH PROCEDURE
-- Allows regenerating demo data with current timestamps for repeated presentations
--------------------------------------------------------------------------------

-- Demo Data Generator Stored Procedure
-- Regenerates all demo data with current timestamps for fresh demos
-- Can be called from Streamlit app via: CALL IPA.APP.GENERATE_DEMO_DATA();
CREATE OR REPLACE PROCEDURE IPA.APP.GENERATE_DEMO_DATA()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    rows_generated INTEGER DEFAULT 0;
BEGIN
    -- Clear existing data
    TRUNCATE TABLE IPA.SCADA_CORE.TAG_HISTORY;
    TRUNCATE TABLE IPA.SCADA_CORE.MISSION_CONTROL_ALERTS;
    TRUNCATE TABLE IPA.SCADA_CORE.FINANCIAL_ACTUALS;
    TRUNCATE TABLE IPA.SCADA_CORE.ACTIVE_PERMITS;
    TRUNCATE TABLE IPA.KNOWLEDGE_BASE.DOCUMENTS_CHUNKED;
    DELETE FROM IPA.SCADA_CORE.ASSET_MASTER;
    DELETE FROM IPA.SCADA_CORE.TAG_REGISTRY;
    DELETE FROM IPA.SCADA_CORE.INSPECTION_RESULTS;

    -- ========================================
    -- MASTER DATA
    -- ========================================
    
    -- Assets
    INSERT INTO IPA.SCADA_CORE.ASSET_MASTER (ASSET_ID, ASSET_TYPE, ASSET_NAME, BASIN, GEO_ZONE)
    VALUES 
        ('WELL-RP-05', 'Rod Pump', 'Rod Pump Well 05', 'Permian', 'Zone-A'),
        ('WELL-A10', 'Gas Lift', 'Gas Lift Well A10', 'Delaware', 'Zone-B'),
        ('RIG-9', 'Drilling Rig', 'Rig 9 Delaware', 'Delaware', 'Zone-C'),
        ('WELL-B01', 'Rod Pump', 'Well B01', 'Bakken', 'Zone-D'),
        ('WELL-B02', 'Gas Lift', 'Well B02', 'Bakken', 'Zone-D'),
        ('WELL-B03', 'PCP', 'Well B03', 'Eagle Ford', 'Zone-E'),
        ('WELL-B04', 'Rod Pump', 'Well B04', 'Permian', 'Zone-A'),
        ('WELL-B05', 'Gas Lift', 'Well B05', 'Delaware', 'Zone-B'),
        ('WELL-B06', 'Rod Pump', 'Well B06', 'Bakken', 'Zone-D'),
        ('WELL-B07', 'PCP', 'Well B07', 'Eagle Ford', 'Zone-E'),
        ('COMP-01', 'Compressor', 'Main Compressor Station', 'Permian', 'Zone-A'),
        ('SENSOR-ZONE-B', 'Gas Detector', 'Zone B H2S Monitor', 'Delaware', 'Zone-B');

    -- Tag Registry
    INSERT INTO IPA.SCADA_CORE.TAG_REGISTRY (TAG_ID, ASSET_ID, ATTRIBUTE_NAME, UOM, MIN_VALUE, MAX_VALUE)
    VALUES
        ('WELL-RP-05.DYN_LOAD', 'WELL-RP-05', 'Dynamometer_Load', 'Lbs', 0, 25000),
        ('WELL-RP-05.TUBING_PRESS', 'WELL-RP-05', 'Tubing_Pressure', 'PSI', 0, 3000),
        ('WELL-RP-05.CASING_PRESS', 'WELL-RP-05', 'Casing_Pressure', 'PSI', 0, 3000),
        ('WELL-A10.GAS_INJ_RATE', 'WELL-A10', 'Gas_Injection_Rate', 'MSCF/D', 0, 5000),
        ('WELL-A10.CASING_PRESS', 'WELL-A10', 'Casing_Pressure', 'PSI', 0, 3000),
        ('RIG-9.ROP', 'RIG-9', 'Rate_Of_Penetration', 'ft/hr', 0, 300),
        ('RIG-9.WOB', 'RIG-9', 'Weight_On_Bit', 'Klbs', 0, 50),
        ('RIG-9.HOOKLOAD', 'RIG-9', 'Hookload', 'Klbs', 0, 500),
        ('ZONE-B-H2S', 'SENSOR-ZONE-B', 'H2S_Concentration', 'PPM', 0, 100),
        ('ZONE-B-LEL', 'SENSOR-ZONE-B', 'LEL_Percent', '%', 0, 100);

    -- Active Permits (with current timestamps)
    INSERT INTO IPA.SCADA_CORE.ACTIVE_PERMITS (PERMIT_ID, PERMIT_TYPE, ZONE, WORK_TYPE, DESCRIPTION, MAX_H2S_LIMIT, APPROVER, VALID_FROM, VALID_TO, STATUS)
    VALUES
        ('HWP-2026-001', 'Hot Work Permit', 'Zone-B', 'Welding', 'Welding on flowline repair', 10.0, 'Offshore Installation Manager', DATEADD(day, -1, CURRENT_TIMESTAMP()), DATEADD(day, 1, CURRENT_TIMESTAMP()), 'ACTIVE'),
        ('CWP-2026-002', 'Confined Space Entry', 'Zone-A', 'Tank Inspection', 'Annual tank inspection', 5.0, 'Safety Manager', DATEADD(day, -2, CURRENT_TIMESTAMP()), DATEADD(day, 2, CURRENT_TIMESTAMP()), 'ACTIVE');

    -- Financial Data (with cost spike for demo)
    INSERT INTO IPA.SCADA_CORE.FINANCIAL_ACTUALS (COST_CENTER_ID, ASSET_ID, DATE, EXPENSE_CATEGORY, VENDOR, DESCRIPTION, AMOUNT)
    VALUES
        ('CC-DEL-001', 'WELL-A10', DATEADD(day, -1, CURRENT_DATE()), 'Chemicals', 'Apex Chemicals', 'Emergency acid stimulation treatment to remove scale buildup', 50000),
        ('CC-DEL-001', 'WELL-A10', DATEADD(day, -5, CURRENT_DATE()), 'Chemicals', 'ChemServ Inc', 'Routine chemical injection', 2500),
        ('CC-DEL-001', 'WELL-A10', DATEADD(day, -10, CURRENT_DATE()), 'Chemicals', 'ChemServ Inc', 'Routine chemical injection', 2500),
        ('CC-PER-001', 'WELL-RP-05', DATEADD(day, -3, CURRENT_DATE()), 'Maintenance', 'PumpTech Services', 'Routine pump inspection', 3500),
        ('CC-PER-001', 'WELL-B01', DATEADD(day, -2, CURRENT_DATE()), 'Rental Equipment', 'OilField Rentals', 'Workover rig rental', 15000),
        ('CC-DEL-002', 'RIG-9', DATEADD(day, -1, CURRENT_DATE()), 'Maintenance', 'DrillTech', 'Mud motor replacement', 25000),
        ('CC-DEL-002', 'RIG-9', DATEADD(day, -7, CURRENT_DATE()), 'Supplies', 'Mud Systems Inc', 'Drilling fluids', 8000);

    -- Sample Alerts
    INSERT INTO IPA.SCADA_CORE.MISSION_CONTROL_ALERTS (AGENT_TYPE, SEVERITY, ASSET_ID, TITLE, DESCRIPTION, STATUS)
    VALUES
        ('SENTINEL', 'CRITICAL', 'WELL-RP-05', 'Rod Pump Failure Detected', 'Well RP-05 Down. Pattern: Rod Part. History of fluid pound detected in dynamometer cards over previous 24 hours.', 'NEW'),
        ('GUARDIAN', 'CRITICAL', 'SENSOR-ZONE-B', 'H2S Safety Violation', 'CRITICAL: Hot Work active in Zone B with H2S at 15ppm. Permit limit is 10ppm. STOP WORK REQUIRED.', 'NEW'),
        ('SENTINEL', 'WARNING', 'RIG-9', 'Non-Productive Time Detected', 'Rig-9 experienced 6 hours of zero ROP yesterday. Correlates with mud motor failure.', 'ACKNOWLEDGED');

    -- ========================================
    -- TIME-SERIES DATA (13 tags × 14,400 rows = 187,200 rows)
    -- ========================================

    -- Rod Pump Dynamometer Data (with failure pattern)
    INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
    WITH time_series AS (
        SELECT 
            DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts,
            SEQ4() as seq
        FROM TABLE(GENERATOR(ROWCOUNT => 14400))
    )
    SELECT 
        'WELL-RP-05.DYN_LOAD' AS TAG_ID,
        ts AS TIMESTAMP,
        CASE 
            WHEN ts < DATEADD(hour, -36, CURRENT_TIMESTAMP()) 
                THEN 15000 + SIN(seq * 0.1) * 5000 + UNIFORM(-500, 500, RANDOM())
            WHEN ts BETWEEN DATEADD(hour, -36, CURRENT_TIMESTAMP()) AND DATEADD(hour, -8, CURRENT_TIMESTAMP())
                THEN GREATEST(0, 10000 + SIN(seq * 0.1) * 2500 + UNIFORM(-200, 200, RANDOM()))
            ELSE 0
        END AS VALUE,
        192 AS QUALITY
    FROM time_series;

    -- Rod Pump Tubing Pressure
    INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
    WITH time_series AS (
        SELECT DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts, SEQ4() as seq
        FROM TABLE(GENERATOR(ROWCOUNT => 14400))
    )
    SELECT 'WELL-RP-05.TUBING_PRESS', ts, 850 + SIN(seq * 0.05) * 100 + UNIFORM(-25, 25, RANDOM()), 192
    FROM time_series;

    -- Rod Pump Casing Pressure
    INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
    WITH time_series AS (
        SELECT DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts, SEQ4() as seq
        FROM TABLE(GENERATOR(ROWCOUNT => 14400))
    )
    SELECT 'WELL-RP-05.CASING_PRESS', ts, 450 + SIN(seq * 0.03) * 50 + UNIFORM(-15, 15, RANDOM()), 192
    FROM time_series;

    -- H2S Data (with spike in last 2 hours)
    INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
    WITH time_series AS (
        SELECT DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts, SEQ4() as seq
        FROM TABLE(GENERATOR(ROWCOUNT => 14400))
    )
    SELECT 
        'ZONE-B-H2S', ts,
        CASE 
            WHEN ts > DATEADD(hour, -2, CURRENT_TIMESTAMP()) THEN 15 + UNIFORM(-2, 2, RANDOM())
            ELSE UNIFORM(0, 3, RANDOM())
        END,
        192
    FROM time_series;

    -- LEL Percent Data
    INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
    WITH time_series AS (
        SELECT DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts, SEQ4() as seq
        FROM TABLE(GENERATOR(ROWCOUNT => 14400))
    )
    SELECT 
        'ZONE-B-LEL', ts,
        CASE 
            WHEN ts > DATEADD(hour, -2, CURRENT_TIMESTAMP()) THEN 8 + UNIFORM(-1, 1, RANDOM())
            ELSE UNIFORM(0, 2, RANDOM())
        END,
        192
    FROM time_series;

    -- RIG-9 ROP Data (with NPT event)
    INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
    WITH time_series AS (
        SELECT DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts, SEQ4() as seq
        FROM TABLE(GENERATOR(ROWCOUNT => 14400))
    )
    SELECT 
        'RIG-9.ROP', ts,
        CASE 
            WHEN ts BETWEEN DATEADD(hour, -38, CURRENT_TIMESTAMP()) AND DATEADD(hour, -32, CURRENT_TIMESTAMP()) THEN 0
            ELSE 150 + UNIFORM(-20, 20, RANDOM())
        END,
        192
    FROM time_series;

    -- RIG-9 Weight on Bit
    INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
    WITH time_series AS (
        SELECT DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts, SEQ4() as seq
        FROM TABLE(GENERATOR(ROWCOUNT => 14400))
    )
    SELECT 
        'RIG-9.WOB', ts,
        CASE 
            WHEN ts BETWEEN DATEADD(hour, -38, CURRENT_TIMESTAMP()) AND DATEADD(hour, -32, CURRENT_TIMESTAMP()) THEN 0
            ELSE 25 + UNIFORM(-5, 5, RANDOM())
        END,
        192
    FROM time_series;

    -- RIG-9 Hookload
    INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
    WITH time_series AS (
        SELECT DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts, SEQ4() as seq
        FROM TABLE(GENERATOR(ROWCOUNT => 14400))
    )
    SELECT 
        'RIG-9.HOOKLOAD', ts,
        CASE 
            WHEN ts BETWEEN DATEADD(hour, -38, CURRENT_TIMESTAMP()) AND DATEADD(hour, -32, CURRENT_TIMESTAMP()) THEN 180 + UNIFORM(-10, 10, RANDOM())
            ELSE 250 + UNIFORM(-20, 20, RANDOM())
        END,
        192
    FROM time_series;

    -- Gas Lift Well A10 - Gas Injection Rate
    INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
    WITH time_series AS (
        SELECT DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts, SEQ4() as seq
        FROM TABLE(GENERATOR(ROWCOUNT => 14400))
    )
    SELECT 'WELL-A10.GAS_INJ_RATE', ts, 1500 + SIN(seq * 0.02) * 200 + UNIFORM(-50, 50, RANDOM()), 192
    FROM time_series;

    -- Gas Lift Well A10 - Casing Pressure
    INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
    WITH time_series AS (
        SELECT DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts, SEQ4() as seq
        FROM TABLE(GENERATOR(ROWCOUNT => 14400))
    )
    SELECT 'WELL-A10.CASING_PRESS', ts, 1200 + SIN(seq * 0.04) * 100 + UNIFORM(-30, 30, RANDOM()), 192
    FROM time_series;

    -- H2S Data (with spike in last 2 hours) - for HSE Guardian
    INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
    WITH time_series AS (
        SELECT DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts, SEQ4() as seq
        FROM TABLE(GENERATOR(ROWCOUNT => 14400))
    )
    SELECT 
        'ZONE-B-H2S', ts,
        CASE 
            WHEN ts > DATEADD(hour, -2, CURRENT_TIMESTAMP()) THEN 15 + UNIFORM(-2, 2, RANDOM())
            ELSE UNIFORM(0, 3, RANDOM())
        END,
        192
    FROM time_series;

    -- LEL Percent Data - for HSE Guardian
    INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
    WITH time_series AS (
        SELECT DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts, SEQ4() as seq
        FROM TABLE(GENERATOR(ROWCOUNT => 14400))
    )
    SELECT 
        'ZONE-B-LEL', ts,
        CASE 
            WHEN ts > DATEADD(hour, -2, CURRENT_TIMESTAMP()) THEN 8 + UNIFORM(-1, 1, RANDOM())
            ELSE UNIFORM(0, 2, RANDOM())
        END,
        192
    FROM time_series;

    -- RIG-9 ROP Data (with NPT event) - for Production Sentinel
    INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
    WITH time_series AS (
        SELECT DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts, SEQ4() as seq
        FROM TABLE(GENERATOR(ROWCOUNT => 14400))
    )
    SELECT 
        'RIG-9.ROP', ts,
        CASE 
            WHEN ts BETWEEN DATEADD(hour, -38, CURRENT_TIMESTAMP()) AND DATEADD(hour, -32, CURRENT_TIMESTAMP()) THEN 0
            ELSE 150 + UNIFORM(-20, 20, RANDOM())
        END,
        192
    FROM time_series;

    -- RIG-9 Weight on Bit - for Production Sentinel
    INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
    WITH time_series AS (
        SELECT DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts, SEQ4() as seq
        FROM TABLE(GENERATOR(ROWCOUNT => 14400))
    )
    SELECT 
        'RIG-9.WOB', ts,
        CASE 
            WHEN ts BETWEEN DATEADD(hour, -38, CURRENT_TIMESTAMP()) AND DATEADD(hour, -32, CURRENT_TIMESTAMP()) THEN 0
            ELSE 25 + UNIFORM(-5, 5, RANDOM())
        END,
        192
    FROM time_series;

    -- RIG-9 Hookload - for Production Sentinel
    INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
    WITH time_series AS (
        SELECT DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts, SEQ4() as seq
        FROM TABLE(GENERATOR(ROWCOUNT => 14400))
    )
    SELECT 
        'RIG-9.HOOKLOAD', ts,
        CASE 
            WHEN ts BETWEEN DATEADD(hour, -38, CURRENT_TIMESTAMP()) AND DATEADD(hour, -32, CURRENT_TIMESTAMP()) THEN 180 + UNIFORM(-10, 10, RANDOM())
            ELSE 250 + UNIFORM(-20, 20, RANDOM())
        END,
        192
    FROM time_series;

    -- PCP Well B03 - Pump Speed - for Production Sentinel
    INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
    WITH time_series AS (
        SELECT DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts, SEQ4() as seq
        FROM TABLE(GENERATOR(ROWCOUNT => 14400))
    )
    SELECT 'WELL-B03.PUMP_SPEED', ts, 200 + SIN(seq * 0.03) * 30 + UNIFORM(-10, 10, RANDOM()), 192
    FROM time_series;

    -- PCP Well B03 - Intake Pressure - for Production Sentinel
    INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
    WITH time_series AS (
        SELECT DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts, SEQ4() as seq
        FROM TABLE(GENERATOR(ROWCOUNT => 14400))
    )
    SELECT 'WELL-B03.INTAKE_PRESS', ts, 150 + SIN(seq * 0.02) * 25 + UNIFORM(-8, 8, RANDOM()), 192
    FROM time_series;

    -- PCP Well B03 - Torque - for Production Sentinel
    INSERT INTO IPA.SCADA_CORE.TAG_HISTORY (TAG_ID, TIMESTAMP, VALUE, QUALITY)
    WITH time_series AS (
        SELECT DATEADD(minute, -SEQ4(), CURRENT_TIMESTAMP()) AS ts, SEQ4() as seq
        FROM TABLE(GENERATOR(ROWCOUNT => 14400))
    )
    SELECT 'WELL-B03.TORQUE', ts, 800 + SIN(seq * 0.04) * 100 + UNIFORM(-25, 25, RANDOM()), 192
    FROM time_series;

    -- ========================================
    -- KNOWLEDGE BASE DOCUMENTS
    -- ========================================

    -- Signal Documents (contain the "answers")
    INSERT INTO IPA.KNOWLEDGE_BASE.DOCUMENTS_CHUNKED (DOC_ID, CHUNK_ID, TEXT_CHUNK, DOC_TYPE, ASSET_ID, DOC_DATE)
    VALUES
        ('DOC-SIGNAL-001', 'CHUNK-001', 'Field Service Ticket - Apex Chemicals
Date: Yesterday
Asset: Well-A10
Description: Emergency acid stimulation treatment to remove scale buildup.
Cost: $50,000
Note: Treatment approved by Superintendent to restore flow. Scale buildup was restricting production by 40%. Immediate intervention required to prevent complete blockage.', 'Invoice', 'WELL-A10', DATEADD(day, -1, CURRENT_DATE())),
        
        ('DOC-SIGNAL-002', 'CHUNK-002', 'Hot Work Permit HWP-2026-001
Zone: Zone B
Date: Today
Work Type: Welding on flowline repair
Gas Test Requirement: Continuous monitoring
Stop Work Trigger: H2S > 10 ppm
Approver: Offshore Installation Manager
Work Description: Repair corroded section of flowline. Weld joint replacement required.', 'Work Permit', 'SENSOR-ZONE-B', CURRENT_DATE()),
        
        ('DOC-SIGNAL-003', 'CHUNK-003', 'Well Failure Analysis Report
Asset: Well-RP-05
Date: Today
Incident: Unit shut down on low load alarm at 08:15
Diagnosis: Sucker rod parted at approximately 2,000 ft depth
Root Cause: Review of dynamometer cards from previous 24 hours shows severe fluid pound pattern due to gas interference, causing fatigue stress on the rod string. Recommend gas anchor installation upon workover.', 'Tech Note', 'WELL-RP-05', CURRENT_DATE()),
        
        ('DOC-SIGNAL-004', 'CHUNK-004', 'Daily Drilling Report - Rig 9
Date: Yesterday
Time Log:
00:00-10:00: Drilling ahead at 150 ft/hr average
10:00-16:00: Drilling halted - NPT Event
16:00-24:00: Resumed drilling
Activity Description: At 8,500 ft MD, lost pump pressure. POOH (Pull Out Of Hole). Found mud motor stator rubber chunked out. Lay down motor, pick up new BHA. Total NPT: 6 hours.
Code: Unplanned Maintenance / NPT', 'Drilling Report', 'RIG-9', DATEADD(day, -1, CURRENT_DATE()));

    -- Noise Documents (routine logs)
    INSERT INTO IPA.KNOWLEDGE_BASE.DOCUMENTS_CHUNKED (DOC_ID, CHUNK_ID, TEXT_CHUNK, DOC_TYPE, ASSET_ID, DOC_DATE)
    SELECT 
        'DOC-NOISE-' || LPAD(seq::VARCHAR, 3, '0') AS DOC_ID,
        'CHUNK-' || LPAD((seq + 4)::VARCHAR, 3, '0') AS CHUNK_ID,
        CASE MOD(seq, 3)
            WHEN 0 THEN 'Daily Operations Report - ' || well || '
Date: ' || seq || ' days ago
Production steady, no variance from targets.
Routine chemical injection rate verified at standard levels.
All equipment operating within normal parameters.
No safety incidents reported.'
            WHEN 1 THEN 'Routine Maintenance Log - ' || well || '
Date: ' || seq || ' days ago
Preventive maintenance completed on surface equipment.
Choke adjusted to optimize flow.
Pressure readings within acceptable range.
No anomalies detected.'
            ELSE 'Production Summary - ' || well || '
Date: ' || seq || ' days ago
Oil production: On target
Gas production: On target
Water cut: Stable
Operating hours: 24/24
Downtime: None'
        END AS TEXT_CHUNK,
        CASE MOD(seq, 3)
            WHEN 0 THEN 'Operations Report'
            WHEN 1 THEN 'Maintenance Log'
            ELSE 'Production Report'
        END AS DOC_TYPE,
        well AS ASSET_ID,
        DATEADD(day, -MOD(seq, 7) - 1, CURRENT_DATE()) AS DOC_DATE
    FROM (
        SELECT 
            ROW_NUMBER() OVER (ORDER BY 1) as seq,
            wells.well
        FROM TABLE(GENERATOR(ROWCOUNT => 27)) g,
        (SELECT column1 as well FROM VALUES 
            ('WELL-B01'), ('WELL-B02'), ('WELL-B03'), ('WELL-B04'), 
            ('WELL-B05'), ('WELL-B06'), ('WELL-B07'), ('COMP-01')
        ) wells
        LIMIT 27
    );

    -- Get row count
    SELECT COUNT(*) INTO :rows_generated FROM IPA.SCADA_CORE.TAG_HISTORY;

    RETURN 'Demo data generated successfully! ' || rows_generated::VARCHAR || ' time-series rows created with current timestamps.';
END;
$$;

-- Grant execute permissions
GRANT USAGE ON PROCEDURE IPA.APP.GENERATE_DEMO_DATA() TO PUBLIC;

--------------------------------------------------------------------------------
-- STEP 14: DEPLOY STREAMLIT APP FROM GIT
--------------------------------------------------------------------------------

CREATE OR REPLACE STREAMLIT IPA.APP.IPA_DEMO
FROM '@IPA.APP.IPA_REPO/branches/main/streamlit/'
MAIN_FILE = '1_Mission_Control.py'
QUERY_WAREHOUSE = 'IPA_WH'
TITLE = 'Intelligent Production Assistant'
COMMENT = '{"origin":"sf_sit-is", "name":"ipa_oil_gas", "version":{"major":2, "minor":0}, "attributes":{"is_quickstart":1, "source":"streamlit"}}';

-- Add live version
ALTER STREAMLIT IPA.APP.IPA_DEMO ADD LIVE VERSION FROM LAST;

-- Verify Cortex Search is created
SHOW CORTEX SEARCH SERVICES IN SCHEMA IPA.KNOWLEDGE_BASE;

--------------------------------------------------------------------------------
-- SETUP COMPLETE
--------------------------------------------------------------------------------

-- Setup complete
SELECT 'IPA Demo Setup Complete! Go to Projects > Streamlit to find Intelligent Production Assistant app.' as STATUS;

/*******************************************************************************
 * SETUP COMPLETE!
 * 
 * Your IPA demo is ready with:
 * - Database with 187,000+ rows of time-series SCADA data
 * - 3 detection services (Sentinel, Guardian, Fiscal)
 * - Cortex Search with 31 documents for RAG
 * - Cortex Agent (IPA_AGENT) for Q&A
 * - 5-page Streamlit app deployed from Git
 * 
 * Next: Go to Projects > Streamlit to find Intelligent Production Assistant app
 * 
 * To refresh demo data with current timestamps:
 * CALL IPA.APP.GENERATE_DEMO_DATA();
 * 
 * To remove everything: Run scripts/teardown.sql
 ******************************************************************************/
