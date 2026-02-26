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
 * IPA DEMO - TEARDOWN SCRIPT
 * 
 * Purpose: Completely removes the Intelligent Production Assistant demo
 * 
 * Usage:
 *   1. Run this entire script in Snowsight to remove all demo objects
 * 
 * WARNING: This will permanently delete:
 *   - All tables and 187,000+ rows of data
 *   - All stored procedures
 *   - Cortex Search service
 *   - Cortex Agent
 *   - Streamlit application
 *   - Git integration
 *   - Entire IPA database
 *   - IPA_WH warehouse
 *   - IPA_DEMO_ROLE role
 * 
 ******************************************************************************/

USE ROLE ACCOUNTADMIN;

--------------------------------------------------------------------------------
-- DELETE IN REVERSE ORDER (dependencies first)
-- Using ACCOUNTADMIN throughout so teardown works even if IPA_DEMO_ROLE is missing
--------------------------------------------------------------------------------

-- Drop database and everything inside it (tables, stages, agents, etc.)
DROP DATABASE IF EXISTS IPA CASCADE;

-- Drop warehouse
DROP WAREHOUSE IF EXISTS IPA_WH;

-- Drop API integration
DROP API INTEGRATION IF EXISTS IPA_GIT_API_INTEGRATION;

-- Drop demo role
DROP ROLE IF EXISTS IPA_DEMO_ROLE;

--------------------------------------------------------------------------------
-- TEARDOWN COMPLETE
--------------------------------------------------------------------------------

SELECT '✅ Teardown Complete - All IPA demo objects have been removed' as STATUS;

/*******************************************************************************
 * ✅ TEARDOWN COMPLETE
 * 
 * All IPA demo objects removed successfully.
 * 
 * To reinstall: Run scripts/setup.sql
 ******************************************************************************/
