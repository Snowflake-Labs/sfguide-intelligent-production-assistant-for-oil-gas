# Intelligent Production Assistant (IPA) - Oil & Gas Operational Intelligence

> An AI-powered operational intelligence platform for oil & gas production, built entirely in Snowflake. This solution combines Cortex AI, Cortex Analyst, and Cortex Search to deliver autonomous monitoring of production assets, real-time safety compliance, and proactive cost optimization. From detecting rod pump failures to enforcing H2S permit limits, IPA transforms raw SCADA data into actionable insights through natural language conversations.

---

## What You Build

This demo creates a complete operational intelligence platform with:

- **5-Page Streamlit Application:** Mission Control dashboard, asset monitoring, safety inspection, and natural language Q&A
- **3 Detection Services:** Sentinel (production), Guardian (safety), Fiscal (costs) - Python stored procedures using AI_COMPLETE
- **1 Cortex Agent (IPA_AGENT):** True Cortex Agent combining text-to-SQL (Cortex Analyst) + document search (Cortex Search) 
- **Document Search Service:** Cortex Search over technical documents for RAG capabilities
- **Complete Data Pipeline:** SCADA sensor data, work permits, financial records, and alert management

---

## What You Will Learn

By implementing this demo, you'll gain hands-on experience with:

1. **AI Complete:** Generate AI insights using `AI_COMPLETE()` in Python stored procedures
2. **Cortex Analyst:** Build semantic models for natural language to SQL conversion
3. **Cortex Search:** Create vector search services for document retrieval (RAG)
4. **Cortex Agents:** Combine multiple AI tools (Analyst + Search) into unified agents
6. **Streamlit in Snowflake:** Deploy multi-page applications with Git integration
7. **Python Stored Procedures:** Automate workflows using Snowpark Python
8. **Time-Series Analysis:** Process and analyze industrial sensor data
9. **Alert Management:** Implement deduplication, severity classification, and workflow automation
10. **Multimodal AI:** Process both text and images using `AI_COMPLETE()` with vision models

---

## Prerequisites

### Snowflake Account Requirements
- **Snowflake Account:** [Free trial account](https://signup.snowflake.com/) or existing account
- **Role:** ACCOUNTADMIN (for initial setup only - creates IPA_DEMO_ROLE for ongoing usage)

---

## Quick Start

### Step 1: Run Setup SQL

1. Open Snowsight SQL Worksheet
2. Copy and paste **entire contents** of [`scripts/setup.sql`](https://github.com/Snowflake-Labs/sfguide-oil-gas-operational-intelligence/blob/main/scripts/setup.sql)
3. Run the script as **ACCOUNTADMIN** 
   - Creates database with sample SCADA data
   - Creates **IPA_DEMO_ROLE** with proper access controls
   - Deploys Streamlit app from Git
   - Creates and registers Cortex Agent

### Step 2: Access the Application

1. In Snowsight, navigate to **Projects** → **Streamlit**
2. Click on **Intelligent Production Assistant** to open the application
3. Start exploring the 5-page dashboard!

*Note: After setup, users with **IPA_DEMO_ROLE** can access all demo features without requiring ACCOUNTADMIN privileges.*

### Step 3: Access Snowflake Intelligence

Once deployed, you can also interact with the IPA_AGENT through Snowflake Intelligence:

1. Navigate to **Snowsight** → **AI & ML** → **Snowflake Intelligence**
2. Look for **IPA_AGENT** in your available agents
3. Ask questions like:
   - "What are the current critical alerts?"
   - "Show me H2S readings in Zone-B"
   - "Find documents about Well-RP-05 failures"
4. The agent combines SQL queries (via Cortex Analyst) with document search (via Cortex Search)

---

## Technical Implementation

The setup script automatically creates:

- **Database:** IPA with 3 schemas (SCADA_CORE, KNOWLEDGE_BASE, APP)
- **Tables:** Asset master, tags, time-series data, documents, alerts, permits, financials
- **Detection Services:** Sentinel, Guardian, Fiscal (Python stored procedures)
- **Cortex Search:** Document retrieval service for RAG
- **Cortex Agent:** IPA_AGENT (true Cortex Agent with text-to-SQL + document search)
- **Streamlit App:** 5-page dashboard deployed from Git
- **Snowflake Intelligence:** Agent registered for natural language queries

---

## 📱 Application Pages

### 1. 🖥️ Mission Control
**Purpose:** Unified command center showing alerts from all 3 detection services

**Features:**
- Real-time alert dashboard with critical/warning/info counts
- Scenario generators for demo storytelling (inject synthetic failures)
- "Run All Services" button to trigger Sentinel, Guardian, and Fiscal scans
- Live alert feed with acknowledge workflow

**Demo Flow:**
1. Click "🤖 Run All Services" to scan current conditions
2. Or click scenario buttons to inject test events:
   - "🔴 Sentinel – Rod Pump Failure"
   - "⚠️ Guardian – H₂S Safety Event"
   - "💰 Fiscal – Cost Variance"
3. Review alerts in live feed, acknowledge critical items

---

### 2. 🛡️ Production Sentinel
**Purpose:** Asset-specific diagnostics for rod pumps and drilling rigs

**Features:**
- Asset selector with 4 key assets (RIG-9, WELL-A10, WELL-B03, WELL-RP-05)
- Rod Pump: Dynamometer chart shows failure patterns (fluid pound → rod part)
- Drilling Rig: ROP (Rate of Penetration) analysis detects NPT (Non-Productive Time)
- Time range selector (24h, 48h, 7 days)
- Active alert count per asset

**Demo Flow:**
1. Select "WELL-RP-05" → See rod pump failure pattern (load drops to zero)
2. Select "RIG-9" → See 6-hour NPT event (zero ROP) with business impact calculation
3. Click "Run All Services" on Mission Control → Sentinel generates alerts for these anomalies

---

### 3. 🦺 HSE Guardian
**Purpose:** Safety monitoring with permit-gas correlation

**Features:**
- Real-time H2S gas monitoring
- Active work permit tracking with safety constraints
- Permit violation detection (e.g., Hot Work Permit allows max 10 ppm H2S)
- Zone status map showing geographic zones

**Demo Flow:**
1. Observe H2S spike > 15 ppm in Zone-B (violates Hot Work Permit HWP-2026-001)
2. See "STOP WORK REQUIRED" alert
3. Review permit details and corrective actions
4. Check Guardian alert history

**Value Prop:** Prevents fatal H2S incidents through real-time safety barrier monitoring

---

### 4. 👁️ Visual Inspector
**Purpose:** AI-powered safety violation detection from worksite images

**Features:**
- **Image Analysis Tab:** Select images from Snowflake stage, analyze with `AI_COMPLETE(claude-3-5-sonnet)`
- **Upload Image Tab:** Upload new images to stage for immediate analysis
- Structured JSON output: violation type, severity, corrective actions
- Auto-generates alerts for CRITICAL/STOP_WORK findings
- Zone and location tagging for each inspection

**Demo Flow:**
1. Select zone location (e.g., Zone-A, Zone-B)
2. Choose an image from the stage (pre-loaded safety violation images)
3. Click "🔍 Analyze with Cortex AI"
4. Review AI-generated safety analysis with severity classification
5. View violation details, corrective actions, and safe observations
6. Critical findings automatically create alerts in Mission Control

---

### 5. 🤖 Cortex Strategist
**Purpose:** Natural language Q&A powered by Cortex Agent

**Features:**
- Ask questions in plain English
- IPA_AGENT combines text-to-SQL (Cortex Analyst) + document search (Cortex Search)
- Suggested questions organized by category:
  - Root Cause Analysis
  - Safety & Compliance  
  - Operations
- Real-time response with SQL results and document citations

**Demo Questions:**
- "What caused the rod pump failure on Well-RP-05?"
- "Are there any active safety violations in Zone B?"
- "How many critical alerts are still unacknowledged?"
- "Why is NPT high on Rig 9?"

**Value Prop:** Operators get instant answers without writing SQL or manually searching docs

---

## Demo Scenarios

### Scenario 1: Rod Pump Failure (Sentinel)
**Story:** Well-RP-05 experienced a rod part due to gas interference

**Demo Steps:**
1. Go to **Mission Control** → Click "🔴 Sentinel – Rod Pump Failure"
2. Alert appears: "Rod Part at 2,000 ft - Dynamometer analysis shows fluid pound pattern"
3. Go to **Production Sentinel** → Select "WELL-RP-05"
4. Chart shows load dropping from 15,000 lbs to zero (rod failure signature)
5. Go to **Cortex Strategist** → Ask: "Why did Well-RP-05 fail? Search technical documents"
6. AI finds document DOC-SIGNAL-003 with root cause analysis: "fluid pound due to gas interference"

**Business Impact:** Avoided workover cost: $30,000 + 3 days lost production

---

### Scenario 2: H2S Safety Violation (Guardian)
**Story:** Hot work ongoing in Zone-B while H2S exceeds permit limit

**Demo Steps:**
1. Go to **HSE Guardian** page
2. See real-time H2S chart: spike from 3 ppm to 15 ppm in last 2 hours
3. Active permit HWP-2026-001 allows max 10 ppm for welding operations
4. Red alert: "⛔ STOP WORK REQUIRED - Zone-B"
5. Go to **Mission Control** → Click "⚠️ Guardian – H₂S Safety Event"
6. New alert generated: "GUARDIAN: H2S at 15 ppm - STOP WORK"
7. Go to **Cortex Strategist** → Ask: "Are there any active permits with gas violations?"
8. AI queries structured data + searches permit documents to confirm violation

**Business Impact:** Prevented fatal H2S incident (H2S is lethal at 100+ ppm)

---

### Scenario 3: Cost Variance (Fiscal)
**Story:** Well-A10 had unexpected $50k chemical treatment

**Demo Steps:**
1. Go to **Mission Control** → Click "💰 Fiscal – Cost Variance"
2. Alert: "Chemical Cost Spike: $50,000 on Well-A10"
3. Go to **Cortex Strategist** → Ask: "What was the $50,000 charge on Well-A10?"
4. AI runs SQL: finds emergency acid stimulation treatment
5. Ask follow-up: "Find documents about Well-A10 scale treatment"
6. AI retrieves DOC-SIGNAL-001 (Apex Chemicals invoice) explaining root cause: "Scale buildup restricting production by 40%"

**Business Impact:** Treatment prevented complete well blockage (potential $2M workover)

---

## Repository Structure

```
sfguide-oil-gas-operational-intelligence/
├── streamlit/
│   ├── 1_Mission_Control.py   # Main dashboard with alert feed
│   ├── pages/
│   │   ├── 2_Production_Sentinel.py
│   │   ├── 3_HSE_Guardian.py
│   │   ├── 4_Visual_Inspector.py
│   │   └── 5_Cortex_Strategist.py
│   ├── theme.py               # Unified dark theme (oil & gas aesthetic)
│   ├── images/                # Logos and icons
│   ├── environment.yml        # Python dependencies
│   └── requirements.txt
├── scripts/
│   ├── setup.sql              # Complete deployment (one script, all-in-one)
│   ├── teardown.sql           # Clean removal of all objects
│   └── semantic_model/
│       └── ipa_semantic_model.yaml  # Cortex Analyst semantic model
├── README.md
└── LICENSE
```

---

## Congratulations!

You've successfully built a complete AI-powered operational intelligence platform! 

### What You've Accomplished

- **Deployed 5 AI-powered applications** - Real-time dashboards and natural language Q&A  
- **Integrated Cortex services** - AI_COMPLETE, Analyst, Search, and Agents working in harmony  
- **Processed sensor readings** - Real-world scale time-series data pipeline  
- **Created autonomous monitoring** - 3 detection services (Sentinel, Guardian, Fiscal) detecting failures before they become critical  
- **Built conversational AI** - Natural language interface to your industrial data  

### Business Impact Delivered

- 🛡️ **Safety:** Prevented H2S incidents through real-time permit compliance monitoring
- ⚡ **Reliability:** Detected rod pump failures 3 days before complete breakdown
- 💰 **Cost Control:** Automated detection for $50k+ expense spikes  
- 🎯 **Efficiency:** Reduced NPT (Non-Productive Time) through ROP analysis

---

## License

Copyright (c) Snowflake Inc. All rights reserved.

The code in this repository is licensed under the [Apache 2.0 License](LICENSE).
