import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
from theme import apply_dark_theme, show_logo

st.set_page_config(page_title="IPA - HSE Guardian", page_icon="🦺", layout="wide")

apply_dark_theme()
show_logo("🦺 HSE Guardian")

session = get_active_session()

def run_query(sql):
    return session.sql(sql).to_pandas()

st.markdown("*Safety Barrier Management & Compliance Monitoring*")

violations = run_query("""
    SELECT 
        p.PERMIT_ID,
        p.ZONE,
        p.WORK_TYPE,
        p.MAX_H2S_LIMIT,
        h.VALUE as CURRENT_H2S,
        h.TIMESTAMP as READING_TIME,
        CASE WHEN h.VALUE > p.MAX_H2S_LIMIT THEN 'VIOLATION' ELSE 'SAFE' END as STATUS
    FROM IPA.SCADA_CORE.ACTIVE_PERMITS p
    JOIN IPA.SCADA_CORE.ASSET_MASTER a ON a.GEO_ZONE = p.ZONE
    JOIN IPA.SCADA_CORE.TAG_REGISTRY t ON t.ASSET_ID = a.ASSET_ID AND t.ATTRIBUTE_NAME = 'H2S_Concentration'
    JOIN (
        SELECT TAG_ID, VALUE, TIMESTAMP
        FROM IPA.SCADA_CORE.TAG_HISTORY
        WHERE TIMESTAMP = (SELECT MAX(TIMESTAMP) FROM IPA.SCADA_CORE.TAG_HISTORY WHERE TAG_ID = 'ZONE-B-H2S')
        AND TAG_ID = 'ZONE-B-H2S'
    ) h ON h.TAG_ID = t.TAG_ID
    WHERE p.STATUS = 'ACTIVE'
    AND p.WORK_TYPE IN ('Welding', 'Hot Work', 'Cutting')
""")

col1, col2, col3 = st.columns(3)

violation_count = len(violations[violations['STATUS'] == 'VIOLATION']) if not violations.empty else 0

permits_df = run_query("SELECT COUNT(*) as CNT FROM IPA.SCADA_CORE.ACTIVE_PERMITS WHERE STATUS = 'ACTIVE'")
active_permits = int(permits_df['CNT'].values[0])

with col1:
    if violation_count > 0:
        st.error(f"🚨 **{violation_count} ACTIVE VIOLATION(S)**")
    else:
        st.success("✅ All Zones Safe")

with col2:
    st.metric("Active Permits", active_permits)

with col3:
    st.metric("Monitored Zones", "5")

if violation_count > 0:
    st.divider()
    st.subheader("🚨 CRITICAL SAFETY VIOLATIONS")
    
    violation_rows = violations[violations['STATUS'] == 'VIOLATION']
    for i in range(len(violation_rows)):
        row = violation_rows.iloc[i]
        permit_id = str(row['PERMIT_ID'])
        zone = str(row['ZONE'])
        work_type = str(row['WORK_TYPE'])
        h2s_limit = float(row['MAX_H2S_LIMIT'])
        current_h2s = float(row['CURRENT_H2S'])
        reading_time = str(row['READING_TIME'])
        
        st.error(f"""
        **⛔ STOP WORK REQUIRED - {zone}**
        
        | Parameter | Value |
        |-----------|-------|
        | Permit | {permit_id} |
        | Work Type | {work_type} |
        | H2S Limit | {h2s_limit} ppm |
        | **Current H2S** | **{current_h2s:.1f} ppm** |
        | Reading Time | {reading_time} |
        
        **Action Required:** Immediately cease all hot work operations in {zone}. 
        Evacuate non-essential personnel. Verify gas detection equipment calibration.
        """)

st.divider()

st.subheader("📊 Real-Time Gas Monitoring")

h2s_history = run_query("""
    SELECT TIMESTAMP, VALUE
    FROM IPA.SCADA_CORE.TAG_HISTORY
    WHERE TAG_ID = 'ZONE-B-H2S'
    AND TIMESTAMP > DATEADD(hour, -6, CURRENT_TIMESTAMP())
    ORDER BY TIMESTAMP
""")

if not h2s_history.empty:
    st.line_chart(h2s_history.set_index('TIMESTAMP')['VALUE'])
    
    col1, col2 = st.columns(2)
    with col1:
        latest_h2s = float(h2s_history['VALUE'].values[-1])
        st.metric("Current H2S (Zone B)", f"{latest_h2s:.1f} ppm", 
                  delta="DANGER" if latest_h2s > 10 else "Safe",
                  delta_color="inverse" if latest_h2s > 10 else "normal")
    with col2:
        st.markdown("""
        **Thresholds:**
        - 🟠 Hot Work Permit Limit: 10 ppm
        - 🔴 IDLH Limit: 20 ppm
        """)

st.divider()

col1, col2 = st.columns(2)

with col1:
    st.subheader("📋 Active Work Permits")
    permits = run_query("""
        SELECT 
            PERMIT_ID,
            PERMIT_TYPE,
            ZONE,
            WORK_TYPE,
            MAX_H2S_LIMIT,
            VALID_TO,
            STATUS
        FROM IPA.SCADA_CORE.ACTIVE_PERMITS
        WHERE STATUS = 'ACTIVE'
        ORDER BY VALID_TO
    """)
    
    if not permits.empty:
        for i in range(len(permits)):
            row = permits.iloc[i]
            permit_id = str(row['PERMIT_ID'])
            zone = str(row['ZONE'])
            work_type = str(row['WORK_TYPE'])
            permit_type = str(row['PERMIT_TYPE'])
            h2s_limit = float(row['MAX_H2S_LIMIT'])
            valid_to = str(row['VALID_TO'])
            
            zone_has_violation = not violations.empty and len(violations[(violations['ZONE'] == zone) & (violations['STATUS'] == 'VIOLATION')]) > 0
            
            icon = "🔴" if zone_has_violation else "🟢"
            with st.expander(f"{icon} {permit_id} - {work_type}"):
                st.markdown(f"""
                - **Zone:** {zone}
                - **Type:** {permit_type}
                - **H2S Limit:** {h2s_limit} ppm
                - **Valid Until:** {valid_to}
                """)
                if zone_has_violation:
                    st.error("⚠️ PERMIT CONSTRAINT VIOLATED")

with col2:
    st.subheader("🗺️ Zone Status")
    
    zones = ['Zone-A', 'Zone-B', 'Zone-C', 'Zone-D', 'Zone-E']
    zone_data = []
    
    for zone in zones:
        has_violation = not violations.empty and len(violations[(violations['ZONE'] == zone) & (violations['STATUS'] == 'VIOLATION')]) > 0
        zone_data.append({
            'Zone': zone,
            'Status': '🔴 VIOLATION' if has_violation else '🟢 SAFE',
        })
    
    zone_df = pd.DataFrame(zone_data)
    st.dataframe(zone_df, use_container_width=True)

st.divider()

st.subheader("📜 Guardian Alert History")
guardian_alerts = run_query("""
    SELECT 
        CREATED_AT,
        SEVERITY,
        ASSET_ID,
        TITLE,
        DESCRIPTION,
        STATUS
    FROM IPA.SCADA_CORE.MISSION_CONTROL_ALERTS
    WHERE AGENT_TYPE = 'GUARDIAN'
    ORDER BY CREATED_AT DESC
    LIMIT 10
""")

if not guardian_alerts.empty:
    st.dataframe(guardian_alerts, use_container_width=True)
else:
    st.info("No HSE alerts in history.")
