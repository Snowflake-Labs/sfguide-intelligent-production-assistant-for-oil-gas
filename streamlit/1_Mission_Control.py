import streamlit as st
from datetime import datetime
from snowflake.snowpark.context import get_active_session
import random
import pandas as pd
import plotly.express as px
import time
from theme import apply_dark_theme, show_logo

st.set_page_config(
    page_title="IPA Command Center",
    page_icon=":material/monitor_heart:",
    layout="wide",
    initial_sidebar_state="expanded",
)

apply_dark_theme()
show_logo("🖥️ Mission Control")

st.markdown("*IPA Command Center*")

st.markdown(
    """
    <style>
    /* Mission Control specific styles */
    .metric-card {
        padding: 0.9rem 1rem;
        border-radius: 0.8rem;
        background: linear-gradient(135deg, rgba(17, 86, 127, 0.85), rgba(8, 45, 68, 0.85));
        border: 1px solid rgba(41, 181, 232, 0.35);
        box-shadow: 0 10px 25px rgba(17, 86, 127, 0.9);
    }
    .metric-card-critical {
        border-color: #f97373;
        box-shadow: 0 0 35px rgba(239, 68, 68, 0.4);
    }
    .metric-label {
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #29b5e8;
    }
    .metric-value {
        font-size: 1.6rem;
        font-weight: 600;
        color: #29b5e8;
    }
    .metric-sub {
        font-size: 0.75rem;
        color: rgba(41, 181, 232, 0.8);
    }
    .agent-tile {
        padding: 0.6rem 0.7rem;
        border-radius: 0.6rem;
        display: flex;
        align-items: center;
        justify-content: space-between;
        background: linear-gradient(135deg, rgba(17, 86, 127, 0.85), rgba(8, 45, 68, 0.85));
        border: 1px solid rgba(41, 181, 232, 0.4);
        margin-bottom: 0.3rem;
    }
    .agent-left {
        display: flex;
        flex-direction: column;
        gap: 0.1rem;
    }
    .agent-name {
        font-weight: 600;
        font-size: 0.9rem;
        color: #29b5e8;
    }
    .agent-domain {
        font-size: 0.75rem;
        color: rgba(41, 181, 232, 0.8);
    }
    .agent-badge {
        padding: 0.1rem 0.5rem;
        border-radius: 999px;
        font-size: 0.7rem;
        background: rgba(34, 197, 94, 0.2);
        color: #4ade80;
        border: 1px solid rgba(34, 197, 94, 0.5);
    }
    .hero-critical {
        box-shadow: 0 0 40px rgba(248, 113, 113, 0.75);
        border-color: rgba(248, 113, 113, 0.9) !important;
        animation: pulseGlow 1.5s ease-in-out infinite;
    }
    @keyframes pulseGlow {
        0% { box-shadow: 0 0 10px rgba(248, 113, 113, 0.3); }
        50% { box-shadow: 0 0 40px rgba(248, 113, 113, 0.9); }
        100% { box-shadow: 0 0 10px rgba(248, 113, 113, 0.3); }
    }
    .live-badge {
        display: inline-flex;
        align-items: center;
        padding: 0.25rem 0.6rem;
        background: rgba(239, 68, 68, 0.15);
        border: 1px solid #ef4444;
        border-radius: 999px;
        font-size: 0.7rem;
        font-weight: 600;
        color: #ef4444;
        margin-left: 0.5rem;
    }
    .live-badge::before {
        content: '';
        display: inline-block;
        width: 8px;
        height: 8px;
        background: #ef4444;
        border-radius: 50%;
        margin-right: 0.4rem;
        animation: livePulse 1.5s ease-in-out infinite;
    }
    @keyframes livePulse {
        0%, 100% { opacity: 1; transform: scale(1); }
        50% { opacity: 0.5; transform: scale(1.2); }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

if "last_alert_count" not in st.session_state:
    st.session_state.last_alert_count = 0
if "seen_alert_ids" not in st.session_state:
    st.session_state.seen_alert_ids = set()

session = get_active_session()

def run_query(sql: str) -> pd.DataFrame:
    return session.sql(sql).to_pandas()

ROD_PUMP_WELLS = ["WELL-RP-05", "WELL-B01", "WELL-B04", "WELL-B06"]
GAS_LIFT_WELLS = ["WELL-A10", "WELL-B02", "WELL-B05"]
ALL_WELLS = ROD_PUMP_WELLS + GAS_LIFT_WELLS + ["WELL-B03", "WELL-B07"]
ZONES = ["Zone-A", "Zone-B", "Zone-C", "Zone-D", "Zone-E"]

ROD_PUMP_FAILURES = [
    (
        "Rod Part at {depth} ft",
        "SENTINEL detected a sucker rod part at approximately {depth} ft. "
        "Dynamometer analysis shows {pattern} pattern over previous {hours} hours.",
    ),
    (
        "Pump-Off Condition",
        "SENTINEL reports pump-off condition on {well}. Fluid level dropped below pump intake. "
        "Recommended action: adjust stroke rate.",
    ),
    (
        "Stuck Pump",
        "SENTINEL detected a stuck pump on {well}. High polished rod load and suspected sand accumulation. "
        "Workover recommended.",
    ),
    (
        "Worn Plunger",
        "SENTINEL identified declining efficiency on {well}. Plunger slippage detected; "
        "pump efficiency at {eff}%.",
    ),
    (
        "Gas Lock",
        "SENTINEL detected gas lock on {well}. Erratic dynamometer pattern. "
        "Recommended action: consider gas anchor installation.",
    ),
]

H2S_SCENARIOS = [
    (
        "H2S Spike - {ppm} PPM",
        "GUARDIAN detected H2S reading of {ppm} PPM in {zone}. {permit_status} {action}",
    ),
    (
        "LEL Warning",
        "GUARDIAN reports combustible gas LEL at {lel}% in {zone}, approaching lower explosive limit. "
        "Evacuate non-essential personnel.",
    ),
    (
        "Gas Detector Fault",
        "GUARDIAN reports gas detector malfunction in {zone}. Last reading: {ppm} PPM H2S. "
        "Dispatch technician for calibration check.",
    ),
]

COST_SCENARIOS = [
    (
        "Chemical Cost Spike",
        "FISCAL detected unexpected chemical expense of ${amount:,} on {well}. {reason}. "
        "{variance}% over budget.",
    ),
    (
        "Equipment Rental Overrun",
        "FISCAL reports extended equipment rental on {well}. Additional ${amount:,} charges. "
        "Original PO exceeded by {variance}%.",
    ),
    (
        "Emergency Repair Cost",
        "FISCAL logged emergency repair on {well} totaling ${amount:,}. {reason}. "
        "Unplanned OPEX impact.",
    ),
    (
        "Vendor Invoice Variance",
        "FISCAL flagged vendor {vendor} invoice for ${amount:,}, exceeding estimate by {variance}%. "
        "Review recommended.",
    ),
]

VENDORS = [
    "Apex Chemicals",
    "ChemServ Inc",
    "PumpTech Services",
    "DrillTech",
    "OilField Rentals",
    "Mud Systems Inc",
    "WellServ Corp",
]

def inject_rod_pump_failure():
    well = random.choice(ROD_PUMP_WELLS)
    failure = random.choice(ROD_PUMP_FAILURES)
    title_template, desc_template = failure
    depth = random.randint(1500, 4500)
    pattern = random.choice(["fluid pound", "gas interference", "tubing leak", "rod wear"])
    hours = random.randint(12, 48)
    eff = random.randint(45, 75)

    title = title_template.format(depth=depth, well=well)
    description = desc_template.format(
        well=well, depth=depth, pattern=pattern, hours=hours, eff=eff
    )

    session.sql(
        f"""
        INSERT INTO IPA.SCADA_CORE.MISSION_CONTROL_ALERTS 
        (AGENT_TYPE, SEVERITY, ASSET_ID, TITLE, DESCRIPTION, STATUS)
        VALUES ('SENTINEL', 'CRITICAL', '{well}', '{title}', '{description}', 'NEW')
        """
    ).collect()
    return well, title

def inject_h2s_spike():
    zone = random.choice(ZONES)
    scenario = random.choice(H2S_SCENARIOS)
    title_template, desc_template = scenario
    ppm = random.randint(8, 25)
    lel = random.randint(15, 35)

    permit_status = random.choice(
        [
            "Hot Work Permit active – STOP WORK REQUIRED.",
            "Confined Space Entry in progress – EVACUATE IMMEDIATELY.",
            "No active permits – monitor situation.",
            "Multiple work permits active in zone.",
        ]
    )
    action = random.choice(
        [
            "Initiate emergency response protocol.",
            "Sound area alarm and evacuate.",
            "Deploy portable monitors and reassess.",
            "Contact HSE coordinator immediately.",
        ]
    )

    title = title_template.format(ppm=ppm, zone=zone, lel=lel)
    description = desc_template.format(
        zone=zone, ppm=ppm, lel=lel, permit_status=permit_status, action=action
    )
    sensor_id = f"SENSOR-{zone.upper().replace('-', '')}"

    session.sql(
        f"""
        INSERT INTO IPA.SCADA_CORE.MISSION_CONTROL_ALERTS 
        (AGENT_TYPE, SEVERITY, ASSET_ID, TITLE, DESCRIPTION, STATUS)
        VALUES ('GUARDIAN', 'CRITICAL', '{sensor_id}', '{title}', '{description}', 'NEW')
        """
    ).collect()
    return zone, title

def inject_cost_variance():
    well = random.choice(ALL_WELLS)
    scenario = random.choice(COST_SCENARIOS)
    title_template, desc_template = scenario
    amount = random.randint(15000, 75000)
    variance = random.randint(25, 150)
    vendor = random.choice(VENDORS)
    reason = random.choice(
        [
            "Scale buildup treatment required.",
            "Emergency pump replacement.",
            "Unplanned workover operations.",
            "Corrosion remediation.",
            "Flow assurance intervention.",
        ]
    )

    title = title_template.format(well=well, amount=amount)
    description = desc_template.format(
        well=well, amount=amount, variance=variance, vendor=vendor, reason=reason
    )

    session.sql(
        f"""
        INSERT INTO IPA.SCADA_CORE.MISSION_CONTROL_ALERTS 
        (AGENT_TYPE, SEVERITY, ASSET_ID, TITLE, DESCRIPTION, STATUS)
        VALUES ('FISCAL', 'WARNING', '{well}', '{title}', '{description}', 'NEW')
        """
    ).collect()
    return well, title

with st.sidebar:
    st.title("Detection Services")

    st.markdown("##### Service Status")
    st.markdown(
        """
        <div class="agent-tile">
          <div class="agent-left">
            <div class="agent-name">🛡️ Sentinel</div>
            <div class="agent-domain">Production / Rod Pump Intelligence</div>
          </div>
          <span class="agent-badge">ONLINE</span>
        </div>
        <div class="agent-tile">
          <div class="agent-left">
            <div class="agent-name">🦺 Guardian</div>
            <div class="agent-domain">HSE / Safety & Gas Monitoring</div>
          </div>
          <span class="agent-badge">ONLINE</span>
        </div>
        <div class="agent-tile">
          <div class="agent-left">
            <div class="agent-name">💰 Fiscal</div>
            <div class="agent-domain">Finance / OPEX & Variance</div>
          </div>
          <span class="agent-badge">ONLINE</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.divider()
    st.markdown("##### Run Detection")
    st.caption("Run Cortex-powered anomaly detection.")

    if st.button("🤖 Run All Services", use_container_width=True):
        with st.spinner("Running Sentinel, Guardian, and Fiscal detection..."):
            result = session.sql("CALL IPA.SCADA_CORE.RUN_ALL_AGENTS()").collect()
            st.session_state["last_action"] = ("success", f"Detection Complete: {result[0][0]}")
        st.rerun()

    st.divider()
    st.markdown("##### Scenario Generator")
    st.caption("Trigger simulated events to see how detection services populate the alert fabric.")

    if st.button("🔴 Sentinel – Rod Pump Failure", use_container_width=True):
        well, title = inject_rod_pump_failure()
        # Ensure the transaction is committed
        session.sql("COMMIT").collect()
        st.session_state["last_action"] = (
            "success",
            f"SENTINEL injected a production failure on {well}: {title}",
        )
        st.rerun()

    if st.button("⚠️ Guardian – H₂S Safety Event", use_container_width=True):
        zone, title = inject_h2s_spike()
        # Ensure the transaction is committed
        session.sql("COMMIT").collect()
        st.session_state["last_action"] = (
            "warning",
            f"GUARDIAN injected a safety event in {zone}: {title}",
        )
        st.rerun()

    if st.button("💰 Fiscal – Cost Variance", use_container_width=True):
        well, title = inject_cost_variance()
        # Ensure the transaction is committed
        session.sql("COMMIT").collect()
        st.session_state["last_action"] = (
            "info",
            f"FISCAL injected a cost variance on {well}: {title}",
        )
        st.rerun()

    st.divider()
    if st.button("🗑️ Reset Demo", use_container_width=True):
        session.sql("DELETE FROM IPA.SCADA_CORE.MISSION_CONTROL_ALERTS").collect()
        # Ensure the transaction is committed
        session.sql("COMMIT").collect()
        st.session_state["last_action"] = (
            "info",
            "All demo alerts have been cleared from the shared fabric.",
        )
        st.session_state.seen_alert_ids = set()
        st.session_state.last_alert_count = 0
        st.rerun()

try:
    stats = run_query(
        """
        SELECT 
            COUNT(CASE WHEN SEVERITY = 'CRITICAL' AND STATUS = 'NEW' THEN 1 END) as CRITICAL_NEW,
            COUNT(CASE WHEN SEVERITY = 'WARNING' AND STATUS = 'NEW' THEN 1 END) as WARNING_NEW,
            COUNT(CASE WHEN STATUS = 'NEW' THEN 1 END) as TOTAL_NEW,
            COUNT(CASE WHEN STATUS = 'ACKNOWLEDGED' THEN 1 END) as ACKNOWLEDGED
        FROM IPA.SCADA_CORE.MISSION_CONTROL_ALERTS
    """
    )
    critical_count = int(stats["CRITICAL_NEW"].values[0])
    warning_count = int(stats["WARNING_NEW"].values[0])
    total_new = int(stats["TOTAL_NEW"].values[0])
    acknowledged = int(stats["ACKNOWLEDGED"].values[0])
except Exception:
    critical_count = warning_count = total_new = acknowledged = 0

hero_class = "hero-critical" if critical_count > 0 else ""
st.markdown(
    f"""
    <div style="
        border-radius: 1rem;
        padding: 1rem 1.25rem;
        margin-bottom: 0.75rem;
        border: 1px solid rgba(41, 181, 232, 0.8);
        background: radial-gradient(circle at left, rgba(37, 99, 235, 0.35), transparent 55%),
                    radial-gradient(circle at right, rgba(250, 204, 21, 0.3), transparent 55%),
                    rgba(15,23,42,0.9);
        display: flex;
        align-items: center;
        justify-content: space-between;
    " class="{hero_class}">
      <div>
        <div style="font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.15em; color:#29b5e8;">
          Cortex Production Sentinel
          <span class="live-badge">LIVE</span>
        </div>
        <div style="font-size: 1.5rem; font-weight: 600; margin-top: 0.2rem; color:#e5e7eb;">
          IPA Command Center
        </div>
        <div style="font-size: 0.9rem; margin-top: 0.3rem; color:#29b5e8; max-width: 700px;">
          One shared alert fabric orchestrated by production (🛡️ Sentinel), safety (🦺 Guardian), 
          and financial (💰 Fiscal) detection services – all running inside Snowflake.
        </div>
      </div>
      <div style="text-align:right; font-size:0.8rem; color:#29b5e8;">
        <div>Environment: <b>DEMO</b></div>
        <div>Engine: <b>Cortex AI_COMPLETE + Cortex Agent</b></div>
        <div>Local time: <b>{datetime.now().strftime('%H:%M:%S')}</b></div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

if "last_action" in st.session_state:
    level, msg = st.session_state["last_action"]
    alert_fn = {
        "success": st.success,
        "info": st.info,
        "warning": st.warning,
        "error": st.error,
    }.get(level, st.info)
    alert_fn(msg)
    del st.session_state["last_action"]

if critical_count > 0:
    st.error(f"🚨 {critical_count} critical alert(s) require immediate attention.")
elif warning_count > 0:
    st.warning(f"⚠️ {warning_count} warning(s) need review.")
else:
    st.success("✅ All detection services report normal operating conditions.")

m1, m2, m3, m4 = st.columns(4)
with m1:
    st.markdown(
        f"""
        <div class="metric-card {'metric-card-critical' if critical_count>0 else ''}">
          <div class="metric-label">Critical Alerts</div>
          <div class="metric-value">{critical_count}</div>
          <div class="metric-sub">Production-impacting events</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with m2:
    st.markdown(
        f"""
        <div class="metric-card">
          <div class="metric-label">Warnings</div>
          <div class="metric-value">{warning_count}</div>
          <div class="metric-sub">Degraded conditions</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with m3:
    st.markdown(
        f"""
        <div class="metric-card">
          <div class="metric-label">Active Alerts</div>
          <div class="metric-value">{total_new}</div>
          <div class="metric-sub">Across Sentinel, Guardian, Fiscal</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with m4:
    st.markdown(
        f"""
        <div class="metric-card">
          <div class="metric-label">Acknowledged</div>
          <div class="metric-value">{acknowledged}</div>
          <div class="metric-sub">In triage or remediation</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

alerts_df = run_query(
    """
    SELECT 
        ALERT_ID, CREATED_AT, AGENT_TYPE, SEVERITY, ASSET_ID, TITLE, DESCRIPTION, STATUS
    FROM IPA.SCADA_CORE.MISSION_CONTROL_ALERTS
    ORDER BY 
        CASE WHEN STATUS = 'NEW' THEN 0 ELSE 1 END,
        CASE WHEN SEVERITY = 'CRITICAL' THEN 0 WHEN SEVERITY = 'WARNING' THEN 1 ELSE 2 END,
        CREATED_AT DESC
    LIMIT 50
"""
)

st.markdown("---")

st.markdown("""
<style>
.stTabs [data-baseweb="tab-list"] {
    gap: 8px;
    background: transparent !important;
    padding: 0 !important;
    border: none !important;
    box-shadow: none !important;
}
.stTabs [data-baseweb="tab"] {
    background: linear-gradient(135deg, rgba(17, 86, 127, 0.9), rgba(8, 45, 68, 0.9)) !important;
    border-radius: 8px !important;
    padding: 12px 24px !important;
    border: 1px solid rgba(41, 181, 232, 0.5) !important;
    color: #29b5e8 !important;
    font-weight: 500 !important;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3) !important;
}
.stTabs [data-baseweb="tab"]:hover {
    background: linear-gradient(135deg, rgba(41, 181, 232, 0.3), rgba(17, 86, 127, 0.8)) !important;
    border-color: rgba(41, 181, 232, 0.7) !important;
}
.stTabs [aria-selected="true"] {
    background: linear-gradient(135deg, rgba(41, 181, 232, 0.5), rgba(17, 86, 127, 0.9)) !important;
    border: 1px solid rgba(41, 181, 232, 0.8) !important;
    color: #ffffff !important;
    box-shadow: 0 4px 15px rgba(41, 181, 232, 0.3) !important;
}
</style>
""", unsafe_allow_html=True)

ops_tab, log_tab, analytics_tab = st.tabs(
    ["🛰️ Ops Center", "📟 Event Log", "📈 Insights"]
)

with ops_tab:
    st.subheader("Live Alert Feed")
    
    agent_alerts = run_query(
        """
        SELECT 
            ALERT_ID, CREATED_AT, AGENT_TYPE, SEVERITY, ASSET_ID, TITLE, DESCRIPTION, STATUS
        FROM IPA.SCADA_CORE.MISSION_CONTROL_ALERTS
        WHERE STATUS = 'NEW'
        ORDER BY CREATED_AT DESC
        LIMIT 20
        """
    )

    if agent_alerts.empty:
        st.info("No active alerts. Click **Run All Services** to analyze current conditions or use the scenario generators to inject test events.")
    else:
        for _, row in agent_alerts.iterrows():
            severity = row["SEVERITY"]
            agent = row["AGENT_TYPE"]
            agent_icons = {"SENTINEL": "🛡️", "GUARDIAN": "🦺", "FISCAL": "💰"}
            icon = agent_icons.get(agent, "📋")
            severity_icon = "🔴" if severity == "CRITICAL" else "🟡"

            header = f"{severity_icon} {icon} {row['TITLE']} — {row['ASSET_ID']}"
            with st.expander(header, expanded=(severity == "CRITICAL")):
                st.caption(f"{agent} • {row['CREATED_AT']}")
                st.markdown(row["DESCRIPTION"])
                if st.button("✓ Acknowledge", key=f"ack_{row['ALERT_ID']}"):
                    session.sql(
                        f"""
                        UPDATE IPA.SCADA_CORE.MISSION_CONTROL_ALERTS 
                        SET STATUS = 'ACKNOWLEDGED', ACKNOWLEDGED_AT = CURRENT_TIMESTAMP()
                        WHERE ALERT_ID = {row['ALERT_ID']}
                    """
                    ).collect()
                    st.rerun()
            

with log_tab:
    st.subheader("Unified Event Log")

    filter_col1, filter_col2, filter_col3 = st.columns(3)
    with filter_col1:
        status_filter = st.selectbox("Status", ["All", "NEW", "ACKNOWLEDGED"])
    with filter_col2:
        agent_filter = st.selectbox("Source", ["All", "SENTINEL", "GUARDIAN", "FISCAL"])
    with filter_col3:
        severity_filter = st.selectbox("Severity", ["All", "CRITICAL", "WARNING"])

    filtered_df = alerts_df.copy()
    if status_filter != "All":
        filtered_df = filtered_df[filtered_df["STATUS"] == status_filter]
    if agent_filter != "All":
        filtered_df = filtered_df[filtered_df["AGENT_TYPE"] == agent_filter]
    if severity_filter != "All":
        filtered_df = filtered_df[filtered_df["SEVERITY"] == severity_filter]

    if not filtered_df.empty:
        display_df = filtered_df[
            ["CREATED_AT", "SEVERITY", "AGENT_TYPE", "ASSET_ID", "TITLE", "STATUS"]
        ].copy()
        display_df.columns = ["Time", "Severity", "Source", "Asset", "Title", "Status"]
        st.dataframe(display_df)
    else:
        st.info("No alerts match the selected filters.")

with analytics_tab:
    st.subheader("Insights & Trends")
    
    st.markdown("""
    <p style="color: #e5e7eb; margin-bottom: 1.5rem;">
    Analyze alert patterns and asset health across your operations. These insights help identify 
    recurring issues, prioritize maintenance efforts, and optimize detection performance over time.
    </p>
    """, unsafe_allow_html=True)

    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        st.markdown("""
        <p style="color: #a0aec0; font-size: 0.9rem; margin-bottom: 0.5rem;">
        <strong>Alert Volume Timeline</strong> — Track how alert frequency changes throughout the day. 
        Spikes may indicate equipment issues, shift changes, or environmental factors affecting operations.
        </p>
        """, unsafe_allow_html=True)
        
        timeline = run_query(
            """
            SELECT 
                DATE_TRUNC('hour', CREATED_AT) as HOUR,
                SEVERITY,
                COUNT(*) as COUNT
            FROM IPA.SCADA_CORE.MISSION_CONTROL_ALERTS
            WHERE CREATED_AT > DATEADD(day, -1, CURRENT_TIMESTAMP())
            GROUP BY DATE_TRUNC('hour', CREATED_AT), SEVERITY
            ORDER BY HOUR
        """
        )

        if not timeline.empty:
            fig3 = px.area(
                timeline,
                x="HOUR",
                y="COUNT",
                color="SEVERITY",
                title="Alert Volume (last 24 hours)",
                color_discrete_map={
                    "CRITICAL": "#ef4444",
                    "WARNING": "#f59e0b",
                    "INFO": "#29b5e8",
                },
            )
            fig3.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="#71d3dc",
                xaxis_title="Time",
                yaxis_title="Alerts",
                legend=dict(orientation="h", yanchor="bottom", y=-0.3),
                margin=dict(t=40, b=20, l=20, r=20),
                height=350,
            )
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("No alert timeline data available.")

    with chart_col2:
        st.markdown("""
        <p style="color: #a0aec0; font-size: 0.9rem; margin-bottom: 0.5rem;">
        <strong>Top Assets by Alert Count</strong> — Identify which assets require the most attention. 
        High alert counts may signal aging equipment, calibration issues, or operational stress.
        </p>
        """, unsafe_allow_html=True)
        
        by_asset = run_query(
            """
            SELECT ASSET_ID, COUNT(*) as COUNT
            FROM IPA.SCADA_CORE.MISSION_CONTROL_ALERTS
            WHERE STATUS = 'NEW'
            GROUP BY ASSET_ID
            ORDER BY COUNT DESC
            LIMIT 8
        """
        )

        if not by_asset.empty:
            fig4 = px.bar(
                by_asset,
                x="COUNT",
                y="ASSET_ID",
                orientation="h",
                title="Top Assets by Active Alert Count",
                color="COUNT",
                color_continuous_scale="Blues",
            )
            fig4.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="#71d3dc",
                xaxis_title="Alert Count",
                yaxis_title="",
                showlegend=False,
                margin=dict(t=40, b=20, l=20, r=20),
                height=350,
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig4, use_container_width=True)
        else:
            st.info("No active asset-level alert data available.")

    with st.expander("🏭 Asset Health Matrix"):
        st.markdown("""
        <p style="color: #a0aec0; font-size: 0.9rem; margin-bottom: 0.5rem;">
        A comprehensive view of all active assets with their current health status. Assets are ranked by 
        severity—critical issues first—to help operators quickly identify and address problem areas.
        </p>
        """, unsafe_allow_html=True)
        
        asset_status = run_query(
            """
            SELECT 
                a.ASSET_ID,
                a.ASSET_TYPE,
                a.GEO_ZONE,
                COALESCE(SUM(CASE WHEN m.SEVERITY = 'CRITICAL' AND m.STATUS = 'NEW' THEN 1 ELSE 0 END), 0) as CRITICAL,
                COALESCE(SUM(CASE WHEN m.SEVERITY = 'WARNING' AND m.STATUS = 'NEW' THEN 1 ELSE 0 END), 0) as WARNINGS
            FROM IPA.SCADA_CORE.ASSET_MASTER a
            LEFT JOIN IPA.SCADA_CORE.MISSION_CONTROL_ALERTS m 
                ON a.ASSET_ID = m.ASSET_ID
            WHERE a.STATUS = 'ACTIVE'
            GROUP BY a.ASSET_ID, a.ASSET_TYPE, a.GEO_ZONE
            ORDER BY CRITICAL DESC, WARNINGS DESC
        """
        )

        if not asset_status.empty:
            def get_status(row):
                if row["CRITICAL"] > 0:
                    return "🔴 Critical"
                elif row["WARNINGS"] > 0:
                    return "🟡 Warning"
                return "🟢 OK"

            asset_status["Health"] = asset_status.apply(get_status, axis=1)
            st.dataframe(
                asset_status[
                    ["ASSET_ID", "ASSET_TYPE", "GEO_ZONE", "Health", "CRITICAL", "WARNINGS"]
                ]
            )
        else:
            st.info("No asset health data available (check ASSET_MASTER source).")

st.caption(
    f"Last updated: {datetime.now().strftime('%H:%M:%S')} • Streamlit in Snowflake • "
    "Powered by Snowflake Cortex"
)
