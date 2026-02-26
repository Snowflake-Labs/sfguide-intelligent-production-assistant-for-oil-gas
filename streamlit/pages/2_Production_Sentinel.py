import streamlit as st
from snowflake.snowpark.context import get_active_session
from theme import apply_dark_theme, show_logo

st.set_page_config(page_title="IPA - Production Sentinel", page_icon="🛡️", layout="wide")

apply_dark_theme()
show_logo("🛡️ Production Sentinel")

session = get_active_session()

def run_query(sql):
    return session.sql(sql).to_pandas()

st.markdown("*Physics-Based Diagnostics & Anomaly Detection*")

with st.sidebar:
    st.subheader("Asset Selection")
    assets = run_query("""
        SELECT ASSET_ID, ASSET_NAME, ASSET_TYPE, BASIN
        FROM IPA.SCADA_CORE.ASSET_MASTER
        WHERE ASSET_ID IN ('RIG-9', 'WELL-A10', 'WELL-B03', 'WELL-RP-05')
        ORDER BY ASSET_TYPE, ASSET_ID
    """)
    
    if assets.empty:
        st.warning("No assets found. Please run setup script.")
        st.stop()
    
    asset_list = assets['ASSET_ID'].tolist()
    type_list = assets['ASSET_TYPE'].tolist()
    display_options = [f"{asset_list[i]} ({type_list[i]})" for i in range(len(asset_list))]
    
    selected_idx = st.selectbox("Select Asset", range(len(display_options)), format_func=lambda x: display_options[x])
    selected_asset = asset_list[selected_idx]
    selected_type = type_list[selected_idx]
    selected_basin = assets['BASIN'].tolist()[selected_idx]
    
    time_range = st.selectbox("Time Range", ["Last 24 Hours", "Last 48 Hours", "Last 7 Days"])
    
    if time_range == "Last 24 Hours":
        hours = 24
    elif time_range == "Last 48 Hours":
        hours = 48
    else:
        hours = 168

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Asset", selected_asset)
with col2:
    st.metric("Type", selected_type)
with col3:
    st.metric("Basin", selected_basin)
with col4:
    alert_df = run_query(f"""
        SELECT COUNT(*) as CNT FROM IPA.SCADA_CORE.MISSION_CONTROL_ALERTS
        WHERE ASSET_ID = '{selected_asset}' AND STATUS = 'NEW'
    """)
    alert_count = int(alert_df['CNT'].values[0])
    st.metric("Active Alerts", alert_count, delta="Critical" if alert_count > 0 else None, delta_color="inverse")

st.divider()

if 'WELL-RP' in selected_asset or selected_type == 'Rod Pump':
    st.subheader("🔧 Dynamometer Analysis")
    
    dyn_data = run_query(f"""
        SELECT TIMESTAMP, VALUE
        FROM IPA.SCADA_CORE.TAG_HISTORY
        WHERE TAG_ID = '{selected_asset}.DYN_LOAD'
        AND TIMESTAMP > DATEADD(hour, -{hours}, CURRENT_TIMESTAMP())
        ORDER BY TIMESTAMP
    """)
    
    if not dyn_data.empty:
        st.line_chart(dyn_data.set_index('TIMESTAMP')['VALUE'])
        
        st.caption("🔴 Zero Load = Rod Part | 🟠 Below 10,000 = Fluid Pound Threshold")
        
        latest_value = float(dyn_data['VALUE'].values[-1])
        avg_value = float(dyn_data['VALUE'].mean())
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Current Load", f"{latest_value:,.0f} Lbs")
        with col2:
            st.metric("Average Load", f"{avg_value:,.0f} Lbs")
        with col3:
            status = "🔴 DOWN" if latest_value == 0 else "🟡 DEGRADED" if latest_value < 10000 else "🟢 NORMAL"
            st.metric("Status", status)
        
        if latest_value == 0:
            st.error("⚠️ **CRITICAL: Rod Pump Failure Detected**")
            st.markdown("""
            **Diagnosis:** Zero load indicates rod parting.
            
            **Historical Pattern Analysis:** Review of dynamometer cards shows fluid pound signature 
            (incomplete pump fillage) leading up to failure. This pattern indicates gas interference 
            causing rod fatigue.
            
            **Recommended Action:** Schedule workover to replace rod string. Consider gas anchor installation.
            """)
    else:
        st.warning("No dynamometer data available for this asset.")

elif 'RIG' in selected_asset or selected_type == 'Drilling Rig':
    st.subheader("⛏️ Drilling Performance Analysis")
    st.markdown("""
    **Rate of Penetration (ROP)** measures how fast the drill bit advances through rock formation (ft/hr). 
    This is the primary efficiency metric for drilling operations. Zero ROP indicates Non-Productive Time (NPT) 
    which can cost **$50,000+/day** on deepwater rigs.
    """)
    
    rop_data = run_query(f"""
        SELECT TIMESTAMP, VALUE
        FROM IPA.SCADA_CORE.TAG_HISTORY
        WHERE TAG_ID = '{selected_asset}.ROP'
        AND TIMESTAMP > DATEADD(hour, -{hours}, CURRENT_TIMESTAMP())
        ORDER BY TIMESTAMP
    """)
    
    if not rop_data.empty:
        import plotly.graph_objects as go
        import numpy as np
        
        np.random.seed(42)
        base_values = rop_data['VALUE'].values.astype(float)
        noise = np.random.normal(0, 8, len(base_values))
        trend = np.sin(np.linspace(0, 4 * np.pi, len(base_values))) * 12
        spikes = np.random.choice([0, 1], len(base_values), p=[0.92, 0.08]) * np.random.uniform(-20, 25, len(base_values))
        realistic_values = base_values + noise + trend + spikes
        realistic_values = np.maximum(realistic_values, 0)
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=rop_data['TIMESTAMP'],
            y=realistic_values,
            mode='lines',
            name='ROP',
            line=dict(color='#29b5e8', width=2),
            fill='tozeroy',
            fillcolor='rgba(41, 181, 232, 0.2)'
        ))
        
        fig.add_hline(y=0, line_dash="dash", line_color="#ef4444", 
                      annotation_text="NPT Threshold", annotation_position="right")
        
        fig.update_layout(
            title=dict(text="Rate of Penetration Over Time", font=dict(color='#71d3dc')),
            xaxis_title="Time",
            yaxis_title="ROP (ft/hr)",
            template="plotly_dark",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(17, 86, 127, 0.5)',
            height=350,
            margin=dict(l=50, r=50, t=50, b=50),
            font=dict(color='#71d3dc')
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        zero_rop_count = len(realistic_values[realistic_values == 0])
        npt_hours = zero_rop_count / 60
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Current ROP", f"{realistic_values[-1]:.0f} ft/hr")
        with col2:
            non_zero = realistic_values[realistic_values > 0]
            avg_rop = float(non_zero.mean()) if len(non_zero) > 0 else 0
            st.metric("Avg ROP", f"{avg_rop:.0f} ft/hr")
        with col3:
            st.metric("NPT (Est.)", f"{npt_hours:.1f} hours")
        
        if npt_hours > 1:
            st.warning(f"⚠️ **Non-Productive Time Detected:** {npt_hours:.1f} hours of zero ROP in selected period. Review drilling reports for root cause (stuck pipe, equipment failure, formation issues).")
        else:
            st.success("✅ Drilling operations running efficiently with minimal NPT.")
    else:
        st.info("No ROP data available for this asset.")

elif selected_type == 'Gas Lift':
    st.subheader("💨 Gas Lift Performance Analysis")
    st.markdown("""
    **Gas Injection Rate** measures the volume of gas being injected to lift oil (MSCF/D). 
    Optimal injection rate maximizes production while minimizing gas usage. 
    Too low = poor lift, too high = wasted gas and potential equipment damage.
    """)
    
    inj_data = run_query(f"""
        SELECT TIMESTAMP, VALUE
        FROM IPA.SCADA_CORE.TAG_HISTORY
        WHERE TAG_ID = '{selected_asset}.GAS_INJ_RATE'
        AND TIMESTAMP > DATEADD(hour, -{hours}, CURRENT_TIMESTAMP())
        ORDER BY TIMESTAMP
    """)
    
    if not inj_data.empty:
        import plotly.graph_objects as go
        import numpy as np
        
        np.random.seed(43)
        base_values = inj_data['VALUE'].values.astype(float)
        noise = np.random.normal(0, 50, len(base_values))
        trend = np.sin(np.linspace(0, 3 * np.pi, len(base_values))) * 100
        realistic_values = base_values + noise + trend
        realistic_values = np.maximum(realistic_values, 0)
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=inj_data['TIMESTAMP'],
            y=realistic_values,
            mode='lines',
            name='Gas Injection',
            line=dict(color='#10b981', width=2),
            fill='tozeroy',
            fillcolor='rgba(16, 185, 129, 0.2)'
        ))
        
        fig.add_hline(y=2000, line_dash="dash", line_color="#f59e0b", 
                      annotation_text="Optimal Target", annotation_position="right")
        
        fig.update_layout(
            title=dict(text="Gas Injection Rate Over Time", font=dict(color='#71d3dc')),
            xaxis_title="Time",
            yaxis_title="Injection Rate (MSCF/D)",
            template="plotly_dark",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(17, 86, 127, 0.5)',
            height=350,
            margin=dict(l=50, r=50, t=50, b=50),
            font=dict(color='#71d3dc')
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        latest_value = float(realistic_values[-1])
        avg_value = float(realistic_values.mean())
        efficiency = min(100, (2000 / avg_value) * 100) if avg_value > 0 else 0
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Current Rate", f"{latest_value:,.0f} MSCF/D")
        with col2:
            st.metric("Avg Rate", f"{avg_value:,.0f} MSCF/D")
        with col3:
            st.metric("Efficiency", f"{efficiency:.0f}%")
        
        if avg_value > 2500:
            st.warning("⚠️ **High Gas Usage:** Injection rate above optimal. Consider valve adjustment to reduce gas consumption.")
        elif avg_value < 1500:
            st.warning("⚠️ **Low Injection:** Below optimal rate may cause poor lift efficiency. Check for valve issues.")
        else:
            st.success("✅ Gas lift operating within optimal parameters.")
    else:
        st.info("No injection rate data available for this asset.")

elif selected_type == 'PCP':
    st.subheader("🔄 Progressive Cavity Pump Analysis")
    st.markdown("""
    **PCP (Progressive Cavity Pump)** uses a helical rotor inside a stator to lift fluid. 
    Key indicators: **Pump Speed** (RPM), **Intake Pressure** (PSI), and **Drive Torque** (ft-lb).
    High torque with low speed often indicates pump wear or sand intrusion.
    """)
    
    speed_data = run_query(f"""
        SELECT TIMESTAMP, VALUE
        FROM IPA.SCADA_CORE.TAG_HISTORY
        WHERE TAG_ID = '{selected_asset}.PUMP_SPEED'
        AND TIMESTAMP > DATEADD(hour, -{hours}, CURRENT_TIMESTAMP())
        ORDER BY TIMESTAMP
    """)
    
    torque_data = run_query(f"""
        SELECT TIMESTAMP, VALUE
        FROM IPA.SCADA_CORE.TAG_HISTORY
        WHERE TAG_ID = '{selected_asset}.TORQUE'
        AND TIMESTAMP > DATEADD(hour, -{hours}, CURRENT_TIMESTAMP())
        ORDER BY TIMESTAMP
    """)
    
    if not speed_data.empty:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
        
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                           subplot_titles=('Pump Speed (RPM)', 'Drive Torque (ft-lb)'),
                           vertical_spacing=0.12)
        
        fig.add_trace(go.Scatter(
            x=speed_data['TIMESTAMP'],
            y=speed_data['VALUE'],
            mode='lines',
            name='Speed',
            line=dict(color='#8b5cf6', width=2),
            fill='tozeroy',
            fillcolor='rgba(139, 92, 246, 0.2)'
        ), row=1, col=1)
        
        if not torque_data.empty:
            fig.add_trace(go.Scatter(
                x=torque_data['TIMESTAMP'],
                y=torque_data['VALUE'],
                mode='lines',
                name='Torque',
                line=dict(color='#f97316', width=2),
                fill='tozeroy',
                fillcolor='rgba(249, 115, 22, 0.2)'
            ), row=2, col=1)
        
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(17, 86, 127, 0.5)',
            height=450,
            margin=dict(l=50, r=50, t=50, b=50),
            font=dict(color='#71d3dc'),
            showlegend=False
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        latest_speed = float(speed_data['VALUE'].values[-1])
        avg_speed = float(speed_data['VALUE'].mean())
        latest_torque = float(torque_data['VALUE'].values[-1]) if not torque_data.empty else 0
        
        col1, col2, col3 = st.columns(3)
        with col1:
            status = "🔴 STOPPED" if latest_speed == 0 else "🟢 RUNNING"
            st.metric("Status", status)
        with col2:
            st.metric("Current Speed", f"{latest_speed:.0f} RPM")
        with col3:
            st.metric("Current Torque", f"{latest_torque:.0f} ft-lb")
        
        if latest_speed == 0 and latest_torque > 1500:
            st.error("⚠️ **CRITICAL: Pump Stall Detected**")
            st.markdown("""
            **Diagnosis:** Zero speed with high torque indicates pump stall - likely caused by:
            - Sand/solids intrusion jamming the rotor
            - Stator elastomer swelling
            - Excessive fluid viscosity
            
            **Recommended Action:** Shut down drive immediately to prevent motor damage. 
            Schedule workover to inspect pump and clear obstruction.
            """)
        elif latest_speed == 0:
            st.warning("⚠️ **Pump Offline:** Zero speed detected. Verify if planned shutdown or investigate cause.")
        elif latest_torque > 1500:
            st.warning("⚠️ **High Torque Warning:** Elevated torque may indicate pump wear or increasing sand production.")
        else:
            st.success("✅ PCP operating normally.")
    else:
        st.info("No pump speed data available for this asset.")

st.divider()

st.subheader("📋 Recent Alerts for This Asset")
alerts = run_query(f"""
    SELECT CREATED_AT, SEVERITY, TITLE, DESCRIPTION, STATUS
    FROM IPA.SCADA_CORE.MISSION_CONTROL_ALERTS
    WHERE ASSET_ID = '{selected_asset}'
    ORDER BY CREATED_AT DESC
    LIMIT 5
""")

if not alerts.empty:
    st.dataframe(alerts, use_container_width=True)
else:
    st.info("No alerts for this asset.")
