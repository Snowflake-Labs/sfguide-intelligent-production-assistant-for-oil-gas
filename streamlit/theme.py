import streamlit as st
import base64

def get_logo_header():
    """Returns HTML for right-aligned logo header with embedded base64 image"""
    import base64
    import os
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    logo_path = os.path.join(script_dir, "images", "Snowflake_Logo.png")
    
    try:
        with open(logo_path, "rb") as f:
            img_data = base64.b64encode(f.read()).decode()
        img_src = f"data:image/png;base64,{img_data}"
    except:
        img_src = ""
    
    return f"""
    <div style="display: flex; justify-content: flex-end; align-items: center; padding: 0 0 1rem 0; margin-bottom: 0.5rem;">
        <img src="{img_src}" style="height: 50px;" onerror="this.style.display='none'">
    </div>
    """

def show_logo(title=None):
    """Display page title left-aligned and logo right-aligned, vertically centered"""
    import base64
    import os
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    logo_path = os.path.join(script_dir, "images", "Snowflake_Logo.png")
    
    try:
        with open(logo_path, "rb") as f:
            img_data = base64.b64encode(f.read()).decode()
        img_src = f"data:image/png;base64,{img_data}"
    except:
        img_src = ""
    
    st.markdown(f"""
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
            <h1 style="margin: 0; padding: 0;">{title if title else ''}</h1>
            <img src="{img_src}" style="height: auto; max-height: 50px;">
        </div>
    """, unsafe_allow_html=True)

def apply_dark_theme():
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
        
        /* Base theme - slightly lighter for better contrast */
        .main {
            background: linear-gradient(135deg, #1e293b 0%, #0f172a 50%, #020617 100%);
            color: #e2e8f0;
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        }
        
        /* Sidebar - Snowflake cyan accent */
        section[data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0c1929 0%, #0a1628 100%) !important;
            border-right: 1px solid rgba(56, 189, 248, 0.2);
        }
        
        /* Sidebar navigation - styled as buttons */
        [data-testid="stSidebarNav"] {
            max-height: none !important;
            overflow: visible !important;
            padding-top: 0.5rem !important;
        }
        [data-testid="stSidebarNav"] ul {
            max-height: none !important;
            overflow: visible !important;
            padding: 0 !important;
            display: flex !important;
            flex-direction: column !important;
            gap: 0.4rem !important;
        }
        [data-testid="stSidebarNav"] li {
            margin: 0 !important;
        }
        [data-testid="stSidebarNav"] a {
            display: flex !important;
            align-items: center !important;
            color: #cbd5e1 !important;
            background: linear-gradient(135deg, rgba(30, 41, 59, 0.8), rgba(15, 23, 42, 0.9)) !important;
            border-radius: 0.75rem !important;
            margin: 0 0.75rem !important;
            padding: 0.75rem 1rem !important;
            transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1) !important;
            font-weight: 500 !important;
            font-size: 0.9rem !important;
            border: 1px solid rgba(148, 163, 184, 0.15) !important;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2) !important;
            text-decoration: none !important;
        }
        [data-testid="stSidebarNav"] a:hover {
            background: linear-gradient(135deg, rgba(56, 189, 248, 0.2), rgba(14, 165, 233, 0.15)) !important;
            border-color: rgba(56, 189, 248, 0.4) !important;
            color: #f8fafc !important;
            transform: translateX(4px) !important;
            box-shadow: 0 4px 12px rgba(56, 189, 248, 0.2) !important;
        }
        [data-testid="stSidebarNav"] a[aria-current="page"] {
            background: linear-gradient(135deg, #0891b2 0%, #0e7490 100%) !important;
            color: #ffffff !important;
            border: 1px solid rgba(56, 189, 248, 0.5) !important;
            box-shadow: 0 4px 16px rgba(8, 145, 178, 0.4), inset 0 1px 0 rgba(255, 255, 255, 0.1) !important;
            font-weight: 600 !important;
        }
        [data-testid="stSidebarNav"] a[aria-current="page"]:hover {
            transform: translateX(0) !important;
            box-shadow: 0 6px 20px rgba(8, 145, 178, 0.5), inset 0 1px 0 rgba(255, 255, 255, 0.1) !important;
        }
        [data-testid="stSidebarNav"] a span {
            color: inherit !important;
        }
        [data-testid="stSidebarNavItems"] {
            max-height: none !important;
        }
        /* Sidebar nav separator line */
        [data-testid="stSidebarNav"]::after {
            content: '';
            display: block;
            margin: 1rem 1rem 0.5rem 1rem;
            border-bottom: 1px solid rgba(56, 189, 248, 0.2);
        }
        
        /* Typography - better contrast */
        h1 {
            color: #f8fafc !important;
            font-weight: 700 !important;
            letter-spacing: -0.025em !important;
            font-size: 2.25rem !important;
            margin-bottom: 0.5rem !important;
        }
        h2 {
            color: #f1f5f9 !important;
            font-weight: 600 !important;
            font-size: 1.75rem !important;
            letter-spacing: -0.015em !important;
            margin-top: 1.5rem !important;
            margin-bottom: 0.75rem !important;
        }
        h3 {
            color: #e2e8f0 !important;
            font-weight: 600 !important;
            font-size: 1.35rem !important;
            margin-top: 1.25rem !important;
            margin-bottom: 0.5rem !important;
        }
        h4, h5, h6 {
            color: #cbd5e1 !important;
            font-weight: 500 !important;
        }
        
        /* Body text - improved readability */
        p, span, div, label, li {
            color: #e2e8f0 !important;
            line-height: 1.6 !important;
            font-weight: 400 !important;
        }
        .stMarkdown, .stText {
            color: #e2e8f0 !important;
        }
        [data-testid="stMarkdownContainer"] {
            color: #e2e8f0 !important;
        }
        [data-testid="stMarkdownContainer"] p {
            color: #e2e8f0 !important;
        }
        .stCaption, caption {
            color: #94a3b8 !important;
            font-size: 0.875rem !important;
        }
        strong, b {
            color: #f8fafc !important;
            font-weight: 600 !important;
        }
        
        /* Buttons - Snowflake cyan theme */
        .stButton > button {
            background: linear-gradient(135deg, #0891b2 0%, #0e7490 100%) !important;
            color: #ffffff !important;
            border: 1px solid rgba(56, 189, 248, 0.3) !important;
            border-radius: 0.5rem !important;
            font-weight: 500 !important;
            padding: 0.5rem 1rem !important;
            transition: all 0.2s ease !important;
            box-shadow: 0 2px 8px rgba(8, 145, 178, 0.3) !important;
        }
        .stButton > button:hover {
            background: linear-gradient(135deg, #06b6d4 0%, #0891b2 100%) !important;
            border-color: rgba(56, 189, 248, 0.5) !important;
            box-shadow: 0 4px 16px rgba(6, 182, 212, 0.4) !important;
            transform: translateY(-1px) !important;
        }
        .stButton > button:active {
            transform: translateY(0) !important;
        }
        
        /* Selectbox and inputs */
        .stSelectbox > div > div {
            background: rgba(15, 23, 42, 0.8) !important;
            color: #f1f5f9 !important;
            border: 1px solid rgba(148, 163, 184, 0.3) !important;
            border-radius: 0.5rem !important;
            transition: all 0.2s ease !important;
        }
        .stSelectbox > div > div:hover {
            border-color: rgba(56, 189, 248, 0.4) !important;
        }
        .stSelectbox label {
            color: #e2e8f0 !important;
            font-weight: 500 !important;
            margin-bottom: 0.25rem !important;
        }
        [data-baseweb="select"] {
            background: rgba(15, 23, 42, 0.8) !important;
        }
        [data-baseweb="select"] > div {
            background: rgba(15, 23, 42, 0.8) !important;
            border-color: rgba(148, 163, 184, 0.3) !important;
            color: #f1f5f9 !important;
        }
        [data-baseweb="select"]:hover > div {
            border-color: rgba(56, 189, 248, 0.4) !important;
        }
        .stTextInput > div > div > input {
            background: rgba(15, 23, 42, 0.8) !important;
            color: #f1f5f9 !important;
            border: 1px solid rgba(148, 163, 184, 0.3) !important;
            border-radius: 0.5rem !important;
            transition: all 0.2s ease !important;
        }
        .stTextInput > div > div > input:focus {
            border-color: rgba(56, 189, 248, 0.5) !important;
            box-shadow: 0 0 0 3px rgba(56, 189, 248, 0.1) !important;
        }
        .stTextArea > div > div > textarea {
            background: rgba(15, 23, 42, 0.8) !important;
            color: #f1f5f9 !important;
            border: 1px solid rgba(148, 163, 184, 0.3) !important;
            border-radius: 0.5rem !important;
            transition: all 0.2s ease !important;
            line-height: 1.6 !important;
        }
        .stTextArea > div > div > textarea:focus {
            border-color: rgba(56, 189, 248, 0.5) !important;
            box-shadow: 0 0 0 3px rgba(56, 189, 248, 0.1) !important;
        }
        .stTextInput label, .stTextArea label {
            color: #e2e8f0 !important;
            font-weight: 500 !important;
        }
        
        /* Dropdown menu popup - dark theme */
        [data-baseweb="popover"] {
            background: #0f172a !important;
            border: 1px solid rgba(56, 189, 248, 0.3) !important;
            border-radius: 0.5rem !important;
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.5) !important;
        }
        [data-baseweb="menu"] {
            background: #0f172a !important;
        }
        [data-baseweb="menu"] li {
            background: #0f172a !important;
            color: #e2e8f0 !important;
        }
        [data-baseweb="menu"] li:hover {
            background: rgba(56, 189, 248, 0.2) !important;
            color: #f8fafc !important;
        }
        [data-baseweb="menu"] [aria-selected="true"] {
            background: rgba(56, 189, 248, 0.3) !important;
            color: #f8fafc !important;
        }
        [role="listbox"] {
            background: #0f172a !important;
        }
        [role="option"] {
            background: #0f172a !important;
            color: #e2e8f0 !important;
        }
        [role="option"]:hover {
            background: rgba(56, 189, 248, 0.2) !important;
        }
        [role="option"][aria-selected="true"] {
            background: rgba(56, 189, 248, 0.3) !important;
        }
        
        /* Tabs */
        .stTabs [data-baseweb="tab-list"] {
            background: rgba(15, 23, 42, 0.6) !important;
            border-radius: 0.75rem !important;
            padding: 0.25rem !important;
            gap: 0.25rem !important;
        }
        .stTabs [data-baseweb="tab"] {
            color: #94a3b8 !important;
            background: transparent !important;
            border-radius: 0.5rem !important;
            padding: 0.5rem 1rem !important;
            font-weight: 500 !important;
            transition: all 0.2s ease !important;
        }
        .stTabs [data-baseweb="tab"]:hover {
            color: #e2e8f0 !important;
            background: rgba(30, 41, 59, 0.5) !important;
        }
        .stTabs [aria-selected="true"] {
            background: linear-gradient(135deg, #0891b2 0%, #0e7490 100%) !important;
            color: #ffffff !important;
            border-bottom: none !important;
            box-shadow: 0 2px 8px rgba(8, 145, 178, 0.4) !important;
        }
        
        /* Expanders */
        .streamlit-expanderHeader {
            background: rgba(15, 23, 42, 0.7) !important;
            border-radius: 0.5rem !important;
            border: 1px solid rgba(148, 163, 184, 0.2) !important;
            color: #f1f5f9 !important;
            font-weight: 500 !important;
            padding: 0.75rem 1rem !important;
            transition: all 0.2s ease !important;
        }
        .streamlit-expanderHeader:hover {
            background: rgba(15, 23, 42, 0.9) !important;
            border-color: rgba(56, 189, 248, 0.3) !important;
        }
        .streamlit-expanderContent {
            background: rgba(15, 23, 42, 0.5) !important;
            border: 1px solid rgba(148, 163, 184, 0.15) !important;
            border-top: none !important;
            border-radius: 0 0 0.5rem 0.5rem !important;
            padding: 1rem !important;
        }
        
        /* Dataframes */
        .stDataFrame {
            background: rgba(15, 23, 42, 0.6) !important;
            border-radius: 0.5rem !important;
            overflow: hidden !important;
        }
        [data-testid="stDataFrame"] > div {
            background: rgba(15, 23, 42, 0.6) !important;
            border: 1px solid rgba(148, 163, 184, 0.2) !important;
            border-radius: 0.5rem !important;
        }
        [data-testid="stDataFrame"] table {
            color: #e2e8f0 !important;
        }
        [data-testid="stDataFrame"] th {
            background: rgba(30, 41, 59, 0.8) !important;
            color: #f8fafc !important;
            font-weight: 600 !important;
            border-bottom: 2px solid rgba(56, 189, 248, 0.3) !important;
        }
        [data-testid="stDataFrame"] td {
            color: #e2e8f0 !important;
            border-bottom: 1px solid rgba(148, 163, 184, 0.1) !important;
        }
        [data-testid="stDataFrame"] tr:hover {
            background: rgba(56, 189, 248, 0.1) !important;
        }
        
        /* Alert boxes */
        .stAlert {
            background: rgba(15, 23, 42, 0.8) !important;
            color: #f1f5f9 !important;
            border-radius: 0.5rem !important;
            border-left: 4px solid !important;
            padding: 1rem !important;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.2) !important;
        }
        [data-testid="stAlert"] {
            background: rgba(15, 23, 42, 0.8) !important;
        }
        .stSuccess {
            border-left-color: #22c55e !important;
            background: linear-gradient(135deg, rgba(22, 163, 74, 0.15), rgba(15, 23, 42, 0.8)) !important;
        }
        .stInfo {
            border-left-color: #38bdf8 !important;
            background: linear-gradient(135deg, rgba(56, 189, 248, 0.15), rgba(15, 23, 42, 0.8)) !important;
        }
        .stWarning {
            border-left-color: #f59e0b !important;
            background: linear-gradient(135deg, rgba(245, 158, 11, 0.15), rgba(15, 23, 42, 0.8)) !important;
        }
        .stError {
            border-left-color: #ef4444 !important;
            background: linear-gradient(135deg, rgba(239, 68, 68, 0.15), rgba(15, 23, 42, 0.8)) !important;
        }
        
        /* Metric cards */
        [data-testid="stMetric"] {
            background: linear-gradient(135deg, rgba(30, 41, 59, 0.9), rgba(15, 23, 42, 0.9)) !important;
            padding: 1.25rem !important;
            border-radius: 0.75rem !important;
            border: 1px solid rgba(56, 189, 248, 0.15) !important;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3) !important;
            transition: all 0.2s ease !important;
        }
        [data-testid="stMetric"]:hover {
            border-color: rgba(56, 189, 248, 0.3) !important;
            box-shadow: 0 6px 20px rgba(56, 189, 248, 0.15) !important;
            transform: translateY(-2px) !important;
        }
        [data-testid="stMetricLabel"] {
            color: #94a3b8 !important;
            font-weight: 500 !important;
            font-size: 0.875rem !important;
            text-transform: uppercase !important;
            letter-spacing: 0.05em !important;
        }
        [data-testid="stMetricValue"] {
            color: #f8fafc !important;
            font-weight: 700 !important;
            font-size: 2rem !important;
        }
        [data-testid="stMetricDelta"] {
            color: #4ade80 !important;
            font-weight: 600 !important;
        }
        
        /* Code blocks */
        .stCodeBlock {
            background: rgba(15, 23, 42, 0.95) !important;
            border: 1px solid rgba(148, 163, 184, 0.2) !important;
            border-radius: 0.5rem !important;
        }
        code {
            background: rgba(30, 41, 59, 0.6) !important;
            color: #38bdf8 !important;
            padding: 0.2rem 0.4rem !important;
            border-radius: 0.25rem !important;
            font-family: 'Monaco', 'Menlo', 'Consolas', monospace !important;
            font-size: 0.875rem !important;
        }
        pre code {
            background: transparent !important;
            padding: 0 !important;
        }
        
        /* Dividers */
        hr {
            border-color: rgba(148, 163, 184, 0.2) !important;
            margin: 2rem 0 !important;
        }
        
        /* Tables */
        .stTable {
            background: rgba(15, 23, 42, 0.6) !important;
            border-radius: 0.5rem !important;
            overflow: hidden !important;
        }
        table {
            color: #e2e8f0 !important;
        }
        th {
            background: rgba(30, 41, 59, 0.9) !important;
            color: #f8fafc !important;
            font-weight: 600 !important;
            padding: 0.75rem 1rem !important;
            border-bottom: 2px solid rgba(56, 189, 248, 0.3) !important;
        }
        td {
            background: rgba(15, 23, 42, 0.5) !important;
            color: #e2e8f0 !important;
            padding: 0.75rem 1rem !important;
            border-bottom: 1px solid rgba(148, 163, 184, 0.1) !important;
        }
        tr:hover td {
            background: rgba(56, 189, 248, 0.1) !important;
        }
        
        /* Charts */
        .stPlotlyChart {
            background: rgba(15, 23, 42, 0.5) !important;
            border-radius: 0.75rem !important;
            padding: 0.75rem !important;
            border: 1px solid rgba(56, 189, 248, 0.1) !important;
        }
        
        /* Scrollbars */
        ::-webkit-scrollbar {
            width: 8px;
            height: 8px;
        }
        ::-webkit-scrollbar-track {
            background: rgba(15, 23, 42, 0.5);
            border-radius: 4px;
        }
        ::-webkit-scrollbar-thumb {
            background: rgba(56, 189, 248, 0.3);
            border-radius: 4px;
        }
        ::-webkit-scrollbar-thumb:hover {
            background: rgba(56, 189, 248, 0.5);
        }
        
        /* Layout */
        .block-container {
            padding-top: 2rem !important;
            padding-bottom: 2rem !important;
        }
        
        /* Selection */
        ::selection {
            background: rgba(56, 189, 248, 0.3) !important;
            color: #f8fafc !important;
        }
        
        /* Multiselect tags */
        [data-baseweb="select"] [data-baseweb="tag"] {
            background: rgba(56, 189, 248, 0.3) !important;
            color: #f8fafc !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
