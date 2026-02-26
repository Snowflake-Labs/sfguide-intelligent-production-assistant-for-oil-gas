import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
import json
import os
import glob
import base64
from theme import apply_dark_theme, show_logo

st.set_page_config(page_title="IPA - Visual Inspector", page_icon="👁️", layout="wide")

apply_dark_theme()
show_logo("👁️ Visual Inspector")

session = get_active_session()

def run_query(sql):
    return session.sql(sql).to_pandas()

st.markdown("*AI-Powered Safety Violation Detection using AI_COMPLETE*")

INSPECTION_STAGE = "@IPA.APP.INSPECTION_IMAGES"
INSPECTION_STAGE_NAME = "IPA.APP.INSPECTION_IMAGES"

SAFETY_PROMPT = """You are a safety inspector analyzing this worksite image from location: {location}.

Analyze for HSE (Health, Safety, Environment) violations. Consider:
- PPE violations (missing hard hat, safety glasses, gloves, FR clothing, steel-toe boots)
- Unsafe work practices (improper lifting, working at heights without harness)
- Housekeeping issues (spills, trip hazards, blocked exits)
- Equipment safety (missing guards, damaged equipment, improper storage)
- Fire hazards (ignition sources near flammables, blocked fire equipment)
- Chemical hazards (improper labeling, missing containment)
- Electrical hazards (exposed wiring, overloaded outlets)

Provide a JSON response with:
{{
    "violations_found": true/false,
    "violation_count": number,
    "severity": "SAFE" | "WARNING" | "CRITICAL" | "STOP_WORK",
    "violations": [
        {{
            "type": "PPE" | "WORK_PRACTICE" | "HOUSEKEEPING" | "EQUIPMENT" | "FIRE" | "CHEMICAL" | "ELECTRICAL",
            "description": "specific violation description",
            "severity": "LOW" | "MEDIUM" | "HIGH" | "CRITICAL",
            "corrective_action": "recommended action"
        }}
    ],
    "safe_observations": ["list of compliant safety items observed"],
    "overall_assessment": "brief summary",
    "immediate_actions_required": ["list of urgent actions if any"]
}}

Only return valid JSON."""

def parse_json_response(response_text) -> dict:
    if response_text is None:
        return {"error": "Response is None"}
    
    text = str(response_text)
    
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
        elif isinstance(parsed, str):
            inner = json.loads(parsed)
            if isinstance(inner, dict):
                return inner
    except (json.JSONDecodeError, ValueError, TypeError):
        pass
    
    start = text.find('{')
    end = text.rfind('}') + 1
    if start >= 0 and end > start:
        try:
            parsed = json.loads(text[start:end])
            if isinstance(parsed, dict):
                return parsed
        except (json.JSONDecodeError, ValueError):
            pass
    
    if start >= 0 and '}' not in text:
        return {"error": "JSON response was truncated (no closing brace)", "raw": text[:1000]}
    
    return {"error": "Could not parse JSON response", "raw": text[:1000]}

def analyze_local_image(image_path: str, location: str):
    """Handle local image analysis - check if image exists in stage first"""
    try:
        # Get the filename from the local path
        filename = os.path.basename(image_path)
        
        # First, check if this image exists in the stage (uploaded during setup)
        try:
            stage_images = session.sql(f"""
                SELECT RELATIVE_PATH 
                FROM DIRECTORY({INSPECTION_STAGE})
                WHERE RELATIVE_PATH = '{filename}'
            """).collect()
            
            if stage_images:
                # Image exists in stage - analyze it using the stage method
                return analyze_image_from_stage(filename, location)
            else:
                # Image not in stage - provide helpful message
                return {
                    "error": f"Image '{filename}' found locally but not in stage. Run the setup.sql script to automatically upload assets to stage for analysis, or manually upload with: PUT file://{image_path} @IPA.APP.INSPECTION_IMAGES",
                    "local_preview_only": True
                }
                
        except Exception as stage_error:
            # Stage might not exist - setup not run yet
            return {
                "error": f"Setup required: Run scripts/setup.sql to create the IPA database and upload images to stage for analysis. Local file: {filename}",
                "setup_required": True
            }
        
    except Exception as e:
        return {"error": f"Local image analysis failed: {str(e)}"}
def analyze_image_from_stage(image_path: str, location: str):
    """Analyze image from Snowflake stage"""
    prompt = SAFETY_PROMPT.format(location=location)
    safe_prompt = prompt.replace("'", "''")
    
    try:
        result = session.sql(f"""
            SELECT AI_COMPLETE(
                'claude-3-5-sonnet',
                '{safe_prompt}',
                TO_FILE('{INSPECTION_STAGE}', '{image_path}'),
                {{'max_tokens': 4096}}
            ) as response
        """).collect()
        
        response_text = result[0]['RESPONSE']
        parsed = parse_json_response(response_text)
        if not isinstance(parsed, dict):
            return {"error": f"parse_json_response returned {type(parsed).__name__}", "raw": str(parsed)[:500]}
        return parsed
    except Exception as e:
        return {"error": f"Exception: {str(e)}"}

def create_safety_alert(violation_data: dict, location: str):
    if violation_data.get("severity") in ["CRITICAL", "STOP_WORK"]:
        severity = "CRITICAL"
    elif violation_data.get("severity") == "WARNING":
        severity = "WARNING"
    else:
        return
    
    violations = violation_data.get("violations", [])
    violation_summary = "; ".join([v.get("description", "") for v in violations[:3]])
    
    title = f"Safety Violation Detected - {location}"
    description = f"Visual inspection identified {violation_data.get('violation_count', 0)} violation(s). {violation_summary}"
    
    try:
        session.sql(f"""
            INSERT INTO IPA.SCADA_CORE.MISSION_CONTROL_ALERTS 
            (AGENT_TYPE, SEVERITY, ASSET_ID, TITLE, DESCRIPTION, STATUS)
            VALUES ('GUARDIAN', '{severity}', '{location}', '{title.replace("'", "''")}', 
                    '{description.replace("'", "''")}', 'NEW')
        """).collect()
    except Exception as e:
        st.warning(f"Could not create alert: {e}")

def get_stage_images():
    try:
        images_df = session.sql(f"""
            SELECT RELATIVE_PATH, SIZE, LAST_MODIFIED 
            FROM DIRECTORY({INSPECTION_STAGE})
            WHERE LOWER(RELATIVE_PATH) LIKE '%.jpg' 
               OR LOWER(RELATIVE_PATH) LIKE '%.jpeg'
               OR LOWER(RELATIVE_PATH) LIKE '%.png'
               OR LOWER(RELATIVE_PATH) LIKE '%.gif'
               OR LOWER(RELATIVE_PATH) LIKE '%.webp'
            ORDER BY RELATIVE_PATH
        """).to_pandas()
        return images_df
    except Exception as e:
        return pd.DataFrame()

def get_local_images():
    """Get images from local assets directory"""
    try:
        # Get the assets directory path relative to this file
        current_dir = os.path.dirname(os.path.abspath(__file__))
        assets_dir = os.path.join(os.path.dirname(current_dir), "assets")
        
        # Supported image extensions
        extensions = ['*.jpg', '*.jpeg', '*.png', '*.gif', '*.webp', '*.JPG', '*.JPEG', '*.PNG', '*.GIF', '*.WEBP']
        
        local_images = []
        if os.path.exists(assets_dir):
            for ext in extensions:
                pattern = os.path.join(assets_dir, ext)
                files = glob.glob(pattern)
                for file_path in files:
                    filename = os.path.basename(file_path)
                    file_size = os.path.getsize(file_path)
                    mod_time = os.path.getmtime(file_path)
                    local_images.append({
                        'RELATIVE_PATH': f"LOCAL: {filename}",
                        'SIZE': file_size,
                        'LAST_MODIFIED': pd.to_datetime(mod_time, unit='s'),
                        'FULL_PATH': file_path
                    })
        
        return pd.DataFrame(local_images).sort_values('LAST_MODIFIED', ascending=False) if local_images else pd.DataFrame()
    except Exception as e:
        st.warning(f"Could not scan local assets directory: {e}")
        return pd.DataFrame()

def get_all_images():
    """Get images from stage only (demo images are copied from GitHub to stage)"""
    stage_df = get_stage_images()
    if not stage_df.empty:
        stage_df['FULL_PATH'] = None
    return stage_df

def get_image_presigned_url(image_path: str) -> str:
    try:
        result = session.sql(f"""
            SELECT GET_PRESIGNED_URL({INSPECTION_STAGE}, '{image_path}', 3600) as URL
        """).collect()
        return result[0]['URL']
    except Exception as e:
        return None

with st.sidebar:
    st.subheader("Inspection Location")
    
    zones = ["Zone-A", "Zone-B", "Zone-C", "Zone-D", "Zone-E"]
    selected_zone = st.selectbox("Select Zone", zones)
    
    location_detail = st.text_input("Location Details", placeholder="e.g., Wellhead Platform 3")
    
    full_location = f"{selected_zone} - {location_detail}" if location_detail else selected_zone

tab1, tab2 = st.tabs(["📷 Image Analysis", "📤 Upload Image"])

with tab1:
    st.subheader("🔍 Analyze Images from Stage")
    st.markdown(f"*Images are loaded from `{INSPECTION_STAGE}` stage*")

    images_df = get_all_images()
    
    if images_df.empty:
        st.info(f"No images found in stage. Upload images to get started.")
    else:
        st.success(f"Found {len(images_df)} image(s) in stage")
        
        image_options = ["-- Select an image --"] + images_df['RELATIVE_PATH'].tolist()
        selected_image = st.selectbox("Select Image to Analyze", image_options, key="image_selector")
        
        if selected_image != "-- Select an image --":
            image_url = get_image_presigned_url(selected_image)
            
            col_img, col_controls = st.columns([2, 1])
            
            with col_img:
                st.markdown("**Selected Image Preview:**")
                if image_url:
                    st.image(image_url, caption=f"Stage: {selected_image}", use_column_width=True)
                else:
                    st.warning("Unable to load image preview")
            
            with col_controls:
                st.markdown("**Analysis Controls:**")
                st.markdown(f"📍 **Location:** {full_location}")
                st.markdown(f"📁 **File:** {selected_image}")
                analyze_btn = st.button("🔍 Analyze with Cortex AI", type="primary", key="analyze_image")
            
            if analyze_btn:
                with st.spinner(f"Analyzing {selected_image} with AI_COMPLETE (claude-3-5-sonnet)..."):
                    analysis = analyze_image_from_stage(selected_image, full_location)
            
                if not isinstance(analysis, dict):
                    st.error(f"Analysis failed: Response type was {type(analysis).__name__}, expected dict")
                    st.code(str(analysis)[:2000])
                elif "error" in analysis:
                    error_msg = analysis.get('error', 'Unknown error')
                    
                    if analysis.get("setup_required"):
                        st.warning("🔧 **Setup Required**")
                        st.markdown(f"**Issue:** {error_msg}")
                        st.markdown("**Solution:** Run `scripts/setup.sql` to create the IPA database and automatically upload all images from `streamlit/assets/` to the stage.")
                        st.code("PUT file://streamlit/assets/*.jpg @IPA.APP.INSPECTION_IMAGES", language="sql")
                    elif analysis.get("local_preview_only"):
                        st.info("📁 **Image Available Locally Only**")
                        st.markdown(f"**Status:** {error_msg}")
                        st.markdown("**Next Steps:**")
                        st.markdown("1. The image is visible for preview but needs to be in the Snowflake stage for AI analysis")
                        st.markdown("2. Re-run the setup script to automatically upload all assets:")
                        st.code("COPY FILES INTO @IPA.APP.INSPECTION_IMAGES FROM @IPA.APP.IPA_REPO/branches/main/streamlit/assets/ PATTERN='.*\\.(jpg|jpeg|png|webp|gif)';", language="sql")
                    else:
                        st.error(f"Analysis failed: {error_msg}")
                        
                    if "raw" in analysis:
                        with st.expander("Raw Response"):
                            st.code(analysis.get('raw', ''))
                else:
                    st.markdown("---")
                    
                    results_col1, results_col2 = st.columns([1, 1])
                    
                    with results_col1:
                        st.subheader("📷 Analyzed Image")
                        if image_url:
                            st.image(image_url, caption=f"Location: {full_location}", use_column_width=True)
                    
                    with results_col2:
                        st.subheader("📋 Safety Analysis Results")
                        
                        violations_found = analysis.get("violations_found", False)
                        severity = analysis.get("severity", "UNKNOWN")
                        violation_count = analysis.get("violation_count", 0)
                        
                        if severity == "STOP_WORK":
                            st.error("🛑 **STOP WORK ORDER** - Critical safety violations detected!")
                        elif severity == "CRITICAL":
                            st.error(f"🔴 **CRITICAL** - {violation_count} serious violation(s) found")
                        elif severity == "WARNING":
                            st.warning(f"⚠️ **WARNING** - {violation_count} violation(s) require attention")
                        else:
                            st.success("✅ **SAFE** - No significant violations detected")
                        
                        col_a, col_b, col_c = st.columns(3)
                        with col_a:
                            st.metric("Violations", violation_count)
                        with col_b:
                            st.metric("Severity", severity)
                        with col_c:
                            safe_items = len(analysis.get("safe_observations", []))
                            st.metric("Compliant", safe_items)
                        
                        st.markdown(f"**Assessment:** {analysis.get('overall_assessment', 'N/A')}")
                    
                    violations = analysis.get("violations", [])
                    if violations:
                        st.markdown("### ⚠️ Violations Detected")
                        for i, v in enumerate(violations, 1):
                            v_severity = v.get("severity", "MEDIUM")
                            icon = "🔴" if v_severity in ["HIGH", "CRITICAL"] else "🟡" if v_severity == "MEDIUM" else "🟢"
                            
                            with st.expander(f"{icon} Violation {i}: {v.get('type', 'Unknown')} - {v_severity}"):
                                st.markdown(f"**Description:** {v.get('description', 'N/A')}")
                                st.markdown(f"**Corrective Action:** {v.get('corrective_action', 'N/A')}")
                    
                    immediate_actions = analysis.get("immediate_actions_required", [])
                    if immediate_actions:
                        st.markdown("### 🚨 Immediate Actions Required")
                        for action in immediate_actions:
                            st.markdown(f"- **{action}**")
                    
                    safe_obs = analysis.get("safe_observations", [])
                    if safe_obs:
                        with st.expander("✅ Compliant Safety Observations"):
                            for obs in safe_obs:
                                st.markdown(f"- {obs}")
                    
                    if violations_found and severity in ["WARNING", "CRITICAL", "STOP_WORK"]:
                        create_safety_alert(analysis, full_location)
                        st.info("📢 Alert created and sent to Mission Control")
                    
                    try:
                        import uuid
                        analysis_json = json.dumps(analysis).replace("'", "''")
                        session.sql(f"""
                            INSERT INTO IPA.SCADA_CORE.INSPECTION_RESULTS 
                            (INSPECTION_ID, ASSET_ID, IMAGE_PATH, AI_ANALYSIS, CONDITION_SCORE, STATUS)
                            SELECT 
                                'INS-{uuid.uuid4().hex[:8]}',
                                '{full_location.replace("'", "''")}',
                                '{selected_image.replace("'", "''")}',
                                PARSE_JSON('{analysis_json}'),
                                {10 - violation_count if violation_count < 10 else 1},
                                '{severity}'
                        """).collect()
                        st.success("✅ Safety inspection recorded to database")
                    except Exception as e:
                        st.error(f"Failed to save inspection: {str(e)}")
                        st.info("Analysis completed but not saved to database")

with tab2:
    st.subheader("📤 Upload New Image")
    st.markdown("*Upload an image to the stage for safety analysis*")
    
    uploaded_file = st.file_uploader(
        "Choose an image file",
        type=['jpg', 'jpeg', 'png', 'gif', 'webp'],
        help="Supported formats: JPG, JPEG, PNG, GIF, WEBP (max 200MB)"
    )
    
    if uploaded_file is not None:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.image(uploaded_file, caption=f"Preview: {uploaded_file.name}", use_column_width=True)
        
        with col2:
            st.markdown("**File Details:**")
            st.markdown(f"- **Name:** {uploaded_file.name}")
            st.markdown(f"- **Size:** {uploaded_file.size / 1024:.1f} KB")
            st.markdown(f"- **Type:** {uploaded_file.type}")
            
            upload_btn = st.button("📤 Upload to Stage", type="primary")
            
            if upload_btn:
                with st.spinner("Uploading image to stage..."):
                    try:
                        filename = uploaded_file.name
                        uploaded_file.seek(0)
                        session.file.put_stream(
                            input_stream=uploaded_file,
                            stage_location=f"{INSPECTION_STAGE}/{filename}",
                            auto_compress=False,
                            overwrite=True
                        )
                        
                        session.sql(f"ALTER STAGE {INSPECTION_STAGE_NAME} REFRESH").collect()
                        
                        st.success(f"✅ Image '{filename}' uploaded successfully!")
                        st.session_state['uploaded_filename'] = filename
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Upload failed: {str(e)}")
                        st.info("**Alternative:** Upload images via Snowsight > Data > Databases > IPA > APP > Stages > INSPECTION_IMAGES")
    
    if 'uploaded_filename' in st.session_state and st.session_state['uploaded_filename']:
        uploaded_filename = st.session_state['uploaded_filename']
        
        st.markdown("---")
        st.subheader("🔍 Analyze Uploaded Image")
        
        image_url = get_image_presigned_url(uploaded_filename)
        
        col_img, col_controls = st.columns([2, 1])
        
        with col_img:
            if image_url:
                st.image(image_url, caption=f"Uploaded: {uploaded_filename}", use_column_width=True)
            else:
                st.warning("Unable to load image preview")
        
        with col_controls:
            st.markdown("**Analysis Controls:**")
            st.markdown(f"📍 **Location:** {full_location}")
            st.markdown(f"📁 **File:** {uploaded_filename}")
            analyze_uploaded_btn = st.button("🔍 Analyze with Cortex AI", type="primary", key="analyze_uploaded")
            clear_btn = st.button("🗑️ Clear", key="clear_uploaded")
            
            if clear_btn:
                del st.session_state['uploaded_filename']
                st.rerun()
        
        if analyze_uploaded_btn:
            with st.spinner(f"Analyzing {uploaded_filename} with AI_COMPLETE (claude-3-5-sonnet)..."):
                analysis = analyze_image_from_stage(uploaded_filename, full_location)
            
            if not isinstance(analysis, dict):
                st.error(f"Analysis failed: Response type was {type(analysis).__name__}, expected dict")
                st.code(str(analysis)[:2000])
            elif "error" in analysis:
                st.error(f"Analysis failed: {analysis.get('error', 'Unknown error')}")
            else:
                st.markdown("---")
                
                results_col1, results_col2 = st.columns([1, 1])
                
                with results_col1:
                    st.subheader("📷 Analyzed Image")
                    if image_url:
                        st.image(image_url, caption=f"Location: {full_location}", use_column_width=True)
                
                with results_col2:
                    st.subheader("📋 Safety Analysis Results")
                    
                    violations_found = analysis.get("violations_found", False)
                    severity = analysis.get("severity", "UNKNOWN")
                    violation_count = analysis.get("violation_count", 0)
                    
                    if severity == "STOP_WORK":
                        st.error("🛑 **STOP WORK ORDER** - Critical safety violations detected!")
                    elif severity == "CRITICAL":
                        st.error(f"🔴 **CRITICAL** - {violation_count} serious violation(s) found")
                    elif severity == "WARNING":
                        st.warning(f"⚠️ **WARNING** - {violation_count} violation(s) require attention")
                    else:
                        st.success("✅ **SAFE** - No significant violations detected")
                    
                    col_a, col_b, col_c = st.columns(3)
                    with col_a:
                        st.metric("Violations", violation_count)
                    with col_b:
                        st.metric("Severity", severity)
                    with col_c:
                        safe_items = len(analysis.get("safe_observations", []))
                        st.metric("Compliant", safe_items)
                    
                    st.markdown(f"**Assessment:** {analysis.get('overall_assessment', 'N/A')}")
                
                violations = analysis.get("violations", [])
                if violations:
                    st.markdown("### ⚠️ Violations Detected")
                    for i, v in enumerate(violations, 1):
                        v_severity = v.get("severity", "MEDIUM")
                        icon = "🔴" if v_severity in ["HIGH", "CRITICAL"] else "🟡" if v_severity == "MEDIUM" else "🟢"
                        
                        with st.expander(f"{icon} Violation {i}: {v.get('type', 'Unknown')} - {v_severity}"):
                            st.markdown(f"**Description:** {v.get('description', 'N/A')}")
                            st.markdown(f"**Corrective Action:** {v.get('corrective_action', 'N/A')}")
                
                immediate_actions = analysis.get("immediate_actions_required", [])
                if immediate_actions:
                    st.markdown("### 🚨 Immediate Actions Required")
                    for action in immediate_actions:
                        st.markdown(f"- **{action}**")
                
                safe_obs = analysis.get("safe_observations", [])
                if safe_obs:
                    with st.expander("✅ Compliant Safety Observations"):
                        for obs in safe_obs:
                            st.markdown(f"- {obs}")
                
                if violations_found and severity in ["WARNING", "CRITICAL", "STOP_WORK"]:
                    create_safety_alert(analysis, full_location)
                    st.info("📢 Alert created and sent to Mission Control")
                
                try:
                    import uuid
                    analysis_json = json.dumps(analysis).replace("'", "''")
                    session.sql(f"""
                        INSERT INTO IPA.SCADA_CORE.INSPECTION_RESULTS 
                        (INSPECTION_ID, ASSET_ID, IMAGE_PATH, AI_ANALYSIS, CONDITION_SCORE, STATUS)
                        SELECT 
                            'INS-{uuid.uuid4().hex[:8]}',
                            '{full_location.replace("'", "''")}',
                            '{uploaded_filename.replace("'", "''")}',
                            PARSE_JSON('{analysis_json}'),
                            {10 - violation_count if violation_count < 10 else 1},
                            '{severity}'
                    """).collect()
                    st.success("✅ Safety inspection recorded to database")
                except Exception as e:
                    st.error(f"Failed to save inspection: {str(e)}")

st.divider()

st.subheader("📊 Recent Safety Inspections")

try:
    inspections = run_query("""
        SELECT 
            INSPECTION_ID,
            ASSET_ID as LOCATION,
            INSPECTION_DATE,
            STATUS as SEVERITY,
            AI_ANALYSIS:violation_count::NUMBER as VIOLATIONS,
            AI_ANALYSIS:overall_assessment::VARCHAR as ASSESSMENT
        FROM IPA.SCADA_CORE.INSPECTION_RESULTS
        ORDER BY INSPECTION_DATE DESC
        LIMIT 10
    """)
    
    if not inspections.empty:
        st.dataframe(inspections)
    else:
        st.info("No inspections recorded yet. Analyze a scene to create the first safety inspection.")
except:
    st.info("Analyze a scene above to start recording safety inspections.")

with st.expander("ℹ️ Safety Violation Categories"):
    st.markdown("""
    | Category | Examples |
    |----------|----------|
    | **PPE** | Missing hard hat, safety glasses, gloves, FR clothing, steel-toe boots |
    | **Work Practice** | Improper lifting, working at heights without fall protection |
    | **Housekeeping** | Spills, trip hazards, blocked exits, cluttered work areas |
    | **Equipment** | Missing guards, damaged tools, improper storage |
    | **Fire** | Ignition sources near flammables, blocked fire extinguishers |
    | **Chemical** | Improper labeling, missing SDS, no secondary containment |
    | **Electrical** | Exposed wiring, damaged cords, overloaded circuits |
    
    **Severity Levels:**
    - 🟢 **SAFE** - No violations, compliant with safety standards
    - 🟡 **WARNING** - Minor violations requiring attention
    - 🔴 **CRITICAL** - Serious violations requiring immediate correction
    - 🛑 **STOP_WORK** - Imminent danger, all work must cease
    
    **AI Model:** claude-3-5-sonnet (multimodal vision)
    """)
