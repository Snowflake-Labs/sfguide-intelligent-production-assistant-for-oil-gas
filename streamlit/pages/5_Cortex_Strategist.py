import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
import json
import _snowflake
import re
from theme import apply_dark_theme, show_logo

st.set_page_config(page_title="IPA - Cortex Strategist", page_icon="🧠", layout="wide")

apply_dark_theme()
show_logo("🧠 Cortex Strategist")

session = get_active_session()

def run_query(sql):
    return session.sql(sql).to_pandas()

st.markdown("*Intelligent Root Cause Analysis powered by Cortex Agent*")

def run_agent(question: str) -> dict:
    try:
        resp = _snowflake.send_snow_api_request(
            "POST",
            "/api/v2/databases/IPA/schemas/APP/agents/IPA_AGENT:run",
            {},
            {},
            {
                "messages": [
                    {
                        "role": "user",
                        "content": [{"type": "text", "text": question}]
                    }
                ]
            },
            {},
            60000,
        )
        
        if resp["status"] < 400:
            content = resp.get("content", "")
            if isinstance(content, str):
                try:
                    return {"success": True, "data": json.loads(content), "raw_content": content}
                except json.JSONDecodeError:
                    return {"success": True, "data": content, "raw_content": content}
            return {"success": True, "data": content, "raw_content": str(content)}
        else:
            return {"success": False, "error": f"API Error {resp['status']}: {resp.get('content', 'Unknown error')}"}
    except Exception as e:
        return {"success": False, "error": str(e)}

def format_markdown_text(text: str) -> str:
    text = re.sub(r'\*\*([^*]+)\*\*', r'**\1**', text)
    text = re.sub(r'(?<!\n)(#{1,3}\s)', r'\n\1', text)
    text = re.sub(r'(?<!\n)(\d+\.)\s', r'\n\1 ', text)
    text = re.sub(r'(?<!\n)([•\-])\s', r'\n\1 ', text)
    return text.strip()

def extract_key_metrics(text: str) -> list:
    metrics = []
    patterns = [
        r'(\d+(?:\.\d+)?)\s*%',
        r'(\d+(?:,\d+)*(?:\.\d+)?)\s*(?:hours?|days?|minutes?)',
        r'\$\s*(\d+(?:,\d+)*(?:\.\d+)?)',
        r'(\d+(?:,\d+)*)\s*(?:failures?|incidents?|alerts?|events?)',
    ]
    for pattern in patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        if matches:
            for match in matches[:3]:
                metrics.append(match)
    return metrics

def process_agent_response(response: dict) -> dict:
    result = {
        "text": "",
        "sql_queries": [],
        "sql_results": [],
        "search_results": [],
        "citations": [],
        "tools_used": [],
        "thinking": [],
        "raw": None
    }
    
    if not response.get("success"):
        result["text"] = f"❌ Error: {response.get('error', 'Unknown error')}"
        return result
    
    data = response.get("data", {})
    result["raw"] = data
    
    def recursive_extract(obj, depth=0):
        if depth > 10:
            return
        
        if isinstance(obj, str):
            if obj.strip() and len(obj) > 20:
                if not any(obj in t for t in [result["text"]] + [s.get("text", "") for s in result["search_results"]]):
                    pass
            return
        
        if isinstance(obj, list):
            for item in obj:
                recursive_extract(item, depth + 1)
            return
        
        if not isinstance(obj, dict):
            return
        
        item_type = obj.get("type", "")
        
        if item_type == "text":
            text = obj.get("text", "")
            if text and text not in result["text"]:
                result["text"] += text + "\n"
        
        elif item_type == "sql":
            stmt = obj.get("statement", "")
            if stmt and stmt not in result["sql_queries"]:
                result["sql_queries"].append(stmt)
        
        elif item_type in ("search_results", "cortex_search_results"):
            for r in obj.get("results", obj.get("searchResults", [])):
                if isinstance(r, dict):
                    text = r.get("text", r.get("TEXT", r.get("chunk", r.get("CHUNK", ""))))
                    result["search_results"].append({
                        "text": text or str(r),
                        "score": r.get("score", r.get("SCORE")),
                        "source": r.get("source", r.get("SOURCE", r.get("doc_id", r.get("DOC_ID", "Document"))))
                    })
        
        elif item_type == "tool_use":
            tool_name = obj.get("tool", obj.get("name", obj.get("tool_name", "")))
            if tool_name and tool_name != "unknown" and tool_name not in result["tools_used"]:
                result["tools_used"].append(tool_name)
        
        elif item_type == "tool_results":
            tool_name = obj.get("tool", obj.get("name", obj.get("tool_name", "")))
            if tool_name and tool_name != "unknown" and tool_name not in result["tools_used"]:
                result["tools_used"].append(tool_name)
            for tr in obj.get("tool_results", obj.get("results", [])):
                recursive_extract(tr, depth + 1)
        
        if "tool" in obj and obj.get("tool") and obj.get("tool") not in result["tools_used"]:
            result["tools_used"].append(obj["tool"])
        if "tool_name" in obj and obj.get("tool_name") and obj.get("tool_name") not in result["tools_used"]:
            result["tools_used"].append(obj["tool_name"])
        
        elif item_type == "analyst_result":
            sql = obj.get("sql", obj.get("statement", ""))
            if sql and sql not in result["sql_queries"]:
                result["sql_queries"].append(sql)
            text = obj.get("text", obj.get("interpretation", ""))
            if text:
                result["text"] += text + "\n"
        
        elif item_type == "thinking":
            thought = obj.get("text", obj.get("thinking", ""))
            if thought:
                result["thinking"].append(thought)
        
        elif item_type == "citation":
            result["citations"].append({
                "source": obj.get("source", obj.get("title", "")),
                "text": obj.get("text", obj.get("snippet", ""))
            })
        
        for key in ["message", "content", "delta", "data", "events", "response", 
                    "tool_results", "results", "output", "choices", "messages"]:
            if key in obj:
                recursive_extract(obj[key], depth + 1)
    
    recursive_extract(data)
    
    if isinstance(data, dict):
        if "text" in data and isinstance(data["text"], str) and not result["text"]:
            result["text"] = data["text"]
        if "response" in data and isinstance(data["response"], str) and not result["text"]:
            result["text"] = data["response"]
        if "answer" in data and isinstance(data["answer"], str) and not result["text"]:
            result["text"] = data["answer"]
    
    result["text"] = result["text"].strip()
    
    if not result["text"] and not result["sql_queries"] and not result["search_results"]:
        result["raw"] = data
    
    return result

def display_response(parsed: dict):
    tools = [t for t in parsed.get("tools_used", []) if t and t != "unknown"]
    if tools:
        tools_display = " • ".join([f"`{t}`" for t in tools])
        st.caption(f"🔧 Tools used: {tools_display}")
    
    if parsed.get("text"):
        formatted_text = format_markdown_text(parsed["text"])
        
        sections = re.split(r'\n(?=#{1,3}\s|\*\*[A-Z])', formatted_text)
        
        for section in sections:
            if section.strip():
                st.markdown(section)
    
    if parsed.get("sql_results"):
        st.markdown("#### 📊 Query Results")
        for i, result_data in enumerate(parsed["sql_results"]):
            if isinstance(result_data, list) and result_data:
                try:
                    df = pd.DataFrame(result_data)
                    st.dataframe(df, use_container_width=True)
                except Exception:
                    st.json(result_data)
            elif isinstance(result_data, dict):
                st.json(result_data)
    
    if parsed.get("sql_queries"):
        with st.expander(f"📝 SQL Queries ({len(parsed['sql_queries'])})", expanded=False):
            for i, sql in enumerate(parsed["sql_queries"]):
                if len(parsed["sql_queries"]) > 1:
                    st.markdown(f"**Query {i+1}:**")
                st.code(sql.strip(), language="sql")
                
                if st.button(f"▶️ Run Query", key=f"run_sql_{i}"):
                    try:
                        with st.spinner("Executing..."):
                            df = run_query(sql)
                        st.success(f"Returned {len(df)} rows")
                        st.dataframe(df, use_container_width=True)
                    except Exception as e:
                        st.error(f"Query error: {e}")
    
    if parsed.get("search_results"):
        with st.expander(f"📚 Retrieved Documents ({len(parsed['search_results'])})", expanded=False):
            for i, doc in enumerate(parsed["search_results"]):
                col1, col2 = st.columns([4, 1])
                with col1:
                    source = doc.get("source", "Document")
                    st.markdown(f"**Source:** `{source}`")
                with col2:
                    score = doc.get("score")
                    if score is not None:
                        st.markdown(f"**Relevance:** {score:.2f}" if isinstance(score, float) else f"**Relevance:** {score}")
                
                text = doc.get("text", "")
                if len(text) > 500:
                    st.markdown(text[:500] + "...")
                    with st.expander("Show full text"):
                        st.markdown(text)
                else:
                    st.markdown(text)
                
                if i < len(parsed["search_results"]) - 1:
                    st.divider()
    
    if parsed.get("citations"):
        with st.expander("📖 Citations", expanded=False):
            for cite in parsed["citations"]:
                st.markdown(f"- **{cite.get('source', 'Unknown')}**: {cite.get('text', '')}")
    
    if parsed.get("thinking"):
        with st.expander("💭 Agent Reasoning", expanded=False):
            for thought in parsed["thinking"]:
                st.markdown(f"*{thought}*")
    
    if parsed.get("raw") and not parsed.get("text"):
        with st.expander("🔍 Raw Response (Debug)", expanded=True):
            st.json(parsed["raw"])

with st.sidebar:
    st.subheader("💡 Suggested Questions")
    
    question_categories = {
        "Root Cause Analysis": [
            "What caused the rod pump failure on Well-RP-05?",
            "Why is NPT high on Rig 9?",
        ],
        "Safety & Compliance": [
            "Are there any active safety violations in Zone B?",
            "What work permits are active in Zone B?",
        ],
        "Operations": [
            "How many critical alerts are still unacknowledged?",
            "Show me all rod pump assets and their status",
        ]
    }
    
    selected_question = None
    for category, questions in question_categories.items():
        st.markdown(f"**{category}**")
        for q in questions:
            if st.button(q, key=f"q_{hash(q)}", use_container_width=True):
                selected_question = q
        st.markdown("")

st.subheader("Ask the Intelligent Production Assistant")

user_question = st.text_input(
    "Enter your question:", 
    placeholder="e.g., What caused the rod pump failure on Well-RP-05?",
    label_visibility="collapsed"
)

if selected_question:
    user_question = selected_question

col1, col2 = st.columns([1, 5])
with col1:
    ask_button = st.button("🔍 Ask IPA", type="primary", use_container_width=True)

if ask_button or selected_question:
    if user_question:
        st.markdown(f"### 💬 {user_question}")
        st.divider()
        
        with st.spinner("🔄 IPA is analyzing your question..."):
            response = run_agent(user_question)
            parsed = process_agent_response(response)
        
        display_response(parsed)
        
    else:
        st.warning("Please enter a question.")

st.divider()

with st.expander("📊 Direct SQL Query"):
    sample_queries = {
        "Active Assets by Type": """SELECT ASSET_TYPE, COUNT(*) as ASSET_COUNT
FROM IPA.SCADA_CORE.ASSET_MASTER
WHERE STATUS = 'ACTIVE'
GROUP BY ASSET_TYPE
ORDER BY ASSET_COUNT DESC""",
        
        "Critical New Alerts": """SELECT ALERT_ID, AGENT_TYPE, ASSET_ID, TITLE, CREATED_AT
FROM IPA.SCADA_CORE.MISSION_CONTROL_ALERTS
WHERE SEVERITY = 'CRITICAL' AND STATUS = 'NEW'
ORDER BY CREATED_AT DESC""",
        
        "Search Documents": """SELECT DOC_ID, DOC_TYPE, ASSET_ID, LEFT(TEXT_CHUNK, 200) as PREVIEW
FROM IPA.KNOWLEDGE_BASE.DOCUMENTS_CHUNKED
WHERE DOC_TYPE = 'Well Failure Analysis'
LIMIT 5"""
    }
    
    selected_sample = st.selectbox("Sample Queries", list(sample_queries.keys()))
    query = st.text_area("SQL Query", value=sample_queries[selected_sample], height=120)
    
    if st.button("Execute Query"):
        with st.spinner("Running..."):
            try:
                result = run_query(query)
                st.success(f"Query returned {len(result)} rows")
                st.dataframe(result, use_container_width=True)
            except Exception as e:
                st.error(f"Query error: {e}")

with st.expander("ℹ️ About IPA Agent"):
    st.markdown("""
    **Agent:** `IPA.APP.IPA_AGENT`
    
    **Tools Available:**
    | Tool | Description |
    |------|-------------|
    | `production_data` | Query structured data (assets, sensors, alerts, permits) |
    | `docs_search` | Search engineering documents (drilling reports, failure analyses) |
    
    **Capabilities:**
    - 🔍 Root cause analysis combining sensor data with historical documents
    - 🔗 Cross-referencing alerts with maintenance logs
    - ✅ Safety compliance checking against work permits
    """)
