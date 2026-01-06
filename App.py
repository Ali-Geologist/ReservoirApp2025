import streamlit as st
import pandas as pd
import io
import re
from datetime import datetime

# -----------------------------------------------------------------------------
# 1. APP CONFIGURATION & STYLING
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Production Data Uploader",
    page_icon="🛢️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main { background-color: #f9f9f9; }
    .stButton>button {
        width: 100%;
        background-color: #0068c9;
        color: white;
        font-weight: bold;
        border-radius: 8px;
        height: 3em;
    }
    .stButton>button:hover { background-color: #004b91; color: white; }
    div[data-testid="stMetricValue"] { font-size: 1.2rem; }
    h1 { color: #0f2937; }
    h2, h3 { color: #0068c9; }
    </style>
""", unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# 2. CORE LOGIC (HELPER FUNCTIONS)
# -----------------------------------------------------------------------------

@st.cache_data(show_spinner=False)
def normalize_text(text):
    """Standardizes text for comparison."""
    if pd.isna(text) or text == "":
        return ""
    text = str(text).lower()
    return re.sub(r'[\W_]+', '', text)

def find_header_row(df, key_terms):
    """Finds the row index that looks most like a header."""
    df_str = df.astype(str).apply(lambda x: x.str.lower())
    best_idx = None
    max_matches = 0
    
    for idx, row in df_str.iloc[:20].iterrows():
        matches = sum(1 for term in key_terms if row.str.contains(term, regex=False).any())
        if matches > max_matches:
            max_matches = matches
            best_idx = idx
            
    return best_idx if max_matches >= 2 else None

def extract_production_data(file):
    """Extracts data from the Daily Report (Source)."""
    try:
        if file.name.endswith('.csv'):
            df_raw = pd.read_csv(file, header=None)
        else:
            df_raw = pd.read_excel(file, header=None)
    except Exception as e:
        return None, f"Error reading file: {e}"

    target_columns = {
        'well_name': ['well no', 'well name', 'well', 'name'],
        'whfp': ['whfp', 'tubing pressure', 'whp', 'thp'],
        'choke': ['choke', '/64', 'bean'],
        'flp': ['flp', 'flowline', 'line press'],
        'prod_time': ['prod. time', 'hours', 'on stream', 'runtime', 'duration', 'time'],
        'gas': ['raw gas', 'gas rate', 'mmscfd', 'gas'],
        'condensate': ['raw cond', 'condensate', 'bbl/d', 'cond', 'oil'],
        'water': ['raw water', 'water rate', 'wc', 'water'],
        'salinity': ['salinity', 'ppm', 'salt']
    }
    
    all_terms = [t for sub in target_columns.values() for t in sub]
    header_idx = find_header_row(df_raw, all_terms)
    
    if header_idx is None:
        return None, "Could not detect a valid header row. Check file format."

    # Handle 2-Row Header (Row 1: "Prod.", Row 2: "Time")
    is_multi_row = False
    if header_idx + 1 < len(df_raw):
        row2_str = df_raw.iloc[header_idx + 1].astype(str).str.lower().str.cat()
        if any(x in row2_str for x in ['gas', 'bbl', 'time', 'water']):
            is_multi_row = True

    if is_multi_row:
        r1 = df_raw.iloc[header_idx].astype(str).replace('nan', '')
        r2 = df_raw.iloc[header_idx + 1].astype(str).replace('nan', '')
        df_raw.columns = (r1 + " " + r2).str.strip().str.lower()
        df_data = df_raw.iloc[header_idx + 2:].reset_index(drop=True)
    else:
        df_raw.columns = df_raw.iloc[header_idx].astype(str).str.strip().str.lower()
        df_data = df_raw.iloc[header_idx + 1:].reset_index(drop=True)

    extracted_data = {}
    for key, synonyms in target_columns.items():
        found_col = None
        for col in df_data.columns:
            if any(syn in col for syn in synonyms):
                found_col = col
                break
        
        if found_col:
            col_data = df_data[found_col]
            if isinstance(col_data, pd.DataFrame):
                col_data = col_data.iloc[:, 0]
            extracted_data[key] = col_data
        else:
            extracted_data[key] = [None] * len(df_data)

    final_df = pd.DataFrame(extracted_data)
    final_df = final_df.dropna(subset=['well_name'])
    final_df = final_df[~final_df['well_name'].astype(str).str.lower().str.contains('well', na=False)]
    final_df['well_key'] = final_df['well_name'].apply(normalize_text)
    
    return final_df, None

def process_and_match(template_file, source_df, report_date):
    """Matches data to the Master History Sheet."""
    try:
        if template_file.name.endswith('.csv'):
            df_tmpl = pd.read_csv(template_file, header=None)
        else:
            df_tmpl = pd.read_excel(template_file, header=None)
    except Exception as e:
        return None, None, None, f"Error reading Master Sheet: {e}"

    param_keywords = ['whfp', 'gas rate', 'choke', 'flp', 'condensate']
    param_row_idx = find_header_row(df_tmpl, param_keywords)
    
    if param_row_idx is None:
        return None, None, None, "Could not find parameter row (WHFP, Gas Rate) in Master Sheet."
    
    well_row_idx = param_row_idx - 1
    if well_row_idx < 0:
        return None, None, None, "Found parameters on Row 1, but expected Well Names above it."

    well_row = df_tmpl.iloc[well_row_idx]
    param_row = df_tmpl.iloc[param_row_idx]
    
    new_row = [None] * len(df_tmpl.columns)
    
    # Date logic: Ensure it's a string to prevent PyArrow Date/String mismatch errors
    date_col_idx = 0 
    for i in range(len(df_tmpl.columns)):
        if "date" in str(param_row.iloc[i]).lower():
            date_col_idx = i
            break
    
    # Use formatted string for date to avoid type conflicts
    new_row[date_col_idx] = report_date.strftime('%Y-%m-%d')
    
    current_well_key = None
    matched_wells = set()
    target_wells_debug = []

    for i in range(0, len(df_tmpl.columns)):
        if i == date_col_idx: continue

        val = str(well_row.iloc[i])
        if val and val.lower() != 'nan' and val.strip() != '':
            current_well_key = normalize_text(val)
            target_wells_debug.append(f"{val} -> {current_well_key}")
        
        if not current_well_key:
            continue
            
        param_val = str(param_row.iloc[i])
        param_key = normalize_text(param_val)
        
        source_col = None
        if 'whfp' in param_key: source_col = 'whfp'
        elif 'choke' in param_key: source_col = 'choke'
        elif 'flp' in param_key: source_col = 'flp'
        elif 'gas' in param_key: source_col = 'gas'
        elif 'cond' in param_key: source_col = 'condensate'
        elif 'water' in param_key: source_col = 'water'
        elif 'time' in param_key or 'hours' in param_key or 'duration' in param_key: source_col = 'prod_time'
        elif 'salin' in param_key: source_col = 'salinity'
        
        if source_col:
            match = source_df[source_df['well_key'] == current_well_key]
            if not match.empty:
                val = match.iloc[0][source_col]
                if pd.notna(val):
                    new_row[i] = val
                    matched_wells.add(current_well_key)

    new_row_df = pd.DataFrame([new_row], columns=df_tmpl.columns)
    final_df = pd.concat([df_tmpl, new_row_df], ignore_index=True)
    
    return final_df, list(matched_wells), target_wells_debug, None


# -----------------------------------------------------------------------------
# 3. SIDEBAR & INSTRUCTIONS
# -----------------------------------------------------------------------------
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/2821/2821637.png", width=80)
    st.header("Instructions")
    st.markdown("""
    **Step 1:** Upload your **Daily Production Report** (Source).
    **Step 2:** Upload your **Master History Sheet** (Target).
    **Step 3:** Select Date & **Process**.
    """)
    st.info("💡 The tool handles mixed headers automatically.")
    st.divider()
    st.caption("v2.2 | Production Dept")

# -----------------------------------------------------------------------------
# 4. MAIN INTERFACE
# -----------------------------------------------------------------------------

col_h1, col_h2 = st.columns([3, 1])
with col_h1:
    st.title("Production Data Uploader")
    st.markdown("### Automate your daily reservoir data entry")
with col_h2:
    report_date = st.date_input("📅 Select Report Date", datetime.today())

st.divider()

c1, c2 = st.columns(2)
with c1:
    source_file = st.file_uploader("1. Source Data (Daily Report)", type=["xlsx", "xls", "csv"])
with c2:
    template_file = st.file_uploader("2. Target Data (Master History)", type=["xlsx", "xls", "csv"])

if source_file and template_file:
    st.write("") 
    if st.button("🚀 Process & Update History"):
        
        with st.status("Processing Data...", expanded=True) as status:
            st.write("Extracting Daily Report...")
            source_df, error_msg = extract_production_data(source_file)
            
            if error_msg:
                status.update(label="Extraction Failed", state="error")
                st.error(error_msg)
                st.stop()
                
            st.write(f"✅ Extracted **{len(source_df)} wells**.")
            st.write("Matching to Master Sheet...")
            final_df, matched, target_wells_debug, match_err = process_and_match(template_file, source_df, report_date)
            
            if match_err:
                status.update(label="Matching Failed", state="error")
                st.error(match_err)
                st.stop()
                
            status.update(label="Complete!", state="complete", expanded=False)

        # --- METRICS ---
        m1, m2, m3 = st.columns(3)
        m1.metric("Daily Wells Found", len(source_df))
        m2.metric("Matches in History", len(matched))
        rate = int(len(matched)/len(source_df)*100) if len(source_df) > 0 else 0
        m3.metric("Match Rate", f"{rate}%")

        if len(matched) == 0:
            st.error("⚠️ 0 Matches Found. Check 'Diagnostics' tab.")
        else:
            st.success(f"Ready to update for **{report_date.strftime('%Y-%m-%d')}**")

        # --- TABS ---
        tab1, tab2 = st.tabs(["📥 Download & Preview", "🔍 Diagnostics"])
        
        with tab1:
            st.subheader("Preview (Last 3 Rows)")
            # FIX: Convert to string for display to prevent PyArrow TypeError on mixed types
            st.dataframe(final_df.tail(3).astype(str), use_container_width=True)
            
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                # Write original data (numbers preserved) to Excel
                final_df.to_excel(writer, index=False, header=False, sheet_name='Daily Production')
            
            st.download_button(
                label="📥 Download Updated Master Excel",
                data=buffer.getvalue(),
                file_name=f"Updated_History_{report_date.strftime('%Y-%m-%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary"
            )

        with tab2:
            d1, d2 = st.columns(2)
            with d1:
                st.write("**Source Wells (Daily)**")
                st.dataframe(source_df[['well_name', 'well_key']], hide_index=True, use_container_width=True)
            with d2:
                st.write("**Target Wells (Master)**")
                unique_target = sorted(list(set([x.split(" -> ")[0] for x in target_wells_debug])))
                st.dataframe(pd.DataFrame(unique_target, columns=["Well Names Found"]), hide_index=True, use_container_width=True)

else:
    if not source_file and not template_file:
        st.info("👋 Upload both files to begin.")
