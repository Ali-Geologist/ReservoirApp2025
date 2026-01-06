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

# Custom CSS to make it look professional
st.markdown("""
    <style>
    .main {
        background-color: #f9f9f9;
    }
    .stButton>button {
        width: 100%;
        background-color: #0068c9;
        color: white;
        font-weight: bold;
        border-radius: 8px;
        height: 3em;
    }
    .stButton>button:hover {
        background-color: #004b91;
        color: white;
    }
    div[data-testid="stMetricValue"] {
        font-size: 1.2rem;
    }
    .reportview-container .main .block-container {
        padding-top: 2rem;
    }
    h1 {
        color: #0f2937;
    }
    h2, h3 {
        color: #0068c9;
    }
    </style>
""", unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# 2. CORE LOGIC (HELPER FUNCTIONS)
# -----------------------------------------------------------------------------

@st.cache_data(show_spinner=False)
def normalize_text(text):
    """Standardizes text for comparison (removes spaces, symbols, lowercase)."""
    if pd.isna(text) or text == "":
        return ""
    text = str(text).lower()
    return re.sub(r'[\W_]+', '', text)

def find_header_row(df, key_terms):
    """Finds the row index that looks most like a header."""
    df_str = df.astype(str).apply(lambda x: x.str.lower())
    best_idx = None
    max_matches = 0
    
    # Scan first 20 rows
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

    # Find Parameter Row
    param_keywords = ['whfp', 'gas rate', 'choke', 'flp', 'condensate']
    param_row_idx = find_header_row(df_tmpl, param_keywords)
    
    if param_row_idx is None:
        return None, None, None, "Could not find parameter row (WHFP, Gas Rate) in Master Sheet."
    
    well_row_idx = param_row_idx - 1
    if well_row_idx < 0:
        return None, None, None, "Found parameters on Row 1, but expected Well Names above it."

    well_row = df_tmpl.iloc[well_row_idx]
    param_row = df_tmpl.iloc[param_row_idx]
    
    # Prepare Output
    new_row = [None] * len(df_tmpl.columns)
    
    # Date logic: Put date in first column or find 'Date' column
    date_col_idx = 0 
    for i in range(len(df_tmpl.columns)):
        if "date" in str(param_row.iloc[i]).lower():
            date_col_idx = i
            break
    new_row[date_col_idx] = report_date
    
    current_well_key = None
    matched_wells = set()
    target_wells_debug = []

    # Map Data
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
    **Step 1:** Upload your **Daily Production Report** (the raw source).
    
    **Step 2:** Upload your **Master History Sheet** (the target).
    
    **Step 3:** Select the **Report Date**.
    
    **Step 4:** Click **Process & Update**.
    """)
    st.info("💡 The tool automatically aligns wells even if column orders change.")
    st.divider()
    st.caption("v2.1 | Engineering Dept")

# -----------------------------------------------------------------------------
# 4. MAIN INTERFACE
# -----------------------------------------------------------------------------

# Header
col_h1, col_h2 = st.columns([3, 1])
with col_h1:
    st.title("Production Data Uploader")
    st.markdown("### Automate your daily reservoir data entry")

with col_h2:
    report_date = st.date_input("📅 Select Report Date", datetime.today())

st.divider()

# File Upload Section
c1, c2 = st.columns(2)

with c1:
    st.subheader("1. Source Data")
    source_file = st.file_uploader(
        "Upload Daily Production Report (Excel/CSV)", 
        type=["xlsx", "xls", "csv"], 
        help="The daily file from the field."
    )

with c2:
    st.subheader("2. Target Template")
    template_file = st.file_uploader(
        "Upload Master History Sheet (Excel/CSV)", 
        type=["xlsx", "xls", "csv"],
        help="The large historical file where data should be appended."
    )

# Process Button
if source_file and template_file:
    st.write("") # Spacer
    if st.button("🚀 Process & Update History"):
        
        # --- PHASE 1: EXTRACTION ---
        with st.status("Reading files...", expanded=True) as status:
            st.write("Extracting data from Daily Report...")
            source_df, error_msg = extract_production_data(source_file)
            
            if error_msg:
                status.update(label="Extraction Failed", state="error")
                st.error(error_msg)
                st.stop()
                
            st.write(f"✅ Found data for **{len(source_df)} wells** in Daily Report.")
            
            # --- PHASE 2: MATCHING ---
            st.write("Mapping wells to Master Sheet...")
            final_df, matched, target_wells_debug, match_err = process_and_match(template_file, source_df, report_date)
            
            if match_err:
                status.update(label="Matching Failed", state="error")
                st.error(match_err)
                st.stop()
                
            status.update(label="Processing Complete!", state="complete", expanded=False)

        # --- RESULTS DISPLAY ---
        
        # Summary Metrics
        m1, m2, m3 = st.columns(3)
        m1.metric("Daily Wells Found", len(source_df))
        m2.metric("Matches in History", len(matched))
        m3.metric("Success Rate", f"{int(len(matched)/len(source_df)*100)}%")

        if len(matched) == 0:
            st.error("⚠️ **0 Matches Found!** The app could not link any wells.")
            st.warning("Check the 'Diagnostics' tab below to see name discrepancies.")
        else:
            st.success(f"Successfully prepared update for **{report_date.strftime('%Y-%m-%d')}**")

        # Tabs for details
        tab1, tab2 = st.tabs(["📥 Download & Preview", "🔍 Diagnostics"])
        
        with tab1:
            st.subheader("Updated History Preview")
            st.dataframe(final_df.tail(3), use_container_width=True)
            
            # Excel Download
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                final_df.to_excel(writer, index=False, header=False, sheet_name='Daily Production')
            
            st.download_button(
                label="📥 Download Updated Master Excel",
                data=buffer.getvalue(),
                file_name=f"Updated_History_{report_date}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary"
            )

        with tab2:
            st.markdown("Use this to debug why certain wells might not be matching.")
            d1, d2 = st.columns(2)
            with d1:
                st.write("**Wells in Daily Report (Source)**")
                st.dataframe(source_df[['well_name', 'well_key']], hide_index=True, use_container_width=True)
            with d2:
                st.write("**Wells in Master Sheet (Target)**")
                # Clean up list for display
                unique_target = sorted(list(set([x.split(" -> ")[0] for x in target_wells_debug])))
                st.dataframe(pd.DataFrame(unique_target, columns=["Well Names Found"]), hide_index=True, use_container_width=True)

else:
    # Placeholder when no files are uploaded
    if not source_file and not template_file:
        st.info("👋 Welcome! Please upload your files above to get started.")
