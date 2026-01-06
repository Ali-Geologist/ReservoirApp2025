import streamlit as st
import pandas as pd
import io
import re
from datetime import datetime

# --- Helper Functions ---

def normalize_text(text):
    """
    Standardizes well names for comparison.
    Example: "EW-05" -> "ew5", "Well #5" -> "well5"
    """
    if pd.isna(text) or text == "":
        return ""
    text = str(text).lower()
    # Remove special characters (-, _, space, .)
    clean = re.sub(r'[\W_]+', '', text)
    return clean

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
    """Extracts data with advanced header detection."""
    try:
        if file.name.endswith('.csv'):
            df_raw = pd.read_csv(file, header=None)
        else:
            df_raw = pd.read_excel(file, header=None)
    except Exception as e:
        st.error(f"Error reading source file: {e}")
        return None

    # Mapping of Standard Names to Report Synonyms
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
        st.error("Could not find a valid header row in the Daily Report.")
        return None

    # Check for 2-Row Header (e.g. Row 1: "Prod.", Row 2: "Time")
    is_multi_row = False
    if header_idx + 1 < len(df_raw):
        row2_str = df_raw.iloc[header_idx + 1].astype(str).str.lower().str.cat()
        if any(x in row2_str for x in ['gas', 'bbl', 'time', 'water']):
            is_multi_row = True

    # Prepare DataFrame with correct columns
    if is_multi_row:
        # Merge two rows
        r1 = df_raw.iloc[header_idx].astype(str).replace('nan', '')
        r2 = df_raw.iloc[header_idx + 1].astype(str).replace('nan', '')
        df_raw.columns = (r1 + " " + r2).str.strip().str.lower()
        df_data = df_raw.iloc[header_idx + 2:].reset_index(drop=True)
    else:
        df_raw.columns = df_raw.iloc[header_idx].astype(str).str.strip().str.lower()
        df_data = df_raw.iloc[header_idx + 1:].reset_index(drop=True)

    extracted_data = {}
    
    # Extract Columns
    for key, synonyms in target_columns.items():
        found_col = None
        for col in df_data.columns:
            if any(syn in col for syn in synonyms):
                found_col = col
                break
        
        if found_col:
            # Handle duplicate columns
            col_data = df_data[found_col]
            if isinstance(col_data, pd.DataFrame):
                col_data = col_data.iloc[:, 0]
            extracted_data[key] = col_data
        else:
            extracted_data[key] = [None] * len(df_data)

    final_df = pd.DataFrame(extracted_data)
    
    # Filter junk rows
    final_df = final_df.dropna(subset=['well_name'])
    final_df = final_df[~final_df['well_name'].astype(str).str.lower().str.contains('well', na=False)]
    
    # Generate Matching Key
    final_df['well_key'] = final_df['well_name'].apply(normalize_text)
    
    return final_df

def process_and_match(template_file, source_df, report_date):
    """Matches wells and creates the output file."""
    try:
        if template_file.name.endswith('.csv'):
            df_tmpl = pd.read_csv(template_file, header=None)
        else:
            df_tmpl = pd.read_excel(template_file, header=None)
    except Exception as e:
        st.error(f"Error reading Master Sheet: {e}")
        return None, None, None

    # Get Wells from Row 0 (Horizontal)
    well_row = df_tmpl.iloc[0]
    param_row = df_tmpl.iloc[1]
    
    # Debugging: Collect Target Wells
    target_wells_found = []
    
    # Create new data row
    new_row = [None] * len(df_tmpl.columns)
    new_row[0] = report_date
    
    current_well_key = None
    matched_wells = set()

    for i in range(1, len(df_tmpl.columns)):
        # Check for Well Name in Row 0
        val = str(well_row.iloc[i])
        if val and val.lower() != 'nan' and val.strip() != '':
            current_well_key = normalize_text(val)
            target_wells_found.append(val) # For display
        
        if not current_well_key:
            continue
            
        # Check Parameter in Row 1
        param_val = str(param_row.iloc[i])
        param_key = normalize_text(param_val)
        
        # Determine Source Field
        source_col = None
        if 'whfp' in param_key: source_col = 'whfp'
        elif 'choke' in param_key: source_col = 'choke'
        elif 'flp' in param_key: source_col = 'flp'
        elif 'gas' in param_key: source_col = 'gas'
        elif 'cond' in param_key: source_col = 'condensate'
        elif 'water' in param_key: source_col = 'water'
        elif 'time' in param_key or 'hours' in param_key: source_col = 'prod_time'
        elif 'salin' in param_key: source_col = 'salinity'
        
        if source_col:
            # Look up in Source Data
            match = source_df[source_df['well_key'] == current_well_key]
            if not match.empty:
                val = match.iloc[0][source_col]
                if pd.notna(val):
                    new_row[i] = val
                    matched_wells.add(current_well_key)

    # Append
    new_row_df = pd.DataFrame([new_row], columns=df_tmpl.columns)
    final_df = pd.concat([df_tmpl, new_row_df], ignore_index=True)
    
    return final_df, list(matched_wells), target_wells_found

# --- Streamlit App ---
st.set_page_config(page_title="Production Data Matcher", layout="wide")

st.title("🛢️ Production Data Matcher & Patcher")
st.markdown("Upload your Daily Report and Master History Sheet to sync data.")

col1, col2 = st.columns(2)
source_file = col1.file_uploader("1. Daily Report (Source)", type=["xlsx", "csv"])
template_file = col2.file_uploader("2. Master Sheet (Target)", type=["xlsx", "csv"])
report_date = st.date_input("Report Date", datetime.today())

if st.button("Run Matcher") and source_file and template_file:
    st.divider()
    
    # 1. Extract Source
    source_df = extract_production_data(source_file)
    
    if source_df is not None:
        # 2. Process Target & Match
        final_df, matched, target_wells = process_and_match(template_file, source_df, report_date)
        
        if final_df is not None:
            
            # --- DIAGNOSTIC REPORT ---
            st.subheader("🔎 Match Diagnostics")
            
            # Get Source Well Names
            source_wells = source_df['well_name'].unique().tolist()
            source_keys = source_df['well_key'].unique().tolist()
            
            # 1. Show Wells found in Daily Report
            c1, c2, c3 = st.columns(3)
            with c1:
                st.info(f"Daily Report: Found {len(source_wells)} wells")
                st.write(source_wells)
                
            # 2. Show Wells found in Master Sheet
            with c2:
                # Filter unique target wells for display
                unique_target = sorted(list(set(target_wells)))
                st.info(f"Master Sheet: Found {len(unique_target)} wells (Row 1)")
                st.write(unique_target)

            # 3. Show Matches
            with c3:
                if len(matched) > 0:
                    st.success(f"✅ Matched: {len(matched)} wells")
                    st.write(matched)
                else:
                    st.error("❌ Matched: 0 wells")
                    st.write("Reason: The 'Well Keys' (normalized names) did not overlap.")
                    st.write("Check if names are spelled differently (e.g. 'EW-01' vs 'EW 1').")

            st.divider()
            
            if len(matched) > 0:
                st.subheader("✅ Updated Master Sheet Preview")
                st.dataframe(final_df.tail(3))
                
                # Download
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                    final_df.to_excel(writer, index=False, header=False, sheet_name='Daily Production')
                
                st.download_button(
                    "📥 Download Updated Master Excel",
                    data=buffer.getvalue(),
                    file_name=f"Updated_History_{report_date}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
