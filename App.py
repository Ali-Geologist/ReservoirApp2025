import streamlit as st
import pandas as pd
import io
import re
from datetime import datetime

# --- Helper Functions ---

def normalize_text(text):
    """
    Standardizes text for comparison.
    Example: "EW-05" -> "ew5", "Well #5" -> "well5", "SW EW-1" -> "swew1"
    """
    if pd.isna(text) or text == "":
        return ""
    text = str(text).lower()
    # Remove all non-alphanumeric characters (spaces, dashes, underscores)
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
            
    # We need at least 2 keyword matches to consider it a header row
    return best_idx if max_matches >= 2 else None

def extract_production_data(file):
    """Extracts data from the Daily Report (Source)."""
    try:
        if file.name.endswith('.csv'):
            df_raw = pd.read_csv(file, header=None)
        else:
            df_raw = pd.read_excel(file, header=None)
    except Exception as e:
        st.error(f"Error reading source file: {e}")
        return None

    # Synonyms for Daily Report Columns
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
    
    return final_df

def process_and_match(template_file, source_df, report_date):
    """Matches data to the Master History Sheet using Dynamic Row Detection."""
    try:
        if template_file.name.endswith('.csv'):
            df_tmpl = pd.read_csv(template_file, header=None)
        else:
            df_tmpl = pd.read_excel(template_file, header=None)
    except Exception as e:
        st.error(f"Error reading Master Sheet: {e}")
        return None, None, None, None

    # --- FIX: Dynamically find the Parameter Row in Master Sheet ---
    # We look for the row containing "WHFP", "Gas Rate", "Choke" etc.
    # The Well Names are assumed to be in the row ABOVE this one.
    
    param_keywords = ['whfp', 'gas rate', 'choke', 'flp', 'condensate']
    param_row_idx = find_header_row(df_tmpl, param_keywords)
    
    if param_row_idx is None:
        st.error("Could not identify the parameter row (WHFP, Choke, etc.) in the Master Sheet.")
        return None, None, None, None
    
    # Well Name Row is the one immediately above the parameter row
    well_row_idx = param_row_idx - 1
    if well_row_idx < 0:
        st.error("Found parameters on the first row, but need a Well Name row above it.")
        return None, None, None, None

    # Info for user
    # st.info(f"Detected Master Sheet Structure: Well Names on Row {well_row_idx + 1}, Parameters on Row {param_row_idx + 1}")

    well_row = df_tmpl.iloc[well_row_idx]
    param_row = df_tmpl.iloc[param_row_idx]
    
    # Prepare Output
    new_row = [None] * len(df_tmpl.columns)
    
    # Identify Date Column (usually Col 0)
    date_col_idx = 0 
    new_row[date_col_idx] = report_date
    
    current_well_key = None
    matched_wells = set()
    target_wells_debug = []

    # Iterate Columns
    for i in range(0, len(df_tmpl.columns)):
        if i == date_col_idx: continue

        # 1. Check for Well Name (Horizontal Merged Cells Logic)
        val = str(well_row.iloc[i])
        if val and val.lower() != 'nan' and val.strip() != '':
            # Found a new well name
            current_well_key = normalize_text(val)
            target_wells_debug.append(f"{val} -> {current_well_key}")
        
        if not current_well_key:
            continue
            
        # 2. Check Parameter Name
        param_val = str(param_row.iloc[i])
        param_key = normalize_text(param_val)
        
        # 3. Map to Source Data
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
            # Find match in Source DF
            match = source_df[source_df['well_key'] == current_well_key]
            if not match.empty:
                val = match.iloc[0][source_col]
                if pd.notna(val):
                    new_row[i] = val
                    matched_wells.add(current_well_key)

    # Append new row
    new_row_df = pd.DataFrame([new_row], columns=df_tmpl.columns)
    final_df = pd.concat([df_tmpl, new_row_df], ignore_index=True)
    
    return final_df, list(matched_wells), target_wells_debug, well_row_idx

# --- Streamlit UI ---
st.set_page_config(page_title="Production Data Matcher", layout="wide")

st.title("🛢️ Production Data Matcher")

col1, col2 = st.columns(2)
source_file = col1.file_uploader("1. Daily Report (Source)", type=["xlsx", "csv"])
template_file = col2.file_uploader("2. Master Sheet (Target)", type=["xlsx", "csv"])
report_date = st.date_input("Report Date", datetime.today())

if st.button("Run Matcher") and source_file and template_file:
    st.divider()
    
    # 1. Extract Source
    source_df = extract_production_data(source_file)
    
    if source_df is not None:
        # 2. Match Target
        final_df, matched, target_wells_debug, row_idx = process_and_match(template_file, source_df, report_date)
        
        if final_df is not None:
            
            # --- DIAGNOSTICS ---
            st.subheader("🔎 Diagnostics")
            c1, c2 = st.columns(2)
            
            with c1:
                st.write(f"**Daily Report Wells (Source) - {len(source_df)} found:**")
                st.write(source_df['well_name'].unique())
                
            with c2:
                # Deduplicate target wells for display
                unique_target = sorted(list(set([x.split(" -> ")[0] for x in target_wells_debug])))
                st.write(f"**Master Sheet Wells (Target) - {len(unique_target)} found:**")
                st.write(unique_target)

            if len(matched) > 0:
                st.success(f"✅ Matched {len(matched)} wells successfully!")
            else:
                st.error("❌ 0 Matches Found.")
                st.warning("Please check the 'Diagnostics' lists above. The spelling must match exactly (ignoring spaces/dashes).")

            # --- DOWNLOAD ---
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                final_df.to_excel(writer, index=False, header=False, sheet_name='Daily Production')
            
            st.download_button(
                "📥 Download Updated Master Excel",
                data=buffer.getvalue(),
                file_name=f"Updated_History_{report_date}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
