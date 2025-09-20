import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
import json
import requests
from datetime import datetime, timedelta
import sqlite3
from io import StringIO, BytesIO
import warnings
import threading
import queue
warnings.filterwarnings('ignore')

# Page configuration
st.set_page_config(
    page_title="Real-time Data Processing Pipeline",
    page_icon="⚙️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Professional CSS styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        text-align: center;
        margin-bottom: 2rem;
        color: #2c3e50;
        border-bottom: 3px solid #3498db;
        padding-bottom: 1rem;
    }
    .pipeline-card {
        background: linear-gradient(135deg, #34495e 0%, #2c3e50 100%);
        padding: 1.5rem;
        border-radius: 8px;
        color: white;
        margin: 1rem 0;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .status-running {
        background: linear-gradient(135deg, #27ae60 0%, #2ecc71 100%);
        animation: pulse 2s infinite;
    }
    .status-stopped {
        background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%);
    }
    .status-processing {
        background: linear-gradient(135deg, #f39c12 0%, #e67e22 100%);
        animation: spin 2s infinite;
    }
    @keyframes pulse {
        0% { transform: scale(1); }
        50% { transform: scale(1.02); }
        100% { transform: scale(1); }
    }
    @keyframes spin {
        0% { transform: rotate(0deg); }
        100% { transform: rotate(360deg); }
    }
    .data-source-card {
        background-color: #ecf0f1;
        padding: 1rem;
        border-radius: 6px;
        border-left: 4px solid #3498db;
        margin: 0.5rem 0;
    }
    .processing-step {
        background: linear-gradient(135deg, #bdc3c7 0%, #95a5a6 100%);
        padding: 1rem;
        border-radius: 6px;
        margin: 0.5rem 0;
        border-left: 4px solid #34495e;
    }
    .metric-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 6px;
        text-align: center;
        border: 1px solid #dee2e6;
        margin: 0.5rem 0;
    }
    .metric-value {
        font-size: 1.5rem;
        font-weight: bold;
        color: #2c3e50;
    }
    .metric-label {
        color: #7f8c8d;
        font-size: 0.9rem;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'pipeline_running' not in st.session_state:
    st.session_state.pipeline_running = False

if 'processed_data' not in st.session_state:
    st.session_state.processed_data = pd.DataFrame()

if 'processing_log' not in st.session_state:
    st.session_state.processing_log = []

if 'data_sources' not in st.session_state:
    st.session_state.data_sources = {
        'csv_files': [],
        'api_endpoints': [],
        'database_connections': []
    }

if 'pipeline_config' not in st.session_state:
    st.session_state.pipeline_config = {
        'batch_size': 1000,
        'processing_interval': 5,
        'auto_clean': True,
        'data_validation': True,
        'error_handling': 'skip'
    }

# Data generation and processing functions
def generate_sample_streaming_data(n_records=100):
    """Generate sample streaming data"""
    np.random.seed(int(time.time()) % 1000)
    
    data = {
        'timestamp': [datetime.now() + timedelta(seconds=i) for i in range(n_records)],
        'user_id': [f"USER_{np.random.randint(1000, 9999)}" for _ in range(n_records)],
        'event_type': np.random.choice(['login', 'purchase', 'view', 'click', 'logout'], n_records),
        'amount': np.random.exponential(50, n_records),
        'category': np.random.choice(['electronics', 'clothing', 'books', 'food', 'sports'], n_records),
        'location': np.random.choice(['US', 'EU', 'ASIA', 'OTHER'], n_records),
        'device': np.random.choice(['mobile', 'desktop', 'tablet'], n_records),
        'session_duration': np.random.normal(300, 100, n_records),
        'page_views': np.random.poisson(5, n_records),
        'conversion_flag': np.random.choice([0, 1], n_records, p=[0.8, 0.2])
    }
    
    df = pd.DataFrame(data)
    df['amount'] = np.clip(df['amount'], 0, 1000)
    df['session_duration'] = np.clip(df['session_duration'], 10, 3600)
    
    return df

def validate_data(df):
    """Data validation function"""
    validation_results = {
        'total_records': len(df),
        'null_values': df.isnull().sum().sum(),
        'duplicate_records': df.duplicated().sum(),
        'data_types_valid': True,
        'outliers_detected': 0,
        'validation_passed': True
    }
    
    # Check for outliers in numerical columns
    numerical_cols = ['amount', 'session_duration', 'page_views']
    for col in numerical_cols:
        if col in df.columns:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            outliers = ((df[col] < lower_bound) | (df[col] > upper_bound)).sum()
            validation_results['outliers_detected'] += outliers
    
    # Check if validation passed
    if (validation_results['null_values'] > len(df) * 0.1 or 
        validation_results['duplicate_records'] > len(df) * 0.05):
        validation_results['validation_passed'] = False
    
    return validation_results

def clean_data(df):
    """Data cleaning function"""
    df_cleaned = df.copy()
    
    # Remove duplicates
    df_cleaned = df_cleaned.drop_duplicates()
    
    # Handle missing values
    for col in df_cleaned.columns:
        if df_cleaned[col].dtype in ['float64', 'int64']:
            df_cleaned[col] = df_cleaned[col].fillna(df_cleaned[col].median())
        else:
            df_cleaned[col] = df_cleaned[col].fillna(df_cleaned[col].mode()[0] if len(df_cleaned[col].mode()) > 0 else 'Unknown')
    
    # Remove outliers
    numerical_cols = ['amount', 'session_duration', 'page_views']
    for col in numerical_cols:
        if col in df_cleaned.columns:
            Q1 = df_cleaned[col].quantile(0.25)
            Q3 = df_cleaned[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            df_cleaned = df_cleaned[(df_cleaned[col] >= lower_bound) & (df_cleaned[col] <= upper_bound)]
    
    return df_cleaned

def transform_data(df):
    """Data transformation function"""
    df_transformed = df.copy()
    
    # Create derived features
    df_transformed['hour'] = pd.to_datetime(df_transformed['timestamp']).dt.hour
    df_transformed['day_of_week'] = pd.to_datetime(df_transformed['timestamp']).dt.dayofweek
    df_transformed['is_weekend'] = df_transformed['day_of_week'].isin([5, 6]).astype(int)
    
    # Create engagement score
    df_transformed['engagement_score'] = (
        df_transformed['session_duration'] / 60 * 0.3 +
        df_transformed['page_views'] * 0.4 +
        df_transformed['conversion_flag'] * 50 * 0.3
    )
    
    # Categorize amounts
    df_transformed['amount_category'] = pd.cut(
        df_transformed['amount'],
        bins=[0, 20, 50, 100, float('inf')],
        labels=['Low', 'Medium', 'High', 'Premium']
    )
    
    # Create user segments
    conditions = [
        (df_transformed['engagement_score'] >= 75),
        (df_transformed['engagement_score'] >= 50),
        (df_transformed['engagement_score'] >= 25)
    ]
    choices = ['High Value', 'Medium Value', 'Low Value']
    df_transformed['user_segment'] = np.select(conditions, choices, default='Inactive')
    
    return df_transformed

def process_pipeline_step(df, step_name):
    """Process a single pipeline step"""
    try:
        start_time = time.time()
        
        if step_name == "validation":
            result = validate_data(df)
            processing_time = time.time() - start_time
            
            log_entry = {
                'timestamp': datetime.now(),
                'step': step_name,
                'status': 'success' if result['validation_passed'] else 'warning',
                'records_in': len(df),
                'records_out': len(df),
                'processing_time': processing_time,
                'details': f"Validation: {result['validation_passed']}, Nulls: {result['null_values']}, Duplicates: {result['duplicate_records']}"
            }
            
            return df, log_entry
            
        elif step_name == "cleaning":
            df_cleaned = clean_data(df)
            processing_time = time.time() - start_time
            
            log_entry = {
                'timestamp': datetime.now(),
                'step': step_name,
                'status': 'success',
                'records_in': len(df),
                'records_out': len(df_cleaned),
                'processing_time': processing_time,
                'details': f"Cleaned: {len(df) - len(df_cleaned)} records removed"
            }
            
            return df_cleaned, log_entry
            
        elif step_name == "transformation":
            df_transformed = transform_data(df)
            processing_time = time.time() - start_time
            
            log_entry = {
                'timestamp': datetime.now(),
                'step': step_name,
                'status': 'success',
                'records_in': len(df),
                'records_out': len(df_transformed),
                'processing_time': processing_time,
                'details': f"Added {len(df_transformed.columns) - len(df.columns)} new features"
            }
            
            return df_transformed, log_entry
            
    except Exception as e:
        log_entry = {
            'timestamp': datetime.now(),
            'step': step_name,
            'status': 'error',
            'records_in': len(df),
            'records_out': 0,
            'processing_time': time.time() - start_time,
            'details': f"Error: {str(e)}"
        }
        
        return df, log_entry

# Header
st.markdown('<h1 class="main-header">Real-time Data Processing Pipeline</h1>', unsafe_allow_html=True)

# Sidebar - Pipeline Controls
st.sidebar.header("Pipeline Controls")

# Pipeline status
pipeline_status = "Running" if st.session_state.pipeline_running else "Stopped"
status_color = "#27ae60" if st.session_state.pipeline_running else "#e74c3c"
st.sidebar.markdown(f"**Status:** <span style='color: {status_color}'>{pipeline_status}</span>", unsafe_allow_html=True)

# Pipeline control buttons
col1, col2 = st.sidebar.columns(2)

with col1:
    if st.button("Start Pipeline"):
        st.session_state.pipeline_running = True
        st.rerun()

with col2:
    if st.button("Stop Pipeline"):
        st.session_state.pipeline_running = False
        st.rerun()

# Pipeline configuration
st.sidebar.markdown("### Configuration")

batch_size = st.sidebar.slider(
    "Batch Size",
    min_value=100,
    max_value=5000,
    value=st.session_state.pipeline_config['batch_size'],
    step=100
)

processing_interval = st.sidebar.slider(
    "Processing Interval (seconds)",
    min_value=1,
    max_value=30,
    value=st.session_state.pipeline_config['processing_interval']
)

auto_clean = st.sidebar.checkbox(
    "Auto Data Cleaning",
    value=st.session_state.pipeline_config['auto_clean']
)

data_validation = st.sidebar.checkbox(
    "Data Validation",
    value=st.session_state.pipeline_config['data_validation']
)

error_handling = st.sidebar.selectbox(
    "Error Handling",
    ["skip", "stop", "retry"],
    index=["skip", "stop", "retry"].index(st.session_state.pipeline_config['error_handling'])
)

# Update configuration
st.session_state.pipeline_config.update({
    'batch_size': batch_size,
    'processing_interval': processing_interval,
    'auto_clean': auto_clean,
    'data_validation': data_validation,
    'error_handling': error_handling
})

# Main tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs(["Live Pipeline", "Data Sources", "Processing Monitor", "Analytics", "Configuration"])

# Tab 1: Live Pipeline
with tab1:
    st.subheader("Real-time Data Processing Pipeline")
    
    # Pipeline overview
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        status_class = "status-running" if st.session_state.pipeline_running else "status-stopped"
        st.markdown(f"""
        <div class="pipeline-card {status_class}">
            <h3>Pipeline Status</h3>
            <h2>{"Active" if st.session_state.pipeline_running else "Inactive"}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        total_processed = len(st.session_state.processed_data)
        st.markdown(f"""
        <div class="pipeline-card">
            <h3>Records Processed</h3>
            <h2>{total_processed:,}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        success_rate = 0
        if st.session_state.processing_log:
            successful_logs = sum(1 for log in st.session_state.processing_log if log['status'] == 'success')
            success_rate = (successful_logs / len(st.session_state.processing_log)) * 100
        
        st.markdown(f"""
        <div class="pipeline-card">
            <h3>Success Rate</h3>
            <h2>{success_rate:.1f}%</h2>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        avg_processing_time = 0
        if st.session_state.processing_log:
            avg_processing_time = np.mean([log['processing_time'] for log in st.session_state.processing_log])
        
        st.markdown(f"""
        <div class="pipeline-card">
            <h3>Avg Processing Time</h3>
            <h2>{avg_processing_time:.2f}s</h2>
        </div>
        """, unsafe_allow_html=True)
    
    # Real-time processing simulation
    if st.session_state.pipeline_running:
        # Generate and process new data
        with st.spinner("Processing new data batch..."):
            # Generate sample data
            new_data = generate_sample_streaming_data(batch_size)
            
            # Process through pipeline steps
            current_data = new_data.copy()
            
            # Step 1: Validation
            if data_validation:
                current_data, validation_log = process_pipeline_step(current_data, "validation")
                st.session_state.processing_log.append(validation_log)
            
            # Step 2: Cleaning
            if auto_clean:
                current_data, cleaning_log = process_pipeline_step(current_data, "cleaning")
                st.session_state.processing_log.append(cleaning_log)
            
            # Step 3: Transformation
            current_data, transformation_log = process_pipeline_step(current_data, "transformation")
            st.session_state.processing_log.append(transformation_log)
            
            # Add to processed data
            if len(st.session_state.processed_data) == 0:
                st.session_state.processed_data = current_data
            else:
                st.session_state.processed_data = pd.concat([st.session_state.processed_data, current_data], ignore_index=True)
                
                # Keep only recent data (last 10000 records)
                if len(st.session_state.processed_data) > 10000:
                    st.session_state.processed_data = st.session_state.processed_data.tail(10000)
        
        # Auto-refresh
        time.sleep(processing_interval)
        st.rerun()
    
    # Processing steps visualization
    st.markdown("### Pipeline Steps")
    
    steps_col1, steps_col2, steps_col3 = st.columns(3)
    
    with steps_col1:
        st.markdown(f"""
        <div class="processing-step">
            <h4>1. Data Validation</h4>
            <p>Status: {"Enabled" if data_validation else "Disabled"}</p>
            <p>Checks data quality and integrity</p>
        </div>
        """, unsafe_allow_html=True)
    
    with steps_col2:
        st.markdown(f"""
        <div class="processing-step">
            <h4>2. Data Cleaning</h4>
            <p>Status: {"Enabled" if auto_clean else "Disabled"}</p>
            <p>Removes duplicates and outliers</p>
        </div>
        """, unsafe_allow_html=True)
    
    with steps_col3:
        st.markdown(f"""
        <div class="processing-step">
            <h4>3. Data Transformation</h4>
            <p>Status: Always Enabled</p>
            <p>Creates derived features and segments</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Live data preview
    if len(st.session_state.processed_data) > 0:
        st.markdown("### Live Data Preview")
        
        # Show recent records
        recent_data = st.session_state.processed_data.tail(10)
        st.dataframe(recent_data, use_container_width=True)
        
        # Real-time metrics
        col1, col2 = st.columns(2)
        
        with col1:
            # Event type distribution
            if 'event_type' in recent_data.columns:
                event_counts = recent_data['event_type'].value_counts()
                fig_events = px.pie(
                    values=event_counts.values,
                    names=event_counts.index,
                    title="Recent Event Types"
                )
                st.plotly_chart(fig_events, use_container_width=True)
        
        with col2:
            # Amount distribution
            if 'amount' in recent_data.columns:
                fig_amount = px.histogram(
                    recent_data,
                    x='amount',
                    title="Amount Distribution",
                    nbins=20
                )
                st.plotly_chart(fig_amount, use_container_width=True)

# Tab 2: Data Sources
with tab2:
    st.subheader("Data Source Management")
    
    # Data source types
    source_type = st.selectbox(
        "Select Data Source Type",
        ["File Upload", "API Endpoint", "Database Connection", "Mock Data Stream"]
    )
    
    if source_type == "File Upload":
        st.markdown("### File Upload")
        
        uploaded_file = st.file_uploader(
            "Upload CSV File",
            type=['csv'],
            help="Upload a CSV file to process through the pipeline"
        )
        
        if uploaded_file is not None:
            try:
                # Read uploaded file
                df_uploaded = pd.read_csv(uploaded_file)
                
                st.success(f"File uploaded successfully! {len(df_uploaded)} records loaded.")
                
                # Preview uploaded data
                st.markdown("**Data Preview:**")
                st.dataframe(df_uploaded.head(), use_container_width=True)
                
                # Process uploaded data
                if st.button("Process Uploaded Data"):
                    with st.spinner("Processing uploaded data..."):
                        processed_uploaded = df_uploaded.copy()
                        
                        # Add timestamp if not present
                        if 'timestamp' not in processed_uploaded.columns:
                            processed_uploaded['timestamp'] = datetime.now()
                        
                        # Process through pipeline
                        if data_validation:
                            processed_uploaded, val_log = process_pipeline_step(processed_uploaded, "validation")
                            st.session_state.processing_log.append(val_log)
                        
                        if auto_clean:
                            processed_uploaded, clean_log = process_pipeline_step(processed_uploaded, "cleaning")
                            st.session_state.processing_log.append(clean_log)
                        
                        # Only transform if it has required columns
                        if 'amount' in processed_uploaded.columns:
                            processed_uploaded, trans_log = process_pipeline_step(processed_uploaded, "transformation")
                            st.session_state.processing_log.append(trans_log)
                        
                        # Add to processed data
                        if len(st.session_state.processed_data) == 0:
                            st.session_state.processed_data = processed_uploaded
                        else:
                            st.session_state.processed_data = pd.concat([st.session_state.processed_data, processed_uploaded], ignore_index=True)
                        
                        st.success(f"Processed {len(processed_uploaded)} records successfully!")
                        
            except Exception as e:
                st.error(f"Error processing file: {str(e)}")
    
    elif source_type == "API Endpoint":
        st.markdown("### API Endpoint Configuration")
        
        api_url = st.text_input("API Endpoint URL", placeholder="https://api.example.com/data")
        api_method = st.selectbox("HTTP Method", ["GET", "POST"])
        api_headers = st.text_area("Headers (JSON format)", placeholder='{"Authorization": "Bearer token"}')
        
        if st.button("Test API Connection"):
            if api_url:
                try:
                    headers = json.loads(api_headers) if api_headers else {}
                    
                    if api_method == "GET":
                        response = requests.get(api_url, headers=headers, timeout=10)
                    else:
                        response = requests.post(api_url, headers=headers, timeout=10)
                    
                    if response.status_code == 200:
                        st.success("API connection successful!")
                        st.json(response.json()[:5] if isinstance(response.json(), list) else response.json())
                    else:
                        st.error(f"API returned status code: {response.status_code}")
                        
                except Exception as e:
                    st.error(f"API connection failed: {str(e)}")
            else:
                st.warning("Please enter an API endpoint URL")
    
    elif source_type == "Database Connection":
        st.markdown("### Database Connection")
        
        db_type = st.selectbox("Database Type", ["SQLite", "PostgreSQL", "MySQL", "SQL Server"])
        
        if db_type == "SQLite":
            db_file = st.text_input("Database File Path", placeholder="data.db")
            table_name = st.text_input("Table Name", placeholder="sales_data")
            
            if st.button("Test Database Connection"):
                if db_file and table_name:
                    try:
                        # Create a sample SQLite database for demo
                        conn = sqlite3.connect(':memory:')
                        sample_data = generate_sample_streaming_data(100)
                        sample_data.to_sql('sales_data', conn, index=False)
                        
                        # Test query
                        df_db = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 5", conn)
                        st.success("Database connection successful!")
                        st.dataframe(df_db)
                        conn.close()
                        
                    except Exception as e:
                        st.error(f"Database connection failed: {str(e)}")
                else:
                    st.warning("Please provide database file and table name")
        else:
            st.info("Full database integration available in production version")
    
    elif source_type == "Mock Data Stream":
        st.markdown("### Mock Data Stream Generator")
        
        stream_rate = st.slider("Records per Second", 1, 100, 10)
        stream_duration = st.slider("Stream Duration (seconds)", 10, 300, 60)
        
        if st.button("Start Mock Stream"):
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for i in range(stream_duration):
                # Generate data
                batch_data = generate_sample_streaming_data(stream_rate)
                
                # Process through pipeline
                if data_validation:
                    batch_data, val_log = process_pipeline_step(batch_data, "validation")
                    st.session_state.processing_log.append(val_log)
                
                if auto_clean:
                    batch_data, clean_log = process_pipeline_step(batch_data, "cleaning")
                    st.session_state.processing_log.append(clean_log)
                
                batch_data, trans_log = process_pipeline_step(batch_data, "transformation")
                st.session_state.processing_log.append(trans_log)
                
                # Add to processed data
                if len(st.session_state.processed_data) == 0:
                    st.session_state.processed_data = batch_data
                else:
                    st.session_state.processed_data = pd.concat([st.session_state.processed_data, batch_data], ignore_index=True)
                
                # Update progress
                progress = (i + 1) / stream_duration
                progress_bar.progress(progress)
                status_text.text(f"Streaming... {i+1}/{stream_duration} seconds")
                
                time.sleep(1)
            
            st.success(f"Mock stream completed! Generated {stream_rate * stream_duration} records.")

# Tab 3: Processing Monitor
with tab3:
    st.subheader("Processing Monitor & Logs")
    
    # Processing statistics
    if st.session_state.processing_log:
        log_df = pd.DataFrame(st.session_state.processing_log)
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_steps = len(log_df)
            st.metric("Total Processing Steps", total_steps)
        
        with col2:
            success_steps = len(log_df[log_df['status'] == 'success'])
            st.metric("Successful Steps", success_steps)
        
        with col3:
            error_steps = len(log_df[log_df['status'] == 'error'])
            st.metric("Failed Steps", error_steps)
        
        with col4:
            avg_time = log_df['processing_time'].mean()
            st.metric("Avg Processing Time", f"{avg_time:.3f}s")
        
        # Processing time trends
        col1, col2 = st.columns(2)
        
        with col1:
            fig_time = px.line(
                log_df,
                x=range(len(log_df)),
                y='processing_time',
                color='step',
                title="Processing Time by Step",
                labels={'x': 'Execution Order', 'processing_time': 'Time (seconds)'}
            )
            st.plotly_chart(fig_time, use_container_width=True)
        
        with col2:
            step_counts = log_df['step'].value_counts()
            fig_steps = px.bar(
                x=step_counts.index,
                y=step_counts.values,
                title="Processing Steps Count",
                labels={'x': 'Step', 'y': 'Count'}
            )
            st.plotly_chart(fig_steps, use_container_width=True)
        
        # Records throughput
        log_df['records_throughput'] = log_df['records_out'] / log_df['processing_time']
        
        fig_throughput = px.scatter(
            log_df,
            x='records_in',
            y='records_throughput',
            color='step',
            size='processing_time',
            title="Records Throughput Analysis",
            labels={'records_throughput': 'Records/Second'}
        )
        st.plotly_chart(fig_throughput, use_container_width=True)
        
        # Detailed logs
        st.markdown("### Processing Logs")
        
        # Filter options
        col1, col2, col3 = st.columns(3)
        
        with col1:
            status_filter = st.multiselect(
                "Filter by Status",
                options=log_df['status'].unique(),
                default=log_df['status'].unique()
            )