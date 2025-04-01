import streamlit as st
import boto3
from botocore.config import Config
import uuid
from typing import Optional
from botocore.client import BaseClient
from io import BytesIO

st.markdown("# WPILog Uploader ⏫")
st.sidebar.markdown("# WPILog Uploader ⏫")

# Configure S3 client for Cloudflare R2
@st.cache_resource
def getS3Client() -> BaseClient:
    return boto3.client(
        "s3",
        endpoint_url=st.secrets.wpilog_r2.endpoint,
        aws_access_key_id=st.secrets.wpilog_r2.access_key,
        aws_secret_access_key=st.secrets.wpilog_r2.secret_key,
        config=Config(signature_version="s3v4"),
    )

s3_client = getS3Client()

upload_filesize = 0
upload_pct = 0
def update_progress(bytes_amount):
    global upload_filesize, upload_pct
    upload_pct = bytes_amount / upload_filesize

def upload_file(file: BytesIO):
    global upload_filesize
    # Generate a unique filename
    unique_filename = f"{uuid.uuid4()}/{file.name}"
    
    # Calculate file size in bytes
    upload_filesize = file.getbuffer().nbytes
    
    try:
        # Upload the file to S3
        s3_client.upload_fileobj(
            file,
            st.secrets.wpilog_r2.bucket_name,
            unique_filename,
            ExtraArgs={'ContentType': 'application/octet-stream'},
            Callback=update_progress
        )
        
        # Generate the URL for the uploaded file
        url = f"{st.secrets.wpilog_r2.endpoint}/{st.secrets.wpilog_r2.bucket_name}/{unique_filename}"
        return url
    except Exception as e:
        st.error(f"Error uploading file: {str(e)}")
        return None

uploaded_file = st.file_uploader("Choose a file", type="wpilog", accept_multiple_files=True)

if uploaded_file is not None:
    progress_bar = st.progress(0, text="Waiting for upload to server...")
    for file in uploaded_file:
        progress_text = f"Uploading {file.name} to S3"
        progress_bar.progress(upload_pct, text=progress_text)
        upload_file(file)

# List files in S3
st.markdown("## Files On Server")
try:
    response = s3_client.list_objects_v2(Bucket=st.secrets.wpilog_r2.bucket_name)

    if 'Contents' in response:
        files = []
        selected_files = []
        for obj in response['Contents']:
            # Extract original filename by removing UUID prefix
            original_name = obj['Key'].split('/', 1)[1] if '/' in obj['Key'] else obj['Key']

            files.append({
                'Select': False,
                'Filename': original_name,
                'Key': obj['Key'],
                'Size': f"{obj['Size'] / 1024:.1f} KB",
                'Last Modified': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
            })
        dataframe = st.data_editor(
            files,
            column_config={
                "Key": None
            },
            disabled=["Filename", "Size", "Last Modified"],
            hide_index=True,
        )
        selected_files = [row['Key'] for row in dataframe if row['Select']]
        st.write(selected_files)
        
        if st.button("Delete Selected Files"):
            for key in selected_files:
                try:
                    s3_client.delete_object(Bucket=st.secrets.wpilog_r2.bucket_name, Key=key)
                    st.success(f"Deleted {key}")
                except Exception as e:
                    st.error(f"Error deleting {key}: {str(e)}")
    else:
        st.info("No files uploaded yet")
except Exception as e:
    st.error(f"Error listing files: {str(e)}")

