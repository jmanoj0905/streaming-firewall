import streamlit as st
import json
from kafka import KafkaConsumer
import time

st.set_page_config(page_title="Firewall Monitor", layout="wide")
st.title("Real-time Firewall Traffic")

col1, col2 = st.columns(2)

with col1:
    st.header("CLEAN")
    clean_container = st.empty()

with col2:
    st.header("QUARANTINE")
    quarantine_container = st.empty()

if 'clean_data' not in st.session_state:
    st.session_state.clean_data = []
if 'quarantine_data' not in st.session_state:
    st.session_state.quarantine_data = []

consumer = KafkaConsumer(
    'firewall-clean', 
    'firewall-quarantine',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    enable_auto_commit=True
)

st.info("Listening for Kafka messages...")

try:
    for msg in consumer:
        event = msg.value
        display_text = f"**{event.get('level')}** | {event.get('source')} | {event.get('message')}"
        
        if msg.topic == 'firewall-clean':
            st.session_state.clean_data.insert(0, display_text)
        else:
            reason = event.get('reject_reason', 'unknown')
            st.session_state.quarantine_data.insert(0, f"{display_text} | `{reason}`")

        st.session_state.clean_data = st.session_state.clean_data[:15]
        st.session_state.quarantine_data = st.session_state.quarantine_data[:15]

        clean_container.vertical_alignment = "top"
        with clean_container.container():
            for item in st.session_state.clean_data:
                st.success(item)

        with quarantine_container.container():
            for item in st.session_state.quarantine_data:
                st.error(item)

except KeyboardInterrupt:
    st.write("Stopped")
