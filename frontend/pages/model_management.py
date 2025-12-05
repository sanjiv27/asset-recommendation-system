import streamlit as st
from utils.api import list_models, retrain_model, activate_model
from datetime import datetime

st.set_page_config(page_title="Model Management", page_icon="🤖", layout="wide")
st.title("Model Management")

if st.button("Refresh", use_container_width=False):
    st.rerun()

st.divider()

# Fetch models
models_data = list_models()

if models_data.get("status") == "ok":
    models = models_data.get("models", [])
    
    if models:
        st.subheader("Available Models")
        
        for model in models:
            col1, col2, col3, col4 = st.columns([2, 2, 2, 1])
            
            with col1:
                st.markdown(f"**{model.get('name', 'Unknown')}**")
                st.text(f"Type: {model.get('type', 'N/A')}")
            
            with col2:
                status = model.get('status', 'unknown')
                if status == 'active':
                    st.success("ACTIVE")
                else:
                    st.text("Inactive")
                
                col2a, col2b = st.columns(2)
                with col2a:
                    st.metric("Users", model.get('n_users', 'N/A'))
                with col2b:
                    st.metric("Items", model.get('n_items', 'N/A'))
            
            with col3:
                st.metric("Variance", model.get('explained_variance', 'N/A'))
                
                trained_on = model.get('trained_on', 'Unknown')
                if trained_on != 'Unknown':
                    try:
                        dt = datetime.fromisoformat(trained_on)
                        st.text(dt.strftime("%Y-%m-%d %H:%M:%S"))
                    except:
                        st.text(trained_on)
                else:
                    st.text("Unknown")
            
            with col4:
                if status != 'active':
                    if st.button("Activate", key=f"activate_{model.get('filename')}"):
                        result = activate_model(model.get('filename'))
                        if result.get('status') == 'ok':
                            st.success("Activated!")
                            st.rerun()
                        else:
                            st.error(f"Failed: {result.get('message')}")
            
            st.divider()
    else:
        st.info("No models found. Train a model using the button below.")
else:
    st.error(f"Failed to fetch models: {models_data.get('message', 'Unknown error')}")

st.divider()

# Retrain section
st.subheader("Model Retraining")
st.info("Create a new model version trained on latest user interactions")

if st.button("Retrain Model", type="primary"):
    with st.spinner("Retraining model..."):
        result = retrain_model()
        
        if result.get("status") == "ok":
            st.success(f"Model v{result.get('version')} created successfully!")
            st.balloons()
            st.rerun()
        else:
            st.error(f"Retraining failed: {result.get('message', 'Unknown error')}")
