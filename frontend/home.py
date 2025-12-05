import streamlit as st
import pandas as pd
from utils.api import get_recommendations, request_recommendations, log_interaction, check_health, add_to_watchlist, get_watchlist

st.set_page_config(
    page_title="Asset Recommendations",
    page_icon="📈",
    layout="wide"
)

st.title("📈 Asset Recommendation System")

# Sidebar
with st.sidebar:
    st.header("Customer Portal")
    customer_id = st.text_input("Customer ID", value="", placeholder="Enter your customer ID")
    
    st.divider()
    
    # Health check
    health = check_health()
    if health.get("status") == "ok":
        st.success("✅ System Online")
    else:
        st.error("❌ System Offline")

# Main content
if not customer_id:
    st.info("👈 Please enter your Customer ID in the sidebar to view recommendations")
    st.markdown("""
    ### Welcome to the Asset Recommendation System
    
    This system provides personalized investment recommendations based on:
    - **Collaborative Filtering**: What similar customers invested in
    - **Content-Based**: Assets similar to your portfolio
    - **Demographic Matching**: Recommendations based on your risk profile
    
    #### How to use:
    1. Enter your Customer ID in the sidebar
    2. Click "Get My Recommendations" to view personalized suggestions
    3. Click on any asset to view details and log your interest
    4. Use "Refresh Recommendations" to get updated suggestions
    """)
else:
    col1, col2 = st.columns([1, 1])
    
    with col1:
        if st.button("🔍 Get My Recommendations", type="primary", use_container_width=True):
            with st.spinner("Fetching recommendations..."):
                try:
                    result = get_recommendations(customer_id)
                    st.session_state['recommendations'] = result.get('recommendations', [])
                    if st.session_state['recommendations']:
                        st.success(f"Found {len(st.session_state['recommendations'])} recommendations!")
                    else:
                        st.warning("No recommendations found. Try requesting new ones.")
                except Exception as e:
                    st.error(f"Error: {str(e)}")
    
    with col2:
        if st.button("🔄 Refresh Recommendations", use_container_width=True):
            with st.spinner("Generating new recommendations... This may take 10-30 seconds"):
                try:
                    # Request new recommendations
                    request_recommendations(customer_id, "request_recs")
                    
                    # Poll for results (max 60 seconds)
                    import time
                    max_attempts = 30
                    found = False
                    for attempt in range(max_attempts):
                        time.sleep(2)
                        try:
                            result = get_recommendations(customer_id)
                            if result.get('recommendations'):
                                st.session_state['recommendations'] = result['recommendations']
                                found = True
                                break
                        except:
                            pass
                    
                    if found:
                        st.success(f"✅ Generated {len(st.session_state['recommendations'])} new recommendations!")
                        st.rerun()
                    else:
                        st.warning("Recommendations are taking longer than expected. Try 'Get My Recommendations' in a moment.")
                except Exception as e:
                    st.error(f"Error: {str(e)}")
    
    # Display recommendations
    if 'recommendations' in st.session_state and st.session_state['recommendations']:
        st.divider()
        st.subheader("Your Personalized Recommendations")
        
        # Tabs for different asset categories
        stocks = [r for r in st.session_state['recommendations'] if r.get('assetcategory') == 'Stock']
        bonds = [r for r in st.session_state['recommendations'] if r.get('assetcategory') == 'Bond']
        
        tab1, tab2 = st.tabs([f"📊 Stocks ({len(stocks)})", f"💰 Bonds ({len(bonds)})"])
        
        with tab1:
            if stocks:
                for asset in stocks:
                    with st.container():
                        col1, col2, col3, col4 = st.columns([3, 2, 1, 1])
                        
                        with col1:
                            st.markdown(f"**{asset.get('assetname', 'N/A')}**")
                            st.caption(f"ISIN: {asset.get('isin', 'N/A')}")
                            if asset.get('sector'):
                                st.caption(f"🏢 {asset.get('sector')} - {asset.get('industry', 'N/A')}")
                        
                        with col2:
                            st.metric("Current Price", f"${asset.get('current_price', 0):.2f}")
                        
                        with col3:
                            profit = asset.get('profitability', 0)
                            st.metric("Profitability", f"{profit*100:.2f}%", 
                                     delta=f"{profit*100:.2f}%",
                                     delta_color="normal" if profit >= 0 else "inverse")
                        
                        with col4:
                            if st.button("👁️ View", key=f"view_{asset.get('isin')}"):
                                log_interaction(customer_id, asset.get('isin'), "click")
                                st.session_state['selected_asset'] = asset
                                st.rerun()
                        
                        st.divider()
            else:
                st.info("No stock recommendations available")
        
        with tab2:
            if bonds:
                for asset in bonds:
                    with st.container():
                        col1, col2, col3, col4 = st.columns([3, 2, 1, 1])
                        
                        with col1:
                            st.markdown(f"**{asset.get('assetname', 'N/A')}**")
                            st.caption(f"ISIN: {asset.get('isin', 'N/A')}")
                            if asset.get('sector'):
                                st.caption(f"🏢 {asset.get('sector')} - {asset.get('industry', 'N/A')}")
                        
                        with col2:
                            st.metric("Current Price", f"${asset.get('current_price', 0):.2f}")
                        
                        with col3:
                            profit = asset.get('profitability', 0)
                            st.metric("Profitability", f"{profit*100:.2f}%",
                                     delta=f"{profit*100:.2f}%",
                                     delta_color="normal" if profit >= 0 else "inverse")
                        
                        with col4:
                            if st.button("👁️ View", key=f"view_{asset.get('isin')}"):
                                log_interaction(customer_id, asset.get('isin'), "click")
                                st.session_state['selected_asset'] = asset
                                st.rerun()
                        
                        st.divider()
            else:
                st.info("No bond recommendations available")

# Asset detail modal
if 'selected_asset' in st.session_state:
    asset = st.session_state['selected_asset']
    
    @st.dialog("Asset Details")
    def show_asset_details():
        st.markdown(f"### {asset.get('assetname', 'N/A')}")
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**ISIN:** {asset.get('isin', 'N/A')}")
            st.markdown(f"**Category:** {asset.get('assetcategory', 'N/A')}")
            st.markdown(f"**Sector:** {asset.get('sector', 'N/A')}")
            st.markdown(f"**Industry:** {asset.get('industry', 'N/A')}")
        
        with col2:
            st.metric("Current Price", f"${asset.get('current_price', 0):.2f}")
            profit = asset.get('profitability', 0)
            st.metric("Profitability", f"{profit*100:.2f}%")
        
        st.divider()
        if st.button("⭐ Add to Watchlist", use_container_width=True):
            try:
                result = add_to_watchlist(customer_id, asset.get('isin'))
                if result.get('added'):
                    log_interaction(customer_id, asset.get('isin'), "add_watchlist", weight=3)
                    st.success("✅ Added to watchlist!")
                else:
                    st.info("This asset is already in your watchlist")
            except Exception as e:
                st.error(f"Error: {str(e)}")
    
    show_asset_details()
    del st.session_state['selected_asset']
