import streamlit as st
import sys
sys.path.insert(0, '/app')
from utils.api import get_watchlist, remove_from_watchlist

st.set_page_config(page_title="My Watchlist", page_icon="⭐", layout="wide")

st.title("⭐ My Watchlist")

# Get customer ID from session or input
if 'customer_id' not in st.session_state:
    customer_id = st.text_input("Customer ID", placeholder="Enter your customer ID")
    if customer_id:
        st.session_state['customer_id'] = customer_id
else:
    customer_id = st.session_state['customer_id']

if customer_id:
    # Fetch fresh watchlist data
    result = get_watchlist(customer_id)
    watchlist = result.get('watchlist', [])
    
    if watchlist:
        st.success(f"You have {len(watchlist)} assets in your watchlist")
        
        for asset in watchlist:
            col1, col2, col3, col4 = st.columns([3, 2, 1, 1])
            
            with col1:
                st.markdown(f"**{asset.get('assetname', 'N/A')}**")
                st.caption(f"ISIN: {asset.get('isin', 'N/A')}")
                if asset.get('sector'):
                    st.caption(f"🏢 {asset.get('sector')} - {asset.get('industry', 'N/A')}")
            
            with col2:
                st.metric("Current Price", f"${asset.get('current_price', 0):.2f}")
            
            with col3:
                profit = asset.get('profitability', 0) or 0
                st.metric("Profitability", f"{profit*100:.2f}%")
            
            with col4:
                if st.button("🗑️", key=f"remove_{asset.get('isin')}", help="Remove from watchlist"):
                    remove_from_watchlist(customer_id, asset.get('isin'))
                    st.rerun()
            
            st.divider()
    else:
        st.info("Your watchlist is empty. Add assets from the recommendations page!")
else:
    st.info("👈 Please enter your Customer ID to view your watchlist")
