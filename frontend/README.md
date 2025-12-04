# Asset Recommendation Frontend

Streamlit-based web interface for the Asset Recommendation System.

## Features

- **Personalized Recommendations**: View stock and bond recommendations
- **Real-time Updates**: Request and refresh recommendations
- **Interaction Tracking**: Automatically logs clicks for improved recommendations
- **Asset Details**: View detailed information about each asset
- **Health Monitoring**: Check system status

## Access

Once running, access the UI at: **http://localhost:8501**

## Usage

1. Enter your Customer ID in the sidebar
2. Click "Get My Recommendations" to view suggestions
3. Browse recommendations organized by Stocks and Bonds
4. Click "View" on any asset to see details (automatically logs interaction)
5. Use "Refresh Recommendations" to get updated suggestions

## Architecture

- **Home.py**: Main dashboard with recommendations
- **utils/api.py**: API client for backend communication
- **Port**: 8501
- **Backend**: Connects to server container on port 8000

## Development

Run locally:
```bash
streamlit run Home.py
```

Note: Update `API_BASE_URL` in `utils/api.py` to `http://localhost:8000` for local development.
