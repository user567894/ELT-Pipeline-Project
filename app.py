# NYC Taxi Analytics/app.py
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine

from dotenv import load_dotenv
import os

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# Database connection
@st.cache_resource
def get_connection():
    return create_engine(DATABASE_URL)

engine = get_connection()

st.set_page_config(page_title="NYC Taxi Analytics", page_icon="🚕", layout="wide")

st.title("NYC Yellow Taxi Analytics Dashboard")
st.markdown("Insights from NYC TLC Yellow Taxi trip records")

# Sidebar
st.sidebar.header("Dashboard Controls")
st.sidebar.markdown("---")

# Load data functions
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_daily_summary():
    query = "SELECT * FROM analytics.daily_trip_summary ORDER BY trip_date DESC LIMIT 1000"
    return pd.read_sql(query, engine)

@st.cache_data(ttl=300)
def load_hourly_demand():
    query = "SELECT * FROM analytics.hourly_demand ORDER BY trip_date DESC, hour_of_day"
    return pd.read_sql(query, engine)

@st.cache_data(ttl=300)
def load_top_routes():
    query = "SELECT * FROM analytics.top_routes LIMIT 50"
    return pd.read_sql(query, engine)

@st.cache_data(ttl=300)
def load_payment_analysis():
    query = "SELECT * FROM analytics.payment_analysis ORDER BY trip_date DESC, trip_count DESC"
    return pd.read_sql(query, engine)

try:
    daily_data = load_daily_summary()
    hourly_data = load_hourly_demand()
    top_routes = load_top_routes()
    payment_data = load_payment_analysis()
    
    # Top-level metrics
    st.markdown("### Key Metrics")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_trips = daily_data['total_trips'].sum()
        st.metric("Total Trips", f"{total_trips:,}")
    
    with col2:
        avg_fare = daily_data['avg_fare'].mean()
        st.metric("Avg Fare", f"${avg_fare:.2f}")
    
    with col3:
        total_revenue = daily_data['total_revenue'].sum()
        st.metric("Total Revenue", f"${total_revenue:,.0f}")
    
    with col4:
        avg_distance = daily_data['avg_distance'].mean()
        st.metric("Avg Distance", f"{avg_distance:.2f} mi")
    
    st.markdown("---")
    
    # Daily trips over time
    st.markdown("### Daily Trip Volume")
    daily_agg = daily_data.groupby('trip_date').agg({
        'total_trips': 'sum',
        'total_revenue': 'sum'
    }).reset_index()
    
    fig_daily = go.Figure()
    fig_daily.add_trace(go.Scatter(
        x=daily_agg['trip_date'], 
        y=daily_agg['total_trips'],
        mode='lines+markers',
        name='Trips',
        line=dict(color='#FF6B6B', width=2)
    ))
    fig_daily.update_layout(
        title='Daily Trip Volume Over Time',
        xaxis_title='Date',
        yaxis_title='Number of Trips',
        hovermode='x unified'
    )
    st.plotly_chart(fig_daily, use_container_width=True)
    
    # Two column layout
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Hourly Demand Heatmap")
        hourly_pivot = hourly_data.pivot_table(
            values='trip_count', 
            index='hour_of_day', 
            columns='day_of_week',
            aggfunc='mean'
        )
        
        # Rename columns to day names
        day_names = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
        hourly_pivot.columns = [day_names[int(col)] for col in hourly_pivot.columns]
        
        fig_heatmap = px.imshow(
            hourly_pivot,
            labels=dict(x="Day of Week", y="Hour of Day", color="Avg Trips"),
            color_continuous_scale='YlOrRd',
            aspect='auto'
        )
        fig_heatmap.update_layout(title="Average Trips by Hour and Day")
        st.plotly_chart(fig_heatmap, use_container_width=True)
    
    with col2:
        st.markdown("### Payment Method Distribution")
        payment_summary = payment_data.groupby('payment_method').agg({
            'trip_count': 'sum',
            'total_revenue': 'sum'
        }).reset_index().sort_values('trip_count', ascending=False)
        
        fig_payment = px.pie(
            payment_summary,
            values='trip_count',
            names='payment_method',
            title='Trips by Payment Method',
            hole=0.4
        )
        st.plotly_chart(fig_payment, use_container_width=True)
    
    # Top routes
    st.markdown("### Top 20 Routes by Trip Volume")
    top_routes_display = top_routes.head(20).copy()
    top_routes_display['route'] = (
        'Zone ' + top_routes_display['pickup_location_id'].astype(str) + 
        ' → Zone ' + top_routes_display['dropoff_location_id'].astype(str)
    )
    
    fig_routes = px.bar(
        top_routes_display,
        x='trip_count',
        y='route',
        title='Most Popular Routes',
        labels={'trip_count': 'Number of Trips', 'route': 'Route'},
        orientation='h',
        color='avg_total_amount',
        color_continuous_scale='Viridis'
    )
    fig_routes.update_layout(height=600, showlegend=True)
    st.plotly_chart(fig_routes, use_container_width=True)
    
    # Tip analysis
    st.markdown("### Tip Analysis (Credit Card Only)")
    hourly_tips = hourly_data[hourly_data['avg_tip_percentage'].notna()]
    tip_by_hour = hourly_tips.groupby('hour_of_day')['avg_tip_percentage'].mean().reset_index()
    
    fig_tips = px.line(
        tip_by_hour,
        x='hour_of_day',
        y='avg_tip_percentage',
        title='Average Tip Percentage by Hour of Day',
        labels={'hour_of_day': 'Hour', 'avg_tip_percentage': 'Tip %'}
    )
    fig_tips.update_traces(line_color='#4ECDC4', line_width=3)
    st.plotly_chart(fig_tips, use_container_width=True)
    
    # Expandable raw data sections
    with st.expander("View Daily Summary Data"):
        st.dataframe(daily_data.head(100), use_container_width=True)
    
    with st.expander("View Top Routes Data"):
        st.dataframe(top_routes.head(50), use_container_width=True)
    
    with st.expander("View Payment Analysis Data"):
        st.dataframe(payment_data.head(100), use_container_width=True)

except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.info("Make sure your database is running and dbt models have been executed.")
    st.code(f"Error details: {e}")

# Footer
st.markdown("---")
st.markdown("Data source: NYC Taxi & Limousine Commission (TLC)")
