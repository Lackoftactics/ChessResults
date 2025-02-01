import streamlit as st
import pandas as pd
import altair as alt

# Cache data loading from an uploaded file.
@st.cache_data
def load_data(file) -> pd.DataFrame:
    try:
        df = pd.read_csv(file)
        return df
    except Exception as e:
        st.error(f"Error loading CSV: {e}")
        return None

# Cache data loading from a local file path.
@st.cache_data
def load_data_from_path(path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path)
        return df
    except Exception as e:
        st.error(f"Error loading CSV from path '{path}': {e}")
        return None

st.title("Chess Dataset Dashboard")

# Sidebar: choose the data source.
data_source = st.sidebar.radio(
    "Select Data Source",
    ("Upload CSV File", "Use Sample Dataset")
)

df = None  # initialize dataframe variable

if data_source == "Upload CSV File":
    uploaded_file = st.sidebar.file_uploader("Choose a CSV file", type=["csv"])
    if uploaded_file is not None:
        df = load_data(uploaded_file)
    else:
        st.info("Please upload a CSV file to continue.")
else:
    # Dictionary mapping sample names to file paths.
    sample_files = {
        "ESP Games": "data/games/ESP_games.csv",
        "ESP Results": "data/results/ESP_results.csv",
        "ESP Tournaments": "data/countries/ESP_chess_tournaments_selenium.csv",
    }
    sample_choice = st.sidebar.selectbox("Choose a sample dataset", list(sample_files.keys()))
    path = sample_files[sample_choice]
    df = load_data_from_path(path)

if df is not None:
    st.subheader("Data Preview")
    st.dataframe(df.head(10))

    st.subheader("Dataset Summary")
    st.write(df.describe(include='all'))

    st.subheader("Column List")
    st.write(list(df.columns))

    # --- Interactive Timeline Chart for Date Columns (if available) ---
    # Search for columns whose name contains 'date', 'start', or 'end'
    date_cols = [col for col in df.columns if any(x in col.lower() for x in ['date', 'start', 'end'])]
    if date_cols:
        st.subheader("Timeline Chart")
        date_choice = st.selectbox("Select a date column", date_cols)
        try:
            df[date_choice] = pd.to_datetime(df[date_choice])
            df['year'] = df[date_choice].dt.year
            timeline_chart = alt.Chart(df).mark_bar().encode(
                alt.X("year:O", title="Year"),
                alt.Y("count()", title="Number of Records")
            ).properties(
                width=700,
                height=400,
                title=f"Records by Year (from {date_choice})"
            )
            st.altair_chart(timeline_chart, use_container_width=True)
        except Exception as e:
            st.error(f"Error converting column '{date_choice}' to datetime: {e}")
    else:
        st.info("No date columns found for timeline chart.")

    st.subheader("Data Shape")
    st.write(f"Rows: {df.shape[0]} | Columns: {df.shape[1]}")
