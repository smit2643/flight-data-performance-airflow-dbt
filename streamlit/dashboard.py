import streamlit as st
import pandas as pd
import snowflake.connector
import altair as alt

def get_connection(selected_schema):
    return snowflake.connector.connect(
        account=st.secrets["snowflake"]["account"],
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        role = "ACCOUNTADMIN",
        warehouse = "FLIGHT_WH",
        database = "FLIGHT",
        schema=selected_schema
    )

def fetch_paginated_data(conn, table, offset, limit=250):
    query = f"SELECT * FROM {table} LIMIT {limit} OFFSET {offset}"
    return pd.read_sql(query, conn)


schema_tables = {
    "STAGING_MARTS": [
        "AIRLINE_ON_TIME_RANKING",
        "AIRPORT_DAILY_PERFORMANCE",
        "FLIGHT_PERFORMANCE_SUMMARY",
        "HOURLY_DELAY_DISTRIBUTION"
    ],
    "STAGING_MARTS_JFK": [
        "AIRLINE_ON_TIME_RANKING_JFK",
        "AIRPORT_DAILY_PERFORMANCE_JFK",
        "FLIGHT_PERFORMANCE_SUMMARY_JFK",
        "HOURLY_DELAY_DISTRIBUTION_JFK",
        "WEATHER_DELAY_IMPACT_JFK"
    ]
}

st.title("✈️ Flight Performance Dashboard")

schema = st.sidebar.selectbox("Select Schema", list(schema_tables.keys()))
table = st.sidebar.selectbox("Select Table", schema_tables[schema])

st.write(f"### Showing: `{schema}.{table}`")

conn = get_connection(schema)

offset = st.session_state.get("offset", 0)

df = fetch_paginated_data(conn, table, offset)
total_rows = conn.cursor().execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

st.write(f"Rows Loaded: {len(df)} / {total_rows}")

st.dataframe(df, use_container_width=True, height=400)

col1, col2 = st.columns(2)

if offset > 0:
    if col1.button("⬅️ Previous 250"):
        st.session_state["offset"] = max(offset - 250, 0)
        st.rerun()

if len(df) == 250:
    if col2.button("➡️ Load More"):
        st.session_state["offset"] = offset + 250
        st.rerun()

st.write("---")
st.write("## 📊 Charts")

def numeric_columns(df):
    return df.select_dtypes(include=["float", "int"]).columns.tolist()

if len(df) > 0:
    nums = numeric_columns(df)

    if len(nums) >= 1:
        chart_col = st.selectbox("Select numeric column for visualization:", nums)

        st.write("### Line Chart")
        st.altair_chart(
            alt.Chart(df.reset_index()).mark_line().encode(
                x="index",
                y=chart_col
            ).interactive(),
            use_container_width=True
        )

        st.write("### Bar Chart")
        st.altair_chart(
            alt.Chart(df.reset_index()).mark_bar().encode(
                x="index",
                y=chart_col
            ).interactive(),
            use_container_width=True
        )
else:
    st.warning("No data available to plot.")
