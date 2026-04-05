import streamlit as st
import requests
import pandas as pd
import matplotlib.pyplot as plt

st.set_page_config(page_title="NYC taxi Data Analysis", layout="wide")
st.title("NYC Taxi Data Analysis")

st.header("Rider per Day")

response = requests.get("http://127.0.0.1:8000/rides-per-day")
data = pd.DataFrame(response.json())

st.write("Rides per day data:")
st.write(data)

plt.figure(figsize=(10, 4))
plt.plot(data["pickup_date"], data["count"], marker='o')
plt.xticks(rotation=45)
plt.xlabel("Date")
plt.ylabel("Number of Rides")
plt.title("Ride Demand by Day")

st.pyplot(plt)

st.subheader("Question: On which day was demand highest?")

max_day = data.sort_values("count", ascending=False).iloc[0]
st.write(f"Highest demand was on **{max_day['pickup_date']}** with **{max_day['count']} rides**.")

st.subheader("Insight")
st.write("Ride demand varies significantly between days, which may indicate weekdays vs weekend behaviour or external factors like weather or events.")

st.header("Top Routes")

response2 = requests.get("http://127.0.0.1:8000/top-routes")
data2 = pd.DataFrame(response2.json())

st.write("Top 10 routes:")
st.write(data2.head(10))

plt.figure(figsize=(10, 4))
plt.bar(data2["route"].head(10), data2["count"].head(10))
plt.xticks(rotation=45)
plt.xlabel("Top Routes")
plt.ylabel("Ride Count")
plt.title("Top 10 Most Popular Routes")

st.pyplot(plt)

st.subheader("Question: What is the most used route?")
most_used_route = data2.sort_values("count", ascending=False).iloc[0]
st.write(f"The most used route is **{most_used_route['route']}** with **{most_used_route['count']} rides**.")

st.subheader("Question: Are rides concerated on a few routes?")

top10_total = data2["count"].head(10).sum()
total = data2["count"].sum()

st.write(f"Top 10 routes account for **{round((top10_total/total)*100, 2)}%** of all rides.")

st.subheader("Insight")
st.write("A high precentage suggest that ride demand is concerated on a small number of popular routes. ")

st.header("Shared Rides Analysis")

response3 = requests.get("http://127.0.0.1:8000/Shared-Rides")
data3 = pd.DataFrame(response3.json())

st.write("Sample of shared rides data:")
st.dataframe(data3.head(10))

st.subheader("Shared Rides per Day")
rides_per_day = data3.groupby("pickup_date").size().reset_index(name="count")
st.bar_chart(rides_per_day.set_index("pickup_date"))

st.subheader("Top Shared Routes")
top_shared_routes = data3.groupby("route").size().reset_index(name="count").sort_values("count", ascending=False)
st.dataframe(top_shared_routes.head(10))
st.bar_chart(top_shared_routes.set_index("route").head(10))

st.subheader("Question: What is the most common shared route?")

top_shared = top_shared_routes.iloc[0]
st.write(f"Most common shared route is **{top_shared['route']}** with **{top_shared['count']} rides**.")

st.subheader("Average Fares and Fees")
avg_fares = data3[["base_passenger_fare", "tolls", "tips", "driver_pay"]].mean()
st.write(avg_fares)

st.subheader("Special Flags Analysis")
flag_counts = data3[["access_a_ride_flag", "wav_request_flag"]].value_counts().reset_index(name="count")
st.write(flag_counts)

st.subheader("Question: How common are special ride request?")

st.subheader("Insight")
st.write("Special ride request are extremely rare in this dataset.")
