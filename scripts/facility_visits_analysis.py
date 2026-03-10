"""
Facility Visit Data Analysis Pipeline

This script performs an end-to-end ETL and analysis workflow on patient visit and demographic data.
"""

# === 1. IMPORT LIBRARIES ===
import pandas as pd
import pyodbc
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# === 2. ENVIRONMENT INFO ===
print("Pandas version:", pd.__version__)
print("PyODBC version:", pyodbc.version)

# === 3. LOAD DATA ===
print("\nLoading datasets...")

persons_df = pd.read_csv("data/Persons.csv")
visits_df = pd.read_csv("data/FacilityVisits.csv")

print(f" Loaded Persons: {persons_df.shape}")
print(f" Loaded Visits: {visits_df.shape}")

# === 4. DATA CLEANING ===
print("\nCleaning datasets...")

# Drop duplicates
persons_df.drop_duplicates(inplace=True)
visits_df.drop_duplicates(inplace=True)

# Check missing values
print("Missing values in Persons:\n", persons_df.isnull().sum())
print("Missing values in Visits:\n", visits_df.isnull().sum())

# === 5. DATA MERGE ===
print("\nMerging datasets...")
merged_df = pd.merge(visits_df, persons_df, on="PersonId", how="inner")

# Parse dates
merged_df['VisitDate'] = pd.to_datetime(merged_df['VisitDate'])
merged_df['DoBAssigned'] = pd.to_datetime(merged_df['DoBAssigned'])

# Calculate age at visit
merged_df['AgeAtVisit'] = merged_df.apply(
    lambda row: (row['VisitDate'] - row['DoBAssigned']).days // 365, axis=1)

# Add year and month columns
merged_df['VisitYear'] = merged_df['VisitDate'].dt.year
merged_df['VisitMonth'] = merged_df['VisitDate'].dt.month

print(f" Merged Data Shape: {merged_df.shape}")

# === 6. SUMMARY STATS ===
print("\nSummary Statistics:\n", merged_df.describe())
print("\nGender Distribution:\n", persons_df['Sex'].value_counts())

# === 7. VISUALIZATIONS ===
print("\nGenerating visualizations...")

# Visits over years
plt.figure(figsize=(10, 6))
merged_df['VisitYear'].value_counts().sort_index().plot(kind='bar')
plt.title("Facility Visits per Year")
plt.xlabel("Year")
plt.ylabel("Visit Count")
plt.tight_layout()
plt.savefig("output/visits_per_year.png")
plt.close()

# Visits by month
plt.figure(figsize=(10, 6))
merged_df['VisitMonth'].value_counts().sort_index().plot(kind='bar')
plt.title("Facility Visits by Month")
plt.xlabel("Month")
plt.ylabel("Visit Count")
plt.tight_layout()
plt.savefig("output/visits_by_month.png")
plt.close()

# Visits per clinic
plt.figure(figsize=(12, 6))
merged_df['Facility'].value_counts().plot(kind='bar')
plt.title("Visits by Clinic")
plt.xlabel("Clinic")
plt.ylabel("Visit Count")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("output/visits_by_clinic.png")
plt.close()

# Age distribution
plt.figure(figsize=(14, 6))
sns.histplot(merged_df['AgeAtVisit'], bins=30, kde=True)
plt.title("Age Distribution at Time of Visit")
plt.xlabel("Age")
plt.ylabel("Frequency")
plt.tight_layout()
plt.savefig("output/age_distribution.png")
plt.close()

# Gender distribution over time
plt.figure(figsize=(10, 6))
sns.countplot(data=merged_df, x='VisitYear', hue='Sex')
plt.title("Visits by Gender Over Years")
plt.xlabel("Year")
plt.ylabel("Visit Count")
plt.tight_layout()
plt.savefig("output/gender_over_time.png")
plt.close()

print(" All plots saved in the output/ directory.")

# === 8. SAVE CLEANED DATA ===
merged_df.to_csv("output/merged_facility_visits.csv", index=False)
print(" Cleaned data saved.")
