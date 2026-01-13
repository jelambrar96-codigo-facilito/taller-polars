import polars as pl


null_values = ["NA", "null", "N/A"]

df = pl.read_csv("data/life_expectancy.csv", null_values=null_values)
df_female_male = pl.read_csv("data/life_expectancy_female_male.csv", null_values=null_values)
df_ages = pl.read_csv("data/life_expectancy_different_ages.csv", null_values=null_values)

## filtro por valores nulos

df.describe()


df_clean = df.filter(
    # Remove rows with null values 
    pl.all_horizontal(pl.col("Year").is_not_null()) &
    pl.all_horizontal(pl.col("Year").is_not_null()) &
    pl.all_horizontal(pl.col("LifeExpectancy").is_not_null()) &
    # Ensure Year is within reasonable range (1900-2023) 
    (pl.col("Year") >= 1900) & (pl.col("Year") <= 2023) &
    # Ensure LifeExpectancy is positive and reasonable (<100)
    (pl.col("LifeExpectancy") > 0) & (pl.col("LifeExpectancy") < 100)
)

print(df_clean.head())



# Create decade grouping and calculate trends 
df_trends = (
    df_clean.with_columns(
        [ 
        # Create decade column
        (pl.col("Year") // 10 * 10).alias("Decade"), 
        # # Round life expectancy to 2 decimal places 
        pl.col("LifeExpectancy" ).round(2).alias("LifeExpectancy"),
        ]
    ).group_by(["Entity", "Decade"])
    .agg(
        [ 
            # Calculate decade statistics 
            pl.col("LifeExpectancy").mean().alias("Avg_Life_Expectancy"),
            pl.col("LifeExpectancy").min().alias("Min_Life_Expectancy"),
            pl.col("LifeExpectancy" ).max().alias("Max_Life_Expectancy"),
            # Calculate decade-over-decade change
            (pl.col("LifeExpectancy").max() - pl.col("LifeExpectancy").min()).alias("Decade_Change"), 
        ]
    )
)
