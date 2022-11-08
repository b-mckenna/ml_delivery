

def model(dbt, session):
    # Make sure the feature store database is created in Databricks
    fs = feature_store.FeatureStoreClient()

    vax_features_df = df.select("date","total_vaccinations", "people_vaccinated", "people_fully_vaccinated", "new_vaccinations")

    fs.create_table(
        name="feature_store_india_covid.vaccination_features",
        primary_keys=["date"],
        df=vax_features_df,
        partition_columns="date",
        description="Covid cases in India. Vaccination features"
    )

    pop_df = df.select("date","population", "population_density", "aged_65_older", "median_age")

    fs.create_table(
        name="feature_store_india_covid.population_features",
        primary_keys=["date"],
        df=pop_df,
        partition_columns="date",
        description="Covid cases in India. Population features"
    )
    return 
