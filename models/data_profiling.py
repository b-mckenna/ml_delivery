import pandas as pd
from deepchecks.tabular.checks import ClassImbalance

def model(dbt, session):
    df = dbt.ref("ddl")
    dataset = Dataset(df, label=new_cases, features=['date', 'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated', 'new_vaccinations', 'population', 'population_density', 'median_age', 'aged_65_older', ], cat_features=[])
    ClassImbalance().run(dataset)


    return df