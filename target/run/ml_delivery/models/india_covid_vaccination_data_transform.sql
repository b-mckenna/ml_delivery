
  
    
        create or replace table hive_metastore.default.india_covid_vaccination_data_transform
      
      
    using delta
      
      
      
      
      
      
      as
      

with a as (
  select * from hive_metastore.default.india_covid_vaccination_delta_table
)
select make_date(
      concat(20, split_part(date, '/', 3)),
      split_part(date, '/', 2),
      split_part(date, '/', 1)
    ) as record_date,
    new_cases, new_deaths, new_tests, total_vaccinations, people_vaccinated, people_fully_vaccinated, new_vaccinations, population, population_density, 
median_age, aged_65_older from a
  