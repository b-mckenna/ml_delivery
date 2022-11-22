
  
    
        create or replace table hive_metastore.default.ddl
      
      
    using delta
      
      
      
      
      
      
      as
      

select date, new_cases, new_deaths, new_tests, total_vaccinations, people_vaccinated, people_fully_vaccinated, new_vaccinations, population, population_density, 
median_age, aged_65_older from hive_metastore.default.india_covid_vaccination_delta_table
  