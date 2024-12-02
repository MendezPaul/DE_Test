import os
import pandas as pd
from sqlalchemy import create_engine

def generate_state_summary(state_code, database_url):
    engine = create_engine(database_url)
    
    # Get population for the state
    population_query = f"""
    SELECT census_population 
    FROM states_and_territories 
    WHERE state_code = '{state_code.upper()}'
    """
    population = pd.read_sql(population_query, engine).iloc[0]['census_population']
    
    # Get state daily cases
    cases_query = f"""
    SELECT date, cases_total
    FROM state_daily_cases sd
    JOIN states_and_territories st ON sd.state_id = st.state_code
    WHERE st.state_code = '{state_code.upper()}'
    ORDER BY date
    """
    cases_df = pd.read_sql(cases_query, engine)
    
    # Calculate percentage of population infected
    cases_df['percent_infected'] = cases_df['cases_total'] / population
    
    # Output directory
    os.makedirs('outputs', exist_ok=True)
    
    # Write state summary
    output_file = f'outputs/{state_code.lower()}.csv'
    cases_df[['date', 'percent_infected']].to_csv(output_file, index=False)
    
    return output_file

def main():
    database_url = os.getenv('DATABASE_URL', 'postgresql://admin:secretpassword@localhost:5432/covid_data')
    
    # Generate summaries for all states
    states_query = "SELECT DISTINCT state_code FROM states_and_territories"
    engine = create_engine(database_url)
    states = pd.read_sql(states_query, engine)
    
    for state in states['state_code']:
        generate_state_summary(state, database_url)

if __name__ == '__main__':
    main()