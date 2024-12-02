import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import time

class COVIDDataIngestion:
    def __init__(self, database_url):
        self.base_url = "https://api.covidtracking.com/v2"
        self.engine = create_engine(database_url)
        
    def _rate_limited_request(self, url, max_retries=5):
        for attempt in range(max_retries):
            try:
                response = requests.get(url)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException:
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
        
    def ingest_states_metadata(self):
        url = f"{self.base_url}/states.json"
        data = self._rate_limited_request(url)
        
        states_df = pd.DataFrame([
            {
                'id': state['fips'],
                'name': state['name'],
                'state_code': state['state'],
                'census_population': state['census_population']
            } 
            for state in data['data']
        ])
        
        states_df.to_sql('states_and_territories', self.engine, if_exists='replace', index=False)
        
    def ingest_daily_cases(self, month='2021-02'):
        start_date = datetime.strptime(month, '%Y-%m')
        end_date = start_date + timedelta(days=31)
        
        while start_date < end_date:
            date_str = start_date.strftime('%Y-%m-%d')
            
            # National daily cases
            national_url = f"{self.base_url}/us/daily/{date_str}/simple.json"
            national_data = self._rate_limited_request(national_url)
            
            if national_data.get('data'):
                national_df = pd.DataFrame([{
                    'date': date_str,
                    'cases_total': national_data['data']['cases_total']
                }])
                national_df.to_sql('daily_cases', self.engine, if_exists='append', index=False)
            
            # State daily cases
            states_df = pd.read_sql('SELECT state_code FROM states_and_territories', self.engine)
            
            for _, row in states_df.iterrows():
                state_url = f"{self.base_url}/states/{row['state_code']}/{date_str}/simple.json"
                state_data = self._rate_limited_request(state_url)
                
                if state_data.get('data'):
                    state_df = pd.DataFrame([{
                        'state_id': row['state_code'],
                        'date': date_str,
                        'cases_total': state_data['data']['cases_total']
                    }])
                    state_df.to_sql('state_daily_cases', self.engine, if_exists='append', index=False)
            
            start_date += timedelta(days=1)

def main():
    database_url = os.getenv('DATABASE_URL', 'postgresql://admin:secretpassword@localhost:5432/covid_data')
    ingestion = COVIDDataIngestion(database_url)
    
    ingestion.ingest_states_metadata()
    ingestion.ingest_daily_cases()

if __name__ == '__main__':
    main()