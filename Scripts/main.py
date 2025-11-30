from data_ingestion import DataIngestion
from data_cleaning import DataCleaning
from ai_agent import AIAgent

#database configuration
DB_USER= 'postgres'
DB_PASSWORD= '2004'
DB_HOST= 'localhost'
DB_PORT= '5432'
DB_NAME= 'demo'

DB_URL= f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'


ingestion= DataIngestion(DB_URL)
cleaner= DataCleaning()
ai_agent= AIAgent()

df_csv = ingestion.load_csv('sample_data.csv')
if df_csv is not None:
    print("\n cleaned CSV Data:")
    df_csv= cleaner.clean_data(df_csv)
    df_csv= ai_agent.process_data(df_csv)
    print("/n AI cleaned CSV Data:", df_csv)

df_excel = ingestion.load_excel('sample_data.xlsx')
if df_excel is not None:
    print("\n cleaned Excel Data:")
    df_excel= cleaner.clean_data(df_excel)
    df_excel= ai_agent.process_data(df_excel)
    print("/n AI cleaned Excel Data:", df_excel)

df_db = ingestion.load_from_database('SELECT * FROM users;')
if df_db is not None:
    print("\n cleaned Database Data:")
    df_db= cleaner.clean_data(df_db)
    df_db= ai_agent.process_data(df_db)
    print("/n AI cleaned Database Data:", df_db)


API_URL= 'https://jsonplaceholder.typicode.com/users'
df_api = ingestion.fetch_from_api(API_URL)
if df_api is not None:
    print("\n cleaned API Data:")
    df_api= df_api.head(30)  # Limiting to first 20 records for brevity
    if "body" in df_api.columns:
        df_api["body"] = df_api["body"].apply(lambda x: x[:100] if isinstance(x, str) else x)

    df_api= cleaner.clean_data(df_api)
    df_api= ai_agent.process_data(df_api)
    print("/n AI cleaned API Data:", df_api)