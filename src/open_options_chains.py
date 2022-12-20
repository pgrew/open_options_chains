# + -------------------- +
# | COLLECT OPTIONS DATA |
# + -------------------- +

# Imports
import os
import pendulum

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Use the SNP 500
TICKERS = ['ABT', 'ABBV', 'ABMD', 'ACN', 'ATVI', 'ADBE', 'AMD', 'AAP', 'AES', 'AFL', 'A', 'APD', 'AKAM', 'ALK', 'ALB', 'ARE', 'ALGN', 'ALLE', 'ALL', 'AMZN', 'AMCR', 'AEE', 'AAL', 'AEP', 'AXP', 'AIG', 'AMT', 'AWK', 'AMP', 'ABC', 'AME', 'AMGN', 'APH', 'ADI', 'ANSS', 'ANTM', 'AON', 'AOS', 'APA', 'AAPL', 'AMAT', 'APTV', 'ADM', 'ANET', 'AJG', 'AIZ', 'ATO', 'ADSK', 'ADP', 'AZO', 'AVB', 'AVY', 'AVGO', 'BKR', 'BLL', 'BAC', 'BBWI', 'BAX', 'BDX', 'BRK.B', 'BBY', 'BIO', 'BIIB', 'BLK', 'BK', 'BA', 'BKNG', 'BWA', 'BXP', 'BSX', 'BMY', 'BR', 'BRO', 'BF.B', 'BEN', 'CHRW', 'CDNS', 'CZR', 'CPB', 'COF', 'CAH', 'CCL', 'CARR', 'CTLT', 'CAT', 'CBOE', 'CBRE', 'CDW', 'CE', 'CNC', 'CNP', 'CDAY', 'CERN', 'CF', 'CRL', 'CHTR', 'CVX', 'CMG', 'CB', 'CHD', 'CI', 'CINF', 'CTAS', 'CSCO', 'C', 'CFG', 'CTXS', 'CLX', 'CME', 'CMS', 'CTSH', 'CL', 'CMCSA', 'CMA', 'CAG', 'COP', 'COO', 'CPRT', 'CTVA', 'COST', 'CTRA', 'CCI', 'CSX', 'CMI', 'CVS', 'CRM', 'DHI', 'DHR', 'DRI', 'DVA', 'DE', 'DAL', 'DVN', 'DXCM', 'DLR', 'DFS', 'DISCA', 'DISCK', 'DISH', 'DG', 'DLTR', 'D', 'DPZ', 'DOV', 'DOW', 'DTE', 'DUK', 'DRE', 'DD', 'DXC', 'DGX', 'DIS', 'ED', 'EMN', 'ETN', 'EBAY', 'ECL', 'EIX', 'EW', 'EA', 'EMR', 'ENPH', 'ETR', 'EOG', 'EFX', 'EQIX', 'EQR', 'ESS', 'EL', 'ETSY', 'EVRG', 'ES', 'EXC', 'EXPE', 'EXPD', 'EXR', 'FANG', 'FFIV', 'FB', 'FAST', 'FRT', 'FDX', 'FIS', 'FITB', 'FE', 'FRC', 'FISV', 'FLT', 'FMC', 'F', 'FTNT', 'FTV', 'FBHS', 'FOXA', 'FOX', 'FCX', 'GOOGL', 'GOOG', 'GLW', 'GPS', 'GRMN', 'GNRC', 'GD', 'GE', 'GIS', 'GM', 'GPC', 'GILD', 'GL', 'GPN', 'GS', 'GWW', 'HAL', 'HBI', 'HIG', 'HAS', 'HCA', 'HSIC', 'HSY', 'HES', 'HPE', 'HLT', 'HOLX', 'HD', 'HON', 'HRL', 'HST', 'HWM', 'HPQ', 'HUM', 'HBAN', 'HII', 'IT', 'IEX', 'IDXX', 'INFO', 'ITW', 'ILMN', 'INCY', 'IR', 'INTC', 'ICE', 'IBM', 'IP', 'IPG', 'IFF', 'INTU', 'ISRG', 'IVZ', 'IPGP', 'IQV', 'IRM', 'JKHY', 'J', 'JBHT', 'JNJ', 'JCI', 'JPM', 'JNPR', 'KMX', 'KO', 'KSU', 'K', 'KEY', 'KEYS', 'KMB', 'KIM', 'KMI', 'KLAC', 'KHC', 'KR', 'LNT', 'LHX', 'LH', 'LRCX', 'LW', 'LVS', 'LEG', 'LDOS', 'LEN', 'LLY', 'LNC', 'LIN', 'LYV', 'LKQ', 'LMT', 'L', 'LOW', 'LUMN', 'LYB', 'LUV', 'MMM', 'MO', 'MTB', 'MRO', 'MPC', 'MKTX', 'MAR', 'MMC', 'MLM', 'MAS', 'MA', 'MTCH', 'MKC', 'MCD', 'MCK', 'MDT', 'MRK', 'MET', 'MTD', 'MGM', 'MCHP', 'MU', 'MSFT', 'MAA', 'MRNA', 'MHK', 'MDLZ', 'MPWR', 'MNST', 'MCO', 'MS', 'MOS', 'MSI', 'MSCI', 'NDAQ', 'NTAP', 'NFLX', 'NWL', 'NEM', 'NWSA', 'NWS', 'NEE', 'NLSN', 'NKE', 'NI', 'NSC', 'NTRS', 'NOC', 'NLOK', 'NCLH', 'NRG', 'NUE', 'NVDA', 'NVR', 'NXPI', 'NOW', 'ORLY', 'OXY', 'ODFL', 'OMC', 'OKE', 'ORCL', 'OGN', 'OTIS', 'O', 'PEAK', 'PCAR', 'PKG', 'PH', 'PAYX', 'PAYC', 'PYPL', 'PENN', 'PNR', 'PBCT', 'PEP', 'PKI', 'PFE', 'PM', 'PSX', 'PNW', 'PXD', 'PNC', 'POOL', 'PPG', 'PPL', 'PFG', 'PG', 'PGR', 'PLD', 'PRU', 'PTC', 'PEG', 'PSA', 'PHM', 'PVH', 'PWR', 'QRVO', 'QCOM', 'RE', 'RL', 'RJF', 'RTX', 'REG', 'REGN', 'RF', 'RSG', 'RMD', 'RHI', 'ROK', 'ROL', 'ROP', 'ROST', 'RCL', 'SCHW', 'STZ', 'SJM', 'SPGI', 'SBAC', 'SLB', 'STX', 'SEE', 'SRE', 'SHW', 'SPG', 'SWKS', 'SNA', 'SO', 'SWK', 'SBUX', 'STT', 'STE', 'SYK', 'SIVB', 'SYF', 'SNPS', 'SYY', 'T', 'TECH', 'TAP', 'TMUS', 'TROW', 'TTWO', 'TPR', 'TGT', 'TEL', 'TDY', 'TFX', 'TER', 'TSLA', 'TXN', 'TXT', 'TMO', 'TJX', 'TSCO', 'TT', 'TDG', 'TRV', 'TRMB', 'TFC', 'TWTR', 'TYL', 'TSN', 'UDR', 'ULTA', 'USB', 'UAA', 'UA', 'UNP', 'UAL', 'UNH', 'UPS', 'URI', 'UHS', 'VLO', 'VTR', 'VRSN', 'VRSK', 'VZ', 'VRTX', 'VFC', 'VIAC', 'VTRS', 'V', 'VNO', 'VMC', 'WRB', 'WAB', 'WMT', 'WBA', 'WM', 'WAT', 'WEC', 'WFC', 'WELL', 'WST', 'WDC', 'WU', 'WRK', 'WY', 'WHR', 'WMB', 'WLTW', 'WYNN', 'XRAY', 'XOM', 'XEL', 'XLNX', 'XYL', 'YUM', 'ZBRA', 'ZBH', 'ZION', 'ZTS']

# Get variables
API_KEY = os.environ['API_KEY']
DB_PASSWORD = os.environ['APP_DB_PASS']

# Set arguments
us_east_tz = pendulum.timezone('America/New_York')
default_args = {
    'owner': 'pgrew',
    'start_date': datetime(2022, 1, 7, 7, 30, tzinfo=us_east_tz),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Function to create table
def create_table(ticker):
    # Define Postgres hook
    pg_hook = PostgresHook(postgres_conn_id='postgres_optionsdata')

    # Create table if it doesn't exist
    pg_hook.run(f"""
CREATE TABLE IF NOT EXISTS {ticker} (
    put_call VARCHAR(5) NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    description VARCHAR(64) NOT NULL,
    bid DOUBLE PRECISION,
    ask DOUBLE PRECISION,
    last DOUBLE PRECISION,
    bid_size INTEGER,
    ask_size INTEGER,
    last_size INTEGER,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    open_price DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    total_volume INTEGER,
    quote_time BIGINT,
    volatility DOUBLE PRECISION,
    delta DOUBLE PRECISION,
    gamma DOUBLE PRECISION,
    theta DOUBLE PRECISION,
    vega DOUBLE PRECISION,
    rho DOUBLE PRECISION,
    open_interest INTEGER,
    time_value DOUBLE PRECISION,
    theoretical_value DOUBLE PRECISION,
    strike_price DOUBLE PRECISION,
    expiration_date BIGINT,
    dte INTEGER,
    PRIMARY KEY (symbol, quote_time)
)
""")

# Function to get data from TDA API
def extract_options_data_from_tda(ticker, ti):
    # Import modules
    import json
    import requests

    # Configure dates
    TIMEDELTA = timedelta(days=45)
    # start_date = datetime.today(tzinfo=us_east_tz)
    start_date = datetime.utcnow().replace(tzinfo=us_east_tz)
    end_date = start_date + TIMEDELTA
    
    # Configure request
    headers = {
        'Authorization': '',
    }

    params = (
        ('apikey', API_KEY),
        ('symbol', ticker),
        ('contractType', 'PUT'),
        ('strikeCount', '50'),
        ('range', 'ALL'),
        ('fromDate', start_date),
        ('toDate', end_date),
    )

    # Get data
    response = requests.get(
        'https://api.tdameritrade.com/v1/marketdata/chains',
        headers=headers,
        params=params
    )
    data = json.loads(response.content)

    # Push XCOM
    ti.xcom_push(key='raw_data', value=data)

# ---- PARSE DATA ---- #
def transform_options_data(ti):
    
    # Import modules
    import pandas as pd

    # Pull XCOM
    data = ti.xcom_pull(key='raw_data', task_ids=['extract_options_data_from_tda'])[0]

    # Define columns
    columns = ['putCall', 'symbol', 'description', 'exchangeName', 'bid', 'ask',
        'last', 'mark', 'bidSize', 'askSize', 'bidAskSize', 'lastSize',
        'highPrice', 'lowPrice', 'openPrice', 'closePrice', 'totalVolume',
        'tradeDate', 'tradeTimeInLong', 'quoteTimeInLong', 'netChange',
        'volatility', 'delta', 'gamma', 'theta', 'vega', 'rho', 'openInterest',
        'timeValue', 'theoreticalOptionValue', 'theoreticalVolatility',
        'optionDeliverablesList', 'strikePrice', 'expirationDate',
        'daysToExpiration', 'expirationType', 'lastTradingDay', 'multiplier',
        'settlementType', 'deliverableNote', 'isIndexOption', 'percentChange',
        'markChange', 'markPercentChange', 'mini', 'inTheMoney', 'nonStandard']

    # Extract puts data
    puts = []
    dates = list(data['putExpDateMap'].keys())

    for date in dates:

        strikes = data['putExpDateMap'][date]

        for strike in strikes:
            puts += data['putExpDateMap'][date][strike]

    # Convert to dataframe
    puts = pd.DataFrame(puts, columns=columns)

    # Select columns
    puts = puts[['putCall', 'symbol', 'description', 'bid', 'ask', 'last', 'bidSize',
        'askSize', 'lastSize', 'highPrice', 'lowPrice', 'openPrice',
        'closePrice', 'totalVolume', 'quoteTimeInLong', 'volatility', 'delta',
        'gamma', 'theta', 'vega', 'rho', 'openInterest', 'timeValue',
        'theoreticalOptionValue', 'strikePrice', 'expirationDate',
        'daysToExpiration']]

    # Convert floats
    def conv_num(x):
        return pd.to_numeric(x.astype(str).str.replace('NaN|nan', '', regex=True))

    for col in ['bid', 'ask', 'last', 'highPrice', 'lowPrice', 'openPrice',
                'closePrice', 'volatility', 'delta', 'gamma', 'theta', 'vega',
                'rho', 'timeValue', 'theoreticalOptionValue', 'strikePrice']:
        puts[col] = conv_num(puts[col])

    # Specifically for puts delta: make it positive
    puts['delta'] = -puts['delta']

    # Convert strings
    def conv_str(x):
        return x.astype(str)

    for col in ['putCall', 'symbol', 'description']:
        puts[col] = conv_str(puts[col])

    # Convert integers
    def conv_int(x):
        return x.astype(int)

    for col in ['bidSize', 'askSize', 'lastSize', 'totalVolume', 'quoteTimeInLong',
                'openInterest', 'expirationDate', 'daysToExpiration']:
        puts[col] = conv_int(puts[col])

    # Fill missing values
    puts = puts.fillna(-99)

    # Rename columns
    puts = puts.rename(columns={
        'putCall': 'put_call',
        'bidSize': 'bid_size',
        'askSize': 'ask_size',
        'lastSize': 'last_size',
        'highPrice': 'high_price',
        'lowPrice': 'low_price',
        'openPrice': 'open_price',
        'closePrice': 'close_price',
        'totalVolume': 'total_volume',
        'quoteTimeInLong': 'quote_time',
        'openInterest': 'open_interest',
        'timeValue': 'time_value',
        'theoreticalOptionValue': 'theoretical_value',
        'strikePrice': 'strike_price',
        'expirationDate': 'expiration_date',
        'daysToExpiration': 'dte',
    })

    # Push XCOM
    ti.xcom_push(key='transformed_data', value=puts.to_dict('records'))

# ---- LOG INTO POSTGRES ---- #
def load_data_into_postgres(ticker, ti):
    
    # Import modules
    import pandas as pd

    # Define Postgres hook
    pg_hook = PostgresHook(postgres_conn_id='postgres_optionsdata')

    # Pull XCOM
    puts = ti.xcom_pull(key='transformed_data', task_ids=['transform_options_data'])[0]
    puts = pd.DataFrame(puts)

    # Prepare insert query
    col_str = ', '.join(puts.columns.tolist())
    query_insert = f"INSERT INTO {ticker} ({col_str}) VALUES %s ON CONFLICT DO NOTHING"

    # Convert to rows
    rows = list(puts.itertuples(index=False, name=None))
    for row in rows:
        pg_hook.run(query_insert % str(row))

# Function to create DAG
def create_dag(ticker, default_args):
    dag = DAG(
        dag_id=f'get_options_data_{ticker}',
        default_args=default_args,
        description=f'ETL for {ticker} options data',
        schedule_interval='*/15 * * * 1-5',
        catchup=False,
        max_active_runs=1,
        tags=['finance', 'options', ticker]
    )

    with dag:

        # Define operators
        task0_create_table = PythonOperator(
            task_id='create_table',
            python_callable=create_table,
            op_kwargs={'ticker': ticker, 'weight_rule': 'downstream'}
        )

        task1_extract = PythonOperator(
            task_id='extract_options_data_from_tda',
            python_callable=extract_options_data_from_tda,
            op_kwargs={'ticker': ticker, 'weight_rule': 'downstream'}
        )

        task2_transform = PythonOperator(
            task_id = 'transform_options_data',
            python_callable=transform_options_data,
            op_kwargs={'weight_rule': 'downstream'}
        )

        task3_load = PythonOperator(
            task_id='load_data_into_postgres',
            python_callable=load_data_into_postgres,
            op_kwargs={'ticker': ticker, 'weight_rule': 'downstream'}
        )

        # Set up dependencies
        task0_create_table >> task1_extract >> task2_transform >> task3_load

        return dag


# Create DAGs
for ticker in TICKERS:
    globals()[f'get_options_data_{ticker}'] = create_dag(ticker, default_args)
