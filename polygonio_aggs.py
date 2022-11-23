from polygon import RESTClient
import json
import datetime as dt
import os
import pandas as pd


# set up the client
with open('creds.json', 'r') as f:
    data = json.load(f)
key = data['api_key']

client = RESTClient(api_key=key)

def load_contracts():
    """
    Query for options contracts
    Exports two csv files for call and put contract ticker info
    @ params: none
    @ return: none
    """
    call_contracts = []
    for c in client.list_options_contracts(underlying_ticker = TICKER,
                                           limit = 1000,
                                           contract_type='call',
                                           expired=EXPIRED,
                                           expiration_date_gte=EXPIRATION_DATE_GTE,
                                           expiration_date_lte=EXPIRATION_DATE_LTE):
        
        print(f"Appending call contract: {c}")
        call_contracts.append(c)

    put_contracts = []
    for c in client.list_options_contracts(underlying_ticker = TICKER,
                                           limit = 1000,
                                           contract_type='put',
                                           expired=EXPIRED,
                                           expiration_date_gte=EXPIRATION_DATE_GTE,
                                           expiration_date_lte=EXPIRATION_DATE_LTE):
        
        print(f"Appending put contract: {c}")
        put_contracts.append(c)

    print(f"Loaded {len(call_contracts)} call contracts and {len(put_contracts)} put contracts")
    print(f"")
    print("- " * 30)
    print(f"Sample call contract row: {call_contracts[2]}")
    print(f"Sample put contract row: {put_contracts[2]}")
    
    # convert call_contracts and put_contracts to dataframes than export to csv
    call_contracts_df = pd.DataFrame(call_contracts)
    put_contracts_df = pd.DataFrame(put_contracts)
    
    print(f"Exporting {len(call_contracts)} call contracts and {len(put_contracts)} put contracts to csv")
    
    call_contracts_df.to_csv(f"contract_data/{TICKER}_contracts_call_{dt.datetime.now().date()}.csv")
    put_contracts_df.to_csv(f"contract_data/{TICKER}_contracts_put_{dt.datetime.now().date()}.csv")
    
def get_options_data(call_contracts, put_contracts):
    """
    Takes csv file paths for the call and put contracts, queries the polygon api for 
    the options contract data (O, H, L, C, Volume, etc), appends results to a csv file.

    Incase the script is interupted, already queried contract tickers will not be queried again.

    @ params: csv file path for call and put contracts retrieved from load_contracts()
    @ returns: None
    """
    def csv_tickers_to_list(csv_contracts_path) -> list:
        df = pd.read_csv(csv_contracts_path)
        df = df.sort_values(by=['ticker'])
        l = df['ticker'].tolist()
        return l
    
    call_contracts_list = csv_tickers_to_list(call_contracts)
    put_contracts_list = csv_tickers_to_list(put_contracts)

    def txt_to_list(file_name) -> list:     
        if os.path.exists(WORKING_DIR+file_name):
            return [line.strip() for line in open(WORKING_DIR + file_name, 'r')]
        else: return []

    # if resuming the data collection, load the list of tickers that have already been queried
    call_not_working = txt_to_list("/call_requested_not_working.txt")
    put_not_working = txt_to_list("/put_requested_not_working.txt")
    call_requested = txt_to_list("/call_requested.txt")
    put_requested = txt_to_list("/put_requested.txt")
    
    call_contracts_visited = call_not_working + call_requested
    put_contracts_visited = put_not_working + put_requested
    
    # remove the tickers that have already been queried from the list of tickers to query
    call_set = set(call_contracts_list) - set(call_contracts_visited)
    put_set = set(put_contracts_list) - set(put_contracts_visited)
    
    def append_contract_to_file(contract, file_path):        
        if not os.path.exists(file_path):
            with open(f"{file_path}", 'w') as f:
                f.write(contract+'\n')
        else: # else it exists so append without writing the header
            with open(f"{file_path}", 'a') as f:
                f.write(contract+'\n') 
    
    def get_aggregates(contract, contract_type: str):
        """ calls polygon.io for aggregates data for a given contract and saves to csv """
        aggs_raw = client.get_aggs(ticker = contract, 
                                    multiplier = MULTIPLIER, timespan = TIMESPAN,
                                    from_ = AGGREGATES_FORM, to = AGGREGATES_TO,
                                    limit = 1000)
            
        df = pd.DataFrame(aggs_raw)
        df['Date'] = df['timestamp'].apply(lambda x: pd.to_datetime(x*1000000))
        df = df.set_index('Date')
        df['Contract'] = contract
        df['Contract_type'] = contract_type
        df.drop(['timestamp'], axis=1, inplace=True)
        
        file_name = f"{WORKING_DIR}/{TICKER}_{contract_type}_{TIMESPAN}_{MULTIPLIER}x.csv"
        
        # check if filename does not exist
        if not os.path.exists(file_name):
            df.to_csv(file_name)
        else: # else it exists so append without writing the header
            df.to_csv(file_name, mode='a', header=False)        
               
        print(f"Appending: \t\t\t {contract}")
        append_contract_to_file(contract, f"{WORKING_DIR}/{contract_type}_requested.txt")           
    
    for contract in call_set:
        try:
            get_aggregates(contract, 'call')
        except:
            print("Could not retrieve data for contract: ", contract)
            append_contract_to_file(contract, f"{WORKING_DIR}/call_requested_not_working.txt")
            continue

    for contract in put_set:
        try:
            get_aggregates(contract, 'put')
        except:
            print("Could not retrieve data for contract: ", contract)
            append_contract_to_file(contract, f"{WORKING_DIR}/put_requested_not_working.txt")
            continue

if __name__ == '__main__':
    
    # create the directory if it doesn't exist
    WORKING_DIR = "options_data_2022_11_10" 
    if not os.path.isdir(f"{WORKING_DIR}"):
        os.mkdir(f"{WORKING_DIR}")
    
    # constants used for function load_contracts()
    EXPIRATION_DATE_GTE = "2020-01-01"
    EXPIRATION_DATE_LTE = "2024-01-01"
    EXPIRED = True
    
    # constants used for function get_options_data()
    TICKER = 'SPY'
    MULTIPLIER = 15
    TIMESPAN = 'minute'
    AGGREGATES_FORM = "2020-01-01"
    AGGREGATES_TO = "2100-01-01"
    
    # - - 1 - - # load options contracts
    # load_contracts()

    # - - 2 - - # import the call and put contract csv files genertated by func load_contracts()
    call_contracts_path = "contract_data/SPY_call_contracts_2022-11-04.csv"
    put_contracts_path = "contract_data/SPY_put_contracts_2022-11-04.csv"
    
    # - - 3 - - # call the function get_options_data(), may take many hours depending on the time frame
    get_options_data(call_contracts_path, put_contracts_path)


# # # # # # # # # # CODE CEMETARY # # # # # # # # # # # # # # # # # # # # #
#
#
#
#
#
#
#
#
#
#
