import pandas as pd
import requests

from io import StringIO

######################################### Items DF ########################################

def get_items():
    '''
    This function obtains the items data from the base url, 
    loops through items pages,
    makes items df,
    and writes the df to a csv, and 
    
    returns the data in a pandas dataframe
    '''

    items_list = []

    response = requests.get('https://python.zach.lol/api/v1/items')
    data = response.json()
    n = data['payload']['max_page']

    for i in range(1, n+1):
        url = 'https://python.zach.lol/api/v1/items?page=' + str(i)
        response = requests.get(url)
        data = response.json()
        page_items = data['payload']['items']
        items_list += page_items

    #create df
    items = pd.DataFrame(items_list)

    #make items df as a csv
    items.to_csv('items.csv')
    
        
    return items

######################################### Stores DF #######################################

def get_stores():
    
    '''
    This function obtains the stores data from the base url, 
    loops through stores pages,
    makes stores df,
    and writes the df to a csv, and 
    
    returns the data in a pandas dataframe
    '''

    stores_list = []

    response = requests.get('https://python.zach.lol/api/v1/stores')
    data = response.json()
    n = data['payload']['max_page']

    for i in range(1, n+1):
        url = 'https://python.zach.lol/api/v1/stores?page=' + str(i)
        response = requests.get(url)
        data = response.json()
        page_stores = data['payload']['stores']
        stores_list += page_stores

    #create df
    stores = pd.DataFrame(stores_list)

    #make items df as a csv
    stores.to_csv('stores.csv')
    
        
    return stores
    
######################################### Sales DF ########################################

def get_sales():
    
    '''
    This function obtains the sales data from the base url, 
    loops through sales pages,
    makes sales df,
    and writes the df to a csv, and 
    
    returns the data in a pandas dataframe
    '''

    sales_list = []

    response = requests.get('https://python.zach.lol/api/v1/sales')
    data = response.json()
    n = data['payload']['max_page']

    for i in range(1, n+1):
        url = 'https://python.zach.lol/api/v1/sales?page=' + str(i)
        response = requests.get(url)
        data = response.json()
        page_sales = data['payload']['sales']
        sales_list += page_sales

    #create df
    sales = pd.DataFrame(sales_list)

    #make items df as a csv
    sales.to_csv('sales.csv')
    
        
    return sales

####################################### Full DF ########################################

def get_full_data():
    '''
    This function takes in the previous 3 functions above,
    merges the sales, stores, and items dfs,
    writes the df to a csv, and 
    
    returns the data in a pandas dataframe
    '''

    items = pd.read_csv('items.csv')
    stores = pd.read_csv('stores.csv')
    sales= pd.read_csv('sales.csv')

    #merge sales w/ stores
    combined_df = sales.merge(stores, left_on='store', right_on='store_id')

    #merge sales and stores on items
    combined_df = combined_df.merge(items, left_on='item', right_on='item_id')

    #make complete dataframe to a csv
    combined_df.to_csv('combined_df.csv')
        
    return combined_df

######################################### Germany DF ########################################

def get_germany_data():
    '''
    This function gets Open Power Systems Data for Germany,
    reads data as a csv, and 
    
    returns the data in a pandas dataframe
    '''

    url = 'https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv'

    germany_data = pd.read_csv(url)
    
    return germany_data

######################################### alternate functions ########################################


def _create_df_from_payloads(endpoint, max_pages, target_key_name):
    """
    Helper function that loops through the pages returned from the Zach API,\
    adds the information to a list and then converts to a single dataframe that is returned.
    """
    page_list = []
    for i in range(1, max_pages + 1):
        response = requests.get(endpoint + "?page=" + str(i))
        data = response.json()
        page_items = data['payload'][target_key_name]
        page_list += page_items
    return pd.DataFrame(page_list)



def acquire_df_from_zach_api(endpoint, target_key_name):
    """
    Takes in the api endpoint and desired key (items, stores, sales) and\
    returns a dataframe with the data from the response.
    """
    response = requests.get(endpoint)
    response = response.json()
    max_pages = response['payload']['max_page']
    return _create_df_from_payloads(endpoint, max_pages, target_key_name)



def merge_zach_dataframes(items_df, stores_df, sales_df):
    """
    Takes in the three data frames containing the information from the Zach API,\
    renames 'item' and 'store' columns from the sales_df to match the foreign keys\
    on the items_df and stores_df, then peforms left joins using those foreign keys.\
    Returns a single data frame containing the information from the original three.
    """
    merged_df = pd.DataFrame()
    sales_df.rename(columns={'item' : 'item_id'}, inplace=True)
    merged_df = pd.merge(items_df, sales_df, how="left", on="item_id")
    merged_df.rename(columns={'store' : 'store_id'}, inplace=True)
    merged_df = pd.merge(merged_df, stores_df, how="left", on="store_id")
    return merged_df



def acquire_germany():
    """
    Returns a dataframe containing Open Power Systems data for Germany.
    """
    response = req.get("https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv")
    csv = StringIO(response.text)
    
    return pd.read_csv(csv)