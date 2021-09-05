import urllib.request, json
import pandas as pd


class api_reader():
    
    #Initialize Class, default endpoints are embedded
    def __init__(self, values_endpoint = 'https://data.messari.io/api/v1/assets/', assets_endpoint = 'https://data.messari.io/api/v1/assets?fields=symbol'):
        self.values_endpoint = values_endpoint
        self.assets_endpoint = assets_endpoint
        
    def standardize_date(self, date):
        return date[0:10]
        
    def get_all_assets(self):
        url =  self.assets_endpoint
        assets = self.read_url(url)
        
        results = []
        for i in range(len(assets['data'])):
            results.append(assets['data'][i]['symbol'])
        
        return results
            
    
    def read_url(self, url):
        #Read url from endpoint
        with urllib.request.urlopen(url) as url:
            data = json.loads(url.read().decode())
        return(data)
    
    def return_values(self, url):
        
        #Passes in url of messari api with stock ticker, start date, and end date and returns a dataframe with values
        
        data = self.read_url(url)
            
        try:
            
            #Extract values, colnames of values, start_date, end_date
            values = data['data']['values']
            colnames = data['data']['parameters']['columns']
            
            start_date = data['data']['parameters']['start']
            end_date = data['data']['parameters']['end']
            
            start_date = self.standardize_date(start_date)
            end_date = self.standardize_date(end_date)
        
        except:
            print('Api formatting has changed, compare function with json structure')
            return
        
        #construct dataframe and return values
        dates_idx = pd.date_range(start=start_date, end = end_date)
        dates_df = pd.DataFrame({'Dates':dates_idx})
        values_df = pd.DataFrame(values, columns=colnames)
        final_df = pd.concat([dates_df, values_df], axis=1)
        
        #select only closing values
        return final_df[['Dates', 'close']]
    
    
    def return_df(self, stock_list, start_date, end_date):
        
        #Calls the return values function to return a dataframe with all closing values in stock list
        
        base_url = self.values_endpoint
        date_param = '?start=' + start_date + '&end=' + end_date + '&interval=1d'
        values_param = '/metrics/price/time-series'
    
        
        init = 0
        for i in stock_list:
            url = base_url + i + values_param + date_param
            df = self.return_values(url)
            df.rename({"close": i}, axis = "columns", inplace = True)
            
            
            #outer join for all tickers
            if init == 0:
                base = df
                init = 1
            else:
                base = base.merge(df, on ='Dates', how = 'outer')
                
        return(base)
    
                  
    
def main():
    
    #Class demonstration
    Messari = api_reader()
    asset_list = Messari.get_all_assets()
    
    print(asset_list)
    
    df = Messari.return_df(['DOT', 'UNI'], '2020-10-20', '2021-03-03')
    
    print(df)
    
    df.to_csv('results.csv', index = False)  
    
    
if __name__ == "__main__":
    main()
