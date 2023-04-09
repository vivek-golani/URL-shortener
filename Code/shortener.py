import os
import time
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import urllib
import timeit
import math
from multiprocessing import Pool
from multiprocessing import cpu_count
import tqdm

def shorten(long_url, alias):
    
    # url encoding by converting & to %26 otherwise the filled entries in each retailer sheet are lost and all links are just empty forms
    long_url = long_url.replace('&','%26')
    
    # the url that contains the website url and custom alias
    URL = "http://tinyurl.com/create.php?source=indexpage&url=" + long_url + "&submit=Make+TinyURL!&alias=" + alias
    
    # request to open url
    response = urllib.request.urlopen(URL)

    soup = BeautifulSoup(response, 'lxml')
        
    # returning shortened url
    return soup.find_all('div', {'class': 'indent'})[1].b.string
    

if __name__ == '__main__':
    # to get the current working directory
    path = os.getcwd()
    
    # dataframe to read the excel file of long url links
    df = pd.read_excel(path + '/Input/sample links.xlsx',sheet_name = 'Test 2')
    
    dff = pd.DataFrame()
    #start time of the parser
    start = timeit.default_timer()
    
    # chunk size
    chunk = 1000
    size = math.ceil(len(df)/chunk)
    for i in range(size):
        # data frame to hold every chunk
        temp = df[(chunk*i):(chunk*i)+chunk]
        temp = temp.reset_index().drop(columns = ['index'])
        for j in range(len(temp)):
            url_long = temp['Link'][j]
            name = temp['Key'][j]
            temp['Shortened Link'][j] = shorten(url_long, name)

        #appending chunks sequentially
        dff = dff.append(temp, ignore_index = True)
        time.sleep(5)
    
    #stop time of the parser
    stop = timeit.default_timer()
    
    print('Run time = ' + str(stop - start) + 'seconds')

    dff.to_excel(path + '/Output/shortened urls.xlsx')
    

'''  
chunky_monkey = math.ceil(len(df)/cpu_count())
p = Pool(cpu_count())
data = p.map(func = shorten, iterable = df, chunksize = chunky_monkey)
p.terminate()
p.join()
'''