'''
--------------------------------------------------------------------------------
Module Name:    nasdaq
Author:         Jon Peterson
Description:    This module scrapes the NASDAQ.com website for real-time stock
                quotes.
Dependencies:   Python 2.7.x
--------------------------------------------------------------------------------
'''

# Imports
import urllib.parse
import urllib.request
from dateutil import parser
from bs4 import BeautifulSoup
import argparse
import schedule

#-------------------------------------------------------------------------------
#                                 nasdaq class
#-------------------------------------------------------------------------------
class nasdaq(object):
    
    # Constants
    kBaseURI = 'http://www.nasdaq.com/symbol/%s/real-time'
    
    #---------------------------------------------------------------------------
    #                                __init__
    #---------------------------------------------------------------------------
    def __init__(self):
        self.parsedData = {}
        return
    
    #---------------------------------------------------------------------------
    #                               GetQuote
    #---------------------------------------------------------------------------
    def GetQuote(self, symbols=['AAPL','GOOG']):
        '''This method gets a real-time stock quote from the nasdaq website.'''
    
        # Make sure the quoteList is a list
        if type(symbols) != type([]):
            symbols = [symbols]
    
        # Create a list to store the quotes
        quotes = []
    
        # Iterate over all symbols
        for symbol in symbols:
    
            # Create the full query
            url = self.kBaseURI % symbol
    
            # Make sure the URL is formatted correctly
            self.url = urllib.parse.quote(url, safe="%/:=&?~#+!$,;'@()*[]")

            print(self.url)
            
            # Retrieve the data
            urlFile = urllib.request.urlopen(self.url)
            self.urlData = urlFile.read() 
            urlFile.close()
            
            # Parse the HTML data
            parsedData = self._parseHTML(self.urlData, symbol)
            
            # Append the data to the quotes
            quotes.append(parsedData)

        # Save the quotes
        self.quotes = quotes

        return quotes
    
    #---------------------------------------------------------------------------
    #                                _parseHTML
    #---------------------------------------------------------------------------
    def _parseHTML(self, html='', symbol=''):
        ''' A method which parses the HTML returned by the NASDAQ website. '''    

        # # Get the data after "<div class="quote-wrap">"
        # html = html[html.find('<div class="quote-wrap">'):]
        
        # # Get the data before "<!--end quote-wrap-->"
        # html = html[:html.find('<!--end quote-wrap-->')]
        
        # Save the HTML
        self.html = html
        
        # # Get the company name
        # self.name = self._getCompanyName()
        
        # # Get the price
        self.price = self._getPrice()
        
        # # Get the net change
        # self.netChange = self._getNetChange()
        
        # # Get the percent change
        # self.percentChange = self._getPercentChange()
        
        # # Get the timestamp
        # self.timestamp = self._getTimestamp()

        # # Get the exchange
        # self.exchange = self._getExchange()

        # # Get the sector
        # self.sector = self._getSector()
        
        # Create a dictionary with the quote
        quote = {}
        #quote['symbol'] = symbol
        #quote['name'] = self.name
        quote['price'] = self.price
        #quote['netChange'] = self.netChange
        #quote['percentChange'] = self.percentChange
        #quote['timestamp'] = self.timestamp
        #quote['exchange'] = self.exchange
        #quote['sector'] = self.sector
        
        return quote
        
    
    #---------------------------------------------------------------------------
    #                             _getCompanyName
    #---------------------------------------------------------------------------
    def _getCompanyName(self):
        ''' Helper method for parsing the company name out of the HTML. '''
    
        # Get the header DIV
        self.html = self.html[self.html.find('qwidget_pageheader'):]
        header = self.html[:self.html.find('</div>')]
        
        # Narrow the data between the two header tags
        header = header[header.find('<h1>')+4:header.find('</h1>')]
        
        # Remove the " Real Time Stock Quotes" text
        name = header.replace(' Real Time Stock Quotes', '')
                
        return name
        
        
    #---------------------------------------------------------------------------
    #                               _getPrice
    #---------------------------------------------------------------------------
    def _getPrice(self):
        ''' Helper method for parsing the price out of the HTML. '''
        
        # # Get the last-sale div
        # self.html = self.html[self.html.find('qwidget_lastsale'):]
        html = BeautifulSoup(self.html, 'html.parser')
        lastSale = html.find("div", {"id": "qwidget_lastsale"}).string
        
        # # Get everything after the ">"
        # lastSale = lastSale[lastSale.find('>')+1:]
        
        # # Remove the dollar sign
        lastSale = lastSale.replace('$', '')
        
        # # Convert the price to a float
        price = float(lastSale)



        
        return price
        
    
    #---------------------------------------------------------------------------
    #                               _getNetChange
    #---------------------------------------------------------------------------
    def _getNetChange(self):
        ''' Helper method for parsing the net price change out of the HTML. '''
        
        # Get the qwidget_netchange div
        self.html = self.html[self.html.find('qwidget_netchange'):]
        netChange = self.html[:self.html.find('</div>')]
        
        # Get everything after the ">"
        netChange = netChange[netChange.find('>')+1:]
        
        # Remove the dollar sign (if there is one)
        netChange = netChange.replace('$', '')
        
        # Convert the netChange to a float
        netChange = float(netChange)
        
        return netChange
        
    
    #---------------------------------------------------------------------------
    #                             _getPercentChange
    #---------------------------------------------------------------------------
    def _getPercentChange(self):
        ''' Helper method for parsing the percent change out of the HTML. '''
        
        # Get the qwidget_percent div
        self.html = self.html[self.html.find('qwidget_percent'):]
        percentChange = self.html[:self.html.find('</div>')]
        
        # Get everything after the ">"
        percentChange = percentChange[percentChange.find('>')+1:]
        
        # Remove the % sign
        percentChange = percentChange.replace('%', '')
        
        # Convert the percentChange to a float
        percentChange = float(percentChange)
        
        return percentChange

    
    #---------------------------------------------------------------------------
    #                             _getTimestamp
    #---------------------------------------------------------------------------
    def _getTimestamp(self):
        ''' Helper method for parsing the timestamp out of the HTML. '''
        
        # Get the qwidget_markettime span
        self.html = self.html[self.html.find('qwidget_markettime"'):]
        ts = self.html[:self.html.find('</span>')]
        
        # Get everything after the ">"
        ts = ts[ts.find('>')+1:]
        
        # Convert to a datetime object (parse the date string)
        ts = parser.parse(ts)
        
        return ts

    
    #---------------------------------------------------------------------------
    #                             _getExchange
    #---------------------------------------------------------------------------
    def _getExchange(self):
        ''' Helper method for parsing the exchange out of the HTML. '''
        
        # Get the qbar_exchangeLabel div
        self.html = self.html[self.html.find('qbar_exchangeLabel'):]
        exchange = self.html[:self.html.find('</span>')]
        
        # Get everything after the "</b>"
        exchange = exchange[exchange.find('</b>')+4:]
        
        return exchange


    #---------------------------------------------------------------------------
    #                              _getSector
    #---------------------------------------------------------------------------
    def _getSector(self):
        ''' Helper method for parsing the sector out of the HTML. '''
        
        # Get the qbar_sectorLabel div
        self.html = self.html[self.html.find('qbar_sectorLabel'):]
        sector = self.html[:self.html.find('</a>')]
        
        # Remove the link
        sector = sector[sector.find('<a')+2:]
        sector = sector[sector.find('>')+1:]
        
        return sector


    #---------------------------------------------------------------------------
    #                                 __str__
    #---------------------------------------------------------------------------
    def __str__(self):
        ''' Overload the string method for printing. '''
        # This should be updated to output something a little nicer
        return str(self.quotes)

    

#-------------------------------------------------------------------------------
#                                    main
#-------------------------------------------------------------------------------
def main():
    ''' A function used to test the yql class. '''
    
    # Create a nasdaq object
    n = nasdaq()
    
    # Get a quote
    quote = n.GetQuote(['AAPL', 'GOOG', 'CY'])
    
    # Print the quotes
    print(quote)
    
    return

if __name__ == '__main__':

    # Create a nasdaq object
    n = nasdaq()
     # - setup command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('symbol', help='the symbol of the stock to collect', nargs='*')
    symbols = []
    # - parse arguments
    args = parser.parse_args()
    for f in args.symbol:
        symbols.append(f)
    #symbol = args.symbol

    print(n.GetQuote(symbols))

# End of File