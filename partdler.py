from __future__ import print_function

import time
import requests
import dill

from itertools import chain
from bs4 import BeautifulSoup, SoupStrainer
from re import sub
from sys import stdout

from multiprocessing.dummy import Pool as ThreadPool
from pathos.multiprocessing import Pool as ProcessPool

from django.core.management import BaseCommand
from comparison.models import Item, Brand


class PartDLer(object):
    def __init__(self):
        self.brandlist = {}
        self.cookies = {}
        self.params = {}
        dill.copy(self.parseresponse)

    def get_thing(self, url):
        return requests.get(url, timeout=60, cookies=self.cookies, params=self.params)

    def parseresponse(self, response):
        pass
        return None

    def findproducts(self):
        print('[ ] Making requests...', end='')
        stdout.flush()
        dlstarttime = time.time()

        l = self.brandlist.values()

        rpool = ThreadPool(8)
        results = rpool.map(self.get_thing, l)
        rpool.close()
        rpool.join()

        dlfintime = time.time()
        print('\r' + '[x] Making requests...')
        print('Downloaded products in', str(round(dlfintime - dlstarttime)), 'seconds.')

        results = filter(None, results)

        print('[ ] Parsing responses...', end='')
        stdout.flush()

        pstarttime = time.time()

        ppool = ProcessPool()
        items = ppool.map(self.parseresponse, results)
        ppool.close()
        ppool.join()

        items = list(chain.from_iterable(items))

        print('\r' + '[x] Parsing responses...')

        print('[ ] Saving items...', end='')
        stdout.flush()
        Item.objects.bulk_create(filter(None, items))
        print('\r' + '[x] Saving items...')

        pfintime = time.time()

        print('Parsed responses in', str(round(pfintime - pstarttime)), 'seconds.')
        print()


class WPD(PartDLer):
    def __init__(self):
        super(WPD, self).__init__()
        self.params = {"g": '1', "ps": "96", "curr": "GBP"}

    def findbrands(self, url):
        print("[ ] Downloading brand list...", end="")

        f = BeautifulSoup(requests.get(url).text, "lxml")  # opening the site map page

        brandlist = f.find("div", class_="bem-well").find_all('a')

        print('\r' + '[x] Downloading brand list...')
        print('[ ] Parsing brand list...', end="")

        newbrandlist = {}

        for brand in brandlist:
            newbrandlist[brand.get_text()] = brand['href']
            b = Brand(brandname=brand.get_text(), brandurl=brand['href'])
            b.save()

        self.brandlist = newbrandlist

        print("\r" + "[x] Parsing brand list...")
        print()

    def parseresponse(self, response):
        items = []

        x = 1
        self.params['g'] = str(x)

        brand = str(response.url[24:-20]).replace("-", " ")

        prods = BeautifulSoup(response.text, "lxml").find("div", class_="MainColumn").find_all("a")
        for tempprod in prods:
            if 'title' in tempprod.attrs.keys():
                prodname = tempprod['title']
                produrl = tempprod['href']
                prodprice = tempprod.find_next("span", class_="bem-product-price__unit--grid").get_text()
                prodprice = prodprice[1:].replace(",", "")
                i = Item(itemname=prodname, itemprice=prodprice, itemurl=produrl, itembrand=brand,
                         itemretailer="Wiggle")
                items.append(i)
        while True:

            x += 96
            self.params['g'] = str(x)

            r = requests.get(response.url[:-19], params=self.params).text

            if r is None:
                break

            g = BeautifulSoup(r, "lxml")

            if "Sorry, we couldn't find anything that matches your search." not in r:

                prods = g.find("div", class_="MainColumn").find_all("a")
                for tempprod in prods:
                    if 'title' in tempprod.attrs.keys():
                        prodname = tempprod['title']
                        produrl = tempprod['href']
                        prodprice = tempprod.find_next("span", class_="bem-product-price__unit--grid").get_text()
                        prodprice = prodprice[1:].replace(",", "")
                        i = Item(itembrand=brand, itemname=prodname, itemprice=prodprice,
                                 itemurl=produrl, itemretailer='Wiggle')
                        items.append(i)
            else:
                break
        return items

    def main(self):
        print('Wiggle Part Downloader')
        print()
        self.findbrands("http://www.wiggle.co.uk/sitemap")
        self.findproducts()
        print('Finished downloading parts from Wiggle.')
        print()


class CRCPD(PartDLer):
    def __init__(self):
        super(CRCPD, self).__init__()
        self.params = {"perPage": "20000"}
        self.cookies = {"countryCode": "GB", "currencyCode": "GBP", "languageCode": "en"}

    def findbrands(self, url):
        print("[ ] Downloading brand list...", end="")
        stdout.flush()
        try:
            f = BeautifulSoup(requests.get(url, cookies=self.cookies).text, "lxml")
        except requests.exceptions.ConnectionError:
            time.sleep(2)
            f = BeautifulSoup(requests.get(url, cookies=self.cookies).text, "lxml")
        print('\r' + '[x] Downloading brand list...')
        print('[ ] Parsing brand list...', end="")
        stdout.flush()

        brandlist = f.find("ul", id="AllbrandList").find_all('a')

        newbrandlist = {}

        for brand in brandlist:
            newbrandlist[brand.get_text()] = "http://chainreactioncycles.com" + brand['href']

        self.brandlist = newbrandlist

        print("\r" + "[x] Parsing brand list...")
        print()

    def parseresponse(self, response):
        brand = response.url[35:]
        items = []
        if "No items match your search for" not in response:
            parsed_html = BeautifulSoup(response.text, "lxml")
            containerlist = parsed_html.find_all('div', class_='products_details_container')

            if containerlist is not None:
                for container in containerlist:
                    if container.find('li', class_='bundle_msg') is None:
                        produrl = container.find('a')['href']
                        prodname = container.find('a').find('img')['alt']
                        prodprice = container.find('li', class_='fromamt').get_text()

                        if '-' not in prodprice:
                            prodprice = sub(r'[^\d.]+', '', prodprice)

                            if prodprice != '':
                                i = Item(itemname=prodname, itemprice=float(prodprice),
                                         itemurl='http://www.chainreactioncycles.com/gb/en' + produrl, itembrand=brand,
                                         itemretailer='Chain Reaction Cycles')
                                items.append(i)
        return items

    def main(self):
        print('Chain Reaction Cycles Part Downloader')
        print()
        self.findbrands("http://www.chainreactioncycles.com/sitemap")
        self.findproducts()
        print('Finished downloading parts from Chain Reaction Cycles.')
        print()


class BDPD(PartDLer):
    def findbrands(self, url):
        print("[ ] Downloading brand list...", end="")
        stdout.flush()

        newbrandlist = {}

        g = str(BeautifulSoup(requests.get(url).text, "lxml").find('div', class_='otherbrands'))

        print('\r'+'[x] Downloading brand list...')
        print('[ ] Parsing brand list...', end="")
        stdout.flush()

        for brand in BeautifulSoup(g, "lxml", parse_only=SoupStrainer('a')):  # opening the site map page
            if brand.has_attr('href'):
                if '#' not in brand['href']:
                    newbrandlist[brand.get_text()] = 'http://bike-discount.de'+brand['href']

        self.brandlist = newbrandlist

        print("\r"+"[x] Parsing brand list...")
        print()

    def parseresponse(self, response):
        items = []
        for prod in BeautifulSoup(response.text, "lxml").find_all('div', class_="element_artikel_gallery no-buy"):
            prodelement = prod.find('a', attrs={'itemprop': 'url'})

            prodname = prodelement['title'][18:]
            prodprice = prod.find('meta', attrs={'itemprop': 'price'})['content']
            produrl = prodelement['href']
            prodbrand = prod.find('span', attrs={'class': 'manufacturer'}).getText()

            i = Item(itemname=prodname, itemprice=prodprice, itemurl=produrl, itembrand=prodbrand,
                     itemretailer='BikeDiscount.de')
            items.append(i)
        return items

    def main(self):
        print('Bike-Discount.de Part Downloader')
        print()
        self.findbrands("http://www.bike-discount.de/en/brands")
        self.findproducts()
        print('Finished downloading parts from BikeDiscount.de')
        print()


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('--all', action='store_true', dest='all', default=False,
                            help='Scrape products from all websites.')
        parser.add_argument('--wpd', action='store_true', dest='wpd', default=False,
                            help='Scrape products from Wiggle.')
        parser.add_argument('--crcpd', action='store_true', dest='crcpd', default=False,
                            help='Scrape products from Chain Reaction Cycles.')
        parser.add_argument('--bdpd', action='store_true', dest='bdpd', default=False,
                            help='Scrape products from BikeDiscount.de.')
        parser.add_argument('--clearall', action='store_true', dest='clearall', default=False,
                            help='Clear all products from the database before pushing new products.')

    def handle(self, *args, **options):
        if options['clearall']:
            print('[ ] Clearing items...', end="")
            stdout.flush()
            Item.objects.raw("TRUNCATE comparison_item")
            Item.objects.all().delete()
            print('\r'+'[x] Clearing items...')
            print()
        if options['all']:
            mywpd = WPD()
            mywpd.main()
            mycrc = CRCPD()
            mycrc.main()
            mybd = BDPD()
            mybd.main()
        if options['wpd']:
            mywpd = WPD()
            mywpd.main()
        if options['crcpd']:
            mycrc = CRCPD()
            mycrc.main()
        if options['bdpd']:
            mybd = BDPD()
            mybd.main()
