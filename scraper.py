import requests
from bs4 import BeautifulSoup as bs
from datetime import datetime
from csv import DictWriter
import os


def time_stamp():
    return datetime.now().strftime('%d-%m-%Y %H:%M')


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


def rki_scraper(url='https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Fallzahlen.html'):
    rki_raw = requests.get(url)
    soup = bs(rki_raw.text, 'html.parser')
    data_raw = soup.find_all('td')
    return [i for i in list(chunks(data_raw, 6))]


def rki_data_cleaner(data_list):
    return [{
                i[0].text.replace(u'\xad', u'').replace(u'\n', u''):{
                    'total': i[1].text.replace('.', '').replace(',', '.'),
                    'diff. vortag': i[2].text.replace('.', '').replace(',', '.'),
                    'letzte 7 Tage': i[3].text.replace('.', '').replace(',', '.'),
                    'inzidenz': i[4].text.replace('.', '').replace(',', '.'),
                    'todesfälle': i[5].text.replace('.', '').replace(',', '.')
                    }} for i in data_list
                    ]


def data_to_csv(cleaned_data):
    field_names = ['total', 'diff. vortag', 'letzte 7 Tage', 'inzidenz', 'todesfälle', 'scraped']
    for i in cleaned_data:
        for key, value in i.items():
            with open(key + '.csv', 'a') as csv_file:
                dict_writer = DictWriter(csv_file, fieldnames=field_names)
                value['scraped'] = time_stamp()
                if os.stat(key + '.csv').st_size == 0:
                    dict_writer.writeheader()
                dict_writer.writerow(value)


def main():
    scraped_data = rki_scraper()
    clean_data = rki_data_cleaner(scraped_data)
    data_to_csv(clean_data)


if __name__ == '__main__':
    main()






