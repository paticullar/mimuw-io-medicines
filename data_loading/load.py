import datetime
import logging
import re
from typing import List, Dict, Tuple, Callable

import pandas as pd
import os

from sqlalchemy import create_engine

dropped = 0
all_input = 0


def process_group(df: pd.DataFrame) -> pd.DataFrame:
    pattern = re.compile(
        '^\d+ (?:(?:pasków)|(?:amp.-strz.\.?)|(?:szt\.?\.?)|(?:kaps\.?)|(?:ml\.?)|(?:fiol\.?(?: proszku)?)|(?:daw\.?)|(?:g\.?)|(?:sasz\.?)|(?:tabl\.?))(?: ?\(.*\))?$')
    chunked_re_str = '^(\d+) (?:fiol\.|wkł\.|butelka|butelki|but\.|amp\.|poj\.|amp\.-strz\.|szt\.) ?(?:po|a)? (\d+,?\d*) ?(ml|mg|g|daw\.)$'
    chunked_pattern = re.compile(chunked_re_str)
    ret = pd.DataFrame()

    def has_standard_contents(s: str) -> bool:
        return True if pattern.match(s) else False

    def get_amount_and_unit(s: str):
        return re.findall('(\d+) ([^\(]*)', s)[0]

    def has_chunked_contents(s: str) -> bool:
        return True if chunked_pattern.match(s) else False

    def get_chunked_amount_and_unit(s: str) -> Tuple[float, str]:
        found = re.findall(chunked_re_str, s)[0]
        return float(found[0]) * float(found[1].replace(',', '.')), found[2]

    if df['contents'].apply(has_standard_contents).all():
        extracted = df['contents'].apply(get_amount_and_unit)
        ret = df.copy()
        ret['amount'] = extracted.apply(lambda x: float(x[0].replace(',', '.')))
        ret['unit'] = extracted.apply(lambda x: x[1])
        ret['price_per_unit'] = round(ret['price'] / ret['amount'], 4)
    elif (df['contents'].reset_index(drop=True) == df['contents'].reset_index(drop=True)[0]).all():
        ret = df.copy()
        ret['amount'] = 1
        ret['unit'] = ret['contents']
        ret['price_per_unit'] = round(ret['price'], 4)
    elif df['contents'].apply(has_chunked_contents).all():
        extracted = df['contents'].apply(get_chunked_amount_and_unit)
        ret = df.copy()
        ret['amount'] = extracted.apply(lambda x: x[0])
        ret['unit'] = extracted.apply(lambda x: x[1])
        ret['price_per_unit'] = round(ret['price'] / ret['amount'], 4)
    else:
        logging.debug('Dropping\n' + df['contents'].to_string())
        global dropped
        dropped += len(df.index)
    return ret


def process_file(path: str, mysterious_column_name: str, date: datetime.date) -> pd.DataFrame:
    def get_name_form_and_dose(s: str) -> Tuple[str]:
        result = re.findall('(.*?), (.*), (.*)', s)
        if result:
            return result[0]
        return re.findall('(.*?), (.*)', s)[0] + (None,)

    logging.info(f'Processing {path}...')

    global dropped
    dropped = 0

    df = pd.read_csv(path, dtype={'Numer GTIN lub inny kod jednoznacznie identyfikujący produkt': str})

    global all_input
    all_input += len(df.index)

    df = df[['Substancja czynna', mysterious_column_name, 'Zawartość opakowania',
             'Numer GTIN lub inny kod jednoznacznie identyfikujący produkt', 'Cena hurtowa brutto']]

    extracted = df[mysterious_column_name].apply(get_name_form_and_dose)
    df['name'] = extracted.apply(lambda x: x[0])
    df['form'] = extracted.apply(lambda x: x[1])
    df['dose'] = extracted.apply(lambda x: x[2])
    df.drop(columns=[mysterious_column_name], inplace=True)

    df.rename(columns={'Substancja czynna': 'substance', 'Zawartość opakowania': 'contents',
                       'Numer GTIN lub inny kod jednoznacznie identyfikujący produkt': 'gtin',
                       'Cena hurtowa brutto': 'price'}, inplace=True)

    df['price'] = df['price'].apply(lambda x: float(x.replace(',', '.')))

    df['date'] = date

    groups: Dict[Tuple[str, str, str], List[int]] = {}

    for id, row in df.iterrows():
        params = (row['substance'], row['form'], row['dose'])
        if params not in groups.keys():
            groups[params] = []
        groups[params].append(id)

    to_append = [process_group(df.iloc[ids]) for _, ids in groups.items()]
    data = pd.concat(to_append)

    logging.info(
        f'Dropped {dropped} due to strange contents, coverage {round(len(data.index) * 100 / len(df.index), 2)}%')
    logging.info(f'Processed!')

    return data


def read_companies_map(path: str) -> Dict[str, str]:
    map: Dict[str, str] = {}

    with open(path) as f:
        for line in f:
            split = line.strip().split('#')
            map[split[0]] = split[1]

    return map


def get_company(map: Dict[str, str]) -> Callable[[str], str]:
    def ret(s: str) -> str:
        if s in map.keys():
            return map[s]
        if str(int(s)) in map.keys():
            return map[str(int(s))]
        return 'Inna firma'

    return ret


def save_to_db(df: pd.DataFrame):
    logging.info('Saving to database...')
    conn = create_engine('postgresql+psycopg2://user:password@localhost:5432/medicines')
    df.to_sql('medicine_v2', conn, if_exists='replace')
    logging.info('Successfully saved to database')


def process_file_group(path: str) -> pd.DataFrame:
    date = datetime.datetime.strptime(path[-8:], '%d%m%Y').date()

    a1 = process_file(path + '/A1.csv', 'Nazwa  postać i dawka', date)
    a2 = process_file(path + '/A2.csv', 'Nazwa  postać i dawka', date)
    a3 = process_file(path + '/A3.csv', 'Nazwa  postać i dawka', date)
    b = process_file(path + '/B.csv', 'Nazwa  postać i dawka leku', date)
    c = process_file(path + '/C.csv', 'Nazwa  postać i dawka leku', date)

    processed = pd.concat([a1, a2, a3, b, c])
    return processed


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    data_path = os.path.dirname(os.path.abspath(__file__)) + '/data/'

    all_data_dict: Dict[str, pd.DataFrame] = {}

    for name in os.listdir(data_path):
        if os.path.isdir(os.path.join(data_path, name)):
            all_data_dict[name] = process_file_group(os.path.join(data_path, name))

    all_data = pd.concat([data for data in all_data_dict.values()])

    companies_map = read_companies_map(data_path + 'companies.txt')
    all_data['company'] = all_data['gtin'].apply(get_company(companies_map))

    logging.info(f'All data parsed, overall coverage {round(len(all_data.index) * 100. / all_input, 2)}%')

    save_to_db(all_data)
