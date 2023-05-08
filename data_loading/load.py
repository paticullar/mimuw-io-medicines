import logging
import re
from typing import List, Dict, Tuple

import pandas as pd
import os


dropped = 0


def process_group(df: pd.DataFrame) -> pd.DataFrame:
    pattern = re.compile('^\d+ (?:(?:pasków)|(?:amp.-strz.\.?)|(?:szt\.?\.?)|(?:kaps\.?)|(?:ml\.?)|(?:fiol\.?(?: proszku)?)|(?:daw\.?)|(?:g\.?)|(?:sasz\.?)|(?:tabl\.?))(?: ?\(.*\))?$')
    fiolka_pattern = re.compile('^(\d+) fiol\. ?(?:po|a)? (\d+) (ml|mg)$')
    ret = pd.DataFrame()

    def has_standard_contents(s: str) -> bool:
        return True if pattern.match(s) else False

    def get_amount_and_unit(s: str):
        return re.findall('(\d+) ([^\(]*)', s)[0]

    def has_fiolka_contents(s: str) -> bool:
        return True if fiolka_pattern.match(s) else False

    def get_fiolka_amount_and_unit(s: str) -> Tuple[float, str]:
        found = re.findall('^(\d+) fiol\. ?(?:po|a)? (\d+) (ml|mg)$', s)[0]
        return float(found[0]) * float(found[1]), found[2]

    if df['contents'].apply(has_standard_contents).all():
        extracted = df['contents'].apply(get_amount_and_unit)
        ret = df.copy()
        ret['amount'] = extracted.apply(lambda x: float(x[0].replace(',', '.')))
        ret['unit'] = extracted.apply(lambda x: x[1])
        ret['price_per_unit'] = ret['price'] / ret['amount']
    elif (df['contents'].reset_index(drop=True) == df['contents'].reset_index(drop=True)[0]).all():
        ret = df.copy()
        ret['amount'] = 1
        ret['unit'] = ret['contents']
        ret['price_per_unit'] = ret['price']
    elif df['contents'].apply(has_fiolka_contents).all():
        extracted = df['contents'].apply(get_fiolka_amount_and_unit)
        ret = df.copy()
        ret['amount'] = extracted.apply(lambda x: x[0])
        ret['unit'] = extracted.apply(lambda x: x[1])
        ret['price_per_unit'] = ret['price'] / ret['amount']
    else:
        logging.debug('Dropping\n' + df['contents'].to_string())
        global dropped
        dropped += len(df.index)
    return ret


def process_file(path: str, mysterious_column_name: str) -> pd.DataFrame:
    def get_name_form_and_dose(s: str) -> Tuple[str]:
        result = re.findall('(.*?), (.*), (.*)', s)
        if result:
            return result[0]
        return re.findall('(.*?), (.*)', s)[0] + (None,)

    logging.info(f'Processing {path}...')

    global dropped
    dropped = 0

    df = pd.read_csv(path)

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

    groups: Dict[Tuple[str, str, str], List[int]] = {}

    for id, row in df.iterrows():
        params = (row['substance'], row['form'], row['dose'])
        if params not in groups.keys():
            groups[params] = []
        groups[params].append(id)

    to_append = [process_group(df.iloc[ids]) for _, ids in groups.items()]
    data = pd.concat(to_append)

    logging.info(f'Dropped {dropped} due to strange contents, coverage {len(data.index) * 100 / len(df.index)}%')
    logging.info(f'Processed!')

    return data


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    data_path = os.path.dirname(os.path.abspath(__file__)) + '/data/'

    a1 = process_file(data_path + 'A1.csv', 'Nazwa  postać i dawka')
    a2 = process_file(data_path + 'A2.csv', 'Nazwa  postać i dawka')
    a3 = process_file(data_path + 'A3.csv', 'Nazwa  postać i dawka')
    b = process_file(data_path + 'B.csv', 'Nazwa  postać i dawka leku')
    c = process_file(data_path + 'C.csv', 'Nazwa  postać i dawka leku')

    data = pd.concat([a1, a2, a3, b, c])

