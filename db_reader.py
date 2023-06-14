from typing import Dict, List

from sqlalchemy import create_engine, Column, Integer, String, Numeric, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import select

import pandas as pd

Base = declarative_base()


class Medicine(Base):
    __tablename__ = 'medicine_v2'

    index = Column(Integer, primary_key=True)
    substance = Column(String)
    contents = Column(String)
    price = Column(Numeric)
    name = Column(String)
    form = Column(String)
    dose = Column(String)
    amount = Column(Integer)
    unit = Column(String)
    price_per_unit = Column(Numeric)
    company = Column(String)
    gtin = Column(String)
    date = Column(Date)


def get_df_from_query(query) -> pd.DataFrame:
    engine = create_engine('postgresql+psycopg2://user:password@localhost:5432/medicines')
    with engine.connect() as conn:
        data = conn.execute(query)
        data = data.mappings().all()
        return pd.DataFrame.from_records(data, coerce_float=True)


def read_companies():
    query = select(Medicine.company).distinct().order_by(Medicine.company)
    result = {'companies': get_df_from_query(query)['company'].tolist()}
    return result


def read_medicines_for_company(company: str):
    query = (
        select(
            Medicine.name,
            Medicine.substance,
            Medicine.form,
            Medicine.dose,
        )
        .where(Medicine.company == company)
        .distinct()
        .order_by(Medicine.name)
    )
    result = get_df_from_query(query).to_dict('records')
    return result


def read_group(substance: str, form: str, dose: str):
    query = (
        select(
            Medicine.company,
            Medicine.name,
            Medicine.substance,
            Medicine.form,
            Medicine.dose,
            Medicine.contents,
            Medicine.price,
            Medicine.unit,
            Medicine.amount,
            Medicine.price_per_unit,
            Medicine.gtin,
            Medicine.date
        )
        .where(
            Medicine.substance == substance,
            Medicine.form == form,
            Medicine.dose == dose,
        )
    )
    query_result = get_df_from_query(query).to_dict('records')
    map: Dict[str, List[Dict]] = {}
    for entry in query_result:
        gtin = entry['gtin']
        if gtin not in map.keys():
            map[gtin] = []
        map[gtin].append(entry)

    result = [sorted(medicines, key=lambda x: x['date']) for medicines in map.values()]
    result.sort(key=lambda x: x[-1]['price_per_unit'])
    for l in result:
        for m in l:
            m['date'] = m['date'].isoformat()
    return result


