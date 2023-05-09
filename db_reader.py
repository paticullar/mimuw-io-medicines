from sqlalchemy import create_engine, Column, Integer, String, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import select

import pandas as pd


Base = declarative_base()


class Medicine(Base):
    __tablename__ = 'medicine'

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


def get_df_from_query(query) -> pd.DataFrame:
    engine = create_engine('postgresql+psycopg2://user:password@localhost:5432/medicines')
    with engine.connect() as conn:
        data = conn.execute(query)
        data = data.mappings().all()
        return pd.DataFrame.from_records(data, coerce_float=True)


def read_companies():
    query = select(Medicine.company).distinct()
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
            Medicine.price_per_unit
        )
        .where(
            Medicine.substance == substance,
            Medicine.form == form,
            Medicine.dose == dose,
        )
    )
    result = get_df_from_query(query).to_dict('records')
    return result