from typing import Optional

import uvicorn
from fastapi import FastAPI

from db_reader import read_companies, read_medicines_for_company, read_group

app = FastAPI()


@app.get('/companies')
async def get_companies():
    return read_companies()


@app.get('/medicines')
async def get_medicines(company: str):
    return read_medicines_for_company(company)


@app.get('/group')
async def get_group(substance: str, form: str, dose: Optional[str] = None):
    return read_group(substance, form, dose)


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)