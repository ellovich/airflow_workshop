import json
import logging
import pandas as pd
import requests
import pendulum


url = "https://op.itmo.ru/auth/token/login"
auth_data = {
    "username": "asdfg", 
    "password": "itmo2023"
}
token_txt = requests.post(url, auth_data).text
token = json.loads(token_txt)["auth_token"]
headers = {
    "Content-Type": "application/json", 
    "Authorization": "Token " + token
}
target_fields = [
    "constr_id" 
    ,"title"
    ,"discipline_code" 
    ,"prerequisites" 
    ,"outcomes"
    ,"update_ts"
]

def url(disc_id: int) -> str:
    return f"https://op.itmo.ru/api/workprogram/items_isu/{disc_id}?format=json"

for op_id in (
2623,
2625):
    page = requests.get(url(op_id), headers=headers)


    df = pd.DataFrame.from_dict(page.json(), orient="index")
    df = df.T

    df["prerequisites"] = df[~df["prerequisites"].isna()]["prerequisites"].apply(
        lambda st_dict: json.dumps(st_dict)
    )
    df["outcomes"] = df[~df["outcomes"].isna()]["outcomes"].apply(
        lambda st_dict: json.dumps(st_dict)
    )
    print(df)