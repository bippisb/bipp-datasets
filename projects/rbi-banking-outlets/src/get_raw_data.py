# %%
import httpx
from pathlib import Path
from copy import deepcopy
import json
from get_dbie_api_parameters import parse_response, save_json

# %%
DATA_DIR = Path(__file__).parents[1] / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
DESTINATION_DIR = RAW_DATA_DIR / "outlets_and_atms"
DESTINATION_DIR.mkdir(exist_ok=True)
API_URL = "https://cimsdbie.rbi.org.in/CIMS_Gateway_DBIE/GATEWAY/SERVICES"
AUTHORIZATION = "pvc1w01700605341237198"
HEADERS = {
    "authorization": AUTHORIZATION,
    "channelkey": "key2",
    "datatype": "application/json",
}
TOTAL_RECORDS = 13_59_796
with open(Path(__file__).parent / "payload.json", "r") as o:
    PAYLOAD = json.load(o)
# %%


def get_paginated_data(*, offset: int, limit: int, client: httpx.Client = httpx.Client()) -> httpx.Response:
    path = "/dbie_getBankGetData"
    url = API_URL + path
    payload = deepcopy(PAYLOAD)
    payload["body"]["offsetValue"] = offset
    payload["body"]["limitValue"] = limit
    return client.post(url, json=payload, timeout=30.0)


def main():
    client = httpx.Client(headers=HEADERS)
    for offset in range(3_34_700, TOTAL_RECORDS, 100):
        r = get_paginated_data(offset=offset, limit=100, client=client)
        r.raise_for_status()
        data = parse_response(r)
        save_json(data, f"{offset}.json", dest_dir=DESTINATION_DIR)
        print(f"Saved {offset}.json")
    client.close()


if __name__ == "__main__":
    main()
