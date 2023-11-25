# %%
import httpx
import html
from functools import partial
from pathlib import Path
import json

# %%
DATA_DIR = Path(__file__).parents[1] / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
INTERIM_DATA_DIR = DATA_DIR / "interim"
AUTHORIZATION = "iis0f01700551099429198"
API_URL = "https://cimsdbie.rbi.org.in/CIMS_Gateway_DBIE/GATEWAY/SERVICES"
HEADERS = {
    "authorization": AUTHORIZATION,
    "channelkey": "key2",
    "datatype": "application/json",
}
EMPTY_PAYLOAD = {
    "body": {}
}
# %%


def detect_error(response: httpx.Response) -> bool:
    pass


# %%
def parse_response(response: httpx.Response) -> dict:
    response_text = response.text
    response_text = html.unescape(response_text)
    return json.loads(response_text)


# %%
def save_json(data: dict, file_name: str, dest_dir: Path = RAW_DATA_DIR):
    with open(dest_dir / file_name, "w") as f:
        f.write(json.dumps(data, indent=2))


# %%


def get_data(*, path: str, client: httpx.Client = httpx.Client(), payload: dict = EMPTY_PAYLOAD) -> httpx.Response:
    url = API_URL + path
    return client.post(url, json=payload)
# %%


def get_data_by_state_name(state_name: str, path: str = "", client: httpx.Client = httpx.Client()) -> httpx.Response:
    url = API_URL + path
    payload = {
        "body": {
            "state": state_name
        }
    }
    return client.post(url, json=payload)


# %%
get_banks_and_their_groups = partial(get_data, path="/dbie_getBankANDBankGrp")
get_states_and_districts = partial(get_data, path="/dbie_getStateAndDistrict")
get_centers_by_state = partial(
    get_data_by_state_name, path="/dbie_getCenterByState")
get_subdistricts_by_state = partial(
    get_data_by_state_name, path="/dbie_getSubDistrict")

# %%


def main():
    client = httpx.Client(headers=HEADERS)

    bank_groups_r = get_banks_and_their_groups(client=client)
    bank_groups = parse_response(bank_groups_r)

    save_json(bank_groups, "banks_and_their_groups.json")

    states_and_districts_r = get_states_and_districts(client=client)
    states_and_districts = parse_response(states_and_districts_r)
    save_json(states_and_districts, "states_and_districts.json")

    state_names = map(
        lambda x: x["state"],
        states_and_districts["body"]["response"]
    )

    centers_dir = RAW_DATA_DIR / "centers"
    centers_dir.mkdir(exist_ok=True)
    subdistrict_dir = RAW_DATA_DIR / "subdistricts"
    subdistrict_dir.mkdir(exist_ok=True)

    for state_name in state_names:
        centers_by_state_r = get_centers_by_state(state_name, client=client)
        centers_by_state = parse_response(centers_by_state_r)
        save_json(centers_by_state, f"{state_name}.json", dest_dir=centers_dir)

        subdistricts_by_state_r = get_subdistricts_by_state(
            state_name, client=client)
        subdistricts_by_state = parse_response(subdistricts_by_state_r)
        save_json(subdistricts_by_state, f"{state_name}.json", dest_dir=subdistrict_dir)


if __name__ == "__main__":
    main()
