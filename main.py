import os
import requests
import csv

from tqdm import tqdm


BASE_URL = r"https://neo-bolide.ndc.nasa.gov"
EVENTS_RESOURCE = r"/service/event/public"

FIELD_MAP = {
        "_id": "id",
        "status": "status",
        "datetime": "datetime",
        "name": "name",
        "description": "description",
        "latitude": "latitude",
        "longitude": "longitude",
        "detectedBy": "detected_by",
        "confidenceRating": "confidence_rating",
        "otherInformation": "other_information",
        "platform": "platforms",
        # "images.0.url": "map_url",
        }



def get_nested_key_value(data, key_path) -> dict:
    keys = key_path.split(".")
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key)
            if data is None:
                return None
        elif isinstance(data, list):
            data = data[int(key)]
            if data is None:
                return None
        else:
            return data
    return data


def retreive_bolide_data() -> dict:
    """"""
    r = requests.get(BASE_URL + EVENTS_RESOURCE)
    if r.status_code == 200:
        return r.json()
    else:
        raise Exception


def bolide_data_handler(data: dict, platform_id: str=None) -> dict:
    """"""
    subdata = {}

    if platform_id in data["detectedBy"]:
        # Extract wanted data from the retrieved data
        for field, new_field_name in FIELD_MAP.items():
            if "." in field:
                subdata[new_field_name] = get_nested_key_value(data, field)
            else:
                subdata[new_field_name] = data[field]

        # Deal with platform specific data
        for attachment in data["attachments"]:
            if attachment["platformId"] == "G"+platform_id:
                subdata["detection_id"] = attachment["_id"][0]
                subdata["platform"] = attachment["platformId"]
                subdata["detected_start_time"] = attachment["startTime"]
                subdata["detected_end_time"] = attachment["endTime"]
                subdata["detected_duration"] = attachment["duration"]
                subdata["detected_energy"] = attachment["energy"]
                subdata["detected_detected_longitude"] = attachment["location"]["coordinates"][0]
                subdata["detected_latitude"] = attachment["location"]["coordinates"][1]
                break

        for platform, brightness_measure in data["brightness"].items():
            if platform == "GLM-"+platform_id:
                subdata["detected_brightness"] = brightness_measure["category"]
                break

        # Image link for platform
        for image in data["images"]:
            if image["name"] == "Energy Chart" and subdata["detection_id"] in image["url"]:
                subdata["energy_chart_url"] = image["url"]

            if image["name"] == "Trajectory Chart" and subdata["detection_id"] in image["url"]:
                subdata["trajectory_chart_url"] = image["url"]

        # trajectory csv downlaod link (kind of an nc file but its not)
        for link in data["csv"]:
            if subdata["detection_id"] in link:
                subdata["download_trajectory_url"] = link
                break

        # Rebuild URL to full
        for field, value in subdata.items():
            if field.endswith("_url"):
                subdata[field] = BASE_URL + value 

        return subdata


def append_writer(file_path, data):
    file_exists = os.path.isfile(file_path)

    with open(file_path, mode="a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)


def main() -> int:
    """"""
    data = retreive_bolide_data()["data"]

    # Clean Existing files
    for platform_num in [16, 17, 18]:
        file = f"Data/GLM{platform_num}.csv" 
        if os.path.isfile(file):
            os.remove(file)

    for bolide in tqdm(data[:6949]): # Data past this is not in the same format
        cleaned16 = bolide_data_handler(bolide, "16")
        if cleaned16 is not None:
            append_writer("Data/GLM16.csv", cleaned16)

        cleaned17 = bolide_data_handler(bolide, "17")
        if cleaned17 is not None:
            append_writer("Data/GLM17.csv", cleaned17)

        cleaned18 = bolide_data_handler(bolide, "18")
        if cleaned18 is not None:
            append_writer("Data/GLM18.csv", cleaned18)


if __name__ == "__main__":
    main()


