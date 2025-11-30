import requests
from requests.adapters import HTTPAdapter, Retry
import shutil
import piexif
import tarfile
from PIL import Image
from tqdm import tqdm
import os


def get_download_id_and_size_in_bytes(polygon):
    coords = [[x, y] for x, y in polygon.exterior.coords]
    url = "https://mein.wien.gv.at/geodownload-backend/app/register"
    payload = {"data": {"coords": coords, "dataset": "KAPPAZUNDER 2020", "option": 2}}
    response = requests.post(url, json=payload)
    response.raise_for_status()
    items = response.json().get("items")
    return items.get("confirmation"), items.get("size") * 1024 * 1024


def request_confirm_email(download_id, email_address):
    with requests.Session() as session:
        session.get(
            f"https://mein.wien.gv.at/geodownload-ui/confirm/{download_id}",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )
        response = session.patch(
            f"https://mein.wien.gv.at/geodownload-backend/app/confirm/{download_id}",
            json={"mail": email_address},
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Origin": "https://mein.wien.gv.at",
                "Referer": "https://mein.wien.gv.at/",
            },
        )
        response.raise_for_status()


def confirm_email():
    url = "https://mein.wien.gv.at/geodownload-backend/app/mail/2d4bf8b8-88cb-4c9c-b29b-bf2f5ef50c8f"
    response = requests.patch(url)
    response.raise_for_status()


def extract_and_remove_tar(download_id):
    tar_path = download_id + ".tar"
    with tarfile.open(tar_path, "r") as tar:
        tar.extractall(path=download_id)
    os.remove(tar_path)


def get_trajectory_dir_paths(download_id, trajectory_id):
    los_dirs = [f for f in os.listdir(download_id)]
    possible_trajectory_dir_paths = [
        os.path.join(
            download_id, los_dir, "Bild-Rohdaten", f"Trajektorie_{trajectory_id}"
        )
        for los_dir in los_dirs
    ]
    return [d for d in possible_trajectory_dir_paths if os.path.isdir(d)]


def prune_downloaded_data(download_id, trajectory_dir_paths_to_keep):
    for los_dir in os.listdir(download_id):
        bild_rohdaten_dir_path = os.path.join(download_id, los_dir, "Bild-Rohdaten")
        for trajectory_dir in os.listdir(bild_rohdaten_dir_path):
            trajectory_dir_path = os.path.join(bild_rohdaten_dir_path, trajectory_dir)
            if (
                os.path.isdir(trajectory_dir_path)
                and trajectory_dir_path not in trajectory_dir_paths_to_keep
            ):
                shutil.rmtree(trajectory_dir_path)


def remove_top_and_bottom_facing_images(trajectory_dir_paths):
    for trajectory_dir_path in trajectory_dir_paths:
        for name in os.listdir(trajectory_dir_path):
            if name.endswith(("0", "5")):
                shutil.rmtree(os.path.join(trajectory_dir_path, name))


def set_exif_tags(trajectory_dir_paths, points_indexed_by_image):
    def deg_to_dms_rational(deg_float):
        deg_abs = abs(deg_float)
        deg = int(deg_abs)
        min_float = (deg_abs - deg) * 60
        min_ = int(min_float)
        sec = round((min_float - min_) * 60 * 10000)
        return ((deg, 1), (min_, 1), (sec, 10000))

    file_paths = []
    for trajectory_dir_path in trajectory_dir_paths:
        for sensor_dir in os.listdir(trajectory_dir_path):
            sensor_dir_path = os.path.join(trajectory_dir_path, sensor_dir)
            sensor_dir_content_paths = [
                os.path.join(sensor_dir_path, file)
                for file in os.listdir(sensor_dir_path)
            ]
            file_paths.extend(
                [file for file in sensor_dir_content_paths if os.path.isfile(file)]
            )

    for img_path in tqdm(file_paths, desc="Tagging images"):
        file = os.path.basename(img_path)
        row = points_indexed_by_image.loc[file]
        if row.empty:
            print(f"⚠️ No metadata found for {file}, skipping.")
            continue

        img = Image.open(img_path)

        exif_dict = piexif.load(img.info.get("exif", b""))

        exif_dict["GPS"] = {
            piexif.GPSIFD.GPSLatitudeRef: b"N",
            piexif.GPSIFD.GPSLatitude: deg_to_dms_rational(row["lat"]),
            piexif.GPSIFD.GPSLongitudeRef: b"E",
            piexif.GPSIFD.GPSLongitude: deg_to_dms_rational(row["lon"]),
        }

        exif_dict["Exif"][piexif.ExifIFD.DateTimeOriginal] = (
            row["epoch"].strftime("%Y:%m:%d %H:%M:%S").encode("utf-8")
        )

        exif_bytes = piexif.dump(exif_dict)
        piexif.insert(exif_bytes, img_path)
