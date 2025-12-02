from datetime import datetime as dt
import random
from datetime import timedelta
from time import sleep
import geopandas as gpd
import pandas as pd
import shutil
import subprocess
import os
import glob
from constants import (
    EMAIL_ADDRESS,
    GMAIL_APP_PASSWORD,
    IS_DEBUG,
    MAPILLARY_EMAIL,
    MAPILLARY_PASSWORD,
    MAPILLARY_USER,
    VOLUME_SIZE_IN_BYTES,
)
from download_state_db import DownloadStateDb
from ready_to_download_checker import ReadyToDownloadChecker
from utils import (
    confirm_email,
    extract_and_remove_tar,
    get_download_id_and_size_in_bytes,
    get_trajectory_dir_paths,
    prune_downloaded_data,
    remove_top_and_bottom_facing_images,
    request_confirm_email,
    set_exif_tags,
)


points = gpd.read_file("points.gpkg", layer="kappazunder_image_punkte")
points = points.set_index("image_name")
trajectories_df = gpd.read_file("trajectories.gpkg")

download_state_db = DownloadStateDb(trajectories_df)

if IS_DEBUG:
    future = (pd.Timestamp.now() + timedelta(days=1)).isoformat()
    download_state_db.execute(
        f"UPDATE trajectories SET download_id='5276d431-a054-4a84-a38c-6dfbccefdef0', download_expires_at='{future}' WHERE trajectory_id = '17720'"
    )
    download_state_db.execute(
        f"UPDATE trajectories SET download_id='56a27033-35ed-4c3c-ba14-704cc256efac', download_expires_at='{future}' WHERE trajectory_id = '16101'"
    )
    download_state_db.execute(
        f"UPDATE trajectories SET download_id='e4a3d58b-2c58-4991-8e98-ae7b635d25cf', download_expires_at='{future}' WHERE trajectory_id = '16471'"
    )

ready_to_download = ReadyToDownloadChecker(EMAIL_ADDRESS, GMAIL_APP_PASSWORD)

subprocess.run(
    [
        "mapillary_tools",
        "authenticate",
        "--user_name",
        MAPILLARY_USER,
        "--user_email",
        MAPILLARY_EMAIL,
        "--user_password",
        MAPILLARY_PASSWORD,
    ],
    check=True,
)

sensors_completed_column_names = [f"is_sensor{i}_completed" for i in range(1, 5)]
while True:
    uncompleted_trajectories = download_state_db.execute(
        f"SELECT * FROM trajectories WHERE {' = 0 OR '.join(sensors_completed_column_names)} = 0"
    )
    if uncompleted_trajectories == 0:
        break

    print(f"Number of uncompleted trajectories: {len(uncompleted_trajectories)}")
    expiring_trajectories = [
        traj
        for traj in uncompleted_trajectories
        if traj["download_expires_at"] is not None
        and dt.fromisoformat(traj["download_expires_at"])
        < dt.now() + timedelta(hours=5)
    ]
    if len(expiring_trajectories) > 0:
        expiring_trajectory_ids = [
            traj["trajectory_id"] for traj in expiring_trajectories
        ]
        print(f"Resetting expiring trajectories with IDs: {expiring_trajectory_ids}")
        download_state_db.execute(
            f"UPDATE trajectories SET download_id = NULL, download_bytes = NULL, download_expires_at = NULL WHERE trajectory_id IN ('{"','".join(expiring_trajectory_ids)}')"
        )

    ready_to_download.refresh()
    trajectories_to_download = [
        traj
        for traj in uncompleted_trajectories
        if traj["download_id"] in ready_to_download.get_ids()
    ]
    no_of_prepared_trajectories = len(
        [traj for traj in uncompleted_trajectories if traj["download_id"] is not None]
    )
    if (
        len(trajectories_to_download) <= 5
        and no_of_prepared_trajectories < 20
        and not IS_DEBUG
    ):
        print(f"Only {len(trajectories_to_download)} downloadable trajectories left")
        trajectories_to_prepare = [
            traj for traj in uncompleted_trajectories if traj["download_id"] is None
        ]
        if len(trajectories_to_prepare) == 0:
            print("No trajectories left to prepare")
        else:
            trajectories_to_prepare = random.sample(
                trajectories_to_prepare, min(10, len(trajectories_to_prepare))
            )
            print(
                f"Preparing {len(trajectories_to_prepare)} trajectories with ids: {[traj["trajectory_id"] for traj in trajectories_to_prepare]}"
            )
            for trajectory in trajectories_to_prepare:
                trajectory_id = trajectory["trajectory_id"]
                try:
                    download_id, size = get_download_id_and_size_in_bytes(
                        trajectories_df[
                            trajectories_df["trajectoryid"] == trajectory_id
                        ].geometry.values[0],
                    )
                    if size > VOLUME_SIZE_IN_BYTES / 2 * 0.95:
                        print(
                            f"Skipping trajectoryid {trajectory_id} with downloadid {download_id} because size {size} exceeds limit."
                        )
                        continue

                    request_confirm_email(download_id, EMAIL_ADDRESS)
                    sleep(60)
                    confirm_email()
                    download_state_db.execute(
                        f"UPDATE trajectories SET download_id = '{download_id}', download_bytes = {size}, download_expires_at = '{(
                            pd.Timestamp.now() + timedelta(days=7)
                        ).isoformat()}' WHERE trajectory_id = '{trajectory_id}'"
                    )
                    print(
                        f"Successfully prepared trajectoryid {trajectory_id} with downloadid {download_id}"
                    )
                except Exception as e:
                    print(
                        f"Error preparing trajectoryid {trajectory_id} with downloadid {download_id}: {e}"
                    )

    if len(trajectories_to_download) == 0:
        print("No trajectories ready for download. Sleeping five minutes.")
        sleep(300)
        continue

    trajectory_to_download = sorted(
        trajectories_to_download,
        key=lambda traj: dt.fromisoformat(traj["download_expires_at"]),
    )[0]
    trajectory_id = trajectory_to_download["trajectory_id"]
    download_id = trajectory_to_download["download_id"]
    if os.path.isdir(download_id):
        shutil.rmtree(download_id)

    download_result = subprocess.run(
        [
            "aria2c",
            f"https://www.wien.gv.at/ogdgeodata/download/{download_id}.tar",
            f"--out={download_id}.tar",
            "--allow-overwrite=true",
            "--max-tries=5",
            "--retry-wait=300",
            "--timeout=300",
            "--max-file-not-found=1",
        ]
    )
    if download_result.returncode != 0:
        print(
            f"Resetting downloadid {download_id} of trajectory {trajectory_id} because download failed"
        )
        download_state_db.execute(
            f"UPDATE trajectories SET download_id = NULL, download_bytes = NULL, download_expires_at = NULL WHERE trajectory_id = '{trajectory_id}'"
        )
        continue

    extract_and_remove_tar(download_id)
    original_trajectory_id = trajectory_id.split("_", 1)[0]
    trajectory_dir_paths = get_trajectory_dir_paths(download_id, original_trajectory_id)
    prune_downloaded_data(download_id, trajectory_dir_paths)
    remove_top_and_bottom_facing_images(trajectory_dir_paths)
    set_exif_tags(trajectory_dir_paths, points)
    for i in range(1, 5):
        if trajectory_to_download[f"is_sensor{i}_completed"] == 1:
            continue

        subprocess.run(
            [
                "mapillary_tools",
                "process_and_upload",
                "--overwrite_all_EXIF_tags",
                "--device_make",
                "Teledyne",
                "--device_model",
                "Ladybug6",
                "--offset_angle",
                str((i - 1) * 90),
                "--interpolate_directions",
                "--user_name",
                MAPILLARY_USER,
                "--noresume",
                glob.glob(
                    f"{download_id}/*/Bild-Rohdaten/Trajektorie_{original_trajectory_id}/Sensor_*{i}/"
                )[0],
            ],
            check=True,
        )
        download_state_db.execute(
            f"UPDATE trajectories SET is_sensor{i}_completed = 1 WHERE trajectory_id = '{trajectory_id}'"
        )

    shutil.rmtree(download_id)

print("All trajectories completed.")
