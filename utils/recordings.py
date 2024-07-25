import asyncio
import os
from urllib.parse import urljoin

import aiohttp
import requests

OUTPUT_DIR = "downloads/{}/{}"


async def download_ts_file(session, url, output_dir):
    filename = os.path.join(output_dir, url.split("/")[-1])
    async with session.get(url) as response:
        with open(filename, "wb") as f:
            while True:
                chunk = await response.content.read(1024)
                if not chunk:
                    break
                f.write(chunk)


async def download_all_ts_files(ts_urls, output_dir):
    async with aiohttp.ClientSession() as session:
        tasks = [download_ts_file(session, url, output_dir) for url in ts_urls]
        await asyncio.gather(*tasks)


def download_m3u8_and_ts_files(event_id, broadcast_id, env, simulive_server):
    m3u8_url = f"{simulive_server}/{env}/vod/{event_id}/{broadcast_id}/hls/1500.m3u8"
    output_directory = OUTPUT_DIR.format(event_id, broadcast_id)
    os.makedirs(output_directory, exist_ok=True)

    try:
        m3u8_response = requests.get(m3u8_url)
    except Exception as ex:
        print(f"Exception in opening the url: {m3u8_url} \n{ex}")
        raise ex
    m3u8_content = m3u8_response.text

    # Extract the URLs of the TS files
    ts_urls = [urljoin(m3u8_url, line.strip()) for line in m3u8_content.splitlines() if line.endswith(".ts")]

    if not ts_urls:
        return None

    # Download the TS files
    try:
        asyncio.run(download_all_ts_files(ts_urls, output_directory))
    except Exception as e:
        raise e

    # Update the M3U8 file to refer to the local TS files
    local_m3u8_content = ""
    for line in m3u8_content.splitlines():
        if line.endswith(".ts"):
            line = os.path.join(line.split("/")[-1])
        local_m3u8_content += line + "\n"

    # Save the updated M3U8 file
    local_m3u8_filename = os.path.join(output_directory, "local.m3u8")
    with open(local_m3u8_filename, "w") as f:
        f.write(local_m3u8_content)
    return local_m3u8_filename
