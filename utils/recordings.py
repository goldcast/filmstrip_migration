import asyncio
import os
from urllib.parse import urljoin

import aiohttp
import requests


def get_m3u8(event_id, broadcast_id, env, simulive_server):
    return f"{simulive_server}/{env}/vod/{event_id}/{broadcast_id}/hls/1500.m3u8"
