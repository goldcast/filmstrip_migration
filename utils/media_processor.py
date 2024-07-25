import abc
import os
from enum import Enum
import yt_dlp
import boto3

s3_video_file_key = "content-lab/filestack/custom_assets/{project_id}/{content_id}.mp4"
VIDEO_OUTPUT_FILE = "downloads/{}/{}/input.mp4"
STATIC_ASSETS_BUCKET = "staticassets.goldcast.com"


def download_file_from_s3(s3_file_key, local_file_name):
    s3 = boto3.client('s3')
    s3.download_file(STATIC_ASSETS_BUCKET, s3_file_key, local_file_name)
    print(f"{local_file_name} has size: {os.path.getsize(local_file_name)}")
    return local_file_name


class ImportSourceType(Enum):
    YOUTUBE = "YOUTUBE"
    HOSTED_URL = "HOSTED_URL"
    VIMEO = "VIMEO"
    ZOOM = "ZOOM"
    WISTIA = "WISTIA"
    OTHER = "OTHER"


class MediaProcessor:
    def __init__(
            self,
            project_id,
            content_id,
            media_type,
            mediastore_endpoint,
            import_url=None,
            import_source_type=ImportSourceType.HOSTED_URL,
            ves_token="",
    ):
        self.project_id = project_id
        self.content_id = content_id
        self.media_type = media_type
        self.mediastore_endpoint = mediastore_endpoint
        self.import_url = import_url
        self.import_source_type = import_source_type
        self.ves_token = ves_token
        self.input_file = None

    def process_media(self):
        os.makedirs(os.path.dirname(VIDEO_OUTPUT_FILE.format(self.project_id, self.content_id)), exist_ok=True)
        if not self.import_url:
            factory = VideoDownloadFactory(
                self.project_id, self.content_id, self.media_type
            )
        else:
            factory = ImportUrlDownloadFactory(self.import_url, self.import_source_type, self.media_type, self.project_id, self.content_id)

        downloader = factory.create_downloader()
        return downloader.download()


class VideoDownloadFactory(object):

    def __init__(self, project_id, content_id, media_type):
        self.project_id = project_id
        self.content_id = content_id
        self.media_type = media_type

    def create_downloader(self):
        return VideoDownloader(self.project_id, self.content_id)


class VideoDownloader(object):

    def __init__(self, project_id, content_id):
        self.project_id = project_id
        self.content_id = content_id

    def download(self):
        s3_file_key = s3_video_file_key.format(project_id=self.project_id, content_id=self.content_id)
        return download_file_from_s3(s3_file_key, VIDEO_OUTPUT_FILE.format(self.project_id, self.content_id))


class BaseDownloader(abc.ABC):
    """
    Abstract base class for video downloaders.
    """
    YDL_OPTS: dict

    def __init__(self, url, media_type, project_id, content_id):
        self.url = url
        self.media_type = media_type
        self.project_id = project_id
        self.content_id = content_id

    def download(self):
        return self._download_with_ydl()

    def _download_with_ydl(self):
        """
        Download the video using yt-dlp with the provided options.

        :param ydl_opts: Options to configure yt-dlp for the download.
        :return: The path to the downloaded video file.
        """
        self.YDL_OPTS['outtmpl'] = VIDEO_OUTPUT_FILE.format(self.project_id, self.content_id)
        with yt_dlp.YoutubeDL(self.YDL_OPTS) as ydl:
            ydl.extract_info(self.url, download=True)
        return VIDEO_OUTPUT_FILE.format(self.project_id, self.content_id)


# Downloader for videos hosted on generic URLs.
class HostedUrlDownloader(BaseDownloader):
    YDL_OPTS = {'outtmpl': VIDEO_OUTPUT_FILE}

    def __init__(self, url, media_type, project_id, content_id):
        super().__init__(url, media_type, project_id, content_id)


# Downloader for YouTube videos.
class YouTubeDownloader(BaseDownloader):
    YDL_OPTS = {
        'outtmpl': VIDEO_OUTPUT_FILE,
        'format': 'bestvideo[height<=1080][vcodec^=avc]+bestaudio[acodec^=mp4a]/best[height<=1080][vcodec^=avc]',
    }


# Downloader for videos from Vimeo.
class VimeoDownloader(BaseDownloader):
    YDL_OPTS = {
        'outtmpl': VIDEO_OUTPUT_FILE,
        'force_generic_extractor': True,
        'format': 'bestvideo[height<=1080][vcodec^=avc1]+bestaudio[acodec^=mp4a]/best[height<=1080][vcodec^=avc1]',
    }


# Downloader for videos from Vimeo.
class WistiaDownloader(BaseDownloader):
    YDL_OPTS = {
        'outtmpl': VIDEO_OUTPUT_FILE,
        'format': 'bestvideo[vcodec^=h264][height<=1080]+bestaudio/best',
    }


# Downloader for Zoom meeting recordings which are not password protected.
class ZoomDownloader(BaseDownloader):
    YDL_OPTS = {'outtmpl': VIDEO_OUTPUT_FILE}


class ImportUrlDownloadFactory:
    """
    Factory class to create downloaders based on the import source type.
    """

    DOWNLOADERS = {
        ImportSourceType.YOUTUBE: YouTubeDownloader,
        ImportSourceType.VIMEO: VimeoDownloader,
        ImportSourceType.WISTIA: WistiaDownloader,
        ImportSourceType.ZOOM: ZoomDownloader,
        ImportSourceType.HOSTED_URL: HostedUrlDownloader
    }

    def __init__(self, import_url=None, import_source_type=ImportSourceType.HOSTED_URL, media_type="VIDEO", project_id=None,
                 content_id=None):
        """
        Initialize the factory with the import URL and source type.

        :param import_url: The URL of the video to be imported.
        :param import_source_type: The source type of the video.
        """
        self.import_url = import_url
        self.import_source_type = ImportSourceType(import_source_type)
        self.media_type = media_type
        self.project_id = project_id
        self.content_id = content_id

    def create_downloader(self):
        """
        Create a downloader based on the import source type.

        :return: An instance of the appropriate downloader class.
        """
        downloader_class = self.DOWNLOADERS.get(self.import_source_type)
        if downloader_class:
            return downloader_class(self.import_url, self.media_type, self.project_id, self.content_id)
        else:
            raise NotImplementedError(f"Invalid import source type: {self.import_source_type}")