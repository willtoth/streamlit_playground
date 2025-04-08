from __future__ import unicode_literals
import youtube_dl

def download_video(url):
    # Define youtube_dl options
    ydl_opts = {
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]',
        'merge_output_format': 'mp4',
    }

    with youtube_dl.YoutubeDL(ydl_opts) as ydl:
        # Extract video information without downloading
        info_dict = ydl.extract_info(url, download=False)
        
        # Prepare the filename using the info_dict
        file_path = ydl.prepare_filename(info_dict)
        
        # Download the video
        ydl.download([url])
        
    return file_path