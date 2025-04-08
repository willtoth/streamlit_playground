import streamlit as st
import tbapy

tba = tbapy.TBA(st.secrets.tba.key)

def match_name_to_key(matchname: str) -> str:
    matchname = matchname.lower()
    num = int(matchname[1:])
    
    if matchname.startswith('e'):
        if num < 14:
            return f"qf{num}m1"
        else:
            matchnum = (13 - num) * -1
            return f"f1m{matchnum}"
    elif matchname.startswith('q'):
        return f"qm{num}"
    else:
        return ""

def get_match_key(year: int, event: str, matchname:str) -> str:
    return f"{year}{event.lower()}_{match_name_to_key(matchname)}"

def get_tba_url(matchkey: str) -> str:
    return f"https://www.thebluealliance.com/match/{matchkey}"

def get_match_video(matchkey: str) -> str | None:
    match = tba.match(key=matchkey)

    for video in match.get("videos", []):
        if video.get("type") == "youtube":
            video_key = video.get("key")
            return f"https://www.youtube.com/watch?v={video_key}"
    return None