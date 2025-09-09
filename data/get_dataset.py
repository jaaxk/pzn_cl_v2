"""
Reads Last.fm_data.csv with ~76k unique tracks, 
(1) Returns dataset of preview urls for each track (1 url per row)
(2) Returns dataset of n similar preview urls per row (n is 2 by default)
"""

# imports
import pandas as pd
import os
import argparse
from scripts.spotify_preview import get_spotify_preview_url
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
from tqdm import tqdm
import pylast
import csv


def get_preview_urls(artist_track_tuples, sp):
    """takes in list artist, track tuples and returns list of preview urls of same length"""
    preview_urls = []
    successful_tracks = []
    for artist, track in artist_track_tuples:
        #get spotify trackID
        trackID = sp.search(q=f"{track} {artist}", type="track", limit=1)['tracks']['items'][0]['id']
        if trackID is not None:
            preview_urls.append(get_spotify_preview_url(trackID))
            successful_tracks.append((artist, track))
        else:
            print(f"Could not find trackID for {artist} - {track}")

    return preview_urls, successful_tracks



def main():
    parser = argparse.ArgumentParser(description='Get dataset for CL')
    parser.add_argument('-n', '--num_similar', type=int, default=2, help='Number of similar tracks per row for second dataset')
    parser.add_argument('-o', '--output_dir', type=str, default='train_sets', help='Directory to save datasets')
    args = parser.parse_args()

    # read and preprocess Last.fm_data.csv
    if not os.path.exists('lastfm_datasets/lastfm_data_unique.csv'):
        df = pd.read_csv('lastfm_datasets/Last.fm_data.csv')
        df['artist_track'] = df['Artist'] + '_' + df['Track']
        df = df.drop_duplicates(subset='artist_track')
        print(f'Last.fm_data.csv has {len(df)} unique tracks')
        df = df[['Artist', 'Track', 'artist_track']]
        df.to_csv('lastfm_datasets/lastfm_data_unique.csv', index=False)
    else:
        df = pd.read_csv('lastfm_datasets/lastfm_data_unique.csv')
    
    artist_track_tuples = df[['Artist', 'Track']].values.tolist()

    #authenticate spotify and lastfm
    load_dotenv()
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=os.getenv("SPOTIPY_CLIENT_ID"),
        client_secret=os.getenv("SPOTIPY_CLIENT_SECRET")),
        retries=10,
        )
    network = pylast.LastFMNetwork(
        api_key=os.getenv("LASTFM_API_KEY"),
        api_secret=os.getenv("LASTFM_API_SECRET"),
        username=os.getenv("LASTFM_USERNAME"),
        password_hash=pylast.md5(os.getenv("LASTFM_PASSWORD")))



    # get first dataset
    if not os.path.exists(f'{args.output_dir}/dataset_1.csv'):
        print("Getting first dataset")
        with open(f'{args.output_dir}/dataset_1.csv', 'w', newline='', encoding='utf-8') as f:
            #process 100 at a time
            for i in tqdm(range(0, len(artist_track_tuples), 100), desc="Getting dataset 1"):
                preview_urls, successful_tracks = get_preview_urls(artist_track_tuples[i:min(i+100, len(artist_track_tuples))], sp)
                for url, (artist, track) in zip(preview_urls, successful_tracks):
                    f.write(f'{artist},{track},{url}\n') #we need to save artist and track so we can fetch the url in the second dataset easily

    # get second dataset
    print("Getting second dataset")
    with open(f'{args.output_dir}/dataset_1.csv', 'r', newline='', encoding='utf-8') as f1:
        with open(f'{args.output_dir}/dataset_2.csv', 'w', newline='', encoding='utf-8') as f2:
            reader = csv.reader(f1)
            writer = csv.writer(f2)
            for row in reader:
                urls_to_write = [row[2]] #start with the url from the first dataset
                track = network.get_track(row[0], row[1])
                similar_tracks = track.get_similar(limit=args.num_similar-1)
                artist_tracks = []
                for similar_track in similar_tracks:
                    artist_tracks.append((similar_track.item.get_artist().get_name(), similar_track.item.get_name()))
                preview_urls, successful_tracks = get_preview_urls(artist_tracks, sp)
                for url in preview_urls:
                    urls_to_write.append(url)
                writer.writerow(urls_to_write)
                f2.flush()


                    

        
    

if __name__ == '__main__':
    main()
    