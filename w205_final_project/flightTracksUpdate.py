#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import requests, json

# Top 15 airports
airports = ['ATL','LAX','ORD','DFW','JFK','DEN','SFO','LAS','CLT','SEA','PHX','MIA','MCO','IAH','EWR']

# Define variables for API query
base = "https://api.flightstats.com/flex/flightstatus/rest/v2/json/airport/tracks"
dep = "/dep"
auth = "?appId=e49c605d&appKey=d1f6312f2df70b35031c8df8fac39d5d"

for ap in airports:
    # Build up the tracking API query for one airport
    departure = "/" + ap
    track_query = base + departure + dep + auth
    
    # Ping the API to get the tracking data
    response = requests.get(track_query)

    # Load the json into a python object
    tracks = json.loads(response.content)
    
    # Save the json file to a directory where it can be processed and loaded to Hive
    with open('/home/w205/Final_project_data/tracking/tracks_'+ap+'.json', 'w') as fp:
        json.dump(tracks, fp)
