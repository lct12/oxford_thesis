import requests
import os
import json
import time
from datetime import datetime,timedelta
from os import sep

# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'



endpoint="https://api.twitter.com/2/tweets/search/all"
# Put token file in same directory 
#cwd = os.getcwd()
#token_file = f"{cwd}{sep}twitter_bearer_token.txt"




def auth():
	with open(".token", "r") as fh:
		return fh.read().strip()
	#return os.environ.get("BEARER_TOKEN")


def create_url():
	#max query is 1024 characters!
	#next_token:
	# Tweet fields are adjustable.
	# Options include:
	# attachments, author_id, context_annotations,
	# conversation_id, created_at, entities, geo, id,
	# in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
	# possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
	# source, text, and withheld
	url = "https://api.twitter.com/2/tweets/search/all"
	return url


def create_headers(bearer_token):
	headers = {"Authorization": "Bearer {}".format(bearer_token)}
	return headers

class BadStatusCode(Exception):
	def __init__(self,status_code):
		self.status_code=status_code
	def __str__(self):
		return f"BadStatusCode: {self.status_code}"

def connect_to_endpoint(url, headers, params):
	count=0
	while True:
		try:
			response = requests.request("GET", url, headers=headers, params=params)
			if response.status_code == 200:
				return response.json()
			else:
				print(response.text)
				raise BadStatusCode(response.status_code)
		except Exception as e:
			count+=1
			if count<4 or (isinstance(e,BadStatusCode) and e.status_code==503):
				#https://developer.twitter.com/en/support/twitter-api/error-troubleshooting
				#503 is "Service Unavailable"  (temporarily overloaded) -- we'll ignore this any number of times
				print("Error. count={}, {}".format(count,e))
				time.sleep(count*5)
			else:
				raise e
			
	return response.json()

def get_tweets(query,start_time,end_time,directory,token):
	print(directory)
	url = "https://api.twitter.com/2/tweets/search/all"
	headers = create_headers(token)
	params={
			"query":query,
			"max_results":500,
			"start_time":start_time,
			"end_time":end_time,
			"tweet.fields":"attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,source,text,withheld",
			"user.fields":"created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld",
			"expansions":"author_id,entities.mentions.username,geo.place_id,in_reply_to_user_id,referenced_tweets.id,referenced_tweets.id.author_id",
			"place.fields":"contained_within,country,country_code,full_name,geo,id,name,place_type",
		}
	i=0
	while True:
		outfile="{}/output_{:05d}.json".format(directory,i)
		try:
			with open(outfile,"r") as fh:
				json_response=json.load(fh)
		except:
			json_response = connect_to_endpoint(url, headers, params)
			with open(outfile,"w") as fh:
				json.dump(json_response, fh, indent=4, sort_keys=True)
				time.sleep(2)
		i+=1
		if i%100==0:
			print(directory,i)
		#if i>...:
		#	print("Ending to avoid quota")
		#	return i
		try:
			params["next_token"]=json_response["meta"]["next_token"]
		except:
			print("No next_token!")
			return i
	

def main():
	token = auth()
	
	
	#Find the Saturdays of the years in the list
	saturdays=[]
	for year in [2007, 2008, 2009, 2010, 2011, 2012, 2013]:
		for month in range(1,13):
			for day in range(1, 32):
				try:
					date=datetime(year,month,day)
					if date.weekday()==5:
						saturdays.append(date)
					day+=1
				# If date is out of range (e.g. day 31 in the month of Feb), then continue to the next month
				except ValueError: 
					continue


	global_count=0
	# bounding box = bounding_box:[west_long south_lat east_long north_lat]
	# bounding box in 3 sep chunks (needs to be under 25 miles width): Staten Island, Brooklyn/Queens/half of Manhattan, rest of Manhattan/Bronx
	#query=f"(bounding_box:[-74.2556782 40.4960342 -74.0492521 40.6488941] OR bounding_box:[-74.0419691 40.5419011 -73.7001809 40.8009249] OR bounding_box: [-74.0472219 40.6839411 -73.9061585 40.8804489] OR bounding_box[-73.9336575 40.7853712 -73.7653293 40.9152598]) -is:retweet"
	query="(bounding_box:[-74.2556782 40.4960342 -74.0492521 40.6488941] OR bounding_box:[-74.0419691 40.5419011 -73.7001809 40.8009249]" \
	" OR bounding_box:[-74.0472219 40.6839411 -73.9061585 40.8804489] OR bounding_box:[-73.9336575 40.7853712 -73.7653293 40.9152598]) -is:retweet"
	# print(query)
	assert len(query)<1024, f"Query too long {len(query)}"
	# quit()
	print("hello")
	for day in saturdays:
		str_startday=day.strftime("%Y-%m-%d")
		#This is fine and will not go out-of-range given day is middle of the month, but datetime.timedelta is probably a better choice
		str_endday=(day+timedelta(days=1)).strftime("%Y-%m-%d")
		os.makedirs(str_startday, exist_ok=True)
		global_count+=get_tweets(query,start_time=str_startday+"T00:00:00Z",end_time=str_endday+"T00:00:00Z",directory=str_startday,token=token)
		if global_count>20000:
			print("Quota up! Stopping!")
			break
	
	print("DONE",global_count)


if __name__ == "__main__":
	main()
