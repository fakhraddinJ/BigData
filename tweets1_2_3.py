"""
Created on Thu Dec 22 19:31:52 2016

@author: Fakhraddin Jaf
"""
 
#%% 
import collections  #High-performance container datatypes
import pandas as pd
#pandas is an open source, BSD-licensed library providing high-performance,
#easy-to-use data structures and data analysis tools for the Python programming language.


#%% 
#Reads each column of the Tweet's file into seperate DataFrame.
df = pd.read_csv('tweets.tsv', \
                 names=['UID', 'user', 'tweet', 'date', 'latitude', 'longitude', 'location_name'],
                 dtype=str, delimiter='\t')

#%%  
print("\n------------------ Task 1 ---------------------\n"\
      "Top 10 Trends:\n")
#Creating a new string from df.tweet's DataFrame.
all_tweets_str =( " ".join(df.tweet)).split()

#List of words need to be filter out:
common_words = [
"rt","RT","in","to", "at","a", "is", "if","of","-",\
"with","on","but", "and", "the", "not", "use","for",\
"will", "I", "be", "have","has","this","so","&","The"\
]

#Creating a filtered list of tweets' words:
all_tweets_words = [ 
x for x in all_tweets_str if x not in common_words and 'http' not in x ]

# Making a list of (count, unique) tuples:                  
uniques = []
for word in all_tweets_words:
  if word not in uniques:
    uniques.append(word)

counts = []
for unique in uniques:
  count = 0
  for word in all_tweets_words:   
    if word == unique:   
      count += 1        
  counts.append((count, unique))

counts.sort()            # Sorting the list puts the lowest counts first.
counts.reverse()         # Reverse it, putting the highest counts first.

# Print the ten words with the highest counts.
for i in range(min(10, len(counts))):
  count, word = counts[i]
  print("%d) " %(i+1) +"%s -----> %d" % (word, count))

#%%
print("\n\n------------------ Task 2 ---------------------")
#Counting and printing the most active user:
user_numbers = collections.Counter(df.user)
most_active_user = (max(user_numbers, key=user_numbers.get))
print("Most active user: %s" %most_active_user)
print("Number of his/her tweets: %s" %user_numbers.get(most_active_user))


#%%
print ("\n\n------------------ Task 3 ---------------------")
#Counting the length of each tweet and making a list of them:
Tweet_Length = []
for s in df.tweet:
    Tweet_Length.append(len(s))

#Getting the index of shortest tweet in the "df" DataFrame, 
#and printing the requested values of that line :
ind = Tweet_Length.index(min(Tweet_Length))
print("Shortest tweet is: \"%s" %df.tweet[ind] + "\"")
print("By user: %s" %df.user[ind])
print("Date: %s" %df.date[ind])
print("Location: %s" %df.location_name[ind])
print("Latitude: %s" %df.latitude[ind])
print("Longitude: %s\n\n" %df.longitude[ind])
