import bz2
import json
import os
from collections import defaultdict

INPUT_FILE = os.path.join('data', 'raw', 'RC_2015-01.bz2')
OUTPUT_FILE = os.path.join('data', 'processed', 'deep_threads_metadata.json')

# Bumping this to 200 just to give your final script plenty of backup options
TARGET_THREAD_COUNT = 200 

thread_maps = defaultdict(dict) 
thread_authors = defaultdict(set)
thread_word_counts = defaultdict(int) # NEW: Track the word count on the fly!

TARGET_SUBS = {
    'changemyview', 'askscience', 'explainlikeimfive', 'outoftheloop',
    'askreddit', 'relationships', 'personalfinance', 'tifu',
    'technology', 'science', 'futurology', 'worldnews', 'news',
    'movies', 'books', 'games', 'fitness', 'programming'
}

print("--- STARTING PASS 1: Scanning for Quality, Perfectly-Sized Debates ---")

with bz2.open(INPUT_FILE, "rt", encoding="utf-8") as f_in:
    line_count = 0
    for line in f_in:
        line_count += 1
        if line_count % 1000000 == 0:
            print(f"Processed {line_count} lines...")
            
        try:
            comment = json.loads(line)
            subreddit = comment.get('subreddit', '').lower()
            
            if subreddit not in TARGET_SUBS:
                continue
                
            link_id = comment.get('link_id')
            comment_id = comment.get('name') 
            parent_id = comment.get('parent_id') 
            author = comment.get('author')
            body = comment.get('body', '')
            
            if link_id and comment_id:
                thread_maps[link_id][comment_id] = parent_id
                
                # Track unique voices
                if author and author != '[deleted]':
                    thread_authors[link_id].add(author)
                    
                # Track the running word count (ignore deleted text)
                if body != '[deleted]':
                    thread_word_counts[link_id] += len(body.split())
                    
        except Exception:
            continue

def get_depth(comment_id, mapping, memo):
    if comment_id in memo:
        return memo[comment_id]
    
    parent = mapping.get(comment_id)
    if not parent or parent.startswith('t3_'):
        memo[comment_id] = 1
        return 1
    
    depth = 1 + get_depth(parent, mapping, memo)
    memo[comment_id] = depth
    return depth

print("Calculating thread depths and applying size limits...")
thread_quality_scores = []

for link_id, mapping in thread_maps.items():
    
    # 1. NEW SIZE FILTER: If the thread is larger than 8,000 words, ignore it completely!
    if thread_word_counts[link_id] > 8000:
        continue
        
    unique_author_count = len(thread_authors[link_id])
    
    # 2. Require at least 5 different people talking
    if unique_author_count < 5:
        continue
        
    memo = {}
    max_d = 0
    for cid in mapping.keys():
        max_d = max(max_d, get_depth(cid, mapping, memo))
        
    # 3. Sweet spot for a manageable tree: 8 to 100 replies deep
    if 8 <= max_d <= 100:
        thread_quality_scores.append({
            'link_id': link_id,
            'max_depth': max_d,
            'total_comments': len(mapping),
            'unique_authors': unique_author_count,
            'word_count': thread_word_counts[link_id] # Save this so we can see it!
        })

# Sort by unique authors and depth to get the juiciest, most intense arguments
top_quality_threads = sorted(thread_quality_scores, 
                          key=lambda x: (x['unique_authors'], x['max_depth']), 
                          reverse=True)[:TARGET_THREAD_COUNT]

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
with open(OUTPUT_FILE, 'w') as f_out:
    json.dump(top_quality_threads, f_out, indent=4)

print(f"Done! Saved metadata for {len(top_quality_threads)} perfect-sized debate threads.")