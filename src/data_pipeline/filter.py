# FOR FILTERING THE RAW DATA TO A BALANCED 1 MILLION ROWS

import bz2
import json
import csv

# 1. The "Golden 40" Mix (Niche + Hubs)
target_subreddits = {
    'machinelearning', 'datascience', 'statistics', 'rstats', 'learnmachinelearning',
    'programming', 'javascript', 'python', 'reactjs', 'webdev',
    'anime', 'manga', 'japan', 'japanese', 'learnjapanese',
    'guitar', 'guitars', 'guitaramps', 'musictheory', 'luthier',
    'aws', 'sysadmin', 'devops', 'linux', 'networking',
    'business', 'entrepreneur', 'startups', 'personalfinance', 'investing',
    'funny', 'askreddit', 'worldnews', 'gaming', 'todayilearned', 
    'science', 'pics', 'news', 'videos', 'technology'
}

# 2. Smart Limits
MAX_PER_SUB = 50000     # Cap the giant subs so they don't drown out the niche ones
TOTAL_GOAL = 1000000    # STOP the whole script once we hit 1 million total rows
subreddit_counts = {sub: 0 for sub in target_subreddits}

input_file = 'RC_2015-01.bz2'
output_file = 'balanced_reddit_1M.csv'

with open(output_file, 'w', newline='', encoding='utf-8') as f_out:
    writer = csv.writer(f_out)
    # Added 'score' to the header
    writer.writerow(['id', 'author', 'parent_id', 'subreddit', 'score', 'body'])

    print(f"Starting extraction. Goal: {TOTAL_GOAL} total comments.")
    
    with bz2.open(input_file, "rt", encoding="utf-8") as f_in:
        extracted_total = 0
        line_count = 0
        
        for line in f_in:
            line_count += 1
            if line_count % 1000000 == 0:
                print(f"Lines processed: {line_count} | Found: {extracted_total}")

            try:
                comment = json.loads(line)
                sub = comment.get('subreddit')
                
                # Safely handle missing subreddit fields
                if not sub:
                    continue
                sub = sub.lower()
                
                # Logic: Is it a target sub AND is that sub under its personal cap?
                if sub in target_subreddits and subreddit_counts[sub] < MAX_PER_SUB:
                    author = comment.get('author')
                    body = comment.get('body')
                    
                    if author != '[deleted]' and body != '[deleted]':
                        writer.writerow([
                            comment.get('name'),
                            author,
                            comment.get('parent_id'),
                            comment.get('subreddit'),
                            comment.get('score'), # ADDED SCORE HERE
                            body.replace('\n', ' ').replace('\r', '') # Clean newlines for CSV safety
                        ])
                        
                        subreddit_counts[sub] += 1
                        extracted_total += 1
                
                # THE MASTER STOP: Exit as soon as we hit 1 million total
                if extracted_total >= TOTAL_GOAL:
                    print(f"\nSUCCESS: Reached total goal of {TOTAL_GOAL} comments!")
                    break
                            
            except Exception:
                continue

# Print a small report so you can see which subs were "small"
print("\n--- Extraction Report ---")
for sub, count in sorted(subreddit_counts.items(), key=lambda x: x[1], reverse=True):
    if count > 0:
        print(f"{sub}: {count} comments")