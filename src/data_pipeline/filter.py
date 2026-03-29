import bz2
import json
import csv
import os  # Added for folder management

# 1. The 40 subreddits
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

# 2. Setting Limits
MAX_PER_SUB = 50000 
TOTAL_GOAL = 1000000 
subreddit_counts = {sub: 0 for sub in target_subreddits}

input_dir = os.path.join('data', 'raw')
input_file = os.path.join(input_dir, 'RC_2015-01.bz2')
output_dir = os.path.join('data', 'processed')
output_file = os.path.join(output_dir, 'balanced_reddit_1M.csv')

# Ensure the directory exists before we start writing
os.makedirs(output_dir, exist_ok=True)

with open(output_file, 'w', newline='', encoding='utf-8') as f_out:
    writer = csv.writer(f_out)
    # Header includes link_id for post-level grouping
    writer.writerow(['id', 'author', 'parent_id', 'link_id', 'subreddit', 'score', 'body'])

    print(f"Starting extraction to: {output_file}")
    print(f"Goal: {TOTAL_GOAL} total comments.")
    
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
                
                if not sub:
                    continue
                sub = sub.lower()
                
                if sub in target_subreddits and subreddit_counts[sub] < MAX_PER_SUB:
                    author = comment.get('author')
                    body = comment.get('body')
                    
                    if author != '[deleted]' and body != '[deleted]':
                        writer.writerow([
                            comment.get('name'),      # id (t1_xxx)
                            author,
                            comment.get('parent_id'), 
                            comment.get('link_id'),   # link_id (t3_xxx)
                            comment.get('subreddit'),
                            comment.get('score'), 
                            body.replace('\n', ' ').replace('\r', '') 
                        ])
                        
                        subreddit_counts[sub] += 1
                        extracted_total += 1
                
                if extracted_total >= TOTAL_GOAL:
                    print(f"\nSUCCESS: Reached total goal of {TOTAL_GOAL} comments!")
                    break
                            
            except Exception:
                continue

# Print a small report
print("\n--- Extraction Report ---")
for sub, count in sorted(subreddit_counts.items(), key=lambda x: x[1], reverse=True):
    if count > 0:
        print(f"{sub}: {count} comments")