import bz2
import json
import os
from collections import defaultdict

INPUT_FILE = os.path.join('data', 'raw', 'RC_2015-01.bz2')
METADATA_FILE = os.path.join('data', 'processed', 'deep_threads_metadata.json')
OUTPUT_FILE = os.path.join('data', 'processed', 'deep_threads_with_comments.json')

def run_pass_2():
    print("--- STARTING PASS 2: Extracting Comment Bodies ---")
    
    if not os.path.exists(METADATA_FILE):
        print(f"Error: Could not find {METADATA_FILE}. Run find_deep_threads.py first.")
        return

    with open(METADATA_FILE, 'r') as f:
        metadata = json.load(f)
        
    target_link_ids = {thread['link_id'] for thread in metadata}
    print(f"Loaded {len(target_link_ids)} target threads from metadata.")
    
    extracted_data = defaultdict(list)
    
    with bz2.open(INPUT_FILE, "rt", encoding="utf-8") as f_in:
        line_count = 0
        saved_comments = 0
        
        for line in f_in:
            line_count += 1
            if line_count % 1000000 == 0:
                print(f"Scanned {line_count} lines... Extracted {saved_comments} comments so far.")
                
            try:
                comment = json.loads(line)
                link_id = comment.get('link_id')
                
                if link_id in target_link_ids:
                    raw_author = comment.get('author', '[deleted]')
                    raw_body = comment.get('body', '')
                    
                    # 1. Clean text formatting
                    clean_body = " ".join(raw_body.replace('\n', ' ').replace('\r', '').split())
                    
                    # 2. Handle deleted nodes gracefully to preserve the graph tree
                    if raw_author == '[deleted]' or clean_body == '[deleted]':
                        clean_author = "Unknown_Deleted_User"
                        clean_body = "[This comment was deleted, but replies remain]"
                    else:
                        clean_author = raw_author
                    
                    extracted_data[link_id].append({
                        'id': comment.get('name'),
                        'parent_id': comment.get('parent_id'),
                        'author': clean_author,
                        'score': comment.get('score', 0),
                        'body': clean_body
                    })
                    saved_comments += 1
            except Exception:
                continue

    print("\nFormatting output for the LLM pipeline...")
    final_output = []
    for link_id, comments_list in extracted_data.items():
        final_output.append({
            "link_id": link_id,
            "comments": comments_list
        })
        
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f_out:
        json.dump(final_output, f_out, indent=4)
        
    print(f"\n🎉 SUCCESS! Saved {saved_comments} clean comments to {OUTPUT_FILE}.")

if __name__ == "__main__":
    run_pass_2()