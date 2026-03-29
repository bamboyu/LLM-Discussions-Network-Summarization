import pandas as pd
import networkx as nx
import json
import os
from networkx.readwrite import json_graph

# --- CONFIGURATION ---
CSV_PATH = "data/processed/balanced_reddit_1M.csv" 
OUTPUT_DIR = "data/graphs/"

def build_thread_graphs(csv_path, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    print("Starting Graph Construction...")
    
    use_cols = ['id', 'author', 'parent_id', 'link_id', 'score', 'body']
    
    df = pd.read_csv(csv_path, usecols=use_cols)

    print("Calculating global author reputation...")
    # Define author trust score as the sum of their comment scores across the dataset
    df['author_trust_score'] = df.groupby('author')['score'].transform('sum')
    df.drop(columns=['author'], inplace=True)

    print("Grouping comments by Post ID (link_id)...")
    grouped = df.groupby('link_id')
    
    total_posts = len(grouped)
    print(f"Found {total_posts} unique posts. Building full-thread graphs...\n")

    for i, (post_id, group) in enumerate(grouped):
        G = nx.DiGraph()
        
        # This loop builds the discussion tree for one post
        for _, row in group.iterrows():
            node_id = str(row['id'])
            parent_id = str(row['parent_id'])
            
            G.add_node(
                node_id,
                comment_score=int(row['score']),
                author_trust=int(row['author_trust_score']),
                body=str(row['body'])
            )
            G.add_edge(node_id, parent_id)
        
        # Save as JSON
        data = json_graph.node_link_data(G)
        safe_post_id = str(post_id).replace('t3_', '')
        filepath = os.path.join(output_dir, f"post_{safe_post_id}.json")
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f)
        
        del G
        
        if (i + 1) % 1000 == 0:
            print(f"Processed {i + 1} / {total_posts} posts...")

    print("\n Done! You now have a 1-to-1 mapping of Posts to Graphs.")

if __name__ == "__main__":
    build_thread_graphs(CSV_PATH, OUTPUT_DIR)