import json
import os
import sys

# Ensure Python can find the src module from the root directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.data_pipeline.graph_formatter import format_thread_with_graph_features, get_flat_text

def inspect_data():
    # Load the extracted comments
    data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/deep_threads_with_comments.json'))
    
    try:
        with open(data_path, 'r', encoding='utf-8') as f:
            test_threads = json.load(f)
    except FileNotFoundError:
        print(f"Error: Could not find {data_path}. Run extract_comments.py first.")
        return

    if not test_threads:
        print("Your dataset is empty!")
        return

    print(f"✅ Successfully loaded {len(test_threads)} threads.")
    
    # Grab the first thread in the dataset (you can change this index to see others)
    target_index = 0
    thread = test_threads[target_index]
    
    print(f"\n{'='*80}")
    print(f"🧵 THREAD ID: {thread['link_id']} (Index: {target_index})")
    print(f"{'='*80}")

    flat_text = get_flat_text(thread['comments'])
    graph_text = format_thread_with_graph_features(thread['comments'])

    # 1. Print the Raw Flat Text
    print("\n📝 1. RAW FLAT TEXT (What the Baseline sees)")
    print("-" * 80)
    # We print the first 1000 characters so it doesn't flood your terminal
    print(flat_text[:1000] + "\n\n... [TRUNCATED FOR DISPLAY]")

    # 2. Print the Graph Enhanced Text
    print("\n🕸️ 2. ENHANCED GRAPH TEXT (What the Proposed Method sees)")
    print("-" * 80)
    print(graph_text[:1000] + "\n\n... [TRUNCATED FOR DISPLAY]")

    print(f"\n{'='*80}")
    print("📊 DATA PAYLOAD STATS:")
    print(f"  Word Count (Flat Text):  {len(flat_text.split()):,} words")
    print(f"  Char Count (Graph Text): {len(graph_text):,} characters")
    print(f"{'='*80}\n")

if __name__ == "__main__":
    inspect_data()