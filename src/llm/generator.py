import json
import random
import os
import sys
import time
from openai import RateLimitError
from openai import OpenAI
from dotenv import load_dotenv

# Ensure Python can find the src module from the root directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.data_pipeline.graph_formatter import format_thread_with_graph_features, get_flat_text

# Load API key from .env file
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def generate_summary(prompt_text, is_baseline=True):
    if is_baseline:
        system_instruction = "You are an expert summarizer. Read the following raw Reddit thread and summarize the core arguments and main conclusions."
    else:
        system_instruction = "You are an expert summarizer. Read the following hierarchical Reddit thread. Use the provided metadata [Score, Replies, Author's comment count] to identify the most authoritative arguments, ignoring downvoted noise."

    response = client.chat.completions.create(
        model="gpt-5.4-mini", 
        messages=[
            {"role": "system", "content": system_instruction},
            {"role": "user", "content": prompt_text}
        ],
        temperature=0.3
    )
    return response.choices[0].message.content

def evaluate_summaries(original_thread, summary_a, summary_b):
    judge_system_prompt = """You are an impartial, reference-free evaluator. You will be provided with an original Reddit conversation, followed by Summary A and Summary B. 
Score each summary from 1 to 10 on the following metrics:
1. Comprehensiveness: Does it capture the breadth of the discussion?
2. Core_Extraction: Does it highlight the most structurally important arguments?
3. Consistency: Is it factually accurate to the source without hallucinations?

Return ONLY a valid JSON object in this format:
{
  "Summary_A": {"Comprehensiveness": 0, "Core_Extraction": 0, "Consistency": 0, "Total": 0},
  "Summary_B": {"Comprehensiveness": 0, "Core_Extraction": 0, "Consistency": 0, "Total": 0},
  "Reasoning": "Brief explanation of why one won over the other."
}"""
    
    user_prompt = f"ORIGINAL THREAD:\n{original_thread}\n\nSUMMARY A:\n{summary_a}\n\nSUMMARY B:\n{summary_b}"
    
    response = client.chat.completions.create(
        model="gpt-5.4", 
        response_format={ "type": "json_object" },
        messages=[
            {"role": "system", "content": judge_system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0.0
    )
    return json.loads(response.choices[0].message.content)

def run_experiment():
    print("Loading test data...")
    data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/deep_threads_with_comments.json'))
    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/final_evaluation_results.json'))
    
    try:
        with open(data_path, 'r', encoding='utf-8') as f:
            test_threads = json.load(f)
    except FileNotFoundError:
        print(f"Error: Could not find {data_path}. Please run extract_comments.py first.")
        return
        
    results = []
    successful_evaluations = 0
    
    # Wrap the entire loop in a try/except block to catch Ctrl+C
    try:
        for idx, thread in enumerate(test_threads):
            # Stop automatically once we have exactly 50 perfect results
            if successful_evaluations >= 50:
                print("\n🎯 Reached target of 50 successful evaluations!")
                break

            print(f"\n{'='*50}")
            print(f"Checking Thread {idx + 1} (Successful: {successful_evaluations}/50)")
            print(f"ID: {thread['link_id']}")
            print(f"{'='*50}")
            
            flat_text = get_flat_text(thread['comments']) 
            
            # 1. Get the word count FIRST
            word_count = len(flat_text.split())
            print(f"Word Count: ~{word_count} words")
            
            # 2. Skip it IMMEDIATELY if it's too big
            if word_count > 80000:
                print(f"⏭️ Skipping {thread['link_id']} - Too large for limits.")
                continue
                
            # 3. ONLY format the graph if it passed the size test!
            graph_text = format_thread_with_graph_features(thread['comments'])
                
            # The Retry Loop: If we hit a rate limit, sleep and try this thread again
            success = False
            while not success:
                try:
                    print("🤖 Generating baseline summary (Flat Text)...")
                    baseline_summary = generate_summary(flat_text, is_baseline=True)
                    
                    print("🕸️ Generating proposed summary (Graph Text)...")
                    proposed_summary = generate_summary(graph_text, is_baseline=False)
                    
                    print("⚖️ Evaluating with LLM Judge...")
                    
                    # Blind A/B Test logic to prevent position bias
                    is_baseline_a = random.choice([True, False])
                    if is_baseline_a:
                        summary_a, summary_b = baseline_summary, proposed_summary
                    else:
                        summary_a, summary_b = proposed_summary, baseline_summary

                    evaluation = evaluate_summaries(graph_text, summary_a, summary_b)
                    
                    # Map scores back correctly
                    baseline_scores = evaluation['Summary_A'] if is_baseline_a else evaluation['Summary_B']
                    proposed_scores = evaluation['Summary_B'] if is_baseline_a else evaluation['Summary_A']
                    
                    print("✅ Successfully evaluated!")
                    results.append({
                        "link_id": thread['link_id'],
                        "baseline_metrics": baseline_scores,
                        "proposed_metrics": proposed_scores,
                        "judge_reasoning": evaluation['Reasoning']
                    })
                    
                    # 🔥 INCREMENTAL SAVE: Write to disk after EVERY successful thread
                    with open(output_path, 'w', encoding='utf-8') as f:
                        json.dump(results, f, indent=4)
                    
                    success = True 
                    successful_evaluations += 1
                    
                except RateLimitError:
                    print("⏳ Hit OpenAI rate limit. Sleeping for 60 seconds...")
                    time.sleep(60)
                    
    # Handle manual termination
    except KeyboardInterrupt:
        print(f"\n\nSafely halting the experiment.")
        print(f"The {len(results)} threads evaluated so far are safely saved to:")
        print(f"   {output_path}")
        sys.exit(0)

    print(f"\n🎉 Experiment complete! Final results safely stored at {output_path}")

if __name__ == "__main__":
    run_experiment()