import json
import random
import os
import sys
import time
from openai import OpenAI
from dotenv import load_dotenv

# DeepEval Imports
from deepeval.metrics import GEval
from deepeval.test_case import LLMTestCase, LLMTestCaseParams

# Ensure Python can find the src module from the root directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.data_pipeline.graph_formatter import format_thread_with_graph_features, get_flat_text

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ==========================================
# DEEPEVAL METRICS SETUP
# ==========================================
# We define the 3 metrics using G-Eval's evaluation steps for maximum accuracy.

comprehensiveness_metric = GEval(
    name="Comprehensiveness",
    evaluation_steps=[
        "Read the original input thread to understand the full context.",
        "Check if the actual output summary captures the overall big picture of the discussion.",
        "Assess whether it includes the variety of perspectives discussed, rather than just one.",
        "Penalize summaries that are too narrow or miss the wider context."
    ],
    evaluation_params=[LLMTestCaseParams.INPUT, LLMTestCaseParams.ACTUAL_OUTPUT],
    model="gpt-4o" # DeepEval will route this through your OPENAI_API_KEY
)

core_extraction_metric = GEval(
    name="Core Extraction",
    evaluation_steps=[
        "Read the original input thread.",
        "Identify the most structurally important arguments or authoritative statements.",
        "Check if the actual output summary successfully isolated these core arguments.",
        "Reward summaries that filter out irrelevant noise, jokes, or minor tangents."
    ],
    evaluation_params=[LLMTestCaseParams.INPUT, LLMTestCaseParams.ACTUAL_OUTPUT],
    model="gpt-4o"
)

consistency_metric = GEval(
    name="Consistency",
    evaluation_steps=[
        "Cross-reference the actual output summary against the original input thread.",
        "Check for any claims, facts, or numbers in the summary that do not exist in the source text.",
        "Heavily penalize hallucinations, fabrications, or misrepresentations of user statements."
    ],
    evaluation_params=[LLMTestCaseParams.INPUT, LLMTestCaseParams.ACTUAL_OUTPUT],
    model="gpt-4o"
)

# ==========================================
# GENERATION LOGIC
# ==========================================

def generate_summary(prompt_text, is_baseline=True):
    if is_baseline:
        system_instruction = "You are an expert summarizer. Read the following raw Reddit thread and summarize the core arguments and main conclusions."
    else:
        system_instruction = "You are an expert summarizer. Read the following hierarchical Reddit thread. Use the provided metadata [Score, Replies, Author's comment count] to identify the most authoritative arguments, ignoring downvoted noise."

    response = client.chat.completions.create(
        model="gpt-4o-mini", 
        messages=[
            {"role": "system", "content": system_instruction},
            {"role": "user", "content": prompt_text}
        ],
        temperature=0.3
    )
    return response.choices[0].message.content

# ==========================================
# EXPERIMENT RUNNER
# ==========================================

def run_experiment():
    print("Loading test data...")
    data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/deep_threads_with_comments.json'))
    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/deepeval_results.json'))
    
    try:
        with open(data_path, 'r', encoding='utf-8') as f:
            test_threads = json.load(f)
    except FileNotFoundError:
        print(f"Error: Could not find {data_path}.")
        return
        
    results = []
    successful_evaluations = 0
    
    try:
        for idx, thread in enumerate(test_threads):
            if successful_evaluations >= 3:
                print("\n🎯 Reached target of 50 successful DeepEval evaluations!")
                break

            print(f"\n{'='*50}")
            print(f"Processing Thread {idx + 1} (Successful: {successful_evaluations}/50)")
            print(f"{'='*50}")
            
            flat_text = get_flat_text(thread['comments']) 
            
            if len(flat_text.split()) > 8000:
                print(f"⏭️ Skipping {thread['link_id']} - Too large.")
                continue
                
            graph_text = format_thread_with_graph_features(thread['comments'])
            
            try:
                print("🤖 Generating summaries...")
                baseline_summary = generate_summary(flat_text, is_baseline=True)
                proposed_summary = generate_summary(graph_text, is_baseline=False)
                
                print("⚖️ Running DeepEval GEval Metrics (This takes a moment)...")
                
                # Create DeepEval Test Cases
                # We use the graph_text as the 'Input' for both so the judge has the richest context
                baseline_test_case = LLMTestCase(input=graph_text, actual_output=baseline_summary)
                proposed_test_case = LLMTestCase(input=graph_text, actual_output=proposed_summary)
                
                # Measure Baseline
                comprehensiveness_metric.measure(baseline_test_case)
                core_extraction_metric.measure(baseline_test_case)
                consistency_metric.measure(baseline_test_case)
                
                b_scores = {
                    "Comprehensiveness": comprehensiveness_metric.score * 10, # Multiply by 10 to keep your 1-10 scale!
                    "Core_Extraction": core_extraction_metric.score * 10,
                    "Consistency": consistency_metric.score * 10
                }
                b_scores["Total"] = sum(b_scores.values())

                # Measure Proposed
                comprehensiveness_metric.measure(proposed_test_case)
                core_extraction_metric.measure(proposed_test_case)
                consistency_metric.measure(proposed_test_case)
                
                p_scores = {
                    "Comprehensiveness": comprehensiveness_metric.score * 10,
                    "Core_Extraction": core_extraction_metric.score * 10,
                    "Consistency": consistency_metric.score * 10
                }
                p_scores["Total"] = sum(p_scores.values())
                
                print("✅ Successfully evaluated via DeepEval!")
                
                # Save the actual text this time so you can review it later!
                results.append({
                    "link_id": thread['link_id'],
                    "baseline_text": baseline_summary,
                    "proposed_text": proposed_summary,
                    "baseline_metrics": b_scores,
                    "proposed_metrics": p_scores,
                    "baseline_reasoning": comprehensiveness_metric.reason, # Save a sample reason
                    "proposed_reasoning": core_extraction_metric.reason
                })
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(results, f, indent=4)
                
                successful_evaluations += 1
                
            except Exception as e:
                # DeepEval has built-in async/retry handling, but we catch top-level errors just in case
                print(f"⚠️ Error processing thread: {e}. Sleeping 30s and skipping...")
                time.sleep(30)
                    
    except KeyboardInterrupt:
        print(f"\n\n🛑 STOP SIGNAL RECEIVED! Safely halting.")
        sys.exit(0)

    print(f"\n🎉 DeepEval experiment complete! Results saved to {output_path}")

if __name__ == "__main__":
    run_experiment()