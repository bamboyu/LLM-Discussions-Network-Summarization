import json
import os
import time
import hashlib
from openai import RateLimitError, OpenAI
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def evaluate_summaries(summary_a, summary_b):
    system_instruction = (
        "You are an impartial expert judge evaluating two AI-generated summaries of a social media discussion. "
        "Your task is to compare Summary A and Summary B and determine which one is superior. "
        "Evaluate them based on: "
        "1. Depth of Arguments: Which summary extracts more meaningful arguments and specific evidence? "
        "2. Clarity & Noise: Which summary is clearer, more concise, and free of irrelevant/noisy information? "
        "3. Structure: Which summary adheres better to a logical, professional structure? "
        "Respond ONLY with a JSON object."
    )

    prompt_text = (
        f"Summary A:\n{summary_a}\n\n"
        f"Summary B:\n{summary_b}\n\n"
        f"Provide your evaluation in the exact following JSON format:\n"
        f"{{\n"
        f'  "Score_A": [Score from 1 to 10],\n'
        f'  "Score_B": [Score from 1 to 10],\n'
        f'  "Winner": ["A", "B", or "Tie"],\n'
        f'  "Reasoning": ["A brief explanation of why the winner was chosen"]\n'
        f"}}"
    )

    response = client.chat.completions.create(
        model="gpt-4o",
        response_format={ "type": "json_object" }, 
        messages=[
            {"role": "system", "content": system_instruction},
            {"role": "user", "content": prompt_text}
        ],
        temperature=0.1 
    )
    return json.loads(response.choices[0].message.content)

def run_evaluator():
    gnn_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/final_summaries_gpt4o.json'))
    baseline_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/baseline_summaries.json'))
    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/evaluation_results.json'))

    print("Đang nạp dữ liệu từ hai hệ thống...")
    try:
        with open(gnn_path, 'r', encoding='utf-8') as f:
            gnn_data = {item['thread_id']: item['summary'] for item in json.load(f)}
        with open(baseline_path, 'r', encoding='utf-8') as f:
            baseline_data = {item['thread_id']: item['summary'] for item in json.load(f)}
    except FileNotFoundError as e:
        print(f"[LỖI] Không tìm thấy file: {e}")
        return

    common_ids = list(set(gnn_data.keys()).intersection(set(baseline_data.keys())))
    
    results = []
    processed_ids = set()

    if os.path.exists(output_path):
        try:
            with open(output_path, 'r', encoding='utf-8') as f:
                results = json.load(f)
                processed_ids = {item.get('thread_id') for item in results}
            print(f"Tìm thấy {len(processed_ids)} bài đã được chấm điểm.")
        except json.JSONDecodeError:
            print("File output cũ bị lỗi định dạng, sẽ bắt đầu chấm lại từ đầu.")

    print(f"Bắt đầu chấm điểm {len(common_ids)} bài đối chứng...")

    for thread_id in tqdm(common_ids):
        if thread_id in processed_ids:
            continue

        sum_gnn = gnn_data[thread_id]
        sum_baseline = baseline_data[thread_id]

        # Áp dụng hàm băm MD5 để cố định vị trí A/B theo thread_id
        hash_hex = hashlib.md5(thread_id.encode('utf-8')).hexdigest()
        is_gnn_a = int(hash_hex, 16) % 2 == 0
        
        if is_gnn_a:
            summary_a, summary_b = sum_gnn, sum_baseline
        else:
            summary_a, summary_b = sum_baseline, sum_gnn

        success = False
        max_retries = 5
        retries = 0

        while not success and retries < max_retries:
            try:
                eval_result = evaluate_summaries(summary_a, summary_b)
                
                gnn_score = eval_result["Score_A"] if is_gnn_a else eval_result["Score_B"]
                baseline_score = eval_result["Score_B"] if is_gnn_a else eval_result["Score_A"]
                
                if eval_result["Winner"] == "Tie":
                    real_winner = "Tie"
                elif (eval_result["Winner"] == "A" and is_gnn_a) or (eval_result["Winner"] == "B" and not is_gnn_a):
                    real_winner = "GNN"
                else:
                    real_winner = "Baseline"

                results.append({
                    "thread_id": thread_id,
                    "GNN_Score": gnn_score,
                    "Baseline_Score": baseline_score,
                    "Winner": real_winner,
                    "Reasoning": eval_result["Reasoning"]
                })

                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=4)
                    
                success = True
            except RateLimitError:
                retries += 1
                time.sleep(20 * retries)
                print(f"\nRate Limit. Thử lại lần {retries}...")
            except Exception as e:
                print(f"\n[ERROR] Thread {thread_id}: {e}")
                break

    print(f"\n[XONG] Kết quả chấm điểm đã lưu tại: {output_path}")

if __name__ == "__main__":
    run_evaluator()