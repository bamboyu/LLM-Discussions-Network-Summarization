import json
import os
import time
import hashlib
from openai import RateLimitError, OpenAI
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def load_source(raw_threads, thread_id):
    raw_thread = raw_threads.get(thread_id, {})
    comments = raw_thread.get('comments', [])
    return "\n".join([f"- {c.get('author', 'User')}: {c.get('body', '')}" for c in comments])


def evaluate_summaries(source_text, summary_a, summary_b):
    system_instruction = (
        "You are an impartial expert judge evaluating two AI-generated summaries of a social media discussion. "
        "You are provided with the original source discussion and two summaries (A and B). "
        "Each summary is expected to follow a 4-part structure: Core Topic, Main Viewpoints, Key Arguments & Evidence, and Overall Conclusion. "
        "Evaluate both summaries rigorously based on the following 5 distinct criteria: "

        "1. Faithfulness (Crucial): Does the summary stick STRICTLY to the source text? You MUST penalize heavily if it hallucinates or invents details not found in the source. "

        "2. Noise_Reduction: Does the summary effectively filter out irrelevant banter, minor trivial comments, and unhelpful fluff? "

        "3. Coverage: Does the summary successfully capture the central debate and the primary opinion streams? Do NOT penalize a summary for being concise if it captures the main points efficiently. "

        "4. Depth: Does the summary extract strong, specific evidence and reasoning (e.g., user examples) rather than relying on generic statements? "

        "5. Clarity_Structure: Does the summary strictly follow the requested 4-part structure while being concisely and logically organized? "

        "Respond ONLY with a JSON object."
    )

    prompt_text = (
        f"Source Discussion:\n{source_text}\n\n"
        f"Summary A:\n{summary_a}\n\n"
        f"Summary B:\n{summary_b}\n\n"
        f"Provide your evaluation in the exact following JSON format:\n"
        f"{{\n"
        f'  "Scores_A": {{\n'
        f'    "Faithfulness": [1-10],\n'
        f'    "Noise_Reduction": [1-10],\n'
        f'    "Coverage": [1-10],\n'
        f'    "Depth": [1-10],\n'
        f'    "Clarity_Structure": [1-10]\n'
        f'  }},\n'
        f'  "Scores_B": {{\n'
        f'    "Faithfulness": [1-10],\n'
        f'    "Noise_Reduction": [1-10],\n'
        f'    "Coverage": [1-10],\n'
        f'    "Depth": [1-10],\n'
        f'    "Clarity_Structure": [1-10]\n'
        f'  }},\n'
        f'  "Winner": ["A", "B", or "Tie"],\n'
        f'  "Reasoning": "A brief explanation of why the winner was chosen"\n'
        f"}}"
    )

    response = client.chat.completions.create(
        model="gpt-4o",
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system_instruction},
            {"role": "user", "content": prompt_text}
        ],
        temperature=0.1
    )
    return json.loads(response.choices[0].message.content)


def run_evaluator():
    gat_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/final_summaries_gpt4o.json'))
    baseline_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/baseline_summaries.json'))
    raw_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/deep_threads_with_comments.json'))
    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/llm_eval_gat_vs_baseline.json'))

    print("Đang nạp dữ liệu...")
    try:
        with open(gat_path, 'r', encoding='utf-8') as f:
            gat_data = {item['thread_id']: item['summary'] for item in json.load(f)}
        with open(baseline_path, 'r', encoding='utf-8') as f:
            baseline_data = {item['thread_id']: item['summary'] for item in json.load(f)}
        with open(raw_path, 'r', encoding='utf-8') as f:
            raw_threads = {t.get('link_id', ''): t for t in json.load(f)}
    except FileNotFoundError as e:
        print(f"[LỖI] Không tìm thấy file: {e}")
        return

    common_ids = list(set(gat_data.keys()).intersection(set(baseline_data.keys())))

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

    print(f"Bắt đầu chấm điểm {len(common_ids)} bài (GAT vs Baseline)...")

    for thread_id in tqdm(common_ids):
        if thread_id in processed_ids:
            continue

        sum_gat = gat_data[thread_id]
        sum_baseline = baseline_data[thread_id]
        source_text = load_source(raw_threads, thread_id)

        hash_hex = hashlib.md5(thread_id.encode('utf-8')).hexdigest()
        is_gat_a = int(hash_hex, 16) % 2 == 0

        if is_gat_a:
            summary_a, summary_b = sum_gat, sum_baseline
        else:
            summary_a, summary_b = sum_baseline, sum_gat

        success = False
        max_retries = 5
        retries = 0

        while not success and retries < max_retries:
            try:
                eval_result = evaluate_summaries(source_text, summary_a, summary_b)

                scores_a = eval_result["Scores_A"]
                scores_b = eval_result["Scores_B"]

                gat_scores = scores_a if is_gat_a else scores_b
                baseline_scores = scores_b if is_gat_a else scores_a

                gat_total = round(sum(float(v) for v in gat_scores.values()) / len(gat_scores), 2)
                baseline_total = round(sum(float(v) for v in baseline_scores.values()) / len(baseline_scores), 2)

                winner_ab = eval_result["Winner"]
                if winner_ab == "Tie":
                    real_winner = "Tie"
                elif (winner_ab == "A" and is_gat_a) or (winner_ab == "B" and not is_gat_a):
                    real_winner = "GAT"
                else:
                    real_winner = "Baseline"

                results.append({
                    "thread_id": thread_id,
                    "GAT": {**gat_scores, "Total": gat_total},
                    "Baseline": {**baseline_scores, "Total": baseline_total},
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

        if not success and retries >= max_retries:
            print(f"\n[SKIPPED] Thread {thread_id} after {max_retries} attempts.")

    print(f"\n[XONG] Kết quả chấm điểm đã lưu tại: {output_path}")


if __name__ == "__main__":
    run_evaluator()
