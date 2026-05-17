import json
import os
import time
from openai import RateLimitError, OpenAI
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def generate_baseline(prompt_text):
    system_instruction = (
        "You are an expert data analyst. Your task is to summarize social media discussions. "
        "Focus purely on the main opinions, key arguments, and overall conclusion."
    )
    
    response = client.chat.completions.create(
        model="gpt-4o", 
        messages=[
            {"role": "system", "content": system_instruction},
            {"role": "user", "content": prompt_text}
        ],
        temperature=0.3
    )
    return response.choices[0].message.content

def run_baseline_generator():
    raw_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/deep_threads_with_comments.json'))
    proposed_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/final_summaries_gpt4o.json')) 
    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/baseline_summaries.json'))

    try:
        with open(proposed_path, 'r', encoding='utf-8') as f:
            proposed_summaries = json.load(f)
            target_ids = [t['thread_id'] for t in proposed_summaries]
    except FileNotFoundError:
        print(f"[LỖI] Không tìm thấy file {proposed_path}. Hãy chạy file generator.py trước.")
        return

    with open(raw_path, 'r', encoding='utf-8') as f:
        raw_threads = {t.get('link_id', ''): t for t in json.load(f)}

    results = []
    processed_ids = set()
    
    if os.path.exists(output_path):
        try:
            with open(output_path, 'r', encoding='utf-8') as f:
                results = json.load(f)
                processed_ids = {item.get('thread_id') for item in results}
            print(f"Tìm thấy {len(processed_ids)} bài Baseline đã hoàn thành từ lần chạy trước.")
        except json.JSONDecodeError:
            print("File output cũ bị lỗi định dạng, sẽ bắt đầu lại từ đầu.")

    print(f"Bắt đầu tạo {len(target_ids)} bản Baseline bằng GPT-4o từ dữ liệu THÔ...")

    for thread_id in tqdm(target_ids):
        if thread_id in processed_ids:
            continue

        raw_thread = raw_threads.get(thread_id)
        if not raw_thread:
            continue

        # Nối tất cả các bình luận thô lại
        raw_comments = [c.get('body', '') for c in raw_thread.get('comments', [])]
        raw_text = "\n".join([f"- User: {body}" for body in raw_comments])
        
        # PROMPT MỚI: Đồng bộ 100% với file generator.py (Đã xóa Thread_ID, dùng cấu trúc 4 phần)
        prompt = (
            f"Comments:\n{raw_text}\n\n"
            f"Please provide a comprehensive and detailed summary of this discussion using the following strict structure:\n"
            f"1. Core Topic: (Briefly describe the main subject being discussed)\n"
            f"2. Main Viewpoints: (Detail the distinct sides or perspectives of the debate)\n"
            f"3. Key Arguments & Evidence: (What specific reasoning, examples, or data did the users provide to support their views?)\n"
            f"4. Overall Conclusion/Consensus: (Did the community reach an agreement, or did it remain divided?)"
        )

        success = False
        max_retries = 5
        retries = 0

        while not success and retries < max_retries:
            try:
                summary = generate_baseline(prompt)
                results.append({
                    "thread_id": thread_id,
                    "summary": summary
                })
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=4)
                    
                success = True
            except RateLimitError:
                retries += 1
                time.sleep(20 * retries)
                print(f"\nRate Limit. Thử lại lần {retries}/{max_retries}...")
            except Exception as e:
                print(f"\n[ERROR] Thread {thread_id}: {e}")
                break

    print(f"\n[XONG] File Baseline đã sẵn sàng tại: {output_path}")

if __name__ == "__main__":
    run_baseline_generator()