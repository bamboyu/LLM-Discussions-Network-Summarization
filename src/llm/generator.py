import json
import os
import time
from openai import RateLimitError, OpenAI
from dotenv import load_dotenv
from tqdm import tqdm

# Tải các biến môi trường từ file .env
load_dotenv()

# Khởi tạo client OpenAI sử dụng API key đã nạp
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def generate_summary(prompt_text):
    """
    Gửi prompt chứa các bình luận lên OpenAI API để lấy tóm tắt.
    """
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

def run_generator():
    """
    Hàm chính để đọc dữ liệu, chạy vòng lặp tóm tắt và lưu kết quả.
    """
    input_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/extracted_contexts_for_llm.json'))
    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/final_summaries_gpt4o.json'))

    print("Loading extracted contexts from GNN...")
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            threads = json.load(f)
    except FileNotFoundError:
        print(f"[ERROR] Could not find {input_path}. Run inference.py first.")
        return

    results = []
    processed_ids = set()
    
    if os.path.exists(output_path):
        try:
            with open(output_path, 'r', encoding='utf-8') as f:
                results = json.load(f)
                processed_ids = {item.get('thread_id') for item in results}
            print(f"Tìm thấy {len(processed_ids)} bài đã hoàn thành từ lần chạy trước.")
            print("Hệ thống sẽ tự động bỏ qua và chạy tiếp các bài còn lại...")
        except json.JSONDecodeError:
            print("File output cũ bị lỗi định dạng, sẽ bắt đầu lại từ đầu.")

    max_threads_to_process = min(50, len(threads))
    print(f"Starting summarization for {max_threads_to_process} threads...")
    
    for i in tqdm(range(max_threads_to_process)):
        thread = threads[i]
        thread_id = thread.get('thread_id', 'unknown')
        
        if thread_id in processed_ids:
            continue

        comments = thread.get('comments', [])
        
        comments_text = "\n".join([f"- {c.get('author', 'User')}: {c.get('body', '')}" for c in comments])
        
        # PROMPT MỚI: Đã xóa Thread_ID và áp dụng cấu trúc 4 phần chi tiết
        prompt = (
            f"Comments:\n{comments_text}\n\n"
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
                summary = generate_summary(prompt)
                
                results.append({
                    "thread_id": thread_id,
                    "summary": summary
                })
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=4)
                    
                success = True 
                
            except RateLimitError:
                retries += 1
                wait_time = 20 * retries 
                print(f"\nRate Limit. Retrying {retries}/{max_retries} in {wait_time}s...")
                time.sleep(wait_time)
                
            except Exception as e:
                print(f"\n[ERROR] Thread {thread_id}: {e}")
                break 

        if not success and retries >= max_retries:
            print(f"\n[SKIPPED] Thread {thread_id} after {max_retries} attempts.")

    print(f"\n[DONE] Summaries saved to: {output_path}")

if __name__ == "__main__":
    run_generator()