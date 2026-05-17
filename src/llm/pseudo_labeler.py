import json
import os
import time # Thêm thư viện để xử lý thời gian chờ khi lỗi mạng
from openai import OpenAI
from dotenv import load_dotenv
from tqdm import tqdm 

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def generate_labels_for_thread(title, comments_list, max_retries=3):
    """
    Gửi danh sách bình luận (kèm Tiêu đề, Cấu trúc Reply, Upvote) cho LLM chấm điểm.
    Có cơ chế Retry chống rớt mạng.
    """
    # 1. Bổ sung Title để LLM biết chủ đề đang tranh luận là gì
    context_to_score = f"THREAD TITLE: {title}\n\nCOMMENTS (GRAPH STRUCTURE):\n"
    
    for c in comments_list:
        body = c.get('body', '').strip().replace('\n', ' ')
        # 2. Bổ sung parent_id để tái tạo đồ thị (ai trả lời ai)
        parent = c.get('parent_id', 'Unknown')
        # 3. Bổ sung score (upvotes) để biết thái độ cộng đồng
        upvotes = c.get('score', 0) 
        
        context_to_score += f"ID: {c['id']} | Replies to: {parent} | Upvotes: {upvotes} | Author: {c.get('author', 'Unknown')} | Text: {body}\n"

    system_prompt = """You are an expert data annotator for a summarization AI.
    Read the provided Reddit thread comments. Pay close attention to the "THREAD TITLE" and the "Replies to" field to understand the debate's graph structure. 
    The "Upvotes" field shows community consensus.
    
    Score each comment from 0.0 to 1.0 based on how important it is for summarizing the core arguments of the thread.
    - 0.0: Spam, jokes, irrelevant, minimal agreement ("I agree", "lol"), or dead-end replies.
    - 0.5: Provides some context but isn't the main point.
    - 1.0: Core argument, detailed explanation, or a critical turning point in the debate.
    
    CRITICAL INSTRUCTION: You MUST evaluate and return a score for EVERY SINGLE comment ID provided. Do NOT skip or omit any IDs.
    
    Return ONLY a valid JSON object mapping the comment ID to its float score.
    Example format:
    {
        "t1_c1x2y3": 0.1,
        "t1_c9z8w7": 0.9
    }"""

    # Vòng lặp chống rớt mạng (Thử tối đa 3 lần)
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model="gpt-4o", # Đã cập nhật đúng tên model bạn yêu cầu
                response_format={ "type": "json_object" },
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": context_to_score}
                ],
                temperature=0.0 # Bắt buộc là 0 để điểm số ổn định
            )
            return json.loads(response.choices[0].message.content)
            
        except Exception as e:
            print(f"\n[Cảnh báo] Lỗi ở lần thử {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                print("Đang đợi 5 giây để thử lại...")
                time.sleep(5)
            else:
                print("Đã thử 3 lần nhưng vẫn thất bại. Bỏ qua thread này.")
                return None

def run_labeling():
    # Lấy đường dẫn file chuẩn xác trong dự án
    input_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/deep_threads_with_comments.json'))
    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/labeled_threads_for_train.json'))
    
    with open(input_path, 'r', encoding='utf-8') as f:
        threads = json.load(f)
        
    # Giữ đúng 100 threads để vừa đủ dữ liệu, vừa tiết kiệm API, chừa 100 threads để test sau
    threads_to_label = threads[:100] 
    labeled_dataset = []

    print(f"Bắt đầu gán nhãn cho {len(threads_to_label)} threads với GPT-4o...")
    
    for thread in tqdm(threads_to_label):
        thread_title = thread.get('title', 'Unknown Title')
        
        # Gọi API chấm điểm
        labels = generate_labels_for_thread(thread_title, thread['comments'])
        
        if labels:
            # Gắn điểm vào từng node trong JSON
            for comment in thread['comments']:
                cid = comment['id']
                # Nếu LLM vẫn lỡ sót, gán an toàn là 0.0
                comment['target_score'] = labels.get(cid, 0.0) 
            
            labeled_dataset.append(thread)
            
            # Ghi đè file liên tục để bảo toàn dữ liệu nếu bị ngắt giữa chừng
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(labeled_dataset, f, indent=4)

    print(f"\nHoàn thành xuất sắc! Đã lưu data gán nhãn tại: {output_path}")

if __name__ == "__main__":
    run_labeling()