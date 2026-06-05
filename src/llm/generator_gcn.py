import json
import os
import time
from openai import RateLimitError, OpenAI
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

MAX_THREADS = 50
MAX_RETRIES = 5
RETRY_WAIT_BASE = 20  # seconds


def generate_summary(prompt_text):
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
        temperature=0.3,
        max_tokens=200
    )
    return response.choices[0].message.content


def get_depth(cid, comment_by_id, memo):
    if cid in memo:
        return memo[cid]
    memo[cid] = -1  # Sentinel để phát hiện cycle
    comment = comment_by_id.get(cid)
    if not comment:
        memo[cid] = 0
        return 0
    parent_id = comment.get('parent_id', '')
    clean_parent = parent_id.replace('t1_', '') if parent_id and parent_id.startswith('t1_') else None
    if not clean_parent or clean_parent not in comment_by_id:
        memo[cid] = 0
    else:
        parent_depth = get_depth(clean_parent, comment_by_id, memo)
        memo[cid] = 0 if parent_depth == -1 else 1 + parent_depth
    return memo[cid]


def get_direct_parent(comment, all_comment_by_id):
    """
    Lấy đúng 1 comment cha trực tiếp của comment đã cho.
    Trả về comment cha nếu tồn tại, ngược lại trả về None.
    """
    parent_id = comment.get('parent_id', '')
    clean_parent = parent_id.replace('t1_', '') if parent_id and parent_id.startswith('t1_') else None
    if clean_parent and clean_parent in all_comment_by_id:
        return all_comment_by_id[clean_parent]
    return None


def extract_with_one_parent(selected_comments, all_comment_by_id):
    """
    Với mỗi comment được chọn (GCN output), lấy thêm đúng 1 comment cha trực tiếp.
    Deduplicate để tránh trùng khi 2 comment cùng cha.
    Giữ nguyên toàn bộ selected_comments, chỉ bổ sung cha còn thiếu.
    """
    enriched = {c['id']: c for c in selected_comments}

    for c in selected_comments:
        parent = get_direct_parent(c, all_comment_by_id)
        if parent and parent['id'] not in enriched:
            enriched[parent['id']] = parent

    return list(enriched.values())


def format_comments(comments, all_comment_by_id):
    """
    Sắp xếp comments theo depth (tăng dần) rồi score (giảm dần),
    sau đó format thành chuỗi có indent và thông tin reply.
    """
    comment_by_id = {c['id']: c for c in comments}
    memo = {}
    sorted_comments = sorted(
        comments,
        key=lambda c: (get_depth(c['id'], all_comment_by_id, memo), -c.get('score', 0))
    )
    lines = []
    for c in sorted_comments:
        depth = get_depth(c['id'], all_comment_by_id, memo)
        indent = "  " * depth
        parent_id = c.get('parent_id', '')
        clean_parent = parent_id.replace('t1_', '') if parent_id and parent_id.startswith('t1_') else None

        parent_comment = comment_by_id.get(clean_parent) or all_comment_by_id.get(clean_parent)
        parent_author = parent_comment.get('author', 'User') if parent_comment else None

        if parent_author:
            lines.append(
                f"{indent}[Depth {depth}] {c.get('author', 'User')} → replying to {parent_author}: {c.get('body', '')}"
            )
        else:
            lines.append(
                f"{indent}[Depth {depth}] {c.get('author', 'User')}: {c.get('body', '')}"
            )
    return "\n".join(lines)


def run_generator():
    input_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/extracted_contexts_for_llm_gcn.json'))
    raw_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/deep_threads_with_comments.json'))
    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/gcn_summaries_gpt4o.json'))

    print("Loading extracted contexts from GCN...")
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            threads = json.load(f)
    except FileNotFoundError:
        print(f"[ERROR] Could not find {input_path}. Run inference_gcn.py first.")
        return

    print("Loading raw threads for parent lookup...")
    try:
        with open(raw_path, 'r', encoding='utf-8') as f:
            raw_threads = json.load(f)
        raw_dict = {t.get('link_id', ''): t for t in raw_threads}
    except FileNotFoundError:
        print(f"[ERROR] Could not find {raw_path}.")
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

    max_threads_to_process = min(MAX_THREADS, len(threads))
    print(f"Starting summarization for {max_threads_to_process} threads...")

    for i in tqdm(range(max_threads_to_process)):
        thread = threads[i]
        thread_id = thread.get('thread_id', 'unknown')

        if thread_id in processed_ids:
            continue

        selected_comments = thread.get('comments', [])

        raw_thread = raw_dict.get(thread_id, {})
        all_comments = raw_thread.get('comments', [])
        all_comment_by_id = {c['id']: c for c in all_comments}

        enriched_comments = extract_with_one_parent(selected_comments, all_comment_by_id)
        comments_text = format_comments(enriched_comments, all_comment_by_id)

        prompt = (
            f"Comments:\n{comments_text}\n\n"
            f"Please summarize this discussion using the following structure:\n"
            f"1. Core Topic: (1-2 sentences. Briefly describe the main subject being discussed)\n"
            f"2. Main Viewpoints: (2-3 sentences. Detail the distinct sides or perspectives of the debate)\n"
            f"3. Key Arguments & Evidence: (2-3 sentences. What specific reasoning or examples did users provide?)\n"
            f"4. Overall Conclusion: (1 sentence. Did the community reach an agreement or remain divided?)"
        )

        success = False
        retries = 0

        while not success and retries < MAX_RETRIES:
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
                wait_time = RETRY_WAIT_BASE * retries
                print(f"\nRate Limit. Retrying {retries}/{MAX_RETRIES} in {wait_time}s...")
                time.sleep(wait_time)
            except Exception as e:
                print(f"\n[ERROR] Thread {thread_id}: {e}")
                break

        if not success and retries >= MAX_RETRIES:
            print(f"\n[SKIPPED] Thread {thread_id} after {MAX_RETRIES} attempts.")

    print(f"\n[DONE] GCN Summaries saved to: {output_path}")


if __name__ == "__main__":
    run_generator()
