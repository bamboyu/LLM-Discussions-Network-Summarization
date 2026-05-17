import json
import os
import torch
import random
import numpy as np
from torch_geometric.data import Data
from torch_geometric.utils import remove_self_loops, coalesce
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

def clean_body(text):
    if not text or text.strip() in ('[deleted]', '[removed]', ''):
        return '[empty]'
    return text.strip()

def set_seed(seed=42):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)

def build_dataset():
    set_seed(42)
    
    input_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/labeled_threads_for_train.json'))
    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/reddit_graph_dataset.pt'))

    print("Đang tải model ngôn ngữ (all-MiniLM-L6-v2)...")
    encoder = SentenceTransformer('all-MiniLM-L6-v2')

    with open(input_path, 'r', encoding='utf-8') as f:
        threads = json.load(f)

    print(f"\n[INFO] Tổng threads đầu vào ban đầu: {len(threads)}")

    all_texts = []
    thread_slices = []
    valid_threads = []
    current_idx = 0
    
    skipped_duplicates = 0
    skipped_no_comments = 0
    skipped_missing_labels = 0
    skipped_no_edges = 0
    seen_thread_ids = set() 

    print("Đang trích xuất text từ các threads...")
    
    for thread in threads:
        thread_id = thread.get('link_id', 'unknown')
        if thread_id in seen_thread_ids:
            skipped_duplicates += 1
            continue
        seen_thread_ids.add(thread_id)

        comments = thread.get('comments', [])
        if not comments: 
            skipped_no_comments += 1
            continue
            
        texts = [clean_body(c.get('body', '')) for c in comments]
        all_texts.extend(texts)
        thread_slices.append((current_idx, current_idx + len(texts)))
        valid_threads.append(thread)
        current_idx += len(texts)

    print(f"Đã bỏ qua {skipped_duplicates} threads trùng lặp ID.")
    print(f"Đã bỏ qua {skipped_no_comments} threads không có comments.")
    print(f"Đang mã hóa {len(all_texts)} bình luận (Batch Encoding)...")
    
    all_embeddings = encoder.encode(all_texts, convert_to_tensor=True, batch_size=64, show_progress_bar=True).cpu()

    dataset = []
    id_mapping = {} 
    
    print("\nBắt đầu xây dựng cấu trúc Đồ thị (Graph Construction)...")

    for (start, end), thread in tqdm(zip(thread_slices, valid_threads), total=len(valid_threads)):
        comments = thread['comments']
        num_nodes = len(comments)
        text_embeddings = all_embeddings[start:end]

        target_scores = [c.get('target_score') for c in comments]
        
        if any(s is None for s in target_scores):
            skipped_missing_labels += 1
            continue

        y = torch.tensor(target_scores, dtype=torch.float)

        raw_upvotes = torch.tensor([[c.get('score', 0)] for c in comments], dtype=torch.float)
        upvotes_norm = torch.sign(raw_upvotes) * torch.log1p(raw_upvotes.abs())
        
        x = torch.cat([text_embeddings, upvotes_norm], dim=1)

        # --- BẢN VÁ HOÀN HẢO: Gọt t1_ ở cả Key (id) và Value (parent_id) ---
        id_to_index = {}
        for i, c in enumerate(comments):
            raw_id = c['id']
            # Gọt t1_ của chính nó để làm khóa tìm kiếm
            clean_id = raw_id.replace('t1_', '') if raw_id.startswith('t1_') else raw_id
            id_to_index[clean_id] = i

        edges_src = []
        edges_dst = []
        comment_ids = []

        for i, c in enumerate(comments):
            # Vẫn lưu ID gốc nguyên bản (có thể có t1_) vào mapping để file Inference gọi API không bị lỗi
            comment_ids.append(c['id']) 
            
            parent_id = c.get('parent_id')
            
            # Gọt t1_ của parent_id để đi so khớp với danh sách khóa ở trên
            clean_parent_id = parent_id.replace('t1_', '') if parent_id and parent_id.startswith('t1_') else parent_id
            
            if clean_parent_id in id_to_index:
                parent_idx = id_to_index[clean_parent_id]
                child_idx = i
                
                edges_src.extend([child_idx, parent_idx])
                edges_dst.extend([parent_idx, child_idx])
        # -----------------------------------------------------------------

        if len(edges_src) == 0:
            skipped_no_edges += 1
            continue
            
        edge_index = torch.tensor([edges_src, edges_dst], dtype=torch.long)
        edge_index, _ = remove_self_loops(edge_index)
        edge_index = coalesce(edge_index, num_nodes=num_nodes)

        if edge_index.shape[1] == 0:
            skipped_no_edges += 1
            continue

        graph_data = Data(x=x, edge_index=edge_index, y=y)
        thread_id = thread.get('link_id', 'unknown')
        
        graph_data.thread_id = thread_id
        graph_data.num_comments = num_nodes
        
        id_mapping[thread_id] = comment_ids

        dataset.append(graph_data)

    print(f"\nBÁO CÁO PIPELINE")
    print(f"Tổng threads đầu vào           : {len(threads)}")
    print(f"[-] Bỏ qua (Trùng ID)          : {skipped_duplicates}")
    print(f"[-] Bỏ qua (Không comments)    : {skipped_no_comments}")
    print(f"[-] Bỏ qua (Thiếu nhãn Target) : {skipped_missing_labels}")
    print(f"[-] Bỏ qua (Đồ thị mồ côi)     : {skipped_no_edges}")
    
    total_skipped = skipped_duplicates + skipped_no_comments + skipped_missing_labels + skipped_no_edges
    print(f"[+] Hợp lệ (Thành công)        : {len(dataset)}")
    
    if len(threads) != (len(dataset) + total_skipped):
        print(f"[CẢNH BÁO] THẤT THOÁT DỮ LIỆU! Tổng kiểm tra không khớp.")
    else:
        retention_rate = (len(dataset) / len(threads)) * 100 if len(threads) > 0 else 0
        print(f"[OK] Tỷ lệ giữ lại             : {retention_rate:.1f}%\n")

    print("Đang chuẩn hóa phân phối nhãn (Z-score Normalization)...")
    
    if len(dataset) == 0:
        print("[LỖI NGHIÊM TRỌNG] Không có đồ thị nào hợp lệ để xử lý! Vui lòng kiểm tra lại dữ liệu.")
        return

    all_y = torch.cat([d.y for d in dataset])
    mean_y, std_y = all_y.mean(), all_y.std()
    
    print(f"[Sanity Check] y_mean={mean_y:.4f}, y_std={std_y:.4f}, min={all_y.min():.2f}, max={all_y.max():.2f}")

    if std_y < 1e-6:
        print("[Cảnh báo] Độ lệch chuẩn của nhãn gần bằng 0. Bỏ qua chuẩn hóa Z-score.")
    else:
        for d in dataset:
            d.y = (d.y - mean_y) / (std_y + 1e-8)

    random.seed(42)
    random.shuffle(dataset)

    torch.save({
        'dataset': dataset, 
        'y_mean': mean_y.item(), 
        'y_std': std_y.item() if std_y >= 1e-6 else 1.0,
        'id_mapping': id_mapping
    }, output_path)
    
    print(f"Hoàn tất! Đã lưu {len(dataset)} đồ thị sạch tại: {output_path}")

if __name__ == "__main__":
    build_dataset()