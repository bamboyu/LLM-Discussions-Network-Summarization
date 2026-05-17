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

def build_inference_dataset():
    input_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/deep_threads_with_comments.json'))
    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/reddit_graph_dataset_inference.pt'))

    print("Đang nạp mô hình ngôn ngữ (all-MiniLM-L6-v2) để mã hóa text...")
    encoder = SentenceTransformer('all-MiniLM-L6-v2')

    if not os.path.exists(input_path):
        print(f"[LỖI] Không tìm thấy file: {input_path}")
        return

    with open(input_path, 'r', encoding='utf-8') as f:
        threads = json.load(f)

    print(f"\n[INFO] Bắt đầu xử lý {len(threads)} threads thô để chuẩn bị Inference...")

    all_texts = []
    thread_slices = []
    valid_threads = []
    current_idx = 0
    seen_thread_ids = set() 

    for thread in threads:
        thread_id = thread.get('link_id', 'unknown')
        if thread_id in seen_thread_ids:
            continue
        seen_thread_ids.add(thread_id)

        comments = thread.get('comments', [])
        if not comments: 
            continue
            
        texts = [clean_body(c.get('body', '')) for c in comments]
        all_texts.extend(texts)
        thread_slices.append((current_idx, current_idx + len(texts)))
        valid_threads.append(thread)
        current_idx += len(texts)

    print(f"Đang mã hóa {len(all_texts)} bình luận thành vector (vui lòng đợi)...")
    all_embeddings = encoder.encode(all_texts, convert_to_tensor=True, batch_size=64, show_progress_bar=True).cpu()

    dataset = []
    id_mapping = {} 

    print("\nBước 2: Xây dựng cấu trúc Đồ thị (Gán nhãn dummy 0.0)...")

    for (start, end), thread in tqdm(zip(thread_slices, valid_threads), total=len(valid_threads)):
        comments = thread['comments']
        num_nodes = len(comments)
        text_embeddings = all_embeddings[start:end]

        # GÁN NHÃN DUMMY 0.0 ĐỂ LÀM BƯỚC ĐỆM CHO INFERENCE
        y = torch.zeros(num_nodes, dtype=torch.float)

        raw_upvotes = torch.tensor([[c.get('score', 0)] for c in comments], dtype=torch.float)
        upvotes_norm = torch.sign(raw_upvotes) * torch.log1p(raw_upvotes.abs())
        
        x = torch.cat([text_embeddings, upvotes_norm], dim=1)

        # BẢN VÁ GỌT TIỀN TỐ ID ĐỂ SO KHỚP CHUẨN XÁC
        id_to_index = {}
        for i, c in enumerate(comments):
            raw_id = c['id']
            clean_id = raw_id.replace('t1_', '') if raw_id.startswith('t1_') else raw_id
            id_to_index[clean_id] = i

        edges_src = []
        edges_dst = []
        comment_ids = []

        for i, c in enumerate(comments):
            comment_ids.append(c['id']) 
            parent_id = c.get('parent_id')
            clean_parent_id = parent_id.replace('t1_', '') if parent_id and parent_id.startswith('t1_') else parent_id
            
            if clean_parent_id in id_to_index:
                parent_idx = id_to_index[clean_parent_id]
                child_idx = i
                
                edges_src.extend([child_idx, parent_idx])
                edges_dst.extend([parent_idx, child_idx])

        if len(edges_src) == 0:
            continue
            
        edge_index = torch.tensor([edges_src, edges_dst], dtype=torch.long)
        edge_index, _ = remove_self_loops(edge_index)
        edge_index = coalesce(edge_index, num_nodes=num_nodes)

        # CHỐT CHẶN BẢO VỆ MỚI THÊM VÀO THEO ĐỀ XUẤT CỦA BẠN
        if edge_index.shape[1] == 0:
            continue

        graph_data = Data(x=x, edge_index=edge_index, y=y)
        thread_id = thread.get('link_id', 'unknown')
        
        graph_data.thread_id = thread_id
        graph_data.num_comments = num_nodes
        
        id_mapping[thread_id] = comment_ids
        dataset.append(graph_data)

    torch.save({
        'dataset': dataset, 
        'id_mapping': id_mapping
    }, output_path)
    
    print(f"\n[THÀNH CÔNG] Đã tạo xong {len(dataset)} đồ thị từ 200 bài đăng.")
    print(f"File lưu tại: {output_path}")

if __name__ == "__main__":
    build_inference_dataset()