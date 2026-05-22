import os
import json
import torch
import torch.nn.functional as F
from torch_geometric.nn import GCNConv

class RedditGCN(torch.nn.Module):
    def __init__(self, in_channels, hidden_channels=64, out_channels=1):
        super(RedditGCN, self).__init__()
        # GCNConv không dùng heads hay concat
        self.conv1 = GCNConv(in_channels, hidden_channels)
        self.conv2 = GCNConv(hidden_channels, out_channels)

    def forward(self, x, edge_index):
        x = F.dropout(x, p=0.2, training=self.training)
        x = F.elu(self.conv1(x, edge_index))
        x = F.dropout(x, p=0.2, training=self.training)
        x = self.conv2(x, edge_index)
        return x.view(-1)

def run_inference():
    # Cập nhật đường dẫn cho GCN
    model_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/gcn_model_final.pth'))
    graph_data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/reddit_graph_dataset_inference.pt'))
    json_data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/deep_threads_with_comments.json'))
    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/extracted_contexts_for_llm_gcn.json'))

    print("Đang nạp bộ não GCN và các thông số Z-score...")
    checkpoint = torch.load(model_path, weights_only=False)
    
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model = RedditGCN(in_channels=checkpoint['in_channels']).to(device)
    model.load_state_dict(checkpoint['model_state_dict'])
    model.eval()

    y_mean = checkpoint['y_mean']
    y_std = checkpoint['y_std']

    print("Đang nạp 200 đồ thị đi thi và file dữ liệu thô...")
    graph_data = torch.load(graph_data_path, weights_only=False)
    dataset = graph_data['dataset']
    id_mapping = graph_data['id_mapping']

    with open(json_data_path, 'r', encoding='utf-8') as f:
        raw_threads = json.load(f)
    
    raw_threads_dict = {t.get('link_id', ''): t for t in raw_threads}

    final_results = []
    total_comments_before = 0
    total_comments_after = 0

    print("\nBắt đầu chấm điểm và lọc Top K...")
    
    for data in dataset:
        thread_id = data.thread_id
        comment_ids = id_mapping[thread_id]
        num_comments = data.num_comments
        
        total_comments_before += num_comments

        data = data.to(device)
        
        with torch.no_grad():
            predicted_z_scores = model(data.x, data.edge_index)
        
        # Giải chuẩn hóa và ÉP KHOẢNG [0, 1] THEO ĐỀ XUẤT
        real_scores = (predicted_z_scores * y_std) + y_mean
        real_scores = torch.clamp(real_scores, min=0.0, max=1.0)

        actual_k = min(max(5, int(num_comments * 0.3)), 50)
        actual_k = min(actual_k, num_comments) 
        
        top_scores, top_indices = torch.topk(real_scores, k=actual_k)
        top_indices = top_indices.cpu().numpy()
        
        top_comment_ids = set([comment_ids[idx] for idx in top_indices])
        
        if thread_id in raw_threads_dict:
            original_thread = raw_threads_dict[thread_id]
            
            selected_comments = [
                c for c in original_thread.get('comments', []) 
                if c['id'] in top_comment_ids
            ]
            
            total_comments_after += len(selected_comments)
            
            final_results.append({
                "thread_id": thread_id,
                "title": original_thread.get('title', ''),
                "selftext": original_thread.get('selftext', ''),
                "url": original_thread.get('url', ''),
                "num_comments_original": num_comments,
                "num_comments_retained": len(selected_comments),
                "comments": selected_comments
            })

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_results, f, ensure_ascii=False, indent=4)

    print(f"\n[BÁO CÁO KẾT QUẢ INFERENCE]")
    print(f"Tổng số Thread đã xử lý      : {len(final_results)}")
    print(f"Tổng số Comment ban đầu      : {total_comments_before}")
    print(f"Tổng số Comment được giữ lại : {total_comments_after}")
    if total_comments_before > 0:
        print(f"Tỷ lệ nén dữ liệu            : {(total_comments_after / total_comments_before) * 100:.1f}%")
    print(f"\nHOÀN TẤT! File kho báu đã sẵn sàng cho LLM tại: {output_path}")

if __name__ == "__main__":
    run_inference()