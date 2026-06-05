import json
import os
import torch
import gc

# ÉP TẢI MODEL SANG Ổ D CHO NHẸ Ổ C
os.environ["HF_HOME"] = "D:/HuggingFaceCache"


def clean_vram():
    try:
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
            gc.collect()
    except Exception as e:
        print(f"[Cảnh báo] Không thể dọn VRAM: {e}")


def load_data():
    raw_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/deep_threads_with_comments.json'))
    gat_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/final_summaries_gpt4o.json'))
    gcn_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/gcn_summaries_gpt4o.json'))
    baseline_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/baseline_summaries.json'))
    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/summac_results.json'))

    with open(raw_path, 'r', encoding='utf-8') as f:
        raw_threads = {t.get('link_id', ''): t for t in json.load(f)}
    with open(gat_path, 'r', encoding='utf-8') as f:
        gat_data = {item['thread_id']: item['summary'] for item in json.load(f)}
    with open(gcn_path, 'r', encoding='utf-8') as f:
        gcn_data = {item['thread_id']: item['summary'] for item in json.load(f)}
    with open(baseline_path, 'r', encoding='utf-8') as f:
        baseline_data = {item['thread_id']: item['summary'] for item in json.load(f)}

    return raw_threads, gat_data, gcn_data, baseline_data, output_path


def run_metrics_evaluator():
    print("Đang nạp dữ liệu...")
    try:
        raw_threads, gat_data, gcn_data, baseline_data, output_path = load_data()
    except FileNotFoundError as e:
        print(f"[LỖI] Không tìm thấy file: {e}")
        return

    common_ids = list(
        set(gat_data.keys())
        .intersection(set(gcn_data.keys()))
        .intersection(set(baseline_data.keys()))
    )
    print(f"Tìm thấy {len(common_ids)} thread chung giữa GAT, GCN và Baseline.")

    # Nạp dữ liệu cũ để chấm tiếp sức
    results_dict = {}
    if os.path.exists(output_path):
        try:
            with open(output_path, 'r', encoding='utf-8') as f:
                old_data = json.load(f)
                for item in old_data:
                    results_dict[item['thread_id']] = item
        except json.JSONDecodeError:
            pass

    # Khởi tạo sườn JSON cho các thread mới
    for tid in common_ids:
        if tid not in results_dict:
            results_dict[tid] = {"thread_id": tid, "GAT": {}, "GCN": {}, "Baseline": {}}

    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Bắt đầu chấm điểm trên thiết bị: {device}")

    # Chuẩn bị source text từ raw comments thô
    source_texts = {}
    for tid in common_ids:
        raw_thread = raw_threads.get(tid)
        if raw_thread:
            comments = [c.get('body', '') for c in raw_thread.get('comments', [])]
            source_texts[tid] = "\n".join([f"- User: {c}" for c in comments])

    # CHẤM SUMMAC
    print("\nKhởi động SummaC...")
    try:
        from summac.model_summac import SummaCZS
        summac_model = SummaCZS(granularity="sentence", model_name="vitc", device=device)

        for tid in common_ids:
            # Check cả 3 để tránh bỏ sót khi crash giữa chừng
            already_done = all(
                "SummaC" in results_dict[tid][k]
                for k in ["GAT", "GCN", "Baseline"]
            )
            if already_done or tid not in source_texts:
                continue
            try:
                print(f"  [SummaC] Đang chấm Thread: {tid}")
                src = [source_texts[tid]]
                results_dict[tid]["GAT"]["SummaC"] = summac_model.score(src, [gat_data[tid]])["scores"][0]
                results_dict[tid]["GCN"]["SummaC"] = summac_model.score(src, [gcn_data[tid]])["scores"][0]
                results_dict[tid]["Baseline"]["SummaC"] = summac_model.score(src, [baseline_data[tid]])["scores"][0]
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(list(results_dict.values()), f, ensure_ascii=False, indent=4)
            except Exception as e:
                print(f"[LỖI SummaC] Thread {tid} bị bỏ qua: {e}")
                clean_vram()
                continue

        del summac_model
        clean_vram()
        print("Hoàn tất SummaC. Đã giải phóng RAM.")

    except ImportError:
        print("Bỏ qua SummaC do chưa cài đặt.")
        return

    # IN KẾT QUẢ TỔNG HỢP
    gat_scores = [results_dict[tid]["GAT"]["SummaC"] for tid in common_ids if "SummaC" in results_dict[tid]["GAT"]]
    gcn_scores = [results_dict[tid]["GCN"]["SummaC"] for tid in common_ids if "SummaC" in results_dict[tid]["GCN"]]
    baseline_scores = [results_dict[tid]["Baseline"]["SummaC"] for tid in common_ids if "SummaC" in results_dict[tid]["Baseline"]]

    print("\nKẾT QUẢ SUMMAC TRUNG BÌNH:")
    print(f"  GAT      : {sum(gat_scores) / len(gat_scores):.4f}" if gat_scores else "  GAT      : chưa có dữ liệu")
    print(f"  GCN      : {sum(gcn_scores) / len(gcn_scores):.4f}" if gcn_scores else "  GCN      : chưa có dữ liệu")
    print(f"  Baseline : {sum(baseline_scores) / len(baseline_scores):.4f}" if baseline_scores else "  Baseline : chưa có dữ liệu")

    print(f"\nKẾT QUẢ ĐÃ LƯU TẠI: {output_path}")


if __name__ == "__main__":
    run_metrics_evaluator()
