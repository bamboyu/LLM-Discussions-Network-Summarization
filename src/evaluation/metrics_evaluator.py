import json
import os
import torch
import gc
import sys

# ÉP TẢI MODEL SANG Ổ D CHO NHẸ Ổ C
os.environ["HF_HOME"] = "D:/HuggingFaceCache"

def clean_vram():
    try:
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
            gc.collect()
    except Exception as e:
        print(f"⚠️ [Cảnh báo] Không thể dọn VRAM bằng lệnh chuẩn: {e}")

def load_data():
    raw_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/deep_threads_with_comments.json'))
    gnn_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/final_summaries_gpt4o.json'))
    baseline_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/baseline_summaries.json'))
    output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/traditional_metrics_results.json'))

    with open(raw_path, 'r', encoding='utf-8') as f:
        raw_threads = {t.get('link_id', ''): t for t in json.load(f)}
    with open(gnn_path, 'r', encoding='utf-8') as f:
        gnn_data = {item['thread_id']: item['summary'] for item in json.load(f)}
    with open(baseline_path, 'r', encoding='utf-8') as f:
        baseline_data = {item['thread_id']: item['summary'] for item in json.load(f)}
        
    return raw_threads, gnn_data, baseline_data, output_path

def run_metrics_evaluator():
    print("Đang nạp dữ liệu Source và Targets...")
    try:
        raw_threads, gnn_data, baseline_data, output_path = load_data()
    except FileNotFoundError as e:
        print(f"[LỖI] Không tìm thấy file: {e}")
        return

    common_ids = list(set(gnn_data.keys()).intersection(set(baseline_data.keys())))
    
    # Nạp dữ liệu cũ để chấm tiếp sức (Không lo bị mất điểm đã chấm)
    results_dict = {}
    if os.path.exists(output_path):
        try:
            with open(output_path, 'r', encoding='utf-8') as f:
                old_data = json.load(f)
                for item in old_data:
                    results_dict[item['thread_id']] = item
        except json.JSONDecodeError:
            pass

    # Khởi tạo sườn JSON cho các Thread mới
    for tid in common_ids:
        if tid not in results_dict:
            results_dict[tid] = {"thread_id": tid, "GNN": {}, "Baseline": {}}

    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Bắt đầu chấm điểm trên thiết bị: {device}")

    # Xử lý text đầu vào trước để tiết kiệm thời gian
    source_texts = {}
    for tid in common_ids:
        raw_thread = raw_threads.get(tid)
        if raw_thread:
            comments = [c.get('body', '') for c in raw_thread.get('comments', [])]
            source_texts[tid] = "\n".join([f"- User: {c}" for c in comments])

    # GIAI ĐOẠN 1: CHẤM SUMMAC
    print("\nKhởi động SummaC...")
    try:
        from summac.model_summac import SummaCZS
        summac_model = SummaCZS(granularity="sentence", model_name="vitc", device=device)
        for tid in common_ids:
            if "SummaC" not in results_dict[tid]["GNN"] and tid in source_texts:
                try:
                    print(f" 🔄 [SummaC] Đang chấm Thread: {tid}")
                    results_dict[tid]["GNN"]["SummaC"] = summac_model.score([source_texts[tid]], [gnn_data[tid]])["scores"][0]
                    results_dict[tid]["Baseline"]["SummaC"] = summac_model.score([source_texts[tid]], [baseline_data[tid]])["scores"][0]
                    with open(output_path, 'w', encoding='utf-8') as f:
                        json.dump(list(results_dict.values()), f, ensure_ascii=False, indent=4)
                except Exception as e:
                    print(f"❌ [LỖI SummaC] Thread {tid} bị bỏ qua: {e}")
                    clean_vram()
                    continue
        del summac_model
        clean_vram()
        print("✅ Hoàn tất SummaC. Đã giải phóng RAM.")
    except ImportError:
        print("Bỏ qua SummaC do chưa cài đặt.")

    # GIAI ĐOẠN 2: CHẤM BARTSCORE
    print("\nKhởi động BARTScore...")
    try:
        from bart_score import BARTScorer
        bart_scorer = BARTScorer(device=device, checkpoint='facebook/bart-large-cnn')
        for tid in common_ids:
            if "BARTScore" not in results_dict[tid]["GNN"] and tid in source_texts:
                try:
                    print(f" 🔄 [BARTScore] Đang chấm Thread: {tid}")
                    results_dict[tid]["GNN"]["BARTScore"] = bart_scorer.score([source_texts[tid]], [gnn_data[tid]])[0]
                    results_dict[tid]["Baseline"]["BARTScore"] = bart_scorer.score([source_texts[tid]], [baseline_data[tid]])[0]
                    with open(output_path, 'w', encoding='utf-8') as f:
                        json.dump(list(results_dict.values()), f, ensure_ascii=False, indent=4)
                except Exception as e:
                    print(f"❌ [LỖI BARTScore] Thread {tid} bị bỏ qua: {e}")
                    clean_vram()
                    continue
        del bart_scorer
        clean_vram()
        print("✅ Hoàn tất BARTScore. Đã giải phóng RAM.")
    except ImportError:
        print("Bỏ qua BARTScore do chưa cài đặt.")

    # GIAI ĐOẠN 3: CHẤM UNIEVAL
    print("\nKhởi động UniEval...")
    unieval_path = os.path.join(os.path.dirname(__file__), 'unieval')
    if unieval_path not in sys.path:
        sys.path.append(unieval_path)
    
    try:
        from metric.evaluator import get_evaluator
        unieval_scorer = get_evaluator('summarization')
        for tid in common_ids:
            if "UniEval" not in results_dict[tid]["GNN"] and tid in source_texts:
                try:
                    print(f" 🔄 [UniEval] Đang chấm Thread: {tid}")
                    gnn_unieval_data = [{"source": source_texts[tid], "system_output": gnn_data[tid], "reference": ""}]
                    baseline_unieval_data = [{"source": source_texts[tid], "system_output": baseline_data[tid], "reference": ""}]
                    
                    results_dict[tid]["GNN"]["UniEval"] = unieval_scorer.evaluate(gnn_unieval_data)[0]
                    results_dict[tid]["Baseline"]["UniEval"] = unieval_scorer.evaluate(baseline_unieval_data)[0]
                    with open(output_path, 'w', encoding='utf-8') as f:
                        json.dump(list(results_dict.values()), f, ensure_ascii=False, indent=4)
                except Exception as e:
                    print(f"❌ [LỖI UniEval] Thread {tid} bị bỏ qua: {e}")
                    clean_vram()
                    continue
        del unieval_scorer
        clean_vram()
        print("✅ Hoàn tất UniEval. Đã giải phóng RAM.")
    except ImportError as e:
        print(f"Bỏ qua UniEval. Lỗi: {e}")

    print(f"\n🎉 KẾT QUẢ ĐÃ LƯU TẠI: {output_path}")

if __name__ == "__main__":
    run_metrics_evaluator()