import json
import os

def analyze_all_results():
    base_dir = os.path.dirname(__file__)
    
    # 1. Khai báo đúng 2 file input và 1 file output
    judge_gat_base_file = os.path.abspath(os.path.join(base_dir, '../../data/processed/llm_eval_gat_vs_baseline.json'))
    judge_gat_gcn_file = os.path.abspath(os.path.join(base_dir, '../../data/processed/llm_eval_gat_vs_gcn.json'))
    output_report_file = os.path.abspath(os.path.join(base_dir, '../../data/processed/final_report.txt'))

    # 2. Khởi tạo bộ đếm
    match1 = {'GAT': 0, 'Baseline': 0, 'Tie': 0, 'total': 0}
    match2 = {'GAT': 0, 'GCN': 0, 'Tie': 0, 'total': 0}

    # 3. Đọc dữ liệu Match 1: GAT vs Baseline
    try:
        with open(judge_gat_base_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            for item in data:
                w = item.get('Winner')
                if w in match1:
                    match1[w] += 1
                match1['total'] += 1
    except FileNotFoundError:
        pass

    # 4. Đọc dữ liệu Match 2: GAT vs GCN
    try:
        with open(judge_gat_gcn_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            for item in data:
                w = item.get('Winner')
                if w in match2:
                    match2[w] += 1
                match2['total'] += 1
    except FileNotFoundError:
        pass

    # 5. Soạn format Báo cáo
    lines = []
    lines.append("\n" + "="*80 + "\n")
    lines.append("LLM-AS-A-JUDGE EXPERIMENTAL REPORT: BASELINE vs GAT vs GCN".center(80))
    lines.append("\n" + "="*80 + "\n\n")
    
    # Kết quả Match 1
    if match1['total'] > 0:
        lines.append(f"[Match 1] GAT vs Baseline (Total: {match1['total']} threads)\n")
        lines.append(f"   - GAT Wins      : {match1['GAT']:>2} threads ({match1['GAT']/match1['total']*100:.1f}%)\n")
        lines.append(f"   - Baseline Wins : {match1['Baseline']:>2} threads ({match1['Baseline']/match1['total']*100:.1f}%)\n")
        lines.append(f"   - Ties          : {match1['Tie']:>2} threads ({match1['Tie']/match1['total']*100:.1f}%)\n")
    else:
        lines.append("[Match 1] GAT vs Baseline -> No data available\n")

    lines.append("\n" + "-"*80 + "\n\n")
    
    # Kết quả Match 2
    if match2['total'] > 0:
        lines.append(f"[Match 2] GAT vs GCN (Total: {match2['total']} threads)\n")
        lines.append(f"   - GAT Wins      : {match2['GAT']:>2} threads ({match2['GAT']/match2['total']*100:.1f}%)\n")
        lines.append(f"   - GCN Wins      : {match2['GCN']:>2} threads ({match2['GCN']/match2['total']*100:.1f}%)\n")
        lines.append(f"   - Ties          : {match2['Tie']:>2} threads ({match2['Tie']/match2['total']*100:.1f}%)\n")
    else:
        lines.append("[Match 2] GAT vs GCN -> No data available\n")
        
    lines.append("\n" + "="*80 + "\n")

    report_content = "".join(lines)
    
    # IN RA TERMINAL
    print(report_content)
    
    # LƯU VÀO FILE TXT
    try:
        with open(output_report_file, 'w', encoding='utf-8') as f:
            f.write(report_content)
        print(f">>> Báo cáo đã được lưu vào file: {output_report_file} <<< \n")
    except Exception as e:
        print(f"Lỗi khi lưu file: {e}")

if __name__ == "__main__":
    analyze_all_results()
