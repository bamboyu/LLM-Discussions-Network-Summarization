import json
import os

def get_unieval_avg(val):
    if isinstance(val, dict):
        values = [v for v in val.values() if isinstance(v, (int, float))]
        return sum(values) / len(values) if values else 0
    return val if isinstance(val, (int, float)) else 0

def analyze_all_results():
    base_dir = os.path.dirname(__file__)
    
    metrics_gat_base_file = os.path.abspath(os.path.join(base_dir, '../../data/processed/traditional_metrics_results.json'))
    metrics_gcn_gat_file = os.path.abspath(os.path.join(base_dir, '../../data/processed/gcn_vs_gat_metrics_results.json'))
    judge_gat_base_file = os.path.abspath(os.path.join(base_dir, '../../data/processed/evaluation_results.json'))
    judge_gcn_gat_file = os.path.abspath(os.path.join(base_dir, '../../data/processed/gcn_vs_gat_evaluation_results.json'))
    output_report_file = os.path.abspath(os.path.join(base_dir, '../../data/processed/final_report.txt'))

    metrics = {
        'Baseline': {'SummaC': 0, 'BARTScore': 0, 'UniEval': 0, 'count': 0},
        'GAT': {'SummaC': 0, 'BARTScore': 0, 'UniEval': 0, 'count': 0},
        'GCN': {'SummaC': 0, 'BARTScore': 0, 'UniEval': 0, 'count': 0}
    }

    try:
        with open(metrics_gat_base_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            for item in data:
                if 'Baseline' in item:
                    metrics['Baseline']['SummaC'] += item['Baseline'].get('SummaC', 0)
                    metrics['Baseline']['BARTScore'] += item['Baseline'].get('BARTScore', 0)
                    metrics['Baseline']['UniEval'] += get_unieval_avg(item['Baseline'].get('UniEval', 0))
                    metrics['Baseline']['count'] += 1
                
                gat_key = 'GAT' if 'GAT' in item else ('GNN' if 'GNN' in item else None)
                if gat_key:
                    metrics['GAT']['SummaC'] += item[gat_key].get('SummaC', 0)
                    metrics['GAT']['BARTScore'] += item[gat_key].get('BARTScore', 0)
                    metrics['GAT']['UniEval'] += get_unieval_avg(item[gat_key].get('UniEval', 0))
                    metrics['GAT']['count'] += 1
    except FileNotFoundError:
        pass

    try:
        with open(metrics_gcn_gat_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            for item in data:
                if 'GCN' in item:
                    metrics['GCN']['SummaC'] += item['GCN'].get('SummaC', 0)
                    metrics['GCN']['BARTScore'] += item['GCN'].get('BARTScore', 0)
                    metrics['GCN']['UniEval'] += get_unieval_avg(item['GCN'].get('UniEval', 0))
                    metrics['GCN']['count'] += 1
    except FileNotFoundError:
        pass

    avg_metrics = {m: {} for m in ['Baseline', 'GAT', 'GCN']}
    for m in ['Baseline', 'GAT', 'GCN']:
        c = metrics[m]['count']
        for k in ['SummaC', 'BARTScore', 'UniEval']:
            avg_metrics[m][k] = metrics[m][k] / c if c > 0 else 0

    match1 = {'GAT': 0, 'Baseline': 0, 'Tie': 0, 'total': 0}
    try:
        with open(judge_gat_base_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            for item in data:
                w = item.get('Winner')
                if w == 'GNN' or w == 'GAT': match1['GAT'] += 1
                elif w == 'Baseline': match1['Baseline'] += 1
                elif w == 'Tie': match1['Tie'] += 1
                match1['total'] += 1
    except FileNotFoundError:
        pass

    match2 = {'GCN': 0, 'GAT': 0, 'Tie': 0, 'total': 0}
    try:
        with open(judge_gcn_gat_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            for item in data:
                w = item.get('Winner')
                if w in match2: match2[w] += 1
                match2['total'] += 1
    except FileNotFoundError:
        pass

    # SOẠN NỘI DUNG BÁO CÁO VÀO BIẾN
    lines = []
    lines.append("\n")
    lines.append("COMPREHENSIVE EXPERIMENTAL REPORT: BASELINE vs GAT vs GCN".center(80))
    lines.append("\n\n")
    
    lines.append("[1] QUANTITATIVE EVALUATION (TRADITIONAL METRICS)")
    lines.append("\n")
    lines.append(f"{'Evaluation Metrics':<28} | {'Baseline':<12} | {'GCN':<12} | {'GAT':<12}")
    lines.append("\n")
    
    def format_score(model, key):
        return f"{avg_metrics[model][key]:.4f}" if metrics[model]['count'] > 0 else "N/A"

    lines.append(f"{'SummaC (Consistency)':<28} | {format_score('Baseline', 'SummaC'):<12} | {format_score('GCN', 'SummaC'):<12} | {format_score('GAT', 'SummaC'):<12}")
    lines.append(f"{'BARTScore (Fluency)':<28} | {format_score('Baseline', 'BARTScore'):<12} | {format_score('GCN', 'BARTScore'):<12} | {format_score('GAT', 'BARTScore'):<12}")
    lines.append(f"{'UniEval (Comprehensive)':<28} | {format_score('Baseline', 'UniEval'):<12} | {format_score('GCN', 'UniEval'):<12} | {format_score('GAT', 'UniEval'):<12}")
    lines.append("\n")
    lines.append(f"{'(Evaluated Samples)':<28} | {metrics['Baseline']['count']:<12} | {metrics['GCN']['count']:<12} | {metrics['GAT']['count']:<12}")
    
    lines.append("\n\n")
    lines.append("[2] QUALITATIVE EVALUATION (LLM-AS-A-JUDGE)")
    lines.append("\n")
    
    if match1['total'] > 0:
        lines.append(f"Match 1: GAT vs Baseline (Total: {match1['total']} threads)")
        lines.append(f"   GAT Wins      : {match1['GAT']:>2} threads ({match1['GAT']/match1['total']*100:.1f}%)")
        lines.append(f"   Baseline Wins : {match1['Baseline']:>2} threads ({match1['Baseline']/match1['total']*100:.1f}%)")
        lines.append(f"   Ties          : {match1['Tie']:>2} threads ({match1['Tie']/match1['total']*100:.1f}%)")
    else:
        lines.append("Match 1: GAT vs Baseline -> No data available")

    lines.append("\n")
    
    if match2['total'] > 0:
        lines.append(f"Match 2: GCN vs GAT (Total: {match2['total']} threads)")
        lines.append(f"   GCN Wins      : {match2['GCN']:>2} threads ({match2['GCN']/match2['total']*100:.1f}%)")
        lines.append(f"   GAT Wins      : {match2['GAT']:>2} threads ({match2['GAT']/match2['total']*100:.1f}%)")
        lines.append(f"   Ties          : {match2['Tie']:>2} threads ({match2['Tie']/match2['total']*100:.1f}%)")
    else:
        lines.append("Match 2: GCN vs GAT -> No data available")
        
    lines.append("\n")

    report_content = "".join(lines)
    
    # IN RA TERMINAL
    print(report_content)
    
    # LƯU VÀO FILE
    try:
        with open(output_report_file, 'w', encoding='utf-8') as f:
            f.write(report_content)
        print(f"       >>> Báo cáo đã được lưu vào file: {output_report_file} <<<")
        print("\n")
    except Exception as e:
        print(f"Lỗi khi lưu file: {e}")

if __name__ == "__main__":
    analyze_all_results()