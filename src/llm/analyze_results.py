import json
import os

def analyze_results():
    # Load the final evaluation data
    data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/final_evaluation_results.json'))
    
    try:
        with open(data_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: Could not find {data_path}.")
        return

    num_threads = len(data)
    if num_threads == 0:
        print("Your results file is empty!")
        return

    metrics = ['Core_Extraction', 'Consistency', 'Total']
    
    # Accumulators for average calculations
    baseline_totals = {m: 0 for m in metrics}
    proposed_totals = {m: 0 for m in metrics}
    
    # Win trackers
    wins = {'Baseline': 0, 'Proposed': 0, 'Tie': 0}

    # 1. Crunch the numbers
    for thread in data:
        b_metrics = thread['baseline_metrics']
        p_metrics = thread['proposed_metrics']
        
        # Add up the scores
        for m in metrics:
            baseline_totals[m] += b_metrics.get(m, 0)
            proposed_totals[m] += p_metrics.get(m, 0)
            
        # Determine the winner for this specific thread based on Total Score
        b_total = b_metrics.get('Total', 0)
        p_total = p_metrics.get('Total', 0)
        
        if b_total > p_total:
            wins['Baseline'] += 1
        elif p_total > b_total:
            wins['Proposed'] += 1
        else:
            wins['Tie'] += 1

    # 2. Print the final report
    print(f"\n{'='*40}")
    print(f"📊 FINAL EXPERIMENT RESULTS (N={num_threads})")
    print(f"{'='*40}\n")
    
    print("🏆 WIN RATE (Head-to-Head by Total Score):")
    print(f"  Baseline Wins: {wins['Baseline']} ({(wins['Baseline'] / num_threads) * 100:.1f}%)")
    print(f"  Proposed Wins: {wins['Proposed']} ({(wins['Proposed'] / num_threads) * 100:.1f}%)")
    print(f"  Ties:          {wins['Tie']} ({(wins['Tie'] / num_threads) * 100:.1f}%)\n")

    print("📈 AVERAGE METRICS (Out of 10):")
    print(f"  {'Metric':<20} | {'Baseline':<10} | {'Proposed':<10} | {'Diff':<10}")
    print("-" * 60)
    
    for m in metrics:
        b_avg = baseline_totals[m] / num_threads
        p_avg = proposed_totals[m] / num_threads
        diff = p_avg - b_avg
        
        # Format the difference string to show + or -
        diff_str = f"+{diff:.2f}" if diff > 0 else f"{diff:.2f}"
        
        # Make the 'Total' row stand out (Out of 30)
        if m == 'Total':
            print("-" * 60)
            
        print(f"  {m:<20} | {b_avg:<10.2f} | {p_avg:<10.2f} | {diff_str:<10}")
        
    print(f"\n{'='*40}\n")

if __name__ == "__main__":
    analyze_results()