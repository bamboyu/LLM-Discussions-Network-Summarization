import sys
from collections import defaultdict

# Safeguard for extreme Reddit threads
sys.setrecursionlimit(5000) 

def format_thread_with_graph_features(comments_list):
    children_map = defaultdict(list)
    comment_dict = {}
    author_counts = defaultdict(int)
    
    for c in comments_list:
        author = c.get('author', 'Unknown')
        author_counts[author] += 1
        c['in_thread_count'] = author_counts[author] 
        comment_dict[c['id']] = c
        children_map[c['parent_id']].append(c)
        
    roots = [c for c in comments_list if c['parent_id'] not in comment_dict]
    prompt_lines = ["--- START OF ENHANCED THREAD ---"]
    
    def traverse(comment_node, depth):
        # REMOVED THE MASSIVE SPACE INDENTATION
        author = comment_node.get('author', 'Unknown')
        body = comment_node.get('body', '').strip().replace('\n', ' ')
        score = comment_node.get('score', 0)
        in_thread_count = comment_node.get('in_thread_count', 1)
        degree = len(children_map.get(comment_node['id'], []))
        
        # ADDED DEPTH AS A NUMBER TO SAVE TOKENS
        stats_block = f"[Depth: {depth} | Score: {score} | Replies: {degree} | Author's {in_thread_count}th comment]"
        parent_id = comment_node.get('parent_id')
        
        if parent_id in comment_dict:
            parent_author = comment_dict[parent_id].get('author', 'Unknown')
            relation_tag = f"↳ User '{author}' replying to '{parent_author}' {stats_block}:"
        else:
            relation_tag = f"■ User '{author}' replying to Main Post {stats_block}:"
            
        # Append without the visual indent
        prompt_lines.append(f"{relation_tag} {body}")
        
        for child in children_map.get(comment_node['id'], []):
            traverse(child, depth + 1)

    for root in roots:
        traverse(root, 0)
        
    prompt_lines.append("--- END OF ENHANCED THREAD ---")
    return "\n".join(prompt_lines)

def get_flat_text(comments_list):
    lines = [f"{c.get('author', 'Unknown')}: {c.get('body', '').strip().replace('\n', ' ')}" for c in comments_list]
    return "\n".join(lines)