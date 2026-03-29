import os
from dotenv import load_dotenv
from openai import OpenAI

# 1. MOCK DATA (Replace this with your actual graph extraction logic later)
mock_top_nodes = [
    {
        "author": "TechGuru99",
        "score": 145,
        "body": "The real issue isn't that the new framework is slow, it's that people are using it for heavy data processing instead of lightweight UI rendering."
    },
    {
        "author": "DataNerd_x",
        "score": 89,
        "body": "Exactly. I ran a benchmark and if you offload the data processing to a Python backend and just let the framework handle the DOM, performance increases by 400%."
    },
    {
        "author": "AngryDev",
        "score": 42,
        "body": "But that defeats the whole purpose of a full-stack framework! If I have to spin up a separate Python backend, I might as well just use plain HTML."
    }
]

# 2. PROMPT
def build_summary_prompt(nodes):
    """Formats the extracted graph nodes into a clean prompt for the LLM."""
    
    context_text = ""
    for i, node in enumerate(nodes):
        context_text += f"Comment {i+1} (Author: {node['author']}, Score: {node['score']}): {node['body']}\n\n"
    
    system_instruction = """
    You are an expert AI summarizer analyzing a Reddit discussion.
    I will provide you with the most structurally important comments (Expert Nodes) extracted from the thread.
    Your job is to write a concise, 3-sentence abstractive summary of the core debate.
    Do not hallucinate outside information. Rely strictly on the provided comments.
    """
    
    return system_instruction, context_text

# 3. API CALL
def generate_summary(nodes, api_key):
    """Sends the formatted prompt to the LLM and returns the summary."""
    client = OpenAI(api_key=api_key)
    
    system_instruction, context_text = build_summary_prompt(nodes)
    
    print("Sending prompt to LLM... \n")
    
    response = client.chat.completions.create(
        model="gpt-3.5-turbo", 
        messages=[
            {"role": "system", "content": system_instruction},
            {"role": "user", "content": f"Here are the extracted network nodes:\n\n{context_text}"}
        ],
        temperature=0.3 
    )
    
    return response.choices[0].message.content

# --- RUN THE TEST ---
if __name__ == "__main__":
    # Load the environment variables from the .env file
    load_dotenv()
    
    # Securely fetch the API key
    MY_API_KEY = os.getenv("OPENAI_API_KEY")
    
    # Safety check to make sure .env file is set up correctly
    if not MY_API_KEY:
        print("ERROR: Could not find a valid OPENAI_API_KEY.")
        print("Please make sure you have a .env file set up in your project root!")
    else:
        # Run the pipeline
        final_summary = generate_summary(mock_top_nodes, MY_API_KEY)
        
        print("=== GENERATED SUMMARY ===")
        print(final_summary)