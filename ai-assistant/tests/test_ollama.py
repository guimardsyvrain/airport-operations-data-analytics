import ollama

response = ollama.chat(
    model="llama3.2:3b",
    messages=[
        {
            "role": "user",
            "content": "What is on-time performance in airport operations?"
        }
    ]
)

print(response["message"]["content"])