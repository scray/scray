import gradio as gr

print("a")
def greet(name):
    return "Hello " + name + "!"
print("b")
demo = gr.Interface(fn=greet, inputs="text", outputs="text")
    
demo.queue(max_size=20).launch(server_name="0.0.0.0", server_port=7860)