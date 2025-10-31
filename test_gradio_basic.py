import gradio as gr

def simple_echo(text):
    print(f"函数接收到: {repr(text)}")
    if text is None:
        return "输入为None"
    return f"你输入了: {text}"

with gr.Blocks() as demo:
    with gr.Row():
        with gr.Column():
            textbox = gr.Textbox(label="输入")
            button = gr.Button("提交")
        with gr.Column():
            output = gr.Textbox(label="输出")

    button.click(
        fn=simple_echo,
        inputs=[textbox],
        outputs=[output]
    )

if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7871)