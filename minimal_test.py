import gradio as gr

def echo_function(user_input):
    """简单的回显函数，用于测试输入传递"""
    print(f"接收到输入: {repr(user_input)}")
    print(f"输入类型: {type(user_input)}")

    if user_input is None:
        return "输入为None"
    elif isinstance(user_input, str) and not user_input.strip():
        return "输入为空字符串"
    else:
        return f"回显: {user_input}"

# 创建最小化测试界面
with gr.Blocks() as demo:
    gr.Markdown("# 最小化测试")

    with gr.Row():
        with gr.Column():
            user_input = gr.Textbox(label="输入测试", lines=3)
            submit_btn = gr.Button("提交")
        with gr.Column():
            output = gr.Textbox(label="输出结果")

    submit_btn.click(
        fn=echo_function,
        inputs=[user_input],  # 使用列表格式
        outputs=[output]      # 使用列表格式
    )

if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7870, share=False)