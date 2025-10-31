import gradio as gr

def test_function(user_input):
    """测试函数，用于验证输入传递"""
    print(f"函数接收到的输入: {repr(user_input)}")
    print(f"输入类型: {type(user_input)}")

    if user_input is None:
        return "输入为None"
    elif isinstance(user_input, str) and not user_input.strip():
        return "输入为空字符串"
    elif not user_input:
        return f"输入为假值: {repr(user_input)}"
    else:
        return f"成功接收到输入: {user_input}"

# 创建简单的测试界面
with gr.Blocks() as demo:
    gr.Markdown("# Gradio输入测试")

    with gr.Row():
        with gr.Column():
            user_input = gr.Textbox(
                label="输入测试",
                lines=3,
                placeholder="请输入一些文本"
            )
            submit_btn = gr.Button("提交", elem_id="submit-btn")

        with gr.Column():
            output = gr.Textbox(label="输出结果")

    submit_btn.click(
        fn=test_function,
        inputs=[user_input],  # 使用列表格式
        outputs=[output]      # 使用列表格式
    )

if __name__ == "__main__":
    demo.launch(
        server_name="0.0.0.0",
        server_port=7867,
        share=False
    )