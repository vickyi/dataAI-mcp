# dataAI-mcp 项目

这是一个基于 Model Context Protocol (MCP) 的轻量级 AI 服务，用于处理数据相关的任务，特别是 SQL 查询的解释和生成。

## 项目结构

```
dataAI-mcp/
├── src/                 # 源代码目录
│   ├── core/           # 核心模块
│   │   ├── __init__.py
│   │   ├── config.py              # 配置管理
│   │   ├── server.py              # MCP 服务器主入口
│   │   └── sql_assistant_agent.py # SQL 助手智能体
│   ├── web/            # Web 界面模块
│   │   ├── __init__.py
│   │   ├── web_interface.py       # 主要 Web 界面
│   │   └── web_interface_simple.py# 简单 Web 界面
│   ├── rules/          # 规则配置模块
│   │   ├── __init__.py
│   │   ├── sql_rules.toml         # SQL 规则配置
│   │   ├── example_custom_rules.toml # 自定义规则示例
│   │   └── test_rules.toml        # 测试规则
│   ├── utils/          # 工具模块
│   │   ├── __init__.py
│   │   └── utils.py               # 通用工具函数
│   └── __init__.py
├── tests/              # 测试目录
│   ├── __init__.py
│   ├── minimal_test.py
│   ├── test_server.py
│   ├── test_sql_assistant_agent.py
│   ├── test_config_loading.py
│   ├── test_gradio_basic.py
│   ├── test_gradio_input.py
│   ├── test_mcp_functionality.py
│   ├── test_advanced_rules.py
│   ├── test_components.py
│   ├── test_config_rules.py
│   ├── test_ddl_handling.py
│   ├── test_ddl_statements.py
│   ├── test_hive_ddl_rules.py
│   ├── test_join_rule.py
│   └── test_keyword_case.py
├── debug/              # 调试工具目录
│   ├── debug_join_parsing.py
│   └── debug_sql_parsing.py
├── docs/               # 文档目录
├── requirements.txt    # Python 依赖
├── .env.example        # 环境变量示例
├── env-config.txt      # 环境配置
└── start_mcp_server.sh # 启动脚本
```

## 安装依赖

```bash
pip install -r requirements.txt
```

## 运行服务

```bash
# 启动 MCP 服务器 (Linux/Mac)
./start_mcp_server.sh

# 启动 MCP 服务器 (Windows)
cd "c:\Users\admin\Documents\Develop\dataAI-mcp" && python -m src.core.server

# 或者直接运行
python -m src.core.server
```

## 运行 Web 界面

```bash
# 运行 Web 界面 (Linux/Mac/Windows)
python -m src.web.web_interface
```

## 运行测试

```bash
python -m tests.test_server
python -m tests.test_sql_assistant_agent
```