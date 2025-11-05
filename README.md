# dataAI-mcp 项目

这是一个基于 Model Context Protocol (MCP) 的轻量级 AI 服务，专门用于处理 Hive SQL 相关的数据任务。项目主要面向业务研发和分析人员，帮助他们快速、准确地生成符合规范的 Hive SQL 查询。

## 项目主要作用和功能

### 核心价值
- **智能 SQL 生成**：将自然语言描述的业务需求自动转换为规范的 Hive SQL 代码
- **SQL 规范检查**：自动检查生成的 SQL 是否符合大数据开发规范，如禁止 SELECT *、必须包含分区字段等
- **SQL 优化建议**：根据检查结果自动优化 SQL 代码，提高查询性能和规范性
- **降低数据门槛**：让业务人员无需深入了解 SQL 语法即可进行数据分析

### 主要功能
1. **自然语言转 SQL**：用户只需用自然语言描述需求，系统自动生成相应的 Hive SQL
2. **SQL 规范检查**：
   - 禁止使用 SELECT *
   - 必须指定分区字段 dt 的过滤条件
   - 表必须使用别名
   - 字段别名使用下划线命名法
   - 敏感字段访问权限检查
3. **SQL 优化**：根据规范检查结果自动优化 SQL 代码
4. **Web 界面交互**：提供友好的 Web 界面，方便用户输入需求和查看结果
5. **可扩展规则配置**：通过 TOML 文件配置 SQL 检查规则，支持自定义规则

### 适用场景
- 业务数据分析需求快速转换为 SQL 查询
- 数据开发人员代码规范检查和优化
- 新手数据分析师学习和实践 SQL 编写
- 团队 SQL 编写规范统一和质量提升

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