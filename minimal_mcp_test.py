from mcp.server import FastMCP

app = FastMCP("test-server")

@app.tool()
async def hello_world(name: str) -> str:
    """Say hello to someone"""
    return f"Hello, {name}!"

if __name__ == "__main__":
    app.run()