# Reference container for self-hosting this MCP server against your own nodes.
# The hosted service at https://xmr.pub/mcp needs no container.
FROM python:3.12-alpine

WORKDIR /app
COPY mcpserver.py ./

ENV MCP_PORT=8080 \
    USAGE_DB=/data/usage.db \
    PYTHONUNBUFFERED=1

EXPOSE 8080

# All endpoints are configured with environment variables (see examples/env.example).
CMD ["python3", "mcpserver.py"]
