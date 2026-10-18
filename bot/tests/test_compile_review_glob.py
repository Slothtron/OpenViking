"""Final review gets glob without target listing or content reads."""

from types import SimpleNamespace as NS

import pytest
from vikingbot.agent.tools.base import ToolContext
from vikingbot.agent.tools.ov_file import VikingGlobTool, VikingListTool, VikingMultiReadTool
from vikingbot.agent.tools.registry import ToolRegistry
from vikingbot.compile.ops.finalize import FinalReview
from vikingbot.compile.pipeline_agent import agent_runner


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["final_review_agent", "map_agent"])
async def test_only_final_review_gets_glob(stage):
    """Only review inherits the existing glob tool and the task's authenticated connection."""
    tools = ToolRegistry()
    glob = VikingGlobTool()
    for tool in (glob, VikingListTool(), VikingMultiReadTool()):
        tools.register(tool)
    connection = {"account_id": "default", "user_id": "review-user"}

    async def child_loop(**kwargs):
        registry = kwargs["tool_registry"]
        assert registry.get("openviking_glob") is (glob if stage == "final_review_agent" else None)
        assert registry.get("openviking_list") is registry.get("openviking_multi_read") is None
        assert kwargs["openviking_connection"] == connection
        await registry.get("emit").execute(ToolContext(), excluded={})

    loop = NS(
        tools=tools,
        _run_agent_loop=child_loop,
        config=NS(sandbox=NS(backend="direct", backends=NS(direct=NS(allow_compile_exec=False)))),
    )
    model = NS(files=NS(sandbox=None), resources=None, fits=lambda *args: True, metrics={})
    await agent_runner(
        loop, "session", connection, NS(final_review_iterations=2, subagent_iterations=2)
    )("Review", {"artifact_directory": "review-test"}, FinalReview, None, model, stage=stage)
