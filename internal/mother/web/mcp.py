from typing import Literal, Optional, List, Annotated
import sys
import os
import asyncio
import json
import subprocess
from pathlib import Path

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.middleware import Middleware
from pydantic import BaseModel, Field
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, FileResponse
from ripgrepy import Ripgrepy

from app.tool.bash import execute_command
from app.prompt import (
    BASH_TOOL_DESC, GREP_TOOL_DESC, READ_TOOL_DESC, EDIT_TOOL_DESC,
    MULTI_EDIT_TOOL_DESC, WRITE_TOOL_DESC, TODO_WRITE_TOOL_DESC,
)
from app.tool.browser import take_screenshot
from app.tool.edit import EditTool
from app.util.log import get_logger
from app.util.oss_gateway import list_objects
from app.util.skill import (
    extract_official_skills,
    scan_new_skill_impl, get_personal_skills_impl,
    skill_info_impl, skill_get_impl,
    upload_skill_impl, start_skill_watcher_impl, restore_user_skills_initial,
)
from app.util.security import (
    NON_TEXT_SUFFIXES,
    is_caddy_execution, is_symlink_creation,
    check_write_path_in_home, check_path_permission,
)
from app.util.prestop import archive, heartbeat, pre_stop_impl
from app.util.git import (
    ensure_main_branch, get_git_log, git_checkout_by_message, git_commit,
    has_pending_git_op, clean_stale_git_locks, _abort_pending_git_op,
)
from app.util.pfs import (
    copy_project_to_tmp_impl, files_diff_impl, save_project_version_impl,
    restore_files_impl,
)
from app.util.deploy import deploy_impl, git_blob_response

HOME_DIR = "/Users/jinwu/Documents/workspace-agent" if sys.platform == "darwin" else "/home/z"
PROJECT_DIR = f"{HOME_DIR}/my-project"
AGENT_DIR = "/app"

edit_tool: EditTool = EditTool()
mcp = FastMCP(stateless_http=True)


class GitMainBranchMiddleware(Middleware):
    async def on_call_tool(self, context, call_next):
        # Proactively clean stale .git/*.lock files: a SIGKILL'd git process
        # leaves them behind and they otherwise block every tool call.
        clean_stale_git_locks(PROJECT_DIR, log=get_logger())
        # Skip ensure_main_branch when the repo is mid-rebase/merge/cherry-pick:
        # switching to main fails in that state, and silently aborting here
        # would kill any LLM-driven git workflow. Pre-stop and startup self-heal
        # cover the case where the LLM never resolves it.
        if not has_pending_git_op(PROJECT_DIR):
            try:
                await ensure_main_branch(PROJECT_DIR, log=get_logger())
            except Exception as exc:
                # Never let a git failure propagate to unrelated tool calls
                # (Read/Edit/LS/Bash etc.). The LLM will still see git errors
                # when it directly invokes git via the Bash tool.
                get_logger().warning(f"ensure_main_branch skipped: {exc}")
        return await call_next(context)


class GitMessageRequest(BaseModel):
    message: Annotated[str, Field(min_length=1)]


def _success_response(data=None, message: str = "ok", status_code: int = 200) -> JSONResponse:
    return JSONResponse(
        {"code": 0, "data": data, "message": message},
        status_code=status_code,
    )


def _error_response(code: int, message: str, status_code: int) -> JSONResponse:
    return JSONResponse(
        {"code": code, "data": None, "message": message},
        status_code=status_code,
    )


def _parse_git_message_payload(data: dict) -> str:
    try:
        payload = GitMessageRequest.model_validate(data)
    except Exception as exc:
        raise ToolError(f"invalid request payload: {exc}") from exc
    return payload.message.strip()


async def _read_git_message_from_request(request: Request) -> str:
    try:
        data = await request.json()
    except json.JSONDecodeError as exc:
        raise ToolError("request body must be valid JSON") from exc
    return _parse_git_message_payload(data)


mcp.add_middleware(GitMainBranchMiddleware())


# Forbid these as ripgrep search roots: walking them with --follow produces
# pseudo-fs / symlink-cycle blowups that hang the tool indefinitely.
RG_FORBIDDEN_ROOTS = ("/", "/proc", "/sys", "/dev", "/run", "/boot")
RG_TIMEOUT_SEC = 30
RG_MAX_BYTES = 5 * 1024 * 1024


def _validate_search_root(path: str) -> str:
    if not path:
        raise ToolError("path is required")
    norm = os.path.normpath(os.path.abspath(path))
    if norm in RG_FORBIDDEN_ROOTS:
        raise ToolError(f"Refusing to search system root: {norm}")
    for forbidden in RG_FORBIDDEN_ROOTS:
        if forbidden == "/":
            continue
        if norm == forbidden or norm.startswith(forbidden + os.sep):
            raise ToolError(f"Refusing to search inside system path: {norm}")
    return norm


async def _run_rg(args, *, timeout: float = RG_TIMEOUT_SEC) -> str:
    """Run ripgrep with timeout; kill the subprocess on timeout or coroutine cancel.

    Why: ripgrepy.run() is a synchronous subprocess.run(); calling it inside an
    async tool blocks the entire event loop and leaks the rg process if the
    client disconnects. We need explicit lifecycle control.
    """
    proc = await asyncio.create_subprocess_exec(
        "rg", *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        try:
            await asyncio.wait_for(proc.wait(), timeout=2)
        except asyncio.TimeoutError:
            pass
        raise ToolError(
            f"rg search exceeded {timeout}s and was aborted; narrow the path or pattern"
        )
    except asyncio.CancelledError:
        proc.kill()
        try:
            await asyncio.wait_for(proc.wait(), timeout=2)
        except asyncio.TimeoutError:
            pass
        raise
    if proc.returncode not in (0, 1):
        msg = stderr.decode("utf-8", errors="replace")[:2000]
        raise ToolError(f"rg failed (exit={proc.returncode}): {msg}")
    out = stdout.decode("utf-8", errors="replace")
    if len(out) > RG_MAX_BYTES:
        out = out[:RG_MAX_BYTES] + "\n[output truncated]"
    return out


# ---------------------------------------------------------------------------
# MCP Tools
# ---------------------------------------------------------------------------

@mcp.tool(name="Bash", description=BASH_TOOL_DESC)
async def bash(
    command: Annotated[str, Field(description="The command to execute")],
    description: Annotated[str, Field(
        description="Clear, concise description of what this command does in 5-10 words. Examples:\n"
                    "Input: ls\nOutput: Lists files in current directory\n\n"
                    "Input: git status\nOutput: Shows working tree status\n\n"
                    "Input: npm install\nOutput: Installs package dependencies\n\n"
                    "Input: mkdir foo\nOutput: Creates directory 'foo'"
    )] = "",
    timeout: Annotated[int, Field(description="Optional timeout in milliseconds (max 600000)")] = 120000,
):
    log = get_logger()
    if is_caddy_execution(command):
        log.warning(f"Bash tool blocked caddy execution: {command}")
        raise ToolError("can not execute caddy command in bash")
    if is_symlink_creation(command):
        log.warning(f"Bash tool blocked symlink creation: {command}")
        raise ToolError("Creating symbolic links is not allowed.")
    log.info(f"Bash tool(timeout={timeout/1000:.2f}s): {command}")
    try:
        result = await execute_command(command, cwd=PROJECT_DIR, timeout=timeout / 1000)
        log.info(f"Bash tool success: {result}")
        return result
    except Exception as e:
        log.error(f"Bash tool failed: {e}")
        raise e


@mcp.tool(
    name="Glob",
    description=(
        '- Fast file pattern matching tool that works with any codebase size\n'
        '- Supports glob patterns like "**/*.js" or "src/**/*.ts"\n'
        '- Returns matching file paths sorted by modification time\n'
        '- Use this tool when you need to find files by name patterns\n'
        '- When you are doing an open ended search that may require multiple rounds of globbing '
        'and grepping, use the Agent tool instead\n'
        '- You have the capability to call multiple tools in a single response. It is always '
        'better to speculatively perform multiple searches as a batch that are potentially useful.'
    ),
)
async def glob(
    path: Annotated[
        str,
        Field(description=(
            'The directory to search in. If not specified, the current working directory will be '
            'used. IMPORTANT: Omit this field to use the default directory. DO NOT enter '
            '"undefined" or "null" - simply omit it for the default behavior. Must be a valid '
            'directory path if provided.'
        )),
    ],
    pattern: Annotated[str, Field(description="The glob pattern to match files against")] = None,
):
    log = get_logger()
    log.info(f"Glob tool: {path}")
    check_path_permission(path)
    path = _validate_search_root(path)
    # --follow: descend into symlinked dirs (e.g. upload/ → OSS, CSI mounts)
    args = ["--files", "--follow"]
    if pattern:
        args += ["--glob", pattern]
    args.append(path)
    result = await _run_rg(args)
    log.info(f"Glob tool result: {result}")
    return result


@mcp.tool(name="Grep", description=GREP_TOOL_DESC)
async def grep(
    pattern: Annotated[str, Field(description="The regular expression pattern to search for in file contents")],
    after_context: Annotated[int, Field(description='Number of lines to show after each match (rg -A). Requires output_mode: "content", ignored otherwise.', alias="-A")] = 0,
    before_context: Annotated[int, Field(description='Number of lines to show before each match (rg -B). Requires output_mode: "content", ignored otherwise.', alias="-B")] = 0,
    context: Annotated[int, Field(description='Number of lines to show before and after each match (rg -C). Requires output_mode: "content", ignored otherwise.', alias="-C")] = 0,
    case_insensitive: Annotated[bool, Field(description="Case insensitive search (rg -i)", alias="-i")] = False,
    line_numbers: Annotated[bool, Field(description='Show line numbers in output (rg -n). Requires output_mode: "content", ignored otherwise.', alias="-n")] = False,
    glob: Annotated[str, Field(description='Glob pattern to filter files (e.g. "*.js", "*.{ts,tsx}") - maps to rg --glob')] = "",
    head_limit: Annotated[int, Field(description='Limit output to first N lines/entries, equivalent to "| head -N". Works across all output modes. When unspecified, shows all results from ripgrep.')] = None,
    multiline: Annotated[bool, Field(description="Enable multiline mode where . matches newlines and patterns can span lines (rg -U --multiline-dotall). Default: false.")] = False,
    output_mode: Annotated[Literal["content", "files_with_matches", "count"], Field(description='Output mode: "content" shows matching lines, "files_with_matches" shows file paths, "count" shows match counts. Defaults to "files_with_matches".')] = "files_with_matches",
    path: Annotated[str, Field(description="File or directory to search in (rg PATH). Defaults to current working directory.")] = "",
    file_type: Annotated[str, Field(description="File type to search (rg --type). Common types: js, py, rust, go, java, etc.")] = "",
):
    log = get_logger()
    log.info(f"Grep tool: {pattern} -A {after_context} -B {before_context} -C {context} "
             f"-i {case_insensitive} -n {line_numbers} -g {glob} -l {head_limit} "
             f"-U {multiline} -t {file_type} -o {output_mode} -p {path}")
    if not path:
        path = PROJECT_DIR
    check_path_permission(path)
    path = _validate_search_root(path)
    args = ["--follow"]
    if after_context:
        args += ["-A", str(after_context)]
    if before_context:
        args += ["-B", str(before_context)]
    if context:
        args += ["-C", str(context)]
    if case_insensitive:
        args.append("-i")
    if line_numbers:
        args.append("-n")
    if glob:
        args += ["--glob", glob]
    if multiline:
        args += ["--multiline", "--multiline-dotall"]
    if file_type:
        args += ["--type", file_type]
    if output_mode == "files_with_matches":
        args.append("-l")
    if output_mode == "count":
        args.append("-c")
    args += [pattern, path]
    result = await _run_rg(args)
    if head_limit:
        result = "\n".join(result.splitlines()[:head_limit])
    log.info(f"Grep tool result: {result}")
    return result


@mcp.tool(
    name="LS",
    description=(
        "Lists files and directories in a given path. The path parameter must be an absolute path, "
        "not a relative path. You can optionally provide an array of glob patterns to ignore with "
        "the ignore parameter. You should generally prefer the Glob and Grep tools, if you know "
        "which directories to search."
    ),
)
async def ls(
    path: Annotated[str, Field(description="The absolute path to the directory to list (must be absolute, not relative)")],
    ignore: Annotated[List[str], Field(description="List of glob patterns to ignore")] = None,
):
    log = get_logger()
    check_path_permission(path)
    path = _validate_search_root(path)
    args = ["--files", "--follow", "--glob", "!**/node_modules/**"]
    if ignore:
        for p in ignore:
            args += ["--glob", "!" + p]
    args.append(path)
    result = await _run_rg(args)
    log.info(f"LS tool result: {result}")
    return result


@mcp.tool(name="Read", description=READ_TOOL_DESC)
async def read(
    filepath: Annotated[str, Field(description="The absolute path to the file to read (must be absolute, not relative)")],
    limit: Annotated[int, Field(description="The number of lines to read. Only provide if the file is too large to read at once.")] = None,
    offset: Annotated[int, Field(description="The line number to start reading from. Only provide if the file is too large to read at once.")] = None,
):
    log = get_logger()
    check_path_permission(filepath)
    file_ext = Path(filepath).suffix.lower()
    if file_ext in NON_TEXT_SUFFIXES:
        log.info(f"Read tool blocked (non-text file): {filepath}")
        return (
            f"'{filepath}' seems not readable. You may need to read it with proper shell "
            "commands, Python tools, MCP tools or Skills if available."
        )
    result = edit_tool.view(Path(filepath), [offset, offset + limit] if limit and offset else None)
    log.info(f"Read tool result: {result}")
    return result


@mcp.tool(name="Edit", description=EDIT_TOOL_DESC)
async def edit(
    filepath: Annotated[str, Field(description="The absolute path to the file to edit (must be absolute, not relative)")],
    new_str: Annotated[str, Field(description="The text to replace it with (must be different from old_string)")],
    old_str: Annotated[str, Field(description="The text to replace")],
    replace_all: Annotated[bool, Field(description="Replace all occurences of old_string (default false)")] = False,
):
    log = get_logger()
    check_path_permission(filepath)
    check_write_path_in_home(filepath)
    result = edit_tool.str_replace(Path(filepath), old_str, new_str, replace_all)
    log.info(f"Edit tool result: {result}")
    return result


class EditItem(BaseModel):
    old_str: Annotated[str, Field(description="The text to replace")]
    new_str: Annotated[str, Field(description="The text to replace it with (must be different from old_string)")]
    replace_all: Annotated[bool, Field(description="Replace all occurences of old_string (default false)")] = False


@mcp.tool(name="MultiEdit", description=MULTI_EDIT_TOOL_DESC)
async def multi_edit(
    filepath: Annotated[str, Field(description="The absolute path to the file to edit (must be absolute, not relative)")],
    edits: Annotated[List[EditItem], Field(description="Array of edit operations to perform sequentially on the file")],
):
    log = get_logger()
    check_path_permission(filepath)
    check_write_path_in_home(filepath)
    results = []
    for item in edits:
        results.append(
            edit_tool.str_replace(Path(filepath), item.old_str, item.new_str, item.replace_all)
        )
    log.info(f"MultiEdit tool result: {results}")
    return "\n".join(results)


@mcp.tool(name="Write", description=WRITE_TOOL_DESC)
async def write(
    filepath: Annotated[str, Field(description="The absolute path to the file to write (must be absolute, not relative)")],
    content: Annotated[str, Field(description="The content to write to the file")],
):
    log = get_logger()
    check_path_permission(filepath)
    check_write_path_in_home(filepath)
    result = edit_tool.write_file(Path(filepath), content)
    log.info(f"Write tool result: {result}")
    return result

@mcp.tool(
    name="BrowserScreenshot", 
    description="Start a headless browser to visit a specific URL and take a screenshot. Supports controlling viewport size, full-page scrolling, and extra wait time for dynamic rendering. Returns the absolute path of the saved PNG image."
)
async def browser_screenshot(
    url: str, 
    output_path: Optional[str] = None, 
    width: int = 1280, 
    height: int = 720, 
    full_page: bool = False, 
    delay_ms: int = 1000
) -> str:
    log = get_logger()
    if output_path is None:
        output_path = f"{PROJECT_DIR}/download"
    else:
        check_path_permission(output_path)
        check_write_path_in_home(output_path)
    log.info(f"BrowserScreenshot tool: url={url}, full_page={full_page}")
    result = await take_screenshot(url, output_path, width, height, full_page, delay_ms)
    return result

class TodoItem(BaseModel):
    id: str
    content: Annotated[str, Field(min_length=1)]
    status: Literal["pending", "in_progress", "completed"]
    priority: Literal["high", "medium", "low"]


@mcp.tool(name="TodoWrite", description=TODO_WRITE_TOOL_DESC)
async def todo_write(
    todos: Annotated[list[TodoItem], Field(description="The updated todo list")],
):
    log = get_logger()
    todo_path = f"{HOME_DIR}/TODO"
    with open(todo_path, "w") as f:
        json.dump({"items": [todo.model_dump() for todo in todos]}, f)
    log.info("Todos have been modified successfully. Ensure that you continue to use the todo list to track your progress. Please proceed with the current tasks if applicable")
    return "Todos have been modified successfully. Ensure that you continue to use the todo list to track your progress. Please proceed with the current tasks if applicable"


@mcp.tool(
    name="TodoRead",
    description="No input is required, leave this field blank. NOTE that we do not require a dummy object, placeholder string or a key like \"input\" or \"empty\". LEAVE IT BLANK.",
)
async def todo_read():
    log = get_logger()
    with open(f"{HOME_DIR}/TODO", "r") as f:
        log.info("Todos have been read successfully")
        return json.load(f)


@mcp.tool(
    name="scan_new_skill",
    description="Scan for new skills in ./skills directory and package them to /home/skills/. No input required.",
)
async def scan_new_skill():
    return await scan_new_skill_impl(log=get_logger())


@mcp.tool(
    name="get_personal_skills",
    description="Get the personal skills from /home/skills directory. Optionally provide a list of skill names to get specific skills.",
)
async def get_personal_skills(
    skills_list: Annotated[Optional[List[str]], Field(description="Optional list of skill names to retrieve (e.g., ['name1', 'name2']). If not provided, all skills will be retrieved.")] = None,
):
    return await get_personal_skills_impl(skills_list=skills_list, log=get_logger())


@mcp.tool(
    name="skill_info",
    description="Get skill information for a user. Returns skill names and descriptions in XML format. Optionally filter by stage.",
)
async def skill_info(
    user_id: Annotated[str, Field(description="The user ID to get skills for")],
    stage: Annotated[Optional[str], Field(description="Optional stage/scenario filter (e.g., 'analysis', 'coding'). If provided, only skills for this stage will be returned.")] = None,
    include_user_skills: Annotated[bool, Field(description="Whether to always include user-level skills regardless of stage filter. Defaults to true.")] = True,
) -> str:
    return await skill_info_impl(user_id=user_id, stage=stage, include_user_skills=include_user_skills, log=get_logger())


@mcp.tool(
    name="skill_get",
    description="Get detailed information of a specific skill by name. Returns skill name, description, and instructions from SKILL.md.",
)
async def skill_get(
    skill_name: Annotated[str, Field(description="The name of the skill to retrieve")],
) -> str:
    return await skill_get_impl(skill_name=skill_name, log=get_logger())


@mcp.tool(
    name="list_oss_objects",
    description="List OSS objects with a specified prefix. Useful for finding available files in OSS.",
)
async def list_oss_objects(
    prefix: Annotated[str, Field(description="Object name prefix to filter (e.g., 'skills/' to list all skills)")] = "",
    max_keys: Annotated[int, Field(description="Maximum number of objects to return (default 100)")] = 100,
) -> str:
    log = get_logger()
    log.info(f"Listing OSS objects with prefix '{prefix}', max_keys={max_keys}")
    try:
        objects = await list_objects(prefix, max_keys)
        if not objects:
            return f"No objects found with prefix '{prefix}'"
        result = f"Found {len(objects)} object(s) with prefix '{prefix}':\n\n"
        result += "\n".join(f"- {obj}" for obj in objects)
        log.info(f"Successfully listed {len(objects)} objects")
        return result
    except Exception as e:
        error_msg = f"Failed to list OSS objects: {str(e)}"
        log.error(error_msg)
        raise ToolError(error_msg)


@mcp.tool(
    name="copy_project_to_tmp",
    description="Create an initial snapshot of git-tracked files in /home/z/my-project/ for later diff detection by files_diff or save_project_version.",
)
async def copy_project_to_tmp():
    return await copy_project_to_tmp_impl(log=get_logger())


@mcp.tool(
    name="files_diff",
    description=(
        "Copy changed files from /home/z/my-project/ to /tmp/my-project/, clone to PolarFS, "
        "and return diff information. Returns new files, modified files, and deleted files based "
        "on the initial snapshot created by copy_project_to_tmp. Must call copy_project_to_tmp "
        "first to create the initial snapshot."
    ),
)
async def files_diff(
    user_id: Annotated[str, Field(description="User ID for the PolarFS source path")],
    chat_id: Annotated[str, Field(description="Chat ID for the PolarFS source path")],
    message_id: Annotated[str, Field(description="Message ID for the PolarFS target path")],
):
    return await files_diff_impl(
        user_id=user_id, chat_id=chat_id, message_id=message_id, log=get_logger()
    )


@mcp.tool(
    name="save_project_version",
    description=(
        "Save the current project as a snapshot version to PolarFS by calling "
        "ClonePolarFsBasicSnapshot API. This creates a server-side copy from "
        "/{user_id}/{chat_id}/ to /{message_id}/."
    ),
)
async def save_project_version(
    message_id: Annotated[str, Field(description="Message ID for the snapshot target path")],
    user_id: Annotated[str, Field(description="User ID for the snapshot source path")],
    chat_id: Annotated[str, Field(description="Chat ID for the snapshot source path")],
):
    return await save_project_version_impl(
        message_id=message_id, user_id=user_id, chat_id=chat_id, log=get_logger()
    )


@mcp.tool(
    name="restore_files",
    description=(
        "Restore project files from /tmp/my-project/ back to /home/z/my-project/ "
        "by force-overwriting all files. Does nothing if /tmp/my-project/ is empty."
    ),
)
async def restore_files():
    return await restore_files_impl(log=get_logger())


# ---------------------------------------------------------------------------
# Custom Routes
# ---------------------------------------------------------------------------

@mcp.custom_route("/ping", methods=["GET"])
def ping(request: Request):
    return Response("pong", status_code=200)


@mcp.custom_route("/npmRunDev", methods=["POST"])
async def npm_run_dev(request: Request):
    log = get_logger()
    try:
        result = await execute_command(f"npm --prefix {PROJECT_DIR} run dev &")
        log.info(f"npm run dev result: {result}")
        return JSONResponse(result.model_dump(), status_code=200)
    except Exception as e:
        log.error(f"npm run dev failed: {e}")
        return Response(str(e), status_code=500)


@mcp.custom_route("/deploy", methods=["POST"])
async def deploy(request: Request):
    return await deploy_impl(request, log=get_logger())


@mcp.custom_route("/git/blob/{rev}/{path:path}", methods=["GET"])
async def git_cat_file(request: Request):
    return git_blob_response(request)


@mcp.custom_route("/git/ls-tree", methods=["GET"])
async def git_ls_tree(request: Request):
    try:
        # --follow: include files under symlink dirs (upload/, CSI OSS mounts, etc.)
        rg = Ripgrepy("", PROJECT_DIR).files().iglob("!node_modules").iglob("!skills").follow()
        tree = rg.run().as_string
        tree = [line[len(PROJECT_DIR) + 1:] for line in tree.splitlines()]
        return JSONResponse(tree, status_code=200)
    except subprocess.CalledProcessError as e:
        return Response(e.stderr, status_code=500)

@mcp.custom_route("/git/ls-tree-meta", methods=["GET"])
async def git_ls_tree_meta(request: Request):
    """列出项目工作区文件及其元信息"""
    try:
        # FIXME fork 了一下 rgpy，有空改一下支持 wd，不然还要在这块 trim，很丑陋
        # 还有 --files 每次都要手写 EMPTY_DIR，而且不支持 Sort 和 自动 gitignore
        rg = Ripgrepy("", PROJECT_DIR).files().iglob("!node_modules").iglob("!skills").follow()
        tree = rg.run().as_string or ""
        result = []
        for line in tree.splitlines():
            if not line:
                continue
            abs_path = Path(line)
            try:
                stat = abs_path.stat()
            except OSError:
                continue
            result.append({
                "path": line[len(PROJECT_DIR) + 1:],
                "mtime": int(stat.st_mtime),
                "size": stat.st_size,
            })
        result.sort(key=lambda x: x["mtime"], reverse=True)
        return JSONResponse(result, status_code=200)
    except Exception as e:
        return Response(str(e), status_code=500)


@mcp.custom_route("/git/commit", methods=["POST"])
async def git_commit_route(request: Request):
    log = get_logger()
    try:
        message = await _read_git_message_from_request(request)
        result = await git_commit(PROJECT_DIR, message, log=log)
        return _success_response(result)
    except ToolError as e:
        log.error(f"git commit failed: {e}")
        return _error_response(400, str(e), 400)
    except Exception as e:
        log.error(f"git commit failed: {e}")
        return _error_response(500, str(e), 500)


@mcp.custom_route("/git/checkout", methods=["POST"])
async def git_checkout_route(request: Request):
    log = get_logger()
    try:
        message = await _read_git_message_from_request(request)
        result = await git_checkout_by_message(PROJECT_DIR, message, log=log)
        return _success_response(result)
    except ToolError as e:
        log.error(f"git checkout failed: {e}")
        return _error_response(400, str(e), 400)
    except Exception as e:
        log.error(f"git checkout failed: {e}")
        return _error_response(500, str(e), 500)


@mcp.custom_route("/git/log", methods=["GET"])
async def git_log_route(request: Request):
    log = get_logger()
    try:
        limit = int(request.query_params.get("limit", "100"))
        result = await get_git_log(PROJECT_DIR, limit=limit)
        return _success_response(result)
    except ValueError:
        return _error_response(400, "limit must be an integer", 400)
    except ToolError as e:
        log.error(f"git log failed: {e}")
        return _error_response(400, str(e), 400)
    except Exception as e:
        log.error(f"git log failed: {e}")
        return _error_response(500, str(e), 500)


@mcp.custom_route("/git/archive/{rev:path}", methods=["GET"])
async def git_archive(request: Request):
    rev = request.path_params["rev"]
    try:
        filename = os.path.basename(f"{rev}.tar")
        temp_path = f"/tmp/{filename}"
        await archive(temp_path)
        return FileResponse(path=temp_path, filename=filename, media_type="application/x-tar")
    except ToolError as e:
        print(f"ToolError: {str(e)}")
        return Response(str(e), status_code=500)
    except Exception as e:
        return Response(str(e), status_code=500)


@mcp.custom_route("/update_zai_cfg", methods=["PATCH"])
async def update_zai_cfg(request: Request):
    data = await request.json()
    with open("/etc/.z-ai-config", "r") as f:
        config = json.load(f)
    config.update(data)
    with open("/etc/.z-ai-config", "w") as f:
        json.dump(config, f)
    return Response("ok", status_code=200)


@mcp.custom_route("/init_fullstack", methods=["POST"])
async def init_fullstack(request: Request):
    log = get_logger()
    cmd = "curl -fsSL https://z-cdn.chatglm.cn/fullstack/init-fullstack.sh | bash"
    try:
        result = await execute_command(cmd, timeout=600)
        log.info(f"init_fullstack success: {result}")
        return JSONResponse({"message": "ok", "output": result}, status_code=200)
    except Exception as e:
        log.error(f"init_fullstack failed: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@mcp.custom_route("/upload_skill", methods=["POST"])
async def upload_skill(request: Request):
    """
    Accept a skill zip upload (multipart/form-data, field name 'file').
    Extracts to the project skills/ dir
    The zip must contain a top-level skill directory with a SKILL.md inside.
    """
    log = get_logger()
    try:
        form = await request.form()
        file = form.get("file")
        if file is None:
            return Response("Missing 'file' field in form data", status_code=400)
        content = await file.read()
        result = await upload_skill_impl(content, file.filename, log=log)
        return JSONResponse(result, status_code=200)
    except Exception as e:
        log.error(f"upload_skill failed: {e}")
        return Response(str(e), status_code=400)


@mcp.custom_route("/pre-stop", methods=["GET"])
async def pre_stop(request: Request):
    log = get_logger()
    result = await pre_stop_impl(request, log)
    return Response(result, status_code=200)


@mcp.custom_route("/initialize", methods=["POST"])
async def initialize(request: Request):
    log = get_logger()
    log.info("initialize tool: starting workspace initialization")
    official_skills_dir = Path("/home/official_skills")
    target_skills_dir = Path(PROJECT_DIR) / "skills"
    result = await asyncio.to_thread(extract_official_skills, official_skills_dir, target_skills_dir)
    log.info(f"initialize tool completed: {result}")
    return Response(result, status_code=200)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _on_task_done(task: asyncio.Task) -> None:
    if not task.cancelled() and task.exception() is not None:
        import traceback
        get_logger().error(
            f"background task '{task.get_name()}' crashed: "
            + "".join(traceback.format_exception(task.exception()))
        )


async def start():
    await restore_user_skills_initial()
    # Heal mid-rebase/merge state and stale lock files inherited from a
    # poisoned tar restore so the first session post-deploy starts on a clean
    # repo. Wrapped: a failure here must not block the MCP server from coming up.
    # Use loguru's logger directly here — get_logger() needs an HTTP request
    # context that doesn't exist at startup.
    from loguru import logger as _startup_log
    try:
        clean_stale_git_locks(PROJECT_DIR, log=_startup_log)
        await _abort_pending_git_op(PROJECT_DIR, log=_startup_log)
    except Exception as exc:
        _startup_log.warning(f"startup git heal failed: {exc}")
    for coro, name in [
        (heartbeat(), "heartbeat"),
        (start_skill_watcher_impl(), "skill_watcher"),
    ]:
        t = asyncio.create_task(coro, name=name)
        t.add_done_callback(_on_task_done)
    await mcp.run_async(transport="streamable-http", port=12600)

