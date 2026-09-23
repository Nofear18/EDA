"""ExecutionClient and backends (import / HTTP)."""

from __future__ import annotations

import math
import shutil
import tempfile
import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, Optional

from science_eda.config import SandboxClientConfig, SandboxConfig
from science_eda.exceptions import (
    APIConnectionError,
    LanguageMismatchError,
    SandboxPathError,
    SessionAlreadyExistsError,
    SessionNotFoundError,
)
from science_eda.exceptions import TimeoutError as ExecutionTimeoutError
from science_eda.sandbox.file_io import (
    SnapshotCache,
    copy_upload,
    read_text_file,
    stage_snapshot_from_cache,
    write_text_file,
)
from science_eda.sandbox.lang import normalize_lang
from science_eda.sandbox.session import SessionManager
from science_eda.sandbox.types import ExecutionResult


class ExecutionBackend(ABC):
    @abstractmethod
    def execute(
        self,
        code: str,
        lang: str,
        session_id: Optional[str] = None,
        timeout: float | None = None,
    ) -> ExecutionResult:
        raise NotImplementedError

    @abstractmethod
    def create_session(self, config: dict[str, Any]) -> str:
        raise NotImplementedError

    @abstractmethod
    def close_session(self, session_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def is_alive(self) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def read_file(
        self,
        session_id: str,
        path: str,
        encoding: str = "utf-8",
        errors: str = "strict",
    ) -> str:
        raise NotImplementedError

    @abstractmethod
    def write_file(
        self,
        session_id: str,
        path: str,
        content: str,
        encoding: str = "utf-8",
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def upload(
        self,
        session_id: str,
        source_path: str,
        target_path: str,
        *,
        unzip: bool = False,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def stage_snapshot(
        self,
        session_id: str,
        snapshot_ref: str,
        target_path: str,
    ) -> str:
        raise NotImplementedError


class ImportBackend(ExecutionBackend):
    def __init__(self, config: SandboxConfig) -> None:
        self._config = config
        self._session_mgr = SessionManager(config)
        self._snapshot_cache = SnapshotCache(config.innovus_snapshot_cache_root)

    def execute(
        self,
        code: str,
        lang: str,
        session_id: Optional[str] = None,
        timeout: float | None = None,
    ) -> ExecutionResult:
        if not session_id:
            sid = self._session_mgr.create_session({"lang": lang})
            try:
                return self._session_mgr.execute_in_session(sid, code, lang, timeout=timeout)
            finally:
                try:
                    self._session_mgr.close_session(sid)
                except SessionNotFoundError:
                    pass
        return self._session_mgr.execute_in_session(session_id, code, lang, timeout=timeout)

    def create_session(self, config: dict[str, Any]) -> str:
        return self._session_mgr.create_session(config)

    def close_session(self, session_id: str) -> None:
        self._session_mgr.close_session(session_id)

    def close(self) -> None:
        self._session_mgr.close_all()

    def is_alive(self) -> dict[str, Any]:
        return {"is_alive": True, "message": ""}

    def read_file(
        self,
        session_id: str,
        path: str,
        encoding: str = "utf-8",
        errors: str = "strict",
    ) -> str:
        with self._session_mgr.locked_session(session_id) as sess:
            return read_text_file(sess.working_dir, path, encoding=encoding, errors=errors)

    def write_file(
        self,
        session_id: str,
        path: str,
        content: str,
        encoding: str = "utf-8",
    ) -> None:
        with self._session_mgr.locked_session(session_id) as sess:
            write_text_file(sess.working_dir, path, content, encoding=encoding)

    def upload(
        self,
        session_id: str,
        source_path: str,
        target_path: str,
        *,
        unzip: bool = False,
    ) -> None:
        with self._session_mgr.locked_session(session_id) as sess:
            copy_upload(sess.working_dir, source_path, target_path, unzip=unzip)

    def stage_snapshot(
        self,
        session_id: str,
        snapshot_ref: str,
        target_path: str,
    ) -> str:
        with self._session_mgr.locked_session(session_id) as sess:
            return stage_snapshot_from_cache(
                sess.working_dir,
                self._snapshot_cache,
                snapshot_ref,
                target_path,
            )


class HTTPBackend(ExecutionBackend):
    def __init__(
        self,
        endpoint: str,
        config: SandboxClientConfig | None = None,
        *,
        create_session_timeout: Callable[[dict[str, Any]], float | None] | None = None,
    ) -> None:
        self._endpoint = endpoint.rstrip("/")
        self._config = config or SandboxClientConfig()
        self._create_session_timeout = create_session_timeout
        self._owned_session_ids: set[str] = set()

    def _client_timeout(self, timeout: float | None = None) -> float | None:
        if timeout is not None and not math.isfinite(float(timeout)):
            return None
        effective = float(timeout) if timeout is not None else float(self._config.timeout)
        return effective + 45.0

    def _decode_response(self, response: Any) -> dict[str, Any]:
        if response.status_code < 400:
            return response.json()
        try:
            data = response.json()
        except ValueError:
            raise APIConnectionError(
                f"HTTPStatusError: {response.status_code} {response.text}"
            ) from None
        error = data.get("error", {}) if isinstance(data, dict) else {}
        code = str(error.get("code", ""))
        message = str(error.get("message", response.text))
        if code == "session_not_found":
            raise SessionNotFoundError(message)
        if code == "session_already_exists":
            raise SessionAlreadyExistsError(message)
        if code == "language_mismatch":
            raise LanguageMismatchError(message)
        if code == "invalid_path":
            raise SandboxPathError(message)
        if code == "file_not_found":
            raise FileNotFoundError(message)
        if code == "timeout":
            raise ExecutionTimeoutError(message)
        raise APIConnectionError(f"HTTPStatusError: {response.status_code} {message}")

    def _post_json(
        self,
        path: str,
        payload: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        import httpx

        try:
            r = httpx.post(
                f"{self._endpoint}/{path}",
                json=payload or {},
                timeout=self._client_timeout(timeout),
            )
            return self._decode_response(r)
        except httpx.TimeoutException as e:
            raise ExecutionTimeoutError(f"sandbox HTTP {path} timed out") from e
        except httpx.RequestError as e:
            raise APIConnectionError(f"sandbox HTTP request failed: {e}") from e

    def execute(
        self,
        code: str,
        lang: str,
        session_id: Optional[str] = None,
        timeout: float | None = None,
    ) -> ExecutionResult:
        effective_timeout = float(timeout) if timeout is not None else float(self._config.timeout)
        payload: dict[str, Any] = {
            "code": code,
            "lang": lang,
            "timeout": effective_timeout,
        }
        if session_id:
            payload["session_id"] = session_id
            data = self._post_json("run_in_session", payload, effective_timeout)
        else:
            data = self._post_json("execute", payload, effective_timeout)
        raw_metadata = data.get("metadata", {})
        return ExecutionResult(
            stdout=data.get("stdout", ""),
            stderr=data.get("stderr", ""),
            exit_code=int(data.get("exit_code", 1)),
            duration=float(data.get("duration", 0.0)),
            metadata=dict(raw_metadata) if isinstance(raw_metadata, dict) else {},
        )

    def create_session(
        self,
        config: dict[str, Any] | str,
        session_id: Optional[str] = None,
    ) -> str:
        if isinstance(config, str):
            payload: dict[str, Any] = {"lang": config}
            if session_id is not None:
                payload["session_id"] = session_id
        else:
            payload = dict(config)
        timeout = None
        if self._create_session_timeout is not None:
            timeout = self._create_session_timeout(payload)
        data = self._post_json("create_session", payload, timeout=timeout)
        session_id = str(data["session_id"])
        self._owned_session_ids.add(session_id)
        return session_id

    def close_session(self, session_id: str) -> None:
        try:
            self._post_json("close_session", {"session_id": session_id})
        except SessionNotFoundError:
            self._owned_session_ids.discard(session_id)
            raise
        self._owned_session_ids.discard(session_id)

    def close(self) -> None:
        for session_id in list(self._owned_session_ids):
            try:
                self.close_session(session_id)
            except SessionNotFoundError:
                continue

    def is_alive(self) -> dict[str, Any]:
        import httpx

        try:
            r = httpx.get(f"{self._endpoint}/is_alive", timeout=self._client_timeout())
            return self._decode_response(r)
        except httpx.TimeoutException as e:
            raise ExecutionTimeoutError("sandbox HTTP is_alive timed out") from e
        except httpx.RequestError as e:
            raise APIConnectionError(f"sandbox HTTP request failed: {e}") from e

    def read_file(
        self,
        session_id: str,
        path: str,
        encoding: str = "utf-8",
        errors: str = "strict",
    ) -> str:
        data = self._post_json(
            "read_file",
            {
                "session_id": session_id,
                "path": path,
                "encoding": encoding,
                "errors": errors,
            },
        )
        return str(data.get("content", ""))

    def write_file(
        self,
        session_id: str,
        path: str,
        content: str,
        encoding: str = "utf-8",
    ) -> None:
        self._post_json(
            "write_file",
            {
                "session_id": session_id,
                "path": path,
                "content": content,
                "encoding": encoding,
            },
        )

    def upload(
        self,
        session_id: str,
        source_path: str,
        target_path: str,
        *,
        unzip: bool = False,
    ) -> None:
        source = Path(source_path).resolve()
        if source.is_dir():
            with tempfile.TemporaryDirectory() as temp_dir:
                archive_base = Path(temp_dir) / "upload"
                archive_path = Path(shutil.make_archive(str(archive_base), "zip", source))
                self._upload_file(session_id, archive_path, target_path, unzip=True)
                return
        if not source.is_file():
            raise FileNotFoundError(f"upload source does not exist: {source_path!r}")
        self._upload_file(session_id, source, target_path, unzip=unzip)

    def stage_snapshot(
        self,
        session_id: str,
        snapshot_ref: str,
        target_path: str,
    ) -> str:
        data = self._post_json(
            "stage_snapshot",
            {
                "session_id": session_id,
                "snapshot_ref": snapshot_ref,
                "target_path": target_path,
            },
        )
        return str(data.get("snapshot_path", ""))

    def _upload_file(
        self,
        session_id: str,
        source: Path,
        target_path: str,
        *,
        unzip: bool,
    ) -> None:
        import httpx

        data = {
            "session_id": session_id,
            "target_path": target_path,
            "unzip": "true" if unzip else "false",
        }
        try:
            with source.open("rb") as f:
                r = httpx.post(
                    f"{self._endpoint}/upload",
                    data=data,
                    files={"file": (source.name, f)},
                    timeout=self._client_timeout(),
                )
            self._decode_response(r)
        except httpx.TimeoutException as e:
            raise ExecutionTimeoutError("sandbox HTTP upload timed out") from e
        except httpx.RequestError as e:
            raise APIConnectionError(f"sandbox HTTP request failed: {e}") from e


def _http_create_session_timeout(
    config: SandboxConfig,
) -> Callable[[dict[str, Any]], float | None]:
    def _timeout(session_config: dict[str, Any]) -> float | None:
        try:
            lang = normalize_lang(str(session_config.get("lang", "")))
            if lang == "primetime":
                startup_timeout = float(config.primetime_startup_timeout)
                if not config.primetime_use_pool:
                    return startup_timeout
                queue_timeout = float(config.primetime_pool_queue_timeout)
                if queue_timeout > 0:
                    return queue_timeout + (2.0 * startup_timeout)
                return float("inf")
            if lang != "innovus":
                return None
            queue_timeout = float(config.innovus_pool_queue_timeout)
            if queue_timeout > 0:
                startup_timeout = float(config.innovus_startup_timeout)
                return queue_timeout + (2.0 * startup_timeout)
            return float("inf")
        except ValueError:
            return None

    return _timeout


class ExecutionClient:
    def __init__(self, config: SandboxConfig) -> None:
        self._config = config
        if config.client_mode == "import":
            self._backend: ExecutionBackend = ImportBackend(config)
        else:
            self._backend = HTTPBackend(
                config.endpoint,
                SandboxClientConfig(endpoint=config.endpoint, timeout=config.timeout),
                create_session_timeout=_http_create_session_timeout(config),
            )

    def execute(
        self,
        code: str,
        lang: str,
        session_id: Optional[str] = None,
        timeout: float | None = None,
    ) -> ExecutionResult:
        return self._backend.execute(code, lang, session_id, timeout=timeout)

    def create_session(self, lang: str, session_id: Optional[str] = None) -> str:
        cfg: dict[str, Any] = {"lang": lang}
        if session_id is not None:
            cfg["session_id"] = session_id
        return self._backend.create_session(cfg)

    def close_session(self, session_id: str) -> None:
        self._backend.close_session(session_id)

    def close(self) -> None:
        self._backend.close()

    def is_alive(self) -> dict[str, Any]:
        return self._backend.is_alive()

    def read_file(
        self,
        session_id: str,
        path: str,
        encoding: str = "utf-8",
        errors: str = "strict",
    ) -> str:
        return self._backend.read_file(session_id, path, encoding=encoding, errors=errors)

    def write_file(
        self,
        session_id: str,
        path: str,
        content: str,
        encoding: str = "utf-8",
    ) -> None:
        self._backend.write_file(session_id, path, content, encoding=encoding)

    def upload(
        self,
        session_id: str,
        source_path: str,
        target_path: str,
        *,
        unzip: bool = False,
    ) -> None:
        self._backend.upload(session_id, source_path, target_path, unzip=unzip)

    def stage_snapshot(
        self,
        session_id: str,
        snapshot_ref: str,
        target_path: str,
    ) -> str:
        return self._backend.stage_snapshot(session_id, snapshot_ref, target_path)

    def execute_tcl_script(
        self,
        user_tcl: str,
        lang: str,
        session_id: str,
        *,
        setup_tcl: str = "",
        timeout: float | None = None,
        script_path: str | None = None,
        capture_output: bool | None = None,
    ) -> ExecutionResult:
        return execute_tcl_script(
            self,
            user_tcl,
            lang,
            session_id,
            setup_tcl=setup_tcl,
            timeout=timeout,
            script_path=script_path,
            capture_output=capture_output,
        )


class StatefulSession:
    def __init__(
        self,
        client: ExecutionClient,
        lang: str,
        session_id: Optional[str] = None,
    ) -> None:
        self._client = client
        self._lang = lang
        self.session_id = self._client.create_session(lang, session_id)

    def execute(self, code: str, timeout: float | None = None) -> ExecutionResult:
        return self._client.execute(code, self._lang, self.session_id, timeout=timeout)

    def execute_tcl_script(
        self,
        user_tcl: str,
        *,
        setup_tcl: str = "",
        timeout: float | None = None,
        script_path: str | None = None,
        capture_output: bool | None = None,
    ) -> ExecutionResult:
        return self._client.execute_tcl_script(
            user_tcl,
            self._lang,
            self.session_id,
            setup_tcl=setup_tcl,
            timeout=timeout,
            script_path=script_path,
            capture_output=capture_output,
        )

    def close(self) -> None:
        self._client.close_session(self.session_id)

    def __enter__(self) -> StatefulSession:
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        self.close()


def execute_tcl_script(
    client: ExecutionClient,
    user_tcl: str,
    lang: str,
    session_id: str,
    *,
    setup_tcl: str = "",
    timeout: float | None = None,
    script_path: str | None = None,
    capture_output: bool | None = None,
) -> ExecutionResult:
    """Write and source a Tcl script inside an existing sandbox session."""

    normalized = normalize_lang(lang)
    if normalized not in {"tcl", "innovus", "primetime"}:
        raise ValueError(f"execute_tcl_script requires a Tcl language, got: {lang!r}")
    path = script_path or f".science_eda_scripts/script_{uuid.uuid4().hex}.tcl"
    client.write_file(session_id, path, user_tcl.rstrip() + "\n")

    if setup_tcl.strip():
        setup_result = client.execute(setup_tcl, normalized, session_id, timeout=timeout)
        if setup_result.exit_code != 0:
            metadata = dict(setup_result.metadata)
            metadata["tcl_script_phase"] = "setup"
            setup_result.metadata = metadata
            return setup_result

    should_capture = normalized == "primetime" if capture_output is None else capture_output
    source_command = f"source {_tcl_double_quoted_word(path)}"
    if not should_capture:
        result = client.execute(source_command, normalized, session_id, timeout=timeout)
        metadata = dict(result.metadata)
        metadata["tcl_script_phase"] = "execute"
        metadata["tcl_script_path"] = path
        result.metadata = metadata
        return result

    token = uuid.uuid4().hex
    output_var = f"__science_eda_script_output_{token}"
    code_var = f"__science_eda_script_code_{token}"
    message_var = f"__science_eda_script_message_{token}"
    options_var = f"__science_eda_script_options_{token}"
    wrapper = (
        f"set {output_var} \"\"\n"
        f"set {code_var} [catch {{\n"
        f"    redirect -variable {output_var} {{\n"
        f"        {source_command}\n"
        "    }\n"
        f"}} {message_var} {options_var}]\n"
        f"puts -nonewline [set {output_var}]\n"
        f"if {{[set {code_var}] != 0}} {{\n"
        f"    return -options [set {options_var}] [set {message_var}]\n"
        "}\n"
    )
    result = client.execute(wrapper, normalized, session_id, timeout=timeout)
    metadata = dict(result.metadata)
    metadata["tcl_script_phase"] = "execute"
    metadata["tcl_script_path"] = path
    result.metadata = metadata
    return result


def _tcl_double_quoted_word(value: str) -> str:
    escaped = (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("$", "\\$")
        .replace("[", "\\[")
        .replace("]", "\\]")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    return f'"{escaped}"'
