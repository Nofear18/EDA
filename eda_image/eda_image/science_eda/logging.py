import logging
import time
from typing import Optional, Any
from .config import LogConfig

class ScienceEDALogger:
    """统一日志管理"""

    def __init__(self, config: Optional[LogConfig] = None):
        self.config = config or LogConfig()
        self.logger = logging.getLogger("science_eda")
        self.logger.setLevel(getattr(logging, self.config.level.upper(), logging.INFO))
        
        # Avoid duplicate handlers
        if not self.logger.handlers:
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            
            # Console Handler
            ch = logging.StreamHandler()
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

            # File Handler
            if self.config.log_file:
                fh = logging.FileHandler(self.config.log_file)
                fh.setFormatter(formatter)
                self.logger.addHandler(fh)

        self._token_usage = {"input": 0, "output": 0, "total": 0}

    def log_llm_call(self, prompt: Any, response: str, model: str, tokens: Optional[dict], duration: float):
        """记录 LLM 调用并累加 token 用量"""
        if self.config.track_tokens and tokens:
            self._token_usage["input"] += tokens.get("prompt_tokens", 0)
            self._token_usage["output"] += tokens.get("completion_tokens", 0)
            self._token_usage["total"] += tokens.get("total_tokens", 0)
            
        if self.config.log_llm_calls:
            self.logger.info(f"[Inference] Model: {model} | Time: {duration:.2f}s | Tokens: {tokens}")
            self.logger.debug(f"[Inference Request] {prompt}")
            self.logger.debug(f"[Inference Response] {response}")

    def log_execution(self, code: str, lang: str, result: Any, duration: float):
        """记录代码执行"""
        if self.config.log_executions:
            exit_code = getattr(result, 'exit_code', 'unknown')
            self.logger.info(f"[Execution] Lang: {lang} | Exit Code: {exit_code} | Time: {duration:.2f}s")
            self.logger.debug(f"[Exec Code] {code}")
            
            stdout = getattr(result, 'stdout', '')
            stderr = getattr(result, 'stderr', '')
            if stdout or stderr:
                 self.logger.debug(f"[Exec Output] Stdout: {stdout} | Stderr: {stderr}")

    def get_token_usage(self) -> dict:
        """获取累计 token 用量统计"""
        # Return a copy to avoid accidental internal state modification
        return self._token_usage.copy()

    def get_cost_estimate(self, pricing: dict = None) -> float:
        """根据 token 用量估算费用 (Placeholder)"""
        # Could use pricing dictionary to calculate cost based on prompt/completion token sums
        return 0.0
