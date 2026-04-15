from typing import Dict, List

from zstash.utils import run_command


class FakeProcess:
    def __init__(self, returncode: int = 0):
        self.returncode = returncode

    def communicate(self):
        return (b"", b"")


def test_run_command_sanitizes_loader_vars_for_hsi(monkeypatch):
    monkeypatch.setenv("HOME", "/tmp/test-home")
    monkeypatch.setenv("LD_LIBRARY_PATH", "/tmp/pixi/lib")
    monkeypatch.setenv("LD_PRELOAD", "/tmp/pixi/preload.so")

    captured: Dict[str, Dict[str, str]] = {}

    def fake_popen(command_args: List[str], **kwargs) -> FakeProcess:
        captured["command_args"] = command_args
        captured["env"] = kwargs["env"]
        return FakeProcess()

    monkeypatch.setattr("zstash.utils.subprocess.Popen", fake_popen)

    run_command("hsi --help", "test error")

    assert captured["command_args"] == ["hsi", "--help"]
    assert "LD_LIBRARY_PATH" not in captured["env"]
    assert "LD_PRELOAD" not in captured["env"]
    assert captured["env"]["HOME"] == "/tmp/test-home"


def test_run_command_keeps_loader_vars_for_non_hsi(monkeypatch):
    monkeypatch.setenv("LD_LIBRARY_PATH", "/tmp/pixi/lib")
    monkeypatch.setenv("LD_PRELOAD", "/tmp/pixi/preload.so")

    captured: Dict[str, Dict[str, str]] = {}

    def fake_popen(command_args: List[str], **kwargs) -> FakeProcess:
        captured["command_args"] = command_args
        captured["env"] = kwargs["env"]
        return FakeProcess()

    monkeypatch.setattr("zstash.utils.subprocess.Popen", fake_popen)

    run_command("echo hello", "test error")

    assert captured["command_args"] == ["echo", "hello"]
    assert captured["env"]["LD_LIBRARY_PATH"] == "/tmp/pixi/lib"
    assert captured["env"]["LD_PRELOAD"] == "/tmp/pixi/preload.so"
