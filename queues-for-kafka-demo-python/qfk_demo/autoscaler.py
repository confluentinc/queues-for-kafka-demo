"""ChefAutoScaler: spawns/terminates chef consumer processes based on queue depth.

Demonstrates KIP-932 share-group auto-scaling:
- Polls the dashboard's own queue-depth endpoint (fast HTTP call, no blocking
  Kafka admin calls needed).
- Scales up when the queue backs up past a threshold.
- Scales down when the queue drains.
- Can also run in "manual mode", where scaling only happens via the
  dashboard's add-chef/remove-chef buttons.
"""

import subprocess
import sys
import threading
import time
from typing import Dict, List, Optional

import requests

from qfk_demo import config

CHEF_NAME_PREFIX = "Chef-Auto"


class ChefAutoScaler:
    def __init__(self, manual_mode: bool = False) -> None:
        self.manual_mode = manual_mode
        self.dashboard_url = config.get("dashboard.url", "http://localhost:8080")
        self.min_chefs = config.get_int("auto.scale.min.chefs", 1)
        self.max_chefs = config.get_int("auto.scale.max.chefs", 4)
        self.scale_up_threshold = config.get_int("auto.scale.up.threshold", 5)
        self.scale_down_threshold = config.get_int("auto.scale.down.threshold", 2)
        self.check_interval_seconds = config.get_int("auto.scale.check.interval.ms", 3000) / 1000

        self._lock = threading.Lock()
        self._active_chefs: Dict[str, subprocess.Popen] = {}
        self._chef_counter = 1
        self._running = False
        self._last_queue_depth = 0
        self._target_chef_count = self.min_chefs
        self._scale_thread: Optional[threading.Thread] = None
        self._cleanup_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._print_banner()

        self._target_chef_count = self.min_chefs
        for _ in range(self.min_chefs):
            self.spawn_chef()

        self._scale_thread = threading.Thread(target=self._scale_loop, daemon=True)
        self._scale_thread.start()

        self._cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self._cleanup_thread.start()

    def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        print("Stopping auto-scaler...")
        with self._lock:
            processes = list(self._active_chefs.values())
            self._active_chefs.clear()
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()

    def _scale_loop(self) -> None:
        while self._running:
            time.sleep(self.check_interval_seconds)
            try:
                self._check_and_scale()
            except Exception as e:
                print(f"⚠️ Autoscaler error (will retry): {e}")

    def _cleanup_loop(self) -> None:
        while self._running:
            time.sleep(10)
            try:
                self._cleanup_dead_processes()
            except Exception:
                pass

    def _check_and_scale(self) -> None:
        queue_depth = self._get_queue_depth_from_dashboard()
        self._last_queue_depth = queue_depth

        if self.manual_mode:
            print()
            print(f"═══ MANUAL MODE: Queue={queue_depth} | Chefs={self.active_chef_count} ═══")
            sys.stdout.flush()
            return

        current_chefs = self.active_chef_count
        new_target = self._calculate_target_chefs(queue_depth)

        print()
        print(f"═══ AUTOSCALER: Queue={queue_depth} | Chefs={current_chefs} | Target={new_target} ═══")

        if new_target > self._target_chef_count:
            print(f"⬆️  Scaling UP to {new_target} chefs")
        elif new_target < self._target_chef_count:
            print(f"⬇️  Scaling DOWN to {new_target} chefs")
        sys.stdout.flush()

        self._target_chef_count = new_target

        while self.active_chef_count < self._target_chef_count and self.active_chef_count < self.max_chefs:
            self.spawn_chef()

        while self.active_chef_count > self._target_chef_count and self.active_chef_count > self.min_chefs:
            self.terminate_chef()

    def _get_queue_depth_from_dashboard(self) -> int:
        try:
            response = requests.get(f"{self.dashboard_url}/api/autoscale/queue-depth", timeout=1)
            if response.status_code == 200:
                return int(response.json().get("queueDepth", self._last_queue_depth))
        except (requests.RequestException, ValueError):
            pass
        return self._last_queue_depth

    def _calculate_target_chefs(self, queue_depth: int) -> int:
        if queue_depth == 0:
            return self.min_chefs
        if queue_depth >= self.scale_up_threshold:
            needed = -(-queue_depth // self.scale_up_threshold)  # ceil division
            return min(self.max_chefs, max(self.min_chefs, needed))
        if queue_depth <= self.scale_down_threshold:
            return self.min_chefs
        return max(self.min_chefs, self._target_chef_count)

    def spawn_chef(self) -> None:
        with self._lock:
            chef_name = f"{CHEF_NAME_PREFIX}-{self._chef_counter}"
            self._chef_counter += 1

        try:
            process = subprocess.Popen([sys.executable, "-m", "qfk_demo.chef_consumer", chef_name])
        except OSError as e:
            print(f"❌ Failed to spawn {chef_name}: {e}")
            return

        with self._lock:
            self._active_chefs[chef_name] = process
        print(f"✅ Spawned: {chef_name} (PID: {process.pid})")

    def terminate_chef(self) -> None:
        with self._lock:
            if not self._active_chefs:
                return
            chef_name = next(iter(self._active_chefs))
            process = self._active_chefs.pop(chef_name)

        if process.poll() is None:
            print(f"🛑 Terminated: {chef_name}")
            process.terminate()
            threading.Timer(5.0, lambda: process.poll() is None and process.kill()).start()

    def _cleanup_dead_processes(self) -> None:
        with self._lock:
            dead = [name for name, p in self._active_chefs.items() if p.poll() is not None]
            for name in dead:
                del self._active_chefs[name]
        for name in dead:
            print(f"💀 Dead process removed: {name}")

    def _print_banner(self) -> None:
        print("╔═══════════════════════════════════════════╗")
        if self.manual_mode:
            print("║      👆 Chef Scaler (MANUAL MODE)         ║")
        else:
            print("║      🤖 Chef Auto-Scaler Starting         ║")
        print("╠═══════════════════════════════════════════╣")
        print(f"║  Mode: {'Manual' if self.manual_mode else 'Auto':<35} ║")
        print(f"║  Min Chefs: {self.min_chefs:<30} ║")
        print(f"║  Max Chefs: {self.max_chefs:<30} ║")
        if not self.manual_mode:
            print(f"║  Scale Up Threshold: {self.scale_up_threshold:<20} ║")
            print(f"║  Scale Down Threshold: {self.scale_down_threshold:<18} ║")
        print(f"║  Check Interval: {int(self.check_interval_seconds * 1000):<20} ms ║")
        print("╚═══════════════════════════════════════════╝")
        print()

    @property
    def active_chef_count(self) -> int:
        with self._lock:
            return len(self._active_chefs)

    @property
    def target_chef_count(self) -> int:
        return self._target_chef_count

    @property
    def active_chef_names(self) -> List[str]:
        with self._lock:
            return list(self._active_chefs.keys())

    @property
    def last_queue_depth(self) -> int:
        return self._last_queue_depth
