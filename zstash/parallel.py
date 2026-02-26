from __future__ import print_function

import collections
import multiprocessing
import time
from typing import Dict, List, Optional

from .settings import FilesRow


class NotYourTurnError(Exception):
    """
    An error to let a worker know it needs to wait
    to print its stuff.
    """

    pass


class PrintMonitor(object):
    """
    Used to synchronize the printing of the output between workers.
    Depending on the current_tar, the worker processing the work
    for that tar will print it's output.
    """

    def __init__(self, tars_to_print: List[str], manager=None, *args, **kwargs):
        # A list of tars to print.
        # Ex: ['000000.tar', '000008.tar', '00001a.tar']
        if not tars_to_print:
            msg: str = "You must pass in a list of tars, which dictates"
            msg += " the order of which to print the results."
            raise RuntimeError(msg)

        if manager is None:
            raise ValueError("manager must be provided to PrintMonitor")

        # Store the ordered list of tars
        self._tars_list: List[str] = tars_to_print

        # Precomputed mapping from tar name to its position in the ordered list.
        self._tar_to_index: Dict[str, int] = {
            tar: i for i, tar in enumerate(tars_to_print)
        }

        # Use manager-backed primitives instead of bare multiprocessing primitives
        # for Python 3.14 compatibility: Python 3.14 restricts sharing unpickled
        # synchronization objects across processes (see bpo-38119 / gh-84582).
        # A simple counter tracks which tar we're on.
        self._current_tar_index: multiprocessing.managers.ValueProxy = manager.Value(
            "i", 0
        )

        # Lock for updating the counter
        self._lock: multiprocessing.synchronize.Lock = manager.Lock()

    def wait_turn(
        self, worker, workers_curr_tar: str, indef_wait: bool = True, *args, **kwargs
    ):
        """
        Wait until it's this worker's turn to process workers_curr_tar.
        """
        if workers_curr_tar not in self._tar_to_index:
            raise RuntimeError("Tar {} not in ordered list".format(workers_curr_tar))
        tar_index = self._tar_to_index[workers_curr_tar]

        attempted = False
        while True:
            if self._current_tar_index.value == tar_index:
                return

            if attempted and not indef_wait:
                raise NotYourTurnError()

            attempted = True
            # Poll every 10 ms: lower CPU usage vs 1 ms at cost of ~10 ms latency.
            time.sleep(0.01)

    def done_enqueuing_output_for_tar(
        self, worker, workers_curr_tar: str, *args, **kwargs
    ):
        """
        A worker has finished printing output for workers_curr_tar.
        Advance to the next tar in the sequence.
        """
        if workers_curr_tar not in self._tar_to_index:
            raise RuntimeError("Tar {} not in ordered list".format(workers_curr_tar))
        tar_index = self._tar_to_index[workers_curr_tar]

        with self._lock:
            if self._current_tar_index.value == tar_index:
                self._current_tar_index.value += 1
            else:
                raise RuntimeError(
                    "Attempted to advance tar index for tar {} (expected index"
                    " {}) but current index is {}. This indicates a programming"
                    " error.".format(
                        workers_curr_tar, tar_index, self._current_tar_index.value
                    )
                )


class ExtractWorker(object):
    """
    An object that is attached to a Process.
    It redirects all of the output of the logging module to a queue.
    Then with a PrintMonitor, it prints to the
    terminal in the order defined by the PrintMonitor.

    This worker is called during `zstash extract`.
    """

    def __init__(
        self,
        print_monitor: PrintMonitor,
        tars_to_work_on: List[str],
        # TODO: failure_queue has type `multiprocessing.Queue[FilesRow]`
        failure_queue,
        *args,
        **kwargs
    ):
        """
        print_monitor is used to determine if it's this worker's turn to print.
        tars_to_work_on is a list of the tars that this worker will process.
        Any failures are added to the failure_queue, to return any failed values.
        """
        self.print_monitor: PrintMonitor = print_monitor
        # Every call to print() in the original function will
        # be piped to this queue instead of the screen.
        self.print_queue: PrintQueue = PrintQueue()
        # A tar is mapped to True when all of its output is in the queue.
        self.is_output_done_enqueuing: Dict[str, bool] = {
            tar: False for tar in tars_to_work_on
        }
        # After extractFiles is done, all of the failures will be added to this queue.
        self.failure_queue: multiprocessing.Queue[FilesRow] = failure_queue

    def set_curr_tar(self, tar: str):
        """
        Sets the current tar this worker is working on.
        """
        self.print_queue.curr_tar = tar

    def done_enqueuing_output_for_tar(self, tar: str):
        """
        All of the output for extracting this tar is in the print queue.
        """
        msg: str
        if tar not in self.is_output_done_enqueuing:
            msg = "This tar {} isn't assigned to this worker."
            raise RuntimeError(msg.format(tar))

        if self.is_output_done_enqueuing[tar]:
            msg = "This tar ({}) was already told to be done."
            raise RuntimeError(msg.format(tar))

        self.is_output_done_enqueuing[tar] = True

    def print_contents(self):
        """
        Try to print the contents from self.print_queue.
        """
        try:
            self.print_all_contents(indef_wait=False)
        except NotYourTurnError:
            # It's not our turn, so try again the next time this function is called.
            pass

    def has_to_print(self) -> bool:
        """
        Returns True if this Worker still has things to print.
        """
        return len(self.print_queue) >= 1

    def print_all_contents(self, *args, **kwargs):
        """
        Block until all of the contents of self.print_queue are printed.

        If it's not our turn and the passed in timeout to print_monitor.wait_turn
        is over, a NotYourTurnError exception is raised.
        """
        while self.has_to_print():
            # Try to print the first element in the queue.
            tar_to_print: str = self.print_queue[0].tar
            self.print_monitor.wait_turn(self, tar_to_print, *args, **kwargs)

            # Print all applicable values in the print_queue.
            while self.print_queue and (self.print_queue[0].tar == tar_to_print):
                msg: str = self.print_queue.popleft().msg
                print(msg, end="", flush=True)

            # If all output for this tar is done, advance the monitor
            if self.is_output_done_enqueuing[tar_to_print]:
                self.print_monitor.done_enqueuing_output_for_tar(self, tar_to_print)


class PrintQueue(collections.deque):
    """
    A queue with a write() function.
    This is so that this can be replaced with sys.stdout in the extractFiles function.
    This way, all calls to `print()` will be sent here.
    """

    def __init__(self):
        self.curr_tar: Optional[str] = None

    def write(self, msg: str):
        if self.curr_tar:
            self.append(TarAndMsg(self.curr_tar, msg))

    def flush(self):
        # Not needed, but it's called by some internal Python code.
        # So we need to provide a function like this.
        pass


class TarAndMsg(object):
    def __init__(self, tar: str, msg: str):
        self.tar: str = tar
        self.msg: str = msg
