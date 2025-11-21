from __future__ import print_function

import collections
import multiprocessing
import time
from typing import Dict, List, Optional


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

        # Accept manager from outside to avoid pickling issues
        # The manager must be created in the main process before forking
        if manager is None:
            raise ValueError("manager must be provided to PrintMonitor")

        # Store the ordered list of tars
        self._tars_list: List[str] = tars_to_print

        # Use a simple counter instead of condition variables
        # Tracks which tar index we're currently on
        self._current_tar_index: multiprocessing.managers.ValueProxy = manager.Value(
            "i", 0
        )

        # Lock for updating the counter
        self._lock: multiprocessing.synchronize.Lock = manager.Lock()
        # Lock for printing (ensures atomic output)
        self._print_lock: multiprocessing.synchronize.Lock = manager.Lock()

    def wait_turn(
        self, worker, workers_curr_tar: str, indef_wait: bool = True, *args, **kwargs
    ):
        import sys

        # Find the index of the worker's tar in the ordered list
        try:
            tar_index = self._tars_list.index(workers_curr_tar)
        except ValueError:
            sys.stderr.write(f"DEBUG: Tar {workers_curr_tar} not in list!\n")
            sys.stderr.flush()
            return

        sys.stderr.write(
            f"DEBUG: Worker waiting for tar {workers_curr_tar} (index {tar_index}), current index is {self._current_tar_index.value}\n"
        )
        sys.stderr.flush()

        max_wait_time = 180.0
        start_time = time.time()
        attempted = False

        while True:
            if self._current_tar_index.value == tar_index:
                sys.stderr.write(
                    f"DEBUG: Worker got turn for tar {workers_curr_tar}!\n"
                )
                sys.stderr.flush()
                return

            if attempted and not indef_wait:
                # It's not this worker's turn.
                raise NotYourTurnError()

            # Check if we've been waiting too long
            if indef_wait and (time.time() - start_time) > max_wait_time:
                raise TimeoutError(
                    f"Worker timed out waiting for turn to print {workers_curr_tar}. "
                    f"Current tar index is {self._current_tar_index.value} (expecting {tar_index})"
                )

            attempted = True
            # Sleep briefly and check again
            time.sleep(0.1)

    def done_enqueuing_output_for_tar(
        # TODO: worker has type `ExtractWorker`
        self,
        worker,
        workers_curr_tar: str,
        *args,
        **kwargs,
    ):
        """
        A worker has finished printing the output for workers_curr_tar
        from the print queue.
        Advance to the next tar in the sequence.
        """
        # Find our tar's index
        try:
            tar_index = self._tars_list.index(workers_curr_tar)
        except ValueError:
            return

        # Advance to the next tar ONLY if we're the current tar
        # This allows workers to signal completion without blocking
        with self._lock:
            if self._current_tar_index.value == tar_index:
                self._current_tar_index.value += 1


class ExtractWorker(object):
    def __init__(
        self,
        print_monitor: PrintMonitor,
        tars_to_work_on: List[str],
        failure_queue,
        output_queue,  # NEW: queue for output messages
        *args,
        **kwargs,
    ):
        self.print_monitor: PrintMonitor = print_monitor
        self.print_queue: PrintQueue = PrintQueue()
        self.is_output_done_enqueuing: Dict[str, bool] = {
            tar: False for tar in tars_to_work_on
        }
        self.failure_queue = failure_queue
        self.output_queue = output_queue  # NEW

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
            # We only wait for 0.001 seconds.
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
        Send all queued messages to the output queue for the main process to print.
        """
        while self.has_to_print():
            tar_to_print: str = self.print_queue[0].tar

            # Collect all messages for this tar
            messages_to_print = []
            while self.print_queue and (self.print_queue[0].tar == tar_to_print):
                msg: str = self.print_queue.popleft().msg
                messages_to_print.append(msg)

            # Send to output queue instead of printing
            if messages_to_print:
                self.output_queue.put((tar_to_print, messages_to_print))

            # After sending this tar's output, advance counter if done
            if self.is_output_done_enqueuing.get(tar_to_print, False):
                try:
                    self.print_monitor.done_enqueuing_output_for_tar(self, tar_to_print)
                except TimeoutError:
                    pass


class PrintQueue(collections.deque):
    """
    A queue with a write() function.
    This is so that this can be replaced with sys.stdout in the extractFiles function.
    This way, all calls to `print()` will be sent here.
    """

    def __init__(self):
        self.curr_tar: Optional[str] = None

    def write(self, msg: str):
        import sys

        sys.stderr.write(
            f"DEBUG: write() called, curr_tar={self.curr_tar}, msg={msg[:50]}\n"
        )
        sys.stderr.flush()
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
