from __future__ import print_function

import collections
import ctypes
import multiprocessing
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

    def __init__(self, tars_to_print: List[str], *args, **kwargs):
        # A list of tars to print.
        # Ex: ['000000.tar', '000008.tar', '00001a.tar']
        if not tars_to_print:
            msg: str = "You must pass in a list of tars, which dictates"
            msg += " the order of which to print the results."
            raise RuntimeError(msg)

        # The variables below are modified/accessed by different processes,
        # so they need to be in shared memory.
        self._cv: multiprocessing.synchronize.Condition = multiprocessing.Condition()

        self._tars_to_print: multiprocessing.Queue[str] = multiprocessing.Queue()
        tar: str
        for tar in tars_to_print:
            # Add the tar to the queue to be printed.
            self._tars_to_print.put(tar)

        # We need a manager to instantiate the Value instead of multiprocessing.Value.
        # If we didn't use a manager, it seems to get some junk value.
        self._manager: multiprocessing.managers.SyncManager = multiprocessing.Manager()
        self._current_tar: multiprocessing.managers.ValueProxy = self._manager.Value(
            ctypes.c_char_p, self._tars_to_print.get()
        )

    def wait_turn(
        # TODO: worker has type `ExtractWorker`
        self,
        worker,
        workers_curr_tar: str,
        indef_wait: bool = True,
        *args,
        **kwargs
    ):
        """
        While a worker's current tar isn't the one
        needed to be printed, wait.

        A timeout is passed into self._cv.wait(), and if the
        turn isn't given within that, a NotYourTurnError is raised.

        If indef_wait is True, indefinitely wait until it's
        the worker's turn.
        """
        with self._cv:
            attempted: bool = False
            while self._current_tar.value != workers_curr_tar:
                if attempted and not indef_wait:
                    # It's not this worker's turn.
                    raise NotYourTurnError()

                attempted = True
                # Wait 0.001 to see if it's the worker's turn.
                self._cv.wait(0.001)

    def done_dequeuing_output_for_tar(
        # TODO: worker has type `ExtractWorker`
        self,
        worker,
        workers_curr_tar: str,
        *args,
        **kwargs
    ):
        """
        A worker has finished printing the output for workers_curr_tar
        from the print queue.
        If possible, update self._current_tar.
        If there aren't anymore tars to print, set self._current_tar to None.
        """
        # It must be the worker's turn before this can happen.
        self.wait_turn(worker, workers_curr_tar, *args, **kwargs)

        with self._cv:
            self._current_tar.value = (
                self._tars_to_print.get() if not self._tars_to_print.empty() else ""
            )
            self._cv.notify_all()


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

            # If True, then all of the output for extracting tar_to_print was in the queue.
            # Since we just finished printing all of it, we can move onto the next one.
            if self.is_output_done_enqueuing[tar_to_print]:
                # Let all of the other workers know that this worker is done.
                self.print_monitor.done_dequeuing_output_for_tar(self, tar_to_print)


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
