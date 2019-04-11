import sys
import multiprocessing
import collections


class NotYourTurnError(BaseException):
    """
    An error to let a worker know it needs to wait
    to print it's stuff.
    """
    pass


class PrintMonitor(multiprocessing.synchronize.Condition):
    """
    Used to synchronize the printing of the output between workers.
    Depending on the current_tar, the worker processing the work
    for that tar will print it's output.
    """        
    def __init__(self, tars_to_print):
        super().__init__()
        # A list of tars to print.
        # Ex: ['000000.tar', '000008.tar', '00001a.tar']
        if not tars_to_print:
            msg = "You must pass in a list of tars, which dictates"
            msg += " the order of which to print the results."
            raise RuntimeError(msg)
        self._tars_to_print = collections.deque(sorted(tars_to_print))
        self._current_tar = self._tars_to_print.popleft()

    def wait_turn(self, workers_curr_tar, *args, **kwargs):
        """
        While a worker's current tar isn't the one
        needed to be printed, wait.

        A process can pass in a timeout, and if the turn
        isn't given within that, a NotYourTurnError is raised.
        """
        with self:
            while self._current_tar != workers_curr_tar:
                try:
                    self.wait(*args, **kwargs)
                except RuntimeError:
                    raise NotYourTurnError()

    def done_dequeuing_output_for_tar(self, workers_curr_tar, *args, **kwargs):
        """
        A worker has finished printing the output for workers_curr_tar
        from the print queue.
        If possible, update self._current_tar.
        If there aren't anymore tars to print, set self._current_tar to None.
        """
        # It must be the worker's turn before this can happen.
        self.wait_turn(workers_curr_tar, *args, **kwargs)
        
        if self._tars_to_print:
            self._current_tar = self._tars_to_print.popleft()
        else:
            self._current_tar = None


class ExtractWorker(multiprocessing.Process):
    """
    A regular Process, but with a PrintMonitor, it prints to the
    terminal in the order defined by the PrintMonitor.
    
    This worker is called during `zstash extract`.
    """
    class PrintQueue(collections.deque):
        """
        A queue with a write() function.
        This is so that this can be replaced with sys.stdout.
        """
        def __init__(self):
            self.TarAndMsg = collections.namedtuple('TarAndMsg', ['tar', 'msg'])
            self.curr_tar = None
            
        def write(self, msg):
            self.append(self.TarAndMsg(self.curr_tar, msg))

    def __init__(self, print_monitor, tars_to_work_on, failure_queue, *args, **kwargs):
        """
        print_monitor is used to determine if it's this worker's turn to print.
        tars_to_work_on is a list of the tars that this worker will process.
        """
        super().__init__(*args, **kwargs)
        self.orig_stdout = sys.stdout
        # Every call to print() in the original function will
        # be piped to this queue instead of the screen.
        sys.stdout = self.PrintQueue()
        self.print_monitor = print_monitor
        # A tar is mapped to True when all of its output is in the queue.
        self.is_output_done_enqueuing = {tar:False for tar in tars_to_work_on}
        # After extractFiles is done, all of the failures will be added to this queue.
        self.failure_queue = failure_queue

    def set_curr_tar(self, tar):
        """
        Sets the current tar this worker is working on.
        """
        sys.stdout.curr_tar = tar
    
    def done_enqueuing_output_for_tar(self, tar):
        """
        All of the output for extracting this tar is in the print queue.
        """
        if not tar in self.is_output_done_enqueuing:
            msg = 'This tar {} isn\'t assigned to this worker.'
            raise RuntimeError(msg.format(tar))
        self.is_output_done_enqueuing[tar] = True
    
    def print_contents(self, *args, **kwargs):
        """
        Try to print the contents in sys.stdout, which is a queue.
        """
        try:
            # Wait for 0.001 seconds to see if it's our turn.
            self.print_all_contents(0.001)
        except NotYourTurnError:
            # It's not our turn, so try again the next time this function is called.
            pass
    
    def has_to_print(self):
        """
        Returns True if this Worker still has things to print.
        """
        return len(sys.stdout) >= 1

    def print_all_contents(self, *args, **kwargs):
        """
        Block until all of the contents of the sys.stdout queue are printed.

        If it's not our turn and the passed in timeout to print_monitor.wait_turn
        is over, a NotYourTurnError exception is raised.
        """
        while self.has_to_print():
            # Try to print the first element in the queue.
            tar_to_print = sys.stdout[0].tar
            self.print_monitor.wait_turn(tar_to_print, *args, **kwargs)

            # Print all applicable values in the sys.stdout queue.
            while sys.stdout and sys.stdout[0].tar == tar_to_print:
                msg = sys.stdout.popleft().msg
                self.orig_stdout.write(msg)
                self.orig_stdout.write('\n')
            
            # If True, then all of the output for extracting tar_to_print was in the queue.
            # Since we just finished printing all of it, we can move onto the next one.
            if self.is_output_done_enqueuing[tar_to_print]:
                self.print_monitor.done_dequeuing_output_for_tar(tar_to_print)
                # Let all of the other workers know that this worker is done.
                self.print_monitor.notifyAll()
