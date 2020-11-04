import subprocess
from egcg_core.app_logging import AppLogger
from egcg_core.exceptions import EGCGError


class Executor(AppLogger):
    def __init__(self, cmd):
        self.cmd = cmd
        self.proc = None

    def join(self):
        """
        Set self.proc to a Popen and start.
        :rtype: tuple[bytes, bytes]
        :raises: EGCGError on any exception
        """
        try:
            out, err = self._process().communicate()
            for stream, emit in ((out, self.info), (err, self.error)):
                for line in stream.decode('utf-8').split('\n'):
                    emit(line)
            return self.proc.poll()

        except Exception as e:
            raise EGCGError('Command failed: ' + self.cmd) from e

    @staticmethod
    def start():
        """
        Subclasses of Executor implement this method from different sources:
          - StreamExecutor and ArrayExecutor inherit it from threading.Thread
          - ClusterExecutor implements it so it writes and submits a script to a queue
        The definition here has no effect, but allows Executor to be handled identically to other executors.
        """
        pass

    def _process(self):
        """
        Translate self.cmd to a subprocess. Override to manipulate how the process is run, e.g. with different
        resource managers.
        :rtype: subprocess.Popen
        """
        self.info('Executing: %s', self.cmd)
        self.proc = subprocess.Popen(self.cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        return self.proc
