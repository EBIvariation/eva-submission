from os.path import join

from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import AppLogger

from eva_submission.executor import ExecutorError


class ScriptWriter(AppLogger):
    """
    Writes a basic job submission script. Subclassed by SlurmWriter. Initialises with self.lines as an empty list, which
    is appended by self.write_line. This list is then saved line by line to self.script_file by self.save.
    """
    suffix = '.sh'
    array_index = 'JOB_INDEX'
    mapping = {
        'cpus': '# cpus: {}',
        'exclusive': '# exclusive',
        'job_name': '# job name: {}',
        'job_queue': '# queue: {}',
        'jobs': '# job array: 1-{}',
        'log_file': '# log file: {}',
        'mem': '# mem: {}gb',
        'walltime': '# walltime: {}',
    }

    def __init__(self, job_name, working_dir, job_queue=None, log_commands=True, **kwargs):
        self.log_commands = log_commands
        self.working_dir = working_dir
        self.log_file = join(self.working_dir, job_name + '.log')
        self.parameters = dict(
            kwargs,
            job_name=job_name,
            job_queue=job_queue or cfg['executor']['job_queue'],
            log_file=self.log_file
        )

        self.script_name = join(working_dir, job_name + self.suffix)
        self.debug('Writing script: %s', self.script_name)
        self.lines = []

    def register_cmd(self, cmd, log_file=None):
        if log_file:
            cmd += ' > %s 2>&1' % log_file
        self.add_line(cmd)

    def register_cmds(self, *cmds, parallel):
        if parallel:
            self.add_job_array(*cmds)
        else:
            self.lines.extend(list(cmds))

    def add_job_array(self, *cmds):
        if self.parameters.get('jobs'):
            raise ExecutorError('Already written a job array - can only have one per script')

        if len(cmds) == 1:
            self.register_cmd(cmds[0])
        else:
            self._start_array()
            for idx, cmd in enumerate(cmds):
                self._register_array_cmd(
                    idx + 1,
                    cmd,
                    log_file=self.log_file + str(idx + 1) if self.log_commands else None
                )
            self._finish_array()
            self.parameters['jobs'] = len(cmds)

    def _register_array_cmd(self, idx, cmd, log_file=None):
        """
        :param int idx: The index of the job, i.e. which number the job has in the array
        :param str cmd: The command to write
        """
        line = str(idx) + ') ' + cmd
        if log_file:
            line += ' > ' + log_file + ' 2>&1'
        line += '\n' + ';;'
        self.add_line(line)

    def add_line(self, line):
        self.lines.append(line)

    def _start_array(self):
        self.add_line('case $%s in' % self.array_index)

    def _finish_array(self):
        self.add_line('*) echo "Unexpected %s: $%s"' % (self.array_index, self.array_index))
        self.add_line('esac')

    def line_break(self):
        self.lines.append('')

    def save(self):
        """Save self.lines to self.script_name."""
        with open(self.script_name, 'w') as f:
            f.write('\n'.join(self.lines) + '\n')

    def add_header(self):
        """Write a header for a given resource manager. If multiple jobs, split them into a job array."""
        header_lines = ['#!/bin/bash\n']
        for k in sorted(self.parameters):
            v = self.parameters[k]
            header_lines.append(self.mapping[k].format(v))

        header_lines.extend(['', 'cd ' + self.working_dir, ''])

        # prepend the formatted header
        self.lines = header_lines + self.lines


class SlurmWriter(ScriptWriter):
    """Writes a Bash script runnable on Slurm"""
    suffix = '.slurm'
    array_index = 'SLURM_ARRAY_TASK_ID'
    mapping = {
        'cpus': '#SBATCH --cpus-per-task={}',
        'exclusive': '#SBATCH --exclusive',
        'job_name': '#SBATCH --job-name="{}"',
        'job_queue': '#SBATCH --partition={}',
        'jobs': '#SBATCH --array=1-{}',
        'log_file': '#SBATCH --output={}',
        'mem': '#SBATCH --mem={}g',
        'walltime': '#SBATCH --time={}:00:00'
    }