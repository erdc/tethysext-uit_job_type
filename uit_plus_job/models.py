# Put your persistent store models in this file
import os
import re
import shutil
import threading
import inspect
import logging
import datetime as dt
from pathlib import Path, PurePosixPath
from functools import partial
from django.db import models
from django.db.models.signals import pre_delete
from django.dispatch import receiver
from django.utils import timezone
from django.contrib.postgres.fields import ArrayField, JSONField
from django.contrib.auth.models import User
from tethys_apps.base.function_extractor import TethysFunctionExtractor
from uit.exceptions import DpRouteError
from uit import Client, PbsScript, PbsJob, PbsArrayJob
from uit.pbs_script import PbsDirective
from uit.pbs_script import NODE_TYPES
from tethys_compute.models.tethys_job import TethysJob


log = logging.getLogger('tethys.' + __name__)


class EnvironmentProfile(models.Model):
    """
    Model that stores modules and environment
    variables for a specific run profile.

    Attributes:
        user (foreign key): The user to whom the profile belongs
        name (str): The name of the profile
        hpc_system (str): The name of the hpc system the profile was created for (e.g. "onyx")
        environment_variables (str): A Json string of the environment variables
        modules (str): A Json string of the modules to load and unload
        last_used (datetime): The time the profile was last loaded (for sorting)
    """
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    name = models.CharField(max_length=64)
    hpc_system = models.CharField(max_length=64)
    environment_variables = models.CharField(max_length=1024)
    modules = models.CharField(max_length=1024)
    last_used = models.DateTimeField(auto_now_add=True)
    user_default = models.BooleanField(default=False)
    default_for_versions = ArrayField(models.CharField(max_length=16), default=list, null=True)


    @classmethod
    def set_default_for_version(cls, helios_version, profile, usr):
        """
        Set profile as the default for the selected helios version.
        """
        # Find current default for helios version
        ver_default = cls._get_default_for_version(helios_version, usr)
        if ver_default:
            # Remove version from its list of defaults
            ver_default.default_for_versions.remove(helios_version)
            # Save
            ver_default.save()
        # Add version to current profile default
        profile.default_for_versions.append(helios_version)
        # Save
        profile.save()

    @classmethod
    def set_general_default(cls, profile, usr):
        """
        Set the provided profile as the general default
        """
        # Get current default
        old_default = cls._get_general_default(usr)
        # Set profile as default
        profile.user_default = True
        # Save
        profile.save()
        # Remove the old default as general default
        old_default.user_default = False
        # Save
        old_default.save()

    @classmethod
    def get_default(cls, usr, helios_version=None):
        """
        Get the default for this helios version. Return the
        general default if it doesn't exist.
        """
        if helios_version:
            vers_default = cls._get_default_for_version(helios_version, usr)

            if not vers_default:
                gen_default = cls._get_general_default(usr)
                return gen_default
            else:
                return vers_default

        return cls._get_general_default(usr)


    @classmethod
    def _get_default_for_version(cls, helios_version, usr):
        """
        Get the profile listed as default for the specified helios version
        """
        try:
            profiles = cls.objects.get(user=usr,
                                       default_for_versions__contains=[helios_version])

        except cls.DoesNotExist:
            return None

        return profiles

    @classmethod
    def _get_general_default(cls, usr):
        """
        Return the general default
        """
        try:
            profiles = cls.objects.get(user=usr, user_default=True)
        except cls.DoesNotExist:
            return None
        except cls.MultipleObjectsReturned:
            return cls.objects.filter(user=usr, user_default=True)[0]

        return profiles

    def is_default_for_version(self, helios_version):
        """
        Return True if this profile is the default profile for the
        included helios_version.
        """
        return helios_version in self.default_for_versions

    def remove_default_for_version(self, helios_version):
        if helios_version in self.default_for_versions:
            self.default_for_versions.remove(helios_version)


class UitPlusJob(PbsScript, TethysJob):
    """UIT+ Job type for use in Tethys Apps.

    Attributes:
        archive_input_files (list): files to transfer from the archive filesystem to the working directory prior to running the job.
        archive_output_files (list): files to transfer from the working directory to the archive filesystem after the job has finished running.
        home_input_files (list): files to transfer from the user's home directory to the working directory prior to running the job.
        home_output_files (list): files to transfer from the working directory to the user's home directory after the job has finished running.
        intermediate_transfer_interval (int): frequency in minutes to transfer intermediate results.
        job_id (str): id of the job assigned by PBS.
        job_script (str): path to PBS script for the job.
        last_intermediate_transfer (datetime): the last date and time an intermediate data transfer occurred.
        max_cleanup_time (duration): maximum amount of time in minutes the cleanup job should be allowed to run.
        max_time (duration): maximum amount of time in minutes the job should be allowed to run.
        node_type (str): type of node on which the job should run.
        num_nodes (int): number of nodes to request.
        processes_per_node (int): number of processors per node to request.
        project_id (str): project ID to be passed in the PBS Header.
        queue (str): name of the queue into which to submit the job.
        system (str): name of the system to run on.
        transfer_input_files (list): files to transfer from the job workspace in the app to the working directory prior to running the job.
        transfer_intermediate_files (list): files to transfer to the job workspace in the app each intermediate_transfer_interval
        transfer_job_script (bool): transfer the job_script from the app to the working directory when True. Defaults to True.
        transfer_output_files (list): files to transfer from the working directory to the job workspace in the app after the job has finished running
    """  # noqa: E501
    UIT_TO_TETHYS_STATUSES = {
        None: 'PEN',  # No status set so job was just created
        'B': 'RUN',  # Array job: at least one subjob has started
        'E': 'COM',  # Job is exiting after having run.
        'F': 'COM',  # Job is finished.
        'H': 'SUB',  # Job is held.
        'M': 'SUB',  # Job was moved to another server.
        'Q': 'SUB',  # Job is queued.
        'R': 'RUN',  # Job is running.
        'S': 'ABT',  # Job is suspended.
        'T': 'SUB',  # Job is being moved to a new location.
        'U': 'ABT',  # Cycle-harvesting job is suspended due to keyboard activity.
        'W': 'SUB',  # Job is waiting for its submitter-assigned start time to be reached.
        'X': 'RUN',  # Subjob has completed execution or has been deleted.
    }

    SYSTEM_CHOICES = [(s, s) for s in NODE_TYPES.keys()]

    NODE_TYPE_CHOICES = [(nt, nt) for nt in sorted({nt for s in NODE_TYPES.values() for nt in s.keys()})]

    # job vars
    job_id = models.CharField(max_length=1024, null=True)
    archive_input_files = ArrayField(models.CharField(max_length=2048, null=True), null=True)
    home_input_files = ArrayField(models.CharField(max_length=2048, null=True), null=True)
    transfer_input_files = ArrayField(models.CharField(max_length=2048, null=True), null=True)
    _remote_workspace = models.TextField(blank=True)
    _remote_workspace_id = models.CharField(max_length=100)
    qstat = JSONField(null=True)

    # pbs_script vars
    project_id = models.CharField(max_length=1024, null=False)
    num_nodes = models.IntegerField(default=1, null=False)
    processes_per_node = models.IntegerField(default=1, null=False)
    max_time = models.DurationField(null=False)
    queue = models.CharField(max_length=100, default='debug', null=False)
    node_type = models.CharField(max_length=10, choices=NODE_TYPE_CHOICES, default='compute', null=False)
    system = models.CharField(max_length=10, choices=SYSTEM_CHOICES, default='onyx', null=False)
    execution_block = models.TextField(null=False)
    _modules = JSONField(null=True)
    _module_use = JSONField(null=True)
    _optional_directives = ArrayField(models.CharField(max_length=2048, null=True))
    _environment_variables = JSONField(null=True)
    _array_indices = ArrayField(models.IntegerField(), null=True)

    # other
    archive_output_files = ArrayField(models.CharField(max_length=2048, null=True), null=True)
    home_output_files = ArrayField(models.CharField(max_length=2048, null=True), null=True)
    intermediate_transfer_interval = models.IntegerField(default=0, null=False)
    last_intermediate_transfer = models.DateTimeField(null=False, default=timezone.now)
    max_cleanup_time = models.DurationField(null=False, default=dt.timedelta(hours=1))
    transfer_intermediate_files = ArrayField(models.CharField(max_length=2048, null=True), null=True)
    transfer_job_script = models.BooleanField(default=True)
    transfer_output_files = ArrayField(models.CharField(max_length=2048, null=True), null=True)
    custom_logs = JSONField(default=dict)
    _process_intermediate_results_function = models.CharField(max_length=1024, null=True)
    _update_status_interval = dt.timedelta(seconds=30)  # This is not effective until jobs table uses WS

    def __init__(self, *args, **kwargs):
        """Constructor."""
        # Build kwargs for PbsScript constructor
        pbs_kwargs = {}

        # Get arguments of PbsScript constructor dynamically
        pbs_signature = inspect.signature(PbsScript.__init__)

        # Get list of fields
        upj_fields = UitPlusJob._meta.get_fields()

        # Handle case when Django models are instantiated manually with kwargs
        if kwargs:
            for param in pbs_signature.parameters.keys():
                if param != 'self':
                    pbs_kwargs[param] = kwargs.get(param, None)

        # When a Django model loads objects from the database, it passes in args, not kwargs
        if len(args) + 1 == len(upj_fields):
            # Get list of field names in the order Django passes them in
            all_field_names = [field.name for field in upj_fields if field.name != 'tethysjob_ptr']
            # Match up given arg values with field names
            for field_name, value in zip(all_field_names, args):
                if field_name in pbs_signature.parameters:
                    pbs_kwargs[field_name] = value

        PbsScript.__init__(self, **pbs_kwargs)

        TethysJob.__init__(self, *args, **kwargs)

        self._client = None
        self._token = None
        self._home_dir = None
        self._archive_dir = None
        self._working_dir = None
        self._pbs_job = None

        self.remote_workspace_suffix  # initialize "remote" variables

        self.save()

    @classmethod
    def instance_from_pbs_job(cls, job, user):
        script = job.script
        instance = cls(
            name=job.name,
            user=user,
            label=job.label,
            workspace=job.workspace.as_posix(),

            job_id=job.job_id,
            _status=cls.UIT_TO_TETHYS_STATUSES.get(job.status),
            qstat=job.qstat,
            project_id=script.project_id,
            system=script.system,
            node_type=script.node_type,
            num_nodes=script.num_nodes,
            processes_per_node=script.processes_per_node,
            queue=script.queue,
            max_time=script.max_time,
            execution_block=script.execution_block,
            _optional_directives=script._optional_directives,
            _modules=script._modules,
            _module_use=script._module_use,
            _environment_variables=script._environment_variables,
            _array_indices=script._array_indices,

            # max_cleanup_time=None,
            home_input_files=job.home_input_files,
            home_output_files=[],
            archive_input_files=job.archive_input_files,
            archive_output_files=[],
            transfer_input_files=job.transfer_input_files,
            transfer_intermediate_files=[],
            transfer_output_files=[],
            _remote_workspace_id=job._remote_workspace_id,
            _remote_workspace=job._remote_workspace,
        )

        return instance

    @property
    def pbs_job(self):
        if self._pbs_job is None:
            Job = PbsJob if not self._array_indices else PbsArrayJob
            j = Job(
                script=self,
                client=self.client,
                label=self.label,
                workspace=Path(self.workspace),
                transfer_input_files=self.transfer_input_files,
                home_input_files=self.home_input_files,
                archive_input_files=self.archive_input_files,
            )
            j._remote_workspace_id = self._remote_workspace_id
            j._remote_workspace = PurePosixPath(self._remote_workspace)
            j._job_id = self.job_id
            j._status = self._status
            j._qstat = self.qstat
            if self._array_indices and self.qstat is not None:
                for sub_job in j.sub_jobs:
                    sub_job._qstat = self.qstat.get(sub_job.job_id)
                    sub_job._status = sub_job.qstat.get('status')
            self._pbs_job = j
        return self._pbs_job

    @property
    def optional_directives(self):
        """Get a list of all defined directives.

        Returns:
             list: All defined directives.
        """
        return [self.parse_pbs_directive(d) for d in self._optional_directives]

    @staticmethod
    def parse_pbs_directive(directive_str):
        m = re.match("PbsDirective\(directive='(.*?)', options='(.*?)'\)", directive_str)
        return PbsDirective(*m.groups())

    @property
    def archive_dir(self):
        """Get the job archive directory from the HPC.

        Returns:
            str: Archive Directory
        """
        if self._archive_dir is None:
            archive_home = self.get_environment_variable('ARCHIVE_HOME')
            self._archive_dir = os.path.join(archive_home, self.remote_workspace_suffix)
        return self._archive_dir

    @property
    def client(self):
        """Get the UIT client based on a valid token.

        Returns:
            Client: UIT Client object
        """
        if self._client is None:
            # Create a client with token
            self._client = Client(token=self.token)

            # Connect the client
            self._client.connect(system=self.system, retry_on_failure=True)

        # return the client
        return self._client

    @property
    def home_dir(self):
        """Get the job home directory from the HPC.

        Returns:
            str: The job home directory
        """
        if self._home_dir is None:
            self._home_dir = os.path.join(self.client.HOME, self.remote_workspace_suffix)
        return self._home_dir

    @property
    def process_intermediate_results_function(self):
        """Get the function used to process intermediate results.

        Returns:
            Function: The process function, or None if the function cannot be resolved.
        """
        if self._process_intermediate_results_function:
            function_extractor = TethysFunctionExtractor(
                self._process_intermediate_results_function, None)
            if function_extractor.valid:
                return function_extractor.function

    @process_intermediate_results_function.setter
    def process_intermediate_results_function(self, function):
        if isinstance(function, str):
            self._process_results_function = function
            return
        module_path = inspect.getmodule(function).__name__.split('.')
        module_path.append(function.__name__)
        self._process_results_function = '.'.join(module_path)

    @property
    def remote_workspace_id(self):
        """Get the UUID associated with this job to be used as a workspace id.

        Returns:
            str: Remote workspace ID
        """
        if not self._remote_workspace_id:
            self._remote_workspace_id = self.pbs_job.remote_workspace_id
        return self._remote_workspace_id


    @property
    def remote_workspace_suffix(self):
        """Get the job specific suffix.

        Made up of a combination of label, name, and remote workspace ID.

        Returns:
            str: Suffix
        """
        if not self._remote_workspace:
            self._remote_workspace = self.pbs_job.remote_workspace_suffix
        return self._remote_workspace

    @property
    def token(self):
        """Get the user access token.

        Returns:
            str: Access Token
        """
        if self._token is None:
            try:
                social = self.user.social_auth.get(provider='UITPlus')
                self._token = social.extra_data['access_token']
            except (KeyError, AttributeError):
                self._token = None
        return self._token

    @property
    def working_dir(self):
        """Get the job work directory from the HPC.

        Returns:
            str: Work Directory
        """
        return self.pbs_job.working_dir

    def get_logs(self):
        if isinstance(self.pbs_job, PbsArrayJob):
            logs = {}
            for sub_job in self.pbs_job.sub_jobs:
                name = f'{sub_job.name}_{sub_job.job_index}'
                logs[name] = {}
                logs[name]['stdout'] = sub_job.get_stdout_log
                logs[name]['stderr'] = sub_job.get_stderr_log
                logs[name].update({log_type: partial(sub_job.get_custom_log, path, num_lines=1000)
                                   for log_type, path in self.custom_logs.items()})
            return logs

        return {
            'stdout': self.pbs_job.get_stdout_log,
            'stderr': self.pbs_job.get_stderr_log,
        }

    def get_environment_variable(self, variable):
        """Get the value of an environment variable from the HPC.

        Args:
            variable (str): Name of environment variable (e.g.: "WORKDIR").

        Returns:
            str: value of environment variable.
        """
        return self.client.env.get(variable)

    def _execute(self, remote_name=None):
        """Execute the job using the UIT Plus Python client."""
        try:
            # Submit job with PbsScript object and remote workspace
            self.job_id = self.pbs_job.submit(self, remote_name=remote_name)
        except Exception as e:
            self.status_message = f'Error submitting job on "{self.system}": {e}'
            log.exception(e)
            raise e

    def _resubmit(self, *args, **kwargs):
        self.pbs_job._job_id = None
        self.execute()

    def _update_status(self):
        """Retrieve a job’s status using the UIT Plus Python client.

        Translates UitJob status to TethysJob status and saves to the database
        """
        if self._status in TethysJob.TERMINAL_STATUSES:
            return

        status = self.pbs_job.update_status()
        new_status = self.UIT_TO_TETHYS_STATUSES.get(status, 'ERR')

        if new_status == "COM":
            if 'cleanup_job_id' in self.extended_properties:
                if self.job_id != self.extended_properties['cleanup_job_id']:
                    new_status = "SUB"
                    self.job_id = self.extended_properties['cleanup_job_id']
            # else:
            #     raise RuntimeError("Could not find cleanup script ID.")

        self._status = new_status
        self.qstat = self.pbs_job.qstat
        self.save()

        # Get intermediate results, if applicable
        if self.transfer_intermediate_files:
            if self.intermediate_transfer_interval_exceeded:
                self.last_intermediate_transfer = timezone.now()
                thread = threading.Thread(target=self.get_intermediate_results)
                thread.daemon = True
                thread.start()
        self.save()

    @property
    def intermediate_transfer_interval_exceeded(self):
        if self.intermediate_transfer_interval == 0:
            return True
        delta_time = (timezone.now() - self.last_intermediate_transfer)
        minutes = delta_time.days * 24 * 60 + delta_time.seconds / 60
        return minutes > self.intermediate_transfer_interval

    def _process_results(self):
        """Process the results using the UIT Plus Python client."""
        remote_dir = os.path.join(self.home_dir, 'transfer')
        remote_dir = self.working_dir
        self.get_remote_files(remote_dir, self.transfer_output_files)
        # self.get_remote_files(remote_dir, ["log.stdout", "log.stderr"])
        # TODO get log files

    def get_intermediate_results(self):
        """Retrieve intermediate result files from the supercomputer."""
        if self.get_remote_files(self.working_dir, self.transfer_intermediate_files):
            if self.process_intermediate_results_function:
                self.process_intermediate_results_function()

    def get_remote_files(self, remote_dir, remote_filenames):
        """Transfer files from a directory on the super computer.

        Args:
            remote_dir (str): Remote directory from which to pull files
            remote_filenames (List[str]): Files to retrieve from remote_dir

        Returns:
            bool: True if all file transfers succeed.
        """

        # Ensure the local transfer directory exists
        Path(self.workspace).mkdir(parents=True, exist_ok=True)

        success = True
        for remote_file in remote_filenames:
            local_path = os.path.join(self.workspace, remote_file)
            remote_path = os.path.join(remote_dir, remote_file)
            try:
                self.client.get_file(remote_path=remote_path, local_path=local_path)
                if not os.path.exists(local_path):
                    success = False
            except RuntimeError as e:
                success = False
                log.error("Failed to get remote file: {}".format(str(e)))

        return success

    def stop(self):
        """Stops/cancels this job.

        Returns:
            bool: True if job was deleted.
        """
        result = self.pbs_job.terminate()
        if result:
            self.update_status('ABT')
        else:
            self.update_status('ERR')
        return result

    def pause(self):
        """Pauses this job.

        Returns:
            bool: True if job was paused.
        """
        return self.pbs_job.hold()

    def resume(self):
        """Resumes this job if paused.

        Returns:
            bool: True if job was resumed.
        """
        return self.pbs_job.release()

    def clean(self, archive=False):
        """Remove all files and directories associated with the job.

        Removal takes place on unmonitored background thread so as not to disturb the user (as deletes on the HPC can take a long time). This means that we will always return True even if the files were not deleted.

        Args:
            archive (bool): Flag to indicate whether files should be removed from the archive as well.

        Returns:
            bool: True. Always.
        """  # noqa: E501

        # Remove local workspace
        thread = threading.Thread(target=shutil.rmtree, args=(self.workspace, True))
        thread.daemon = True
        thread.start()

        # Remove remote locations
        rm_cmd = "rm -rf {} || true"
        commands = []
        for path in (self.working_dir, self.home_dir):
            commands.append(rm_cmd.format(path))
        if archive:
            commands.append("archive rm -rf {} || true".format(self.archive_dir))

        for cmd in commands:
            thread = threading.Thread(target=self.client.call, kwargs={'command': cmd, 'working_dir': '/'})
            thread.daemon = True
            thread.start()
            log.info(f"Executing command '{cmd}' on {self.system}")
        return True


@receiver(pre_delete, sender=UitPlusJob)
def uit_job_pre_delete(sender, instance, using, **kwargs):
    """Pre-delete hook to make sure we clean up our workspace.

    Args:
        sender: The model's class
        instance: The instance being deleted
        using: The DB alias in use
        **kwargs:
    """
    try:
        instance.stop()
        try:
            if instance.clean_on_delete:
                instance.clean()
        except AttributeError:
            instance.clean()
    except Exception as e:
        log.exception(str(e))
